//go:build linux

package companion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/nodes/internal/jsonstrict"
)

const maxServiceHelperActiveRequests = 1024

const serviceHelperWriteTimeout = 30 * time.Second

type serviceHelperActiveRequest struct {
	cancel    context.CancelFunc
	mutation  bool
	accepted  bool
	canceled  bool
	expiresAt int64
}

type serviceHelperServer struct {
	config   ServiceHelperServiceConfig
	manager  serviceActionExecutor
	identity authorityBrokerCompanionIdentity
	snapshot serviceHelperSnapshot
	now      func() time.Time

	mu          sync.Mutex
	active      map[string]*serviceHelperActiveRequest
	preCanceled map[string]int64
}

func LoadServiceHelperServiceConfig(path string) (ServiceHelperServiceConfig, error) {
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) {
		return ServiceHelperServiceConfig{}, errors.New("service helper config path must be absolute")
	}
	if err := verifyAuthorityBrokerDirectoryChain(filepath.Dir(path)); err != nil {
		return ServiceHelperServiceConfig{}, fmt.Errorf("validate service helper config directory: %w", err)
	}
	descriptor, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return ServiceHelperServiceConfig{}, fmt.Errorf("open service helper config: %w", err)
	}
	file := os.NewFile(uintptr(descriptor), path)
	if file == nil {
		_ = unix.Close(descriptor)
		return ServiceHelperServiceConfig{}, errors.New("open service helper config: invalid descriptor")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return ServiceHelperServiceConfig{}, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	pathInfo, pathErr := os.Lstat(path)
	pathStat, pathStatOK := pathInfoSyscallStat(pathInfo)
	if !ok || pathErr != nil || !pathStatOK || stat.Dev != pathStat.Dev ||
		stat.Ino != pathStat.Ino || stat.Uid != 0 || !info.Mode().IsRegular() ||
		info.Mode().Perm()&0o022 != 0 {
		return ServiceHelperServiceConfig{}, errors.New(
			"service helper config must be a root-owned non-writable regular file",
		)
	}
	raw, err := io.ReadAll(io.LimitReader(file, MaxServiceHelperConfigBytes+1))
	if err != nil {
		return ServiceHelperServiceConfig{}, fmt.Errorf("read service helper config: %w", err)
	}
	if len(raw) > MaxServiceHelperConfigBytes {
		return ServiceHelperServiceConfig{}, errors.New("service helper config exceeds size limit")
	}
	if _, validationErr := jsonstrict.Decode(raw); validationErr != nil {
		return ServiceHelperServiceConfig{}, fmt.Errorf("validate service helper config: %w", validationErr)
	}
	var config ServiceHelperServiceConfig
	if decodeErr := decodeStrictJSON(raw, &config); decodeErr != nil {
		return ServiceHelperServiceConfig{}, fmt.Errorf("decode service helper config: %w", decodeErr)
	}
	config, err = NormalizeServiceHelperServiceConfig(config, filepath.Dir(path))
	if err != nil {
		return ServiceHelperServiceConfig{}, err
	}
	config.SystemctlPath, err = trustedSystemdExecutable(config.SystemctlPath)
	if err != nil {
		return ServiceHelperServiceConfig{}, fmt.Errorf("validate service helper systemctl: %w", err)
	}
	if config.JournalctlPath != "" {
		config.JournalctlPath, err = trustedSystemdExecutable(config.JournalctlPath)
		if err != nil {
			return ServiceHelperServiceConfig{}, fmt.Errorf("validate service helper journalctl: %w", err)
		}
	}
	return config, nil
}

func RunServiceHelper(ctx context.Context, config ServiceHelperServiceConfig) error {
	if os.Geteuid() != 0 {
		return errors.New("service helper must run as root")
	}
	if !config.normalized {
		return errors.New("service helper config is not normalized")
	}
	config, runner, err := pinServiceHelperExecutables(config)
	if err != nil {
		return err
	}
	defer runner.close()
	manager, err := newSystemdServiceManagerWithEnforcement(
		config.Profiles,
		runner,
		time.Now,
		serviceEnforcement{status: true, logs: true, actions: true},
	)
	if err != nil {
		return err
	}
	identity, err := newAuthorityBrokerCgroupIdentity(config.CompanionCgroup)
	if err != nil {
		return err
	}
	defer identity.Close()
	server, err := newServiceHelperServer(config, manager, identity, time.Now)
	if err != nil {
		return err
	}
	directory, err := openAuthorityBrokerSocketDirectory(config.SocketPath)
	if err != nil {
		return fmt.Errorf("open service helper socket directory: %w", err)
	}
	defer directory.Close()
	if prepareErr := directory.prepare(); prepareErr != nil {
		return fmt.Errorf("prepare service helper socket: %w", prepareErr)
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{
		Name: directory.descriptorPath(), Net: "unix",
	})
	if err != nil {
		return fmt.Errorf("listen service helper socket: %w", err)
	}
	listener.SetUnlinkOnClose(false)
	defer listener.Close()
	defer func() { _ = directory.unlink() }()
	if err := unix.Fchownat(
		directory.descriptor,
		directory.name,
		0,
		int(config.AllowedGID),
		unix.AT_SYMLINK_NOFOLLOW,
	); err != nil {
		return fmt.Errorf("own service helper socket: %w", err)
	}
	if err := unix.Fchmodat(directory.descriptor, directory.name, 0o660, 0); err != nil {
		return fmt.Errorf("protect service helper socket: %w", err)
	}
	return server.Serve(ctx, listener)
}

func newServiceHelperServer(
	config ServiceHelperServiceConfig,
	manager serviceActionExecutor,
	identity authorityBrokerCompanionIdentity,
	now func() time.Time,
) (*serviceHelperServer, error) {
	if !config.normalized || manager == nil || identity == nil || now == nil {
		return nil, errors.New("service helper server configuration is incomplete")
	}
	snapshot, err := newServiceHelperSnapshot(config)
	if err != nil {
		return nil, err
	}
	return &serviceHelperServer{
		config: config, manager: manager, identity: identity, snapshot: snapshot, now: now,
		active:      make(map[string]*serviceHelperActiveRequest),
		preCanceled: make(map[string]int64),
	}, nil
}

func (server *serviceHelperServer) Serve(ctx context.Context, listener *net.UnixListener) error {
	if server == nil || listener == nil {
		return errors.New("service helper server is unavailable")
	}
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	var workers sync.WaitGroup
	defer workers.Wait()
	for {
		connection, err := listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept service helper connection: %w", err)
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer connection.Close()
			server.handleConnection(ctx, connection)
		}()
	}
}

func (server *serviceHelperServer) handleConnection(
	serverContext context.Context,
	connection *net.UnixConn,
) {
	peer, err := authorityBrokerPeerCredentials(connection)
	if err != nil || peer.Uid != server.config.AllowedUID || peer.Gid != server.config.AllowedGID {
		return
	}
	if deadlineErr := connection.SetReadDeadline(
		server.now().Add(serviceHelperHandshakeTimeout),
	); deadlineErr != nil {
		return
	}
	request, err := readServiceHelperRequest(connection)
	if err != nil {
		return
	}
	if err := connection.SetReadDeadline(time.Time{}); err != nil {
		return
	}
	identityAction := authorityBrokerActionExecute
	if request.Kind == serviceHelperRequestSnapshot {
		identityAction = authorityBrokerActionSnapshot
	}
	if !server.identity.Authorize(peer.Pid, identityAction) {
		return
	}
	if err := server.validateRequestAuthority(request); err != nil {
		server.writeError(connection, request.RequestID, request.ExpiresAt, "AUTHORITY_DENIED")
		return
	}
	if request.Kind == serviceHelperRequestSnapshot {
		_ = writeServiceHelperConnectionResponse(connection, request.ExpiresAt, serviceHelperResponse{
			Version:   ServiceHelperProtocolVersion,
			Kind:      serviceHelperRequestSnapshot,
			RequestID: request.RequestID,
			Snapshot:  &server.snapshot,
		})
		return
	}
	if request.Kind == serviceHelperRequestCancel {
		if !server.cancel(request.TargetRequestID, request.ExpiresAt) {
			server.writeError(connection, request.RequestID, request.ExpiresAt, "CANCEL_CAPACITY")
			return
		}
		_ = writeServiceHelperConnectionResponse(connection, request.ExpiresAt, serviceHelperResponse{
			Version:   ServiceHelperProtocolVersion,
			Kind:      serviceHelperRequestCancel,
			RequestID: request.RequestID,
		})
		return
	}
	operationContext, cancel := context.WithDeadline(
		serverContext,
		time.Unix(request.ExpiresAt, 0),
	)
	active, registered := server.register(request.RequestID, request.ExpiresAt, cancel, request.Kind)
	if !registered {
		cancel()
		server.writeError(connection, request.RequestID, request.ExpiresAt, "REQUEST_CANCELED")
		return
	}
	defer func() {
		cancel()
		server.finish(request.RequestID, active)
	}()
	response := serviceHelperResponse{
		Version:   ServiceHelperProtocolVersion,
		Kind:      request.Kind,
		RequestID: request.RequestID,
	}
	switch request.Kind {
	case serviceHelperRequestStatus:
		status, statusErr := server.manager.Status(operationContext, ServiceStatusRequest{
			Profile: request.Profile, Service: request.Service,
		})
		if statusErr != nil {
			server.writeError(connection, request.RequestID, request.ExpiresAt, "REQUEST_FAILED")
			return
		}
		response.Status = &status
	case serviceHelperRequestLogs:
		logs, logsErr := server.manager.Logs(operationContext, ServiceLogRequest{
			Profile: request.Profile, Service: request.Service,
			Entries: request.Entries, SinceSeconds: request.SinceSeconds,
		})
		if logsErr != nil {
			server.writeError(connection, request.RequestID, request.ExpiresAt, "REQUEST_FAILED")
			return
		}
		response.Logs = &logs
	case serviceHelperRequestAction:
		result, actionErr := server.manager.executeAction(
			operationContext,
			ServiceActionRequest{
				Profile: request.Profile, Service: request.Service, Action: request.Action,
			},
			func() bool { return server.accept(request.RequestID, active) },
		)
		if actionErr != nil {
			server.writeError(connection, request.RequestID, request.ExpiresAt, "REQUEST_FAILED")
			return
		}
		response.Action = &result
	default:
		server.writeError(connection, request.RequestID, request.ExpiresAt, "INVALID_REQUEST")
		return
	}
	_ = writeServiceHelperConnectionResponse(connection, request.ExpiresAt, response)
}

func (server *serviceHelperServer) validateRequestAuthority(request serviceHelperRequest) error {
	now := server.now()
	if now.Unix() <= 0 || request.ExpiresAt <= now.Unix() ||
		request.ExpiresAt > now.Add(nodes.MaxExecutionPlanTTL).Unix() {
		return errors.New("service helper request expiry is invalid")
	}
	if request.Kind == serviceHelperRequestSnapshot {
		return nil
	}
	if request.SnapshotDigest != server.snapshot.SnapshotDigest {
		return errors.New("service helper snapshot is stale")
	}
	if request.Kind == serviceHelperRequestCancel {
		return nil
	}
	profile, found := server.config.Profiles[request.Profile]
	if !found || !profile.Enabled || profile.Revision != request.Revision {
		return errors.New("service helper profile is stale")
	}
	if request.Kind == serviceHelperRequestLogs &&
		(request.Entries > profile.LogLimits.EntriesMax ||
			request.SinceSeconds > profile.LogLimits.AgeSecondsMax) {
		return errors.New("service helper log bounds exceed policy")
	}
	return nil
}

func pinServiceHelperExecutables(
	config ServiceHelperServiceConfig,
) (ServiceHelperServiceConfig, systemdProcessRunner, error) {
	runner := systemdProcessRunner{env: fixedSystemdEnvironment()}
	var err error
	runner.systemctl, err = openPinnedSystemdExecutable(config.SystemctlPath)
	if err != nil {
		return ServiceHelperServiceConfig{}, systemdProcessRunner{},
			fmt.Errorf("pin service helper systemctl: %w", err)
	}
	config.SystemctlPath = runner.systemctl.path
	config.systemctlIdentity = runner.systemctl.identity
	if config.JournalctlPath != "" {
		runner.journal, err = openPinnedSystemdExecutable(config.JournalctlPath)
		if err != nil {
			_ = runner.close()
			return ServiceHelperServiceConfig{}, systemdProcessRunner{},
				fmt.Errorf("pin service helper journalctl: %w", err)
		}
		config.JournalctlPath = runner.journal.path
		config.journalIdentity = runner.journal.identity
	}
	return config, runner, nil
}

func openPinnedSystemdExecutable(path string) (commandExecutable, error) {
	resolved, err := trustedSystemdExecutable(path)
	if err != nil {
		return commandExecutable{}, err
	}
	if directoryErr := verifyAuthorityBrokerDirectoryChain(filepath.Dir(resolved)); directoryErr != nil {
		return commandExecutable{}, fmt.Errorf("validate executable directory: %w", directoryErr)
	}
	descriptor, err := unix.Open(resolved, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return commandExecutable{}, err
	}
	file := os.NewFile(uintptr(descriptor), resolved)
	if file == nil {
		_ = unix.Close(descriptor)
		return commandExecutable{}, errors.New("open pinned systemd executable: invalid descriptor")
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = file.Close()
		}
	}()
	var opened unix.Stat_t
	if statErr := unix.Fstat(descriptor, &opened); statErr != nil {
		return commandExecutable{}, statErr
	}
	var current unix.Stat_t
	if statErr := unix.Fstatat(unix.AT_FDCWD, resolved, &current, unix.AT_SYMLINK_NOFOLLOW); statErr != nil {
		return commandExecutable{}, statErr
	}
	if opened.Dev != current.Dev || opened.Ino != current.Ino ||
		opened.Uid != 0 || opened.Mode&unix.S_IFMT != unix.S_IFREG ||
		opened.Mode&0o111 == 0 || opened.Mode&0o022 != 0 {
		return commandExecutable{}, errors.New("pinned systemd executable identity is invalid")
	}
	content := sha256.New()
	if _, copyErr := io.Copy(content, file); copyErr != nil {
		return commandExecutable{}, copyErr
	}
	if _, seekErr := file.Seek(0, 0); seekErr != nil {
		return commandExecutable{}, seekErr
	}
	binding, err := json.Marshal(struct {
		Path    string `json:"path"`
		Device  uint64 `json:"device"`
		Inode   uint64 `json:"inode"`
		Mode    uint32 `json:"mode"`
		UID     uint32 `json:"uid"`
		GID     uint32 `json:"gid"`
		Content string `json:"content_sha256"`
	}{
		Path: resolved, Device: opened.Dev, Inode: opened.Ino,
		Mode: opened.Mode, UID: opened.Uid, GID: opened.Gid,
		Content: hex.EncodeToString(content.Sum(nil)),
	})
	if err != nil {
		return commandExecutable{}, err
	}
	identity := sha256.Sum256(binding)
	closeOnError = false
	return commandExecutable{
		path: resolved, file: file, identity: hex.EncodeToString(identity[:]),
	}, nil
}

func (server *serviceHelperServer) register(
	requestID string,
	expiresAt int64,
	cancel context.CancelFunc,
	kind string,
) (*serviceHelperActiveRequest, bool) {
	server.mu.Lock()
	defer server.mu.Unlock()
	server.pruneCanceledLocked(server.now().Unix())
	if _, duplicate := server.active[requestID]; duplicate {
		return nil, false
	}
	if _, canceled := server.preCanceled[requestID]; canceled {
		delete(server.preCanceled, requestID)
		return nil, false
	}
	if len(server.active) >= maxServiceHelperActiveRequests {
		return nil, false
	}
	active := &serviceHelperActiveRequest{
		cancel:    cancel,
		mutation:  kind == serviceHelperRequestAction,
		expiresAt: expiresAt,
	}
	server.active[requestID] = active
	return active, true
}

func (server *serviceHelperServer) accept(
	requestID string,
	active *serviceHelperActiveRequest,
) bool {
	server.mu.Lock()
	defer server.mu.Unlock()
	current, found := server.active[requestID]
	if !found || current != active || current.accepted || current.canceled ||
		current.expiresAt <= server.now().Unix() {
		return false
	}
	current.accepted = true
	return true
}

func (server *serviceHelperServer) cancel(requestID string, expiresAt int64) bool {
	server.mu.Lock()
	defer server.mu.Unlock()
	server.pruneCanceledLocked(server.now().Unix())
	if active, found := server.active[requestID]; found {
		if !active.mutation || !active.accepted {
			active.canceled = true
			active.cancel()
		}
		return true
	}
	if _, retained := server.preCanceled[requestID]; retained {
		server.preCanceled[requestID] = expiresAt
		return true
	}
	if len(server.preCanceled) >= maxServiceHelperActiveRequests {
		return false
	}
	server.preCanceled[requestID] = expiresAt
	return true
}

func (server *serviceHelperServer) finish(
	requestID string,
	active *serviceHelperActiveRequest,
) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.active[requestID] == active {
		delete(server.active, requestID)
	}
}

func (server *serviceHelperServer) pruneCanceledLocked(now int64) {
	for requestID, expiresAt := range server.preCanceled {
		if expiresAt <= now {
			delete(server.preCanceled, requestID)
		}
	}
}

func (server *serviceHelperServer) writeError(
	connection *net.UnixConn,
	requestID string,
	expiresAt int64,
	code string,
) {
	_ = writeServiceHelperConnectionResponse(connection, expiresAt, serviceHelperResponse{
		Version:   ServiceHelperProtocolVersion,
		Kind:      "error",
		RequestID: requestID,
		Code:      code,
	})
}

func writeServiceHelperConnectionResponse(
	connection *net.UnixConn,
	expiresAt int64,
	response serviceHelperResponse,
) error {
	deadline := time.Now().Add(serviceHelperWriteTimeout)
	if expires := time.Unix(expiresAt, 0); expires.Before(deadline) {
		deadline = expires
	}
	if err := connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	if err := writeServiceHelperResponse(connection, response); err != nil {
		return err
	}
	return connection.SetWriteDeadline(time.Time{})
}
