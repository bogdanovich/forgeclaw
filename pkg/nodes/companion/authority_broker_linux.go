//go:build linux

package companion

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const (
	authorityBrokerHandshakeTimeout     = 5 * time.Second
	authorityBrokerResponseWriteTimeout = time.Second
	authorityBrokerSocketWriteBuffer    = 32 * 1024
)

type AuthorityBrokerClient struct {
	socketPath        string
	expectedServerUID uint32
	expectedServerGID uint32
}

func NewAuthorityBrokerClient(socketPath string) (*AuthorityBrokerClient, error) {
	return newAuthorityBrokerClient(socketPath, 0, 0)
}

func newAuthorityBrokerClient(
	socketPath string,
	expectedServerUID uint32,
	expectedServerGID uint32,
) (*AuthorityBrokerClient, error) {
	path, err := resolveAuthorityBrokerPath("", socketPath, false)
	if err != nil || path == string(os.PathSeparator) {
		return nil, errors.New("authority broker socket path is invalid")
	}
	return &AuthorityBrokerClient{
		socketPath:        path,
		expectedServerUID: expectedServerUID,
		expectedServerGID: expectedServerGID,
	}, nil
}

func (client *AuthorityBrokerClient) Snapshot(ctx context.Context) (ShellBrokerSnapshot, error) {
	response, err := client.call(ctx, authorityBrokerRequestFrame{
		Version: AuthorityBrokerProtocolVersion,
		Action:  authorityBrokerActionSnapshot,
	})
	if err != nil {
		return ShellBrokerSnapshot{}, err
	}
	if response.Snapshot == nil || response.Result != nil {
		return ShellBrokerSnapshot{}, errors.New("authority broker returned invalid snapshot response")
	}
	return normalizeShellBrokerSnapshot(*response.Snapshot)
}

func (client *AuthorityBrokerClient) Execute(
	ctx context.Context,
	request ShellBrokerRequest,
) (ShellBrokerResult, error) {
	response, err := client.call(ctx, authorityBrokerRequestFrame{
		Version: AuthorityBrokerProtocolVersion,
		Action:  authorityBrokerActionExecute,
		Execute: &request,
	})
	if err != nil {
		return ShellBrokerResult{}, err
	}
	switch response.Code {
	case "CANCELED":
		return ShellBrokerResult{}, ErrShellBrokerCancellationConfirmed
	case "UNKNOWN":
		return ShellBrokerResult{}, ErrShellBrokerOutcomeUnknown
	}
	if !response.OK {
		return ShellBrokerResult{}, errors.New("authority broker denied execution")
	}
	if response.Result == nil || response.Snapshot != nil {
		return ShellBrokerResult{}, fmt.Errorf(
			"%w: authority broker returned invalid execution response",
			ErrShellBrokerOutcomeUnknown,
		)
	}
	return *response.Result, nil
}

func (client *AuthorityBrokerClient) call(
	ctx context.Context,
	request authorityBrokerRequestFrame,
) (authorityBrokerResponseFrame, error) {
	if client == nil {
		return authorityBrokerResponseFrame{}, errors.New("authority broker client is unavailable")
	}
	connection, err := (&net.Dialer{}).DialContext(ctx, "unix", client.socketPath)
	if err != nil {
		return authorityBrokerResponseFrame{}, fmt.Errorf("connect authority broker: %w", err)
	}
	defer connection.Close()
	unixConnection, ok := connection.(*net.UnixConn)
	if !ok {
		return authorityBrokerResponseFrame{}, errors.New("authority broker connection is not Unix")
	}
	peer, err := authorityBrokerPeerCredentials(unixConnection)
	if err != nil ||
		peer.Uid != client.expectedServerUID ||
		peer.Gid != client.expectedServerGID {
		return authorityBrokerResponseFrame{}, errors.New("authority broker server identity is invalid")
	}
	if err := ctx.Err(); err != nil {
		return authorityBrokerResponseFrame{}, err
	}
	if err := writeAuthorityBrokerFrame(connection, request); err != nil {
		if request.Action == authorityBrokerActionExecute {
			return authorityBrokerResponseFrame{}, fmt.Errorf(
				"%w: write authority broker request: %v",
				ErrShellBrokerOutcomeUnknown,
				err,
			)
		}
		return authorityBrokerResponseFrame{}, fmt.Errorf("write authority broker request: %w", err)
	}
	callDone := make(chan struct{})
	defer close(callDone)
	go func() {
		select {
		case <-ctx.Done():
			_ = unixConnection.CloseWrite()
			_ = unixConnection.SetReadDeadline(
				time.Now().Add(authorityBrokerCleanupLimit + authorityBrokerHandshakeTimeout),
			)
		case <-callDone:
		}
	}()
	var response authorityBrokerResponseFrame
	if err := readAuthorityBrokerFrame(connection, &response); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil && request.Action != authorityBrokerActionExecute {
			return authorityBrokerResponseFrame{}, ctxErr
		}
		if request.Action == authorityBrokerActionExecute {
			return authorityBrokerResponseFrame{}, fmt.Errorf(
				"%w: read authority broker response: %v",
				ErrShellBrokerOutcomeUnknown,
				err,
			)
		}
		return authorityBrokerResponseFrame{}, fmt.Errorf("read authority broker response: %w", err)
	}
	if response.Version != AuthorityBrokerProtocolVersion {
		if request.Action == authorityBrokerActionExecute {
			return authorityBrokerResponseFrame{}, fmt.Errorf(
				"%w: authority broker response version is invalid",
				ErrShellBrokerOutcomeUnknown,
			)
		}
		return authorityBrokerResponseFrame{}, errors.New("authority broker response version is invalid")
	}
	return response, nil
}

type authorityBrokerExecutionRunner interface {
	Execute(
		context.Context,
		preparedAuthorityBrokerExecution,
		ShellBrokerRequest,
	) (ShellBrokerResult, error)
}

type authorityBrokerServer struct {
	config       AuthorityBrokerConfig
	runner       authorityBrokerExecutionRunner
	semaphores   map[string]chan struct{}
	companionMu  sync.Mutex
	companionPID int32
}

func newAuthorityBrokerServer(
	config AuthorityBrokerConfig,
	runner authorityBrokerExecutionRunner,
) (*authorityBrokerServer, error) {
	if len(config.normalizedProfile) != MaxShellBrokerProfiles || runner == nil {
		return nil, errors.New("authority broker server configuration is incomplete")
	}
	semaphores := make(map[string]chan struct{}, len(config.normalizedProfile))
	for alias, profile := range config.normalizedProfile {
		semaphores[alias] = make(chan struct{}, profile.ConcurrentCommands)
	}
	return &authorityBrokerServer{config: config, runner: runner, semaphores: semaphores}, nil
}

func (server *authorityBrokerServer) Serve(
	ctx context.Context,
	listener *net.UnixListener,
) error {
	if server == nil || listener == nil {
		return errors.New("authority broker server is unavailable")
	}
	var workers sync.WaitGroup
	defer workers.Wait()
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	for {
		connection, err := listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept authority broker connection: %w", err)
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer connection.Close()
			server.handleConnection(ctx, connection)
		}()
	}
}

func (server *authorityBrokerServer) handleConnection(
	serverContext context.Context,
	connection *net.UnixConn,
) {
	peer, peerErr := authorityBrokerPeerCredentials(connection)
	if peerErr != nil ||
		peer.Uid != server.config.AllowedUID ||
		peer.Gid != server.config.AllowedGID {
		return
	}
	if err := connection.SetWriteBuffer(authorityBrokerSocketWriteBuffer); err != nil {
		return
	}
	_ = connection.SetReadDeadline(time.Now().Add(authorityBrokerHandshakeTimeout))
	var request authorityBrokerRequestFrame
	if readErr := readAuthorityBrokerFrame(connection, &request); readErr != nil {
		return
	}
	_ = connection.SetReadDeadline(time.Time{})
	if validationErr := validateAuthorityBrokerRequestFrame(request); validationErr != nil {
		_ = server.writeResponse(connection, authorityBrokerResponseFrame{Code: "INVALID_REQUEST"})
		return
	}
	if !server.authorizeCompanionPID(peer.Pid, request.Action) {
		return
	}
	if request.Action == authorityBrokerActionSnapshot {
		snapshot, snapshotErr := server.config.Snapshot()
		if snapshotErr != nil {
			_ = server.writeResponse(connection, authorityBrokerResponseFrame{Code: "UNAVAILABLE"})
			return
		}
		_ = server.writeResponse(connection, authorityBrokerResponseFrame{OK: true, Snapshot: &snapshot})
		return
	}
	prepared, err := server.config.prepareExecution(*request.Execute)
	if err != nil {
		_ = server.writeResponse(connection, authorityBrokerResponseFrame{Code: "DENIED"})
		return
	}
	executeContext, cancel := context.WithCancel(serverContext)
	defer cancel()
	peerClosed := make(chan struct{})
	go func() {
		var discarded [1]byte
		_, _ = connection.Read(discarded[:])
		cancel()
		close(peerClosed)
	}()
	semaphore := server.semaphores[request.Execute.Profile]
	select {
	case semaphore <- struct{}{}:
		defer func() { <-semaphore }()
	case <-executeContext.Done():
		_ = server.writeResponse(connection, authorityBrokerResponseFrame{Code: "CANCELED"})
		return
	}
	result, executeErr := server.runner.Execute(executeContext, prepared, *request.Execute)
	response := authorityBrokerResponseFrame{Result: &result}
	switch {
	case errors.Is(executeErr, ErrShellBrokerCancellationConfirmed):
		response = authorityBrokerResponseFrame{Code: "CANCELED"}
	case executeErr != nil:
		response = authorityBrokerResponseFrame{Code: "UNKNOWN"}
	default:
		response.OK = true
	}
	if err := server.writeResponse(connection, response); err != nil {
		return
	}
	_ = connection.CloseRead()
	select {
	case <-peerClosed:
	case <-time.After(time.Second):
	}
}

func (server *authorityBrokerServer) authorizeCompanionPID(peerPID int32, action string) bool {
	if peerPID <= 0 {
		return false
	}
	server.companionMu.Lock()
	defer server.companionMu.Unlock()
	if server.companionPID == 0 {
		if action != authorityBrokerActionSnapshot {
			return false
		}
		server.companionPID = peerPID
	}
	return server.companionPID == peerPID
}

func (*authorityBrokerServer) writeResponse(
	connection *net.UnixConn,
	response authorityBrokerResponseFrame,
) error {
	response.Version = AuthorityBrokerProtocolVersion
	if err := connection.SetWriteDeadline(
		time.Now().Add(authorityBrokerResponseWriteTimeout),
	); err != nil {
		return err
	}
	return writeAuthorityBrokerFrame(connection, response)
}

func authorityBrokerPeerCredentials(connection *net.UnixConn) (*unix.Ucred, error) {
	raw, err := connection.SyscallConn()
	if err != nil {
		return nil, err
	}
	var credentials *unix.Ucred
	var socketErr error
	if err := raw.Control(func(fd uintptr) {
		credentials, socketErr = unix.GetsockoptUcred(
			int(fd),
			unix.SOL_SOCKET,
			unix.SO_PEERCRED,
		)
	}); err != nil {
		return nil, err
	}
	if socketErr != nil {
		return nil, socketErr
	}
	return credentials, nil
}
