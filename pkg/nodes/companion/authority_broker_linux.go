//go:build linux

package companion

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
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

type AuthorityBrokerTerminal struct {
	connection *net.UnixConn
	terminalID string
	writeMu    sync.Mutex
	readMu     sync.Mutex
	closeOnce  sync.Once
	done       chan struct{}
	cursor     uint64
	ended      bool
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

func (client *AuthorityBrokerClient) OpenTerminal(
	ctx context.Context,
	request TerminalBrokerOpenRequest,
) (*AuthorityBrokerTerminal, TerminalBrokerEvent, error) {
	if client == nil {
		return nil, TerminalBrokerEvent{}, errors.New("authority broker client is unavailable")
	}
	if err := request.validate(); err != nil {
		return nil, TerminalBrokerEvent{}, err
	}
	connection, err := (&net.Dialer{}).DialContext(ctx, "unix", client.socketPath)
	if err != nil {
		return nil, TerminalBrokerEvent{}, fmt.Errorf("connect authority broker: %w", err)
	}
	unixConnection, ok := connection.(*net.UnixConn)
	if !ok {
		_ = connection.Close()
		return nil, TerminalBrokerEvent{}, errors.New("authority broker connection is not Unix")
	}
	peer, err := authorityBrokerPeerCredentials(unixConnection)
	if err != nil ||
		peer.Uid != client.expectedServerUID ||
		peer.Gid != client.expectedServerGID {
		_ = connection.Close()
		return nil, TerminalBrokerEvent{}, errors.New("authority broker server identity is invalid")
	}
	handshakeDone := make(chan struct{})
	defer close(handshakeDone)
	go func() {
		select {
		case <-ctx.Done():
			_ = unixConnection.Close()
		case <-handshakeDone:
		}
	}()
	if err := writeAuthorityBrokerFrame(connection, authorityBrokerRequestFrame{
		Version:  AuthorityBrokerProtocolVersion,
		Action:   authorityBrokerActionTerminal,
		Terminal: &request,
	}); err != nil {
		_ = connection.Close()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, TerminalBrokerEvent{}, ctxErr
		}
		return nil, TerminalBrokerEvent{}, fmt.Errorf(
			"%w: write terminal open request: %v",
			ErrTerminalOutcomeUnknown,
			err,
		)
	}
	var response authorityBrokerResponseFrame
	if err := readAuthorityBrokerFrame(connection, &response); err != nil {
		_ = connection.Close()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, TerminalBrokerEvent{}, ctxErr
		}
		return nil, TerminalBrokerEvent{}, fmt.Errorf(
			"%w: read terminal open response: %v",
			ErrTerminalOutcomeUnknown,
			err,
		)
	}
	if response.Version != AuthorityBrokerProtocolVersion ||
		!response.OK ||
		response.Terminal == nil ||
		response.Result != nil ||
		response.Snapshot != nil ||
		response.Terminal.Type != TerminalEventOpened {
		_ = connection.Close()
		return nil, TerminalBrokerEvent{}, errors.New("authority broker denied terminal open")
	}
	if err := response.Terminal.validate(); err != nil {
		_ = connection.Close()
		return nil, TerminalBrokerEvent{}, fmt.Errorf(
			"%w: invalid terminal open response",
			ErrTerminalOutcomeUnknown,
		)
	}
	terminal := &AuthorityBrokerTerminal{
		connection: unixConnection,
		terminalID: response.Terminal.TerminalID,
		done:       make(chan struct{}),
	}
	go func() {
		select {
		case <-ctx.Done():
			_ = terminal.Close()
		case <-terminal.done:
		}
	}()
	return terminal, *response.Terminal, nil
}

func (terminal *AuthorityBrokerTerminal) ID() string {
	if terminal == nil {
		return ""
	}
	return terminal.terminalID
}

func (terminal *AuthorityBrokerTerminal) Send(
	ctx context.Context,
	control TerminalBrokerControl,
) error {
	if terminal == nil || terminal.connection == nil {
		return errors.New("authority broker terminal is unavailable")
	}
	control.Version = AuthorityBrokerProtocolVersion
	if _, err := control.validate(); err != nil {
		return err
	}
	terminal.writeMu.Lock()
	defer terminal.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	stopInterrupt, err := armAuthorityBrokerTerminalContext(
		ctx,
		terminal.connection.SetWriteDeadline,
	)
	if err != nil {
		return err
	}
	writeErr := writeAuthorityBrokerFrame(terminal.connection, control)
	interrupted := stopInterrupt()
	if writeErr != nil {
		if interrupted {
			_ = terminal.Close()
			return ctx.Err()
		}
		return fmt.Errorf("%w: write terminal control: %v", ErrTerminalOutcomeUnknown, writeErr)
	}
	return nil
}

func (terminal *AuthorityBrokerTerminal) Receive(ctx context.Context) (TerminalBrokerEvent, error) {
	if terminal == nil || terminal.connection == nil {
		return TerminalBrokerEvent{}, errors.New("authority broker terminal is unavailable")
	}
	terminal.readMu.Lock()
	defer terminal.readMu.Unlock()
	if terminal.ended {
		return TerminalBrokerEvent{}, errors.New("authority broker terminal has ended")
	}
	if err := ctx.Err(); err != nil {
		return TerminalBrokerEvent{}, err
	}
	stopInterrupt, err := armAuthorityBrokerTerminalContext(
		ctx,
		terminal.connection.SetReadDeadline,
	)
	if err != nil {
		return TerminalBrokerEvent{}, err
	}
	var event TerminalBrokerEvent
	readErr := readAuthorityBrokerFrame(terminal.connection, &event)
	interrupted := stopInterrupt()
	if readErr != nil {
		if interrupted {
			_ = terminal.Close()
			return TerminalBrokerEvent{}, ctx.Err()
		}
		return TerminalBrokerEvent{}, fmt.Errorf(
			"%w: read terminal event: %v",
			ErrTerminalOutcomeUnknown,
			readErr,
		)
	}
	if err := event.validate(); err != nil || event.TerminalID != terminal.terminalID {
		return TerminalBrokerEvent{}, fmt.Errorf(
			"%w: invalid terminal event",
			ErrTerminalOutcomeUnknown,
		)
	}
	switch event.Type {
	case TerminalEventOpened:
		return TerminalBrokerEvent{}, fmt.Errorf(
			"%w: repeated terminal opened event",
			ErrTerminalOutcomeUnknown,
		)
	case TerminalEventOutput:
		data, _ := base64.StdEncoding.Strict().DecodeString(event.DataBase64)
		if event.Cursor != terminal.cursor+uint64(len(data)) {
			return TerminalBrokerEvent{}, fmt.Errorf(
				"%w: terminal output cursor is discontinuous",
				ErrTerminalOutcomeUnknown,
			)
		}
		terminal.cursor = event.Cursor
	case TerminalEventClosed, TerminalEventUnknown:
		terminal.ended = true
	}
	return event, nil
}

func armAuthorityBrokerTerminalContext(
	ctx context.Context,
	setDeadline func(time.Time) error,
) (func() bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	done := make(chan struct{})
	exited := make(chan struct{})
	interrupted := make(chan struct{}, 1)
	go func() {
		defer close(exited)
		select {
		case <-ctx.Done():
			_ = setDeadline(time.Now())
			interrupted <- struct{}{}
		case <-done:
		}
	}()
	return func() bool {
		close(done)
		<-exited
		_ = setDeadline(time.Time{})
		select {
		case <-interrupted:
			return true
		default:
			return false
		}
	}, nil
}

func (terminal *AuthorityBrokerTerminal) Close() error {
	if terminal == nil {
		return nil
	}
	var closeErr error
	terminal.closeOnce.Do(func() {
		close(terminal.done)
		if terminal.connection != nil {
			closeErr = terminal.connection.Close()
		}
	})
	return closeErr
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

type authorityBrokerTerminalRunner interface {
	Terminal(
		context.Context,
		preparedAuthorityBrokerTerminal,
		TerminalBrokerOpenRequest,
		string,
		<-chan TerminalBrokerControl,
		chan<- TerminalBrokerEvent,
	) error
}

type authorityBrokerServer struct {
	config             AuthorityBrokerConfig
	runner             authorityBrokerExecutionRunner
	semaphores         map[string]chan struct{}
	terminalSemaphores map[string]chan struct{}
	identity           authorityBrokerCompanionIdentity
}

func newAuthorityBrokerServer(
	config AuthorityBrokerConfig,
	runner authorityBrokerExecutionRunner,
	identity authorityBrokerCompanionIdentity,
) (*authorityBrokerServer, error) {
	if len(config.normalizedProfile) != MaxShellBrokerProfiles ||
		runner == nil ||
		identity == nil {
		return nil, errors.New("authority broker server configuration is incomplete")
	}
	semaphores := make(map[string]chan struct{}, len(config.normalizedProfile))
	terminalSemaphores := make(map[string]chan struct{}, len(config.normalizedProfile))
	for alias, profile := range config.normalizedProfile {
		semaphores[alias] = make(chan struct{}, profile.ConcurrentCommands)
		terminalSemaphores[alias] = make(chan struct{}, profile.ConcurrentTerminals)
	}
	return &authorityBrokerServer{
		config: config, runner: runner, semaphores: semaphores,
		terminalSemaphores: terminalSemaphores, identity: identity,
	}, nil
}

func (server *authorityBrokerServer) Serve(
	ctx context.Context,
	listener *net.UnixListener,
) error {
	if server == nil || listener == nil {
		return errors.New("authority broker server is unavailable")
	}
	defer server.identity.Close()
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
	if !server.identity.Authorize(peer.Pid, request.Action) {
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
	if request.Action == authorityBrokerActionTerminal {
		server.handleTerminal(serverContext, connection, *request.Terminal)
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

func (server *authorityBrokerServer) handleTerminal(
	serverContext context.Context,
	connection *net.UnixConn,
	request TerminalBrokerOpenRequest,
) {
	runner, ok := server.runner.(authorityBrokerTerminalRunner)
	if !ok {
		_ = server.writeResponse(connection, authorityBrokerResponseFrame{Code: "UNAVAILABLE"})
		return
	}
	prepared, err := server.config.prepareTerminal(request)
	if err != nil {
		_ = server.writeResponse(connection, authorityBrokerResponseFrame{Code: "DENIED"})
		return
	}
	terminalID, err := newAuthorityBrokerTerminalID()
	if err != nil {
		_ = server.writeResponse(connection, authorityBrokerResponseFrame{Code: "UNAVAILABLE"})
		return
	}
	terminalContext, cancel := context.WithCancel(serverContext)
	defer cancel()
	controls := make(chan TerminalBrokerControl)
	peerDone := make(chan error, 1)
	go func() {
		for {
			var control TerminalBrokerControl
			if readErr := readAuthorityBrokerFrame(connection, &control); readErr != nil {
				peerDone <- readErr
				cancel()
				return
			}
			if _, validationErr := control.validate(); validationErr != nil {
				peerDone <- validationErr
				cancel()
				return
			}
			select {
			case controls <- control:
			case <-terminalContext.Done():
				peerDone <- terminalContext.Err()
				return
			}
		}
	}()
	semaphore := server.terminalSemaphores[request.Profile]
	select {
	case semaphore <- struct{}{}:
		defer func() { <-semaphore }()
	case <-terminalContext.Done():
		return
	}
	events := make(chan TerminalBrokerEvent)
	runnerDone := make(chan error, 1)
	go func() {
		runnerDone <- runner.Terminal(
			terminalContext,
			prepared,
			request,
			terminalID,
			controls,
			events,
		)
	}()
	var opened TerminalBrokerEvent
	select {
	case opened = <-events:
		if opened.Type != TerminalEventOpened ||
			opened.TerminalID != terminalID ||
			opened.validate() != nil {
			cancel()
			<-runnerDone
			_ = server.writeResponse(connection, authorityBrokerResponseFrame{Code: "UNKNOWN"})
			return
		}
	case <-peerDone:
		cancel()
		<-runnerDone
		return
	case <-runnerDone:
		_ = server.writeResponse(connection, authorityBrokerResponseFrame{Code: "UNKNOWN"})
		return
	}
	if err := server.writeResponse(connection, authorityBrokerResponseFrame{
		OK: true, Terminal: &opened,
	}); err != nil {
		cancel()
		<-runnerDone
		return
	}
	for {
		select {
		case event := <-events:
			if event.TerminalID != terminalID || event.validate() != nil {
				cancel()
				<-runnerDone
				return
			}
			if err := server.writeTerminalEvent(connection, event); err != nil {
				cancel()
				<-runnerDone
				return
			}
			if event.Type == TerminalEventClosed || event.Type == TerminalEventUnknown {
				<-runnerDone
				return
			}
		case <-peerDone:
			cancel()
			<-runnerDone
			return
		case <-runnerDone:
			unknown := TerminalBrokerEvent{
				Version: AuthorityBrokerProtocolVersion,
				Type:    TerminalEventUnknown, TerminalID: terminalID, State: "unknown",
				Reason: TerminalCloseDisconnected,
			}
			_ = server.writeTerminalEvent(connection, unknown)
			return
		}
	}
}

func newAuthorityBrokerTerminalID() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return "terminal_" + hex.EncodeToString(random[:]), nil
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

func (*authorityBrokerServer) writeTerminalEvent(
	connection *net.UnixConn,
	event TerminalBrokerEvent,
) error {
	event.Version = AuthorityBrokerProtocolVersion
	if err := connection.SetWriteDeadline(
		time.Now().Add(authorityBrokerResponseWriteTimeout),
	); err != nil {
		return err
	}
	return writeAuthorityBrokerFrame(connection, event)
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
