//go:build linux

package companion

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

type fakeAuthorityBrokerRunner struct {
	mu       sync.Mutex
	requests []ShellBrokerRequest
	started  chan struct{}
	block    bool
	err      error
	result   ShellBrokerResult
}

type exitingAuthorityBrokerTerminalRunner struct {
	fakeAuthorityBrokerRunner
	startedAt int64
}

func (runner *exitingAuthorityBrokerTerminalRunner) Terminal(
	_ context.Context,
	_ preparedAuthorityBrokerTerminal,
	_ TerminalBrokerOpenRequest,
	terminalID string,
	_ <-chan TerminalBrokerControl,
	events chan<- TerminalBrokerEvent,
) error {
	events <- TerminalBrokerEvent{
		Version: AuthorityBrokerProtocolVersion, Type: TerminalEventOpened,
		TerminalID: terminalID, State: "live", StartedAt: runner.startedAt,
	}
	return nil
}

func (runner *fakeAuthorityBrokerRunner) Execute(
	ctx context.Context,
	_ preparedAuthorityBrokerExecution,
	request ShellBrokerRequest,
) (ShellBrokerResult, error) {
	runner.mu.Lock()
	runner.requests = append(runner.requests, request)
	started := runner.started
	block := runner.block
	executeErr := runner.err
	result := runner.result
	runner.mu.Unlock()
	if started != nil {
		select {
		case <-started:
		default:
			close(started)
		}
	}
	if block {
		<-ctx.Done()
		return ShellBrokerResult{}, ErrShellBrokerCancellationConfirmed
	}
	if executeErr != nil {
		return ShellBrokerResult{}, executeErr
	}
	if result.StartedAt != 0 {
		return result, nil
	}
	return ShellBrokerResult{
		ExitCode: 0, Stdout: "ok",
		StartedAt: 1, CompletedAt: 2,
	}, nil
}

func TestAuthorityBrokerUnixRoundTrip(t *testing.T) {
	runner := &fakeAuthorityBrokerRunner{}
	client, stop := startTestAuthorityBrokerServer(t, runner)
	defer stop()
	snapshot, err := client.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Revision != "broker-v1" ||
		len(snapshot.Profiles) != 1 ||
		snapshot.Profiles[0].Alias != "owner-root" {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	result, err := client.Execute(t.Context(), validAuthorityBrokerRequest())
	if err != nil {
		t.Fatal(err)
	}
	if result.Stdout != "ok" || result.ExitCode != 0 {
		t.Fatalf("result = %#v", result)
	}
}

func TestAuthorityBrokerUnixCancellationReturnsProof(t *testing.T) {
	runner := &fakeAuthorityBrokerRunner{started: make(chan struct{}), block: true}
	client, stop := startTestAuthorityBrokerServer(t, runner)
	defer stop()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := client.Execute(ctx, validAuthorityBrokerRequest())
		done <- err
	}()
	<-runner.started
	cancel()
	if err := <-done; !errors.Is(err, ErrShellBrokerCancellationConfirmed) {
		t.Fatalf("cancellation result = %v", err)
	}
}

func TestAuthorityBrokerUnixRejectsWrongPeerIdentity(t *testing.T) {
	runner := &fakeAuthorityBrokerRunner{}
	client, stop := startTestAuthorityBrokerServer(t, runner)
	defer stop()
	client.expectedServerUID++
	if _, err := client.Snapshot(t.Context()); err == nil {
		t.Fatal("wrong server peer identity was accepted")
	}
}

func TestAuthorityBrokerUnixDoesNotSendCanceledExecution(t *testing.T) {
	runner := &fakeAuthorityBrokerRunner{}
	client, stop := startTestAuthorityBrokerServer(t, runner)
	defer stop()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := client.Execute(
		ctx,
		validAuthorityBrokerRequest(),
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute() error = %v", err)
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if len(runner.requests) != 0 {
		t.Fatalf("canceled execution reached runner: %d requests", len(runner.requests))
	}
}

func TestAuthorityBrokerUnixPreservesUnknownRunnerOutcome(t *testing.T) {
	runner := &fakeAuthorityBrokerRunner{err: errors.New("lost process proof")}
	client, stop := startTestAuthorityBrokerServer(t, runner)
	defer stop()
	if _, err := client.Execute(
		t.Context(),
		validAuthorityBrokerRequest(),
	); !errors.Is(err, ErrShellBrokerOutcomeUnknown) {
		t.Fatalf("Execute() error = %v", err)
	}
}

func TestAuthorityBrokerUnixRejectsSecondSameCredentialProcess(t *testing.T) {
	runner := &fakeAuthorityBrokerRunner{}
	client, stop := startUnclaimedTestAuthorityBrokerServer(t, runner)
	defer stop()
	command := exec.Command(
		os.Args[0],
		"-test.run=^TestAuthorityBrokerDirectPeerHelper$",
	)
	command.Env = append(
		os.Environ(),
		"MINTCLAW_AUTHORITY_BROKER_DIRECT_PEER_TEST=1",
		"MINTCLAW_AUTHORITY_BROKER_DIRECT_PEER_SOCKET="+
			client.socketPath,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("direct peer helper: %v\n%s", err, output)
	}
	if _, err := client.Snapshot(t.Context()); err != nil {
		t.Fatal(err)
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if len(runner.requests) != 0 {
		t.Fatalf("direct same-credential process reached runner: %d requests", len(runner.requests))
	}
}

func TestAuthorityBrokerDirectPeerHelper(t *testing.T) {
	if os.Getenv("MINTCLAW_AUTHORITY_BROKER_DIRECT_PEER_TEST") != "1" {
		return
	}
	client, err := newAuthorityBrokerClient(
		os.Getenv("MINTCLAW_AUTHORITY_BROKER_DIRECT_PEER_SOCKET"),
		uint32(os.Getuid()),
		uint32(os.Getgid()),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Snapshot(t.Context()); err == nil {
		t.Fatal("direct same-credential process claimed the first snapshot")
	}
	if _, err := client.Execute(t.Context(), validAuthorityBrokerRequest()); err == nil {
		t.Fatal("direct same-credential process execution was accepted")
	}
}

func TestAuthorityBrokerUnixBoundsNonReadingPeer(t *testing.T) {
	runner := &fakeAuthorityBrokerRunner{
		started: make(chan struct{}),
		result: ShellBrokerResult{
			Stdout:    strings.Repeat("x", 120*1024),
			StartedAt: 1, CompletedAt: 2,
		},
	}
	client, stop := startTestAuthorityBrokerServer(t, runner)
	defer stop()
	connection, err := net.DialUnix(
		"unix",
		nil,
		&net.UnixAddr{Name: client.socketPath, Net: "unix"},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err := writeAuthorityBrokerFrame(
		connection,
		authorityBrokerRequestFrame{
			Version: AuthorityBrokerProtocolVersion,
			Action:  authorityBrokerActionExecute,
			Execute: pointerTo(validAuthorityBrokerRequest()),
		},
	); err != nil {
		t.Fatal(err)
	}
	<-runner.started
	done := make(chan error, 1)
	go func() {
		_, executeErr := client.Execute(t.Context(), validAuthorityBrokerRequest())
		done <- executeErr
	}()
	select {
	case err := <-done:
		t.Fatalf("second execution bypassed occupied semaphore: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("non-reading peer retained broker capacity")
	}
}

func TestAuthorityBrokerTerminalUnixRoundTripRealPTY(t *testing.T) {
	runner := testAuthorityBrokerProcessRunner(t)
	client, stop := startTestAuthorityBrokerServer(t, runner)
	defer stop()
	request := testAuthorityBrokerTerminalRequest()
	terminal, opened, err := client.OpenTerminal(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer terminal.Close()
	if opened.Type != TerminalEventOpened || terminal.ID() != opened.TerminalID {
		t.Fatalf("opened terminal = (%q, %#v)", terminal.ID(), opened)
	}
	if err := terminal.Send(
		t.Context(),
		terminalInputControl(
			1,
			"echo-off-1",
			"stty -echo; printf '\\145cho-off-ready\\n'\n",
		),
	); err != nil {
		t.Fatal(err)
	}
	waitForAuthorityBrokerTerminalAck(t, terminal, 1)
	waitForAuthorityBrokerTerminalOutput(t, terminal, "echo-off-ready")
	if err := terminal.Send(
		t.Context(),
		terminalInputControl(2, "input-2", "printf 'broker-terminal-marker\\n'\n"),
	); err != nil {
		t.Fatal(err)
	}
	waitForAuthorityBrokerTerminalOutput(t, terminal, "broker-terminal-marker")
	if err := terminal.Send(t.Context(), TerminalBrokerControl{
		Sequence: 3, IdempotencyKey: "close-3", Close: true,
	}); err != nil {
		t.Fatal(err)
	}
	waitForAuthorityBrokerTerminalAck(t, terminal, 3)
	closed := waitForAuthorityBrokerTerminalClosed(t, terminal)
	if closed.Type != TerminalEventClosed ||
		closed.Reason != TerminalCloseRequested ||
		!closed.TerminationConfirmed {
		t.Fatalf("closed event = %#v", closed)
	}
}

func TestAuthorityBrokerTerminalRunnerExitPreservesStartedAt(t *testing.T) {
	runner := &exitingAuthorityBrokerTerminalRunner{startedAt: 1_700_000_001}
	client, stop := startTestAuthorityBrokerServer(t, runner)
	defer stop()
	terminal, opened, err := client.OpenTerminal(t.Context(), testAuthorityBrokerTerminalRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer terminal.Close()
	unknown := receiveAuthorityBrokerTerminalEvent(t, terminal)
	if unknown.Type != TerminalEventUnknown ||
		unknown.StartedAt != opened.StartedAt {
		t.Fatalf("runner-exit event = %#v, opened = %#v", unknown, opened)
	}
	if _, err := (nodes.TerminalEvent{
		Version: nodes.TerminalProtocolVersion, Type: unknown.Type,
		TerminalID: unknown.TerminalID, State: unknown.State,
		Reason: unknown.Reason, StartedAt: unknown.StartedAt,
	}).Validate(); err != nil {
		t.Fatalf("runner-exit transport event is invalid: %v", err)
	}
}

func TestAuthorityBrokerTerminalOpenCancellationReleasesPendingClient(t *testing.T) {
	runner := testAuthorityBrokerProcessRunner(t)
	client, stop := startTestAuthorityBrokerServer(t, runner)
	defer stop()
	first, _, err := client.OpenTerminal(t.Context(), testAuthorityBrokerTerminalRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	if _, _, err := client.OpenTerminal(
		ctx,
		testAuthorityBrokerTerminalRequest(),
	); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pending terminal open error = %v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("pending terminal open ignored cancellation")
	}
	if err := first.Send(t.Context(), TerminalBrokerControl{
		Sequence: 1, IdempotencyKey: "close-1", Close: true,
	}); err != nil {
		t.Fatal(err)
	}
	waitForAuthorityBrokerTerminalAck(t, first, 1)
	waitForAuthorityBrokerTerminalClosed(t, first)
}

func TestAuthorityBrokerTerminalOverflowReleasesProfileCapacity(t *testing.T) {
	runner := testAuthorityBrokerProcessRunner(t)
	client, stop := startTestAuthorityBrokerServer(t, runner)
	defer stop()
	overflowRequest := testAuthorityBrokerTerminalRequest()
	overflowRequest.BufferBytes = 1
	first, _, err := client.OpenTerminal(t.Context(), overflowRequest)
	if err != nil {
		t.Fatal(err)
	}
	closed := waitForAuthorityBrokerTerminalClosed(t, first)
	if closed.Type != TerminalEventClosed ||
		closed.Reason != TerminalCloseOutputOverflow {
		t.Fatalf("overflow outcome = %#v", closed)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	second, _, err := client.OpenTerminal(ctx, testAuthorityBrokerTerminalRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := second.Send(t.Context(), TerminalBrokerControl{
		Sequence: 1, IdempotencyKey: "close-1", Close: true,
	}); err != nil {
		t.Fatal(err)
	}
	waitForAuthorityBrokerTerminalAck(t, second, 1)
	waitForAuthorityBrokerTerminalClosed(t, second)
}

func TestAuthorityBrokerTerminalReceiveHonorsDeadlineFreeCancellation(t *testing.T) {
	connection, peer := testAuthorityBrokerUnixPair(t)
	defer connection.Close()
	defer peer.Close()
	terminal := &AuthorityBrokerTerminal{
		connection: connection, terminalID: "terminal_test", done: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, err := terminal.Receive(ctx)
		done <- err
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Receive() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Receive() ignored deadline-free context cancellation")
	}
}

func TestAuthorityBrokerTerminalReceiveDeadlineClosesInterruptedStream(t *testing.T) {
	connection, peer := testAuthorityBrokerUnixPair(t)
	defer connection.Close()
	defer peer.Close()
	terminal := &AuthorityBrokerTerminal{
		connection: connection, terminalID: "terminal_test", done: make(chan struct{}),
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err := terminal.Receive(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Receive() error = %v", err)
	}
	select {
	case <-terminal.done:
	default:
		t.Fatal("Receive() deadline left interrupted stream open")
	}
}

func TestAuthorityBrokerTerminalSendHonorsBackpressuredCancellation(t *testing.T) {
	connection, peer := testAuthorityBrokerUnixPair(t)
	defer connection.Close()
	defer peer.Close()
	fillAuthorityBrokerUnixWriteBuffer(t, connection)
	terminal := &AuthorityBrokerTerminal{
		connection: connection, terminalID: "terminal_test", done: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- terminal.Send(ctx, terminalInputControl(
			1,
			"input-1",
			strings.Repeat("x", MaxTerminalFrameBytes),
		))
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Send() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Send() ignored backpressured context cancellation")
	}
}

func TestAuthorityBrokerTerminalSendDeadlineClosesInterruptedStream(t *testing.T) {
	connection, peer := testAuthorityBrokerUnixPair(t)
	defer connection.Close()
	defer peer.Close()
	fillAuthorityBrokerUnixWriteBuffer(t, connection)
	terminal := &AuthorityBrokerTerminal{
		connection: connection, terminalID: "terminal_test", done: make(chan struct{}),
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err := terminal.Send(ctx, terminalInputControl(
		1,
		"input-1",
		strings.Repeat("x", MaxTerminalFrameBytes),
	))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Send() error = %v", err)
	}
	select {
	case <-terminal.done:
	default:
		t.Fatal("Send() deadline left interrupted stream open")
	}
}

func TestRuntimeShellExecThroughUnixBrokerRealProcess(t *testing.T) {
	runner := testAuthorityBrokerProcessRunner(t)
	client, stop := startTestAuthorityBrokerServer(t, runner)
	defer stop()
	runtime := newRuntimeWithAuthorityBrokerClient(t, client)
	input := json.RawMessage(
		`{"profile":"owner-root","script":"printf alpha | sed s/alpha/beta/; printf failure >&2; exit 7","cwd":"workspace","env":{"LANG":"C"},"timeout_seconds":5}`,
	)
	plan := testRuntimePlan(t, runtime, "shell.exec.v1", input)
	raw, err := runtime.Invoke(t.Context(), plan)
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		ExitCode    int     `json:"exit_code"`
		Stdout      string  `json:"stdout"`
		Stderr      string  `json:"stderr"`
		StartedAt   float64 `json:"started_at"`
		CompletedAt float64 `json:"completed_at"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	if result.ExitCode != 7 ||
		result.Stdout != "beta" ||
		result.Stderr != "failure" ||
		result.StartedAt <= 0 ||
		result.CompletedAt < result.StartedAt {
		t.Fatalf("real broker result = %#v", result)
	}
}

func TestRuntimeShellExecCancellationThroughUnixBroker(t *testing.T) {
	runner := testAuthorityBrokerProcessRunner(t)
	client, stop := startTestAuthorityBrokerServer(t, runner)
	defer stop()
	runtime := newRuntimeWithAuthorityBrokerClient(t, client)
	readyFile := t.TempDir() + "/ready"
	input, err := json.Marshal(map[string]any{
		"profile": "owner-root",
		"script": fmt.Sprintf(
			`printf ready > %q; trap "" TERM; while :; do sleep 1; done`,
			readyFile,
		),
		"cwd": "workspace", "env": map[string]any{"LANG": "C"},
		"timeout_seconds": 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := testRuntimePlan(t, runtime, "shell.exec.v1", input)
	invokeDone := make(chan error, 1)
	go func() {
		_, invokeErr := runtime.Invoke(t.Context(), plan)
		invokeDone <- invokeErr
	}()
	waitForAuthorityBrokerFileOrError(t, readyFile, invokeDone)
	record, err := runtime.Cancel(nodes.InvocationCancelRequest{InvocationID: plan.InvocationID})
	if err != nil {
		t.Fatal(err)
	}
	if record.Cancellation == nil || record.Cancellation.TerminationConfirmed {
		t.Fatalf("initial cancellation = %#v", record)
	}
	if err := <-invokeDone; !errors.Is(err, ErrInvocationCanceled) {
		t.Fatalf("Invoke() cancellation = %v", err)
	}
	record, found, err := runtime.Invocation(plan.InvocationID)
	if err != nil || !found ||
		record.State != nodes.InvocationCanceled ||
		record.Cancellation == nil ||
		!record.Cancellation.TerminationConfirmed {
		t.Fatalf("durable cancellation = (%#v, %v, %v)", record, found, err)
	}
}

func waitForAuthorityBrokerFileOrError(
	t *testing.T,
	path string,
	invokeDone <-chan error,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		select {
		case err := <-invokeDone:
			t.Fatalf("invocation ended before process barrier: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("authority broker process barrier was not reached")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func receiveAuthorityBrokerTerminalEvent(
	t *testing.T,
	terminal *AuthorityBrokerTerminal,
) TerminalBrokerEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	event, err := terminal.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func waitForAuthorityBrokerTerminalAck(
	t *testing.T,
	terminal *AuthorityBrokerTerminal,
	sequence uint64,
) {
	t.Helper()
	for {
		event := receiveAuthorityBrokerTerminalEvent(t, terminal)
		if event.Type == TerminalEventAck && event.AcceptedSequence == sequence {
			return
		}
	}
}

func waitForAuthorityBrokerTerminalOutput(
	t *testing.T,
	terminal *AuthorityBrokerTerminal,
	marker string,
) {
	t.Helper()
	var output strings.Builder
	for {
		event := receiveAuthorityBrokerTerminalEvent(t, terminal)
		if event.Type != TerminalEventOutput {
			continue
		}
		data, err := base64.StdEncoding.DecodeString(event.DataBase64)
		if err != nil {
			t.Fatal(err)
		}
		output.Write(data)
		if strings.Contains(output.String(), marker) {
			return
		}
	}
}

func waitForAuthorityBrokerTerminalClosed(
	t *testing.T,
	terminal *AuthorityBrokerTerminal,
) TerminalBrokerEvent {
	t.Helper()
	for {
		event := receiveAuthorityBrokerTerminalEvent(t, terminal)
		if event.Type == TerminalEventClosed || event.Type == TerminalEventUnknown {
			return event
		}
	}
}

func testAuthorityBrokerUnixPair(t *testing.T) (*net.UnixConn, *net.UnixConn) {
	t.Helper()
	socketPath := t.TempDir() + "/terminal.sock"
	listener, err := net.ListenUnix(
		"unix",
		&net.UnixAddr{Name: socketPath, Net: "unix"},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan *net.UnixConn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		connection, err := listener.AcceptUnix()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- connection
	}()
	connection, err := net.DialUnix(
		"unix",
		nil,
		&net.UnixAddr{Name: socketPath, Net: "unix"},
	)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case peer := <-accepted:
		return connection, peer
	case err := <-acceptErr:
		connection.Close()
		t.Fatal(err)
		return nil, nil
	}
}

func fillAuthorityBrokerUnixWriteBuffer(t *testing.T, connection *net.UnixConn) {
	t.Helper()
	if err := connection.SetWriteBuffer(1024); err != nil {
		t.Fatal(err)
	}
	raw, err := connection.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var fillErr error
	if err := raw.Write(func(descriptor uintptr) bool {
		data := make([]byte, 4096)
		for {
			_, sendErr := unix.SendmsgN(
				int(descriptor),
				data,
				nil,
				nil,
				unix.MSG_DONTWAIT,
			)
			if errors.Is(sendErr, unix.EAGAIN) {
				return true
			}
			if sendErr != nil {
				fillErr = sendErr
				return true
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	if fillErr != nil {
		t.Fatal(fillErr)
	}
}

func startTestAuthorityBrokerServer(
	t *testing.T,
	runner authorityBrokerExecutionRunner,
) (*AuthorityBrokerClient, func()) {
	t.Helper()
	client, stop := startUnclaimedTestAuthorityBrokerServer(t, runner)
	if _, err := client.Snapshot(t.Context()); err != nil {
		stop()
		t.Fatal(err)
	}
	return client, stop
}

func startUnclaimedTestAuthorityBrokerServer(
	t *testing.T,
	runner authorityBrokerExecutionRunner,
) (*AuthorityBrokerClient, func()) {
	t.Helper()
	config := validAuthorityBrokerConfig(t)
	config.AllowedUID = uint32(os.Getuid())
	config.AllowedGID = uint32(os.Getgid())
	profile := config.normalizedProfile["owner-root"]
	profile.UID = uint32(os.Getuid())
	profile.GID = uint32(os.Getgid())
	profile.SupplementaryGroups = nil
	profile.ShellPath = "/bin/sh"
	config.normalizedProfile["owner-root"] = profile
	identity, err := newAuthorityBrokerPIDIdentity(int32(os.Getpid()))
	if err != nil {
		t.Fatal(err)
	}
	server, err := newAuthorityBrokerServer(config, runner, identity)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.ListenUnix(
		"unix",
		&net.UnixAddr{Name: config.SocketPath, Net: "unix"},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	client, err := newAuthorityBrokerClient(
		config.SocketPath,
		uint32(os.Getuid()),
		uint32(os.Getgid()),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client, func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Serve() error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("authority broker server did not stop")
		}
	}
}

func pointerTo[T any](value T) *T {
	return &value
}

func newRuntimeWithAuthorityBrokerClient(
	t *testing.T,
	client *AuthorityBrokerClient,
) *Runtime {
	t.Helper()
	snapshot, err := client.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	policy := testRuntimePolicy([]string{"shell.exec.v1"})
	policy.MaximumRisk = nodes.RiskPrivileged
	policy.MaxTimeoutSeconds = 30
	policy.MaxOutputBytes = 8192
	runtime, err := NewRuntime(
		nodes.ID("node_test"),
		"test",
		policy,
		newMemoryInvocationLedger(),
		WithShellBroker(snapshot, client),
	)
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func validAuthorityBrokerRequest() ShellBrokerRequest {
	return ShellBrokerRequest{
		InvocationID: "inv_test",
		PlanHash:     strings.Repeat("a", 64),
		Profile:      "owner-root", ProfileRevision: "profile-v1",
		Script: "true", WorkingScope: "workspace",
		Environment: map[string]string{}, TimeoutSeconds: 5, OutputBytesMax: 4096,
	}
}
