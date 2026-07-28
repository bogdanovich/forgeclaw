//go:build linux

package companion

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestAuthorityBrokerTerminalWorkerInteractiveLifecycle(t *testing.T) {
	runner := testAuthorityBrokerProcessRunner(t)
	controls := make(chan TerminalBrokerControl)
	events := make(chan TerminalBrokerEvent)
	done := make(chan error, 1)
	go func() {
		done <- runner.Terminal(
			t.Context(),
			testPreparedAuthorityBrokerTerminal(t),
			testAuthorityBrokerTerminalRequest(),
			"terminal_test",
			controls,
			events,
		)
	}()
	opened := receiveTerminalWorkerEvent(t, events)
	if opened.Type != TerminalEventOpened || opened.TerminalID != "terminal_test" {
		t.Fatalf("opened event = %#v", opened)
	}

	controls <- terminalInputControl(
		1,
		"echo-off-1",
		"stty -echo; printf '\\145cho-off-ready\\n'\n",
	)
	waitForTerminalAck(t, events, 1)
	waitForTerminalOutput(t, events, "echo-off-ready")
	drainTerminalEvents(events)

	controls <- terminalInputControl(2, "input-2", "printf 'terminal-marker\\n'\n")
	waitForTerminalOutput(t, events, "terminal-marker")

	controls <- terminalInputControl(2, "input-2", "printf 'terminal-marker\\n'\n")
	waitForTerminalAck(t, events, 2)
	assertNoTerminalOutput(t, events, "terminal-marker")

	controls <- terminalInputControl(2, "input-2", "printf 'altered-marker\\n'\n")
	waitForTerminalDenied(t, events, "invalid_sequence")
	assertNoTerminalOutput(t, events, "altered-marker")

	controls <- TerminalBrokerControl{
		Version: AuthorityBrokerProtocolVersion, Sequence: 4,
		IdempotencyKey: "gap-4", Close: true,
	}
	denied := receiveTerminalWorkerEvent(t, events)
	if denied.Type != TerminalEventDenied || denied.Reason != "invalid_sequence" {
		t.Fatalf("gap response = %#v", denied)
	}

	controls <- TerminalBrokerControl{
		Version: AuthorityBrokerProtocolVersion, Sequence: 3,
		IdempotencyKey: "resize-3",
		Resize:         &TerminalSize{Columns: 100, Rows: 40},
	}
	waitForTerminalAck(t, events, 3)
	controls <- terminalInputControl(4, "input-4", "stty size\n")
	waitForTerminalOutput(t, events, "40 100")

	controls <- TerminalBrokerControl{
		Version: AuthorityBrokerProtocolVersion, Sequence: 5,
		IdempotencyKey: "close-5", Close: true,
	}
	waitForTerminalAck(t, events, 5)
	closed := waitForTerminalClosed(t, events)
	if closed.Type != TerminalEventClosed ||
		closed.Reason != TerminalCloseRequested ||
		!closed.TerminationConfirmed {
		t.Fatalf("closed event = %#v", closed)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestAuthorityBrokerTerminalWorkerBoundsOutputAndCompletesOverflow(t *testing.T) {
	runner := testAuthorityBrokerProcessRunner(t)
	request := testAuthorityBrokerTerminalRequest()
	request.BufferBytes = 1
	controls := make(chan TerminalBrokerControl)
	events := make(chan TerminalBrokerEvent)
	done := make(chan error, 1)
	go func() {
		done <- runner.Terminal(
			t.Context(),
			testPreparedAuthorityBrokerTerminal(t),
			request,
			"terminal_overflow",
			controls,
			events,
		)
	}()
	if opened := receiveTerminalWorkerEvent(t, events); opened.Type != TerminalEventOpened {
		t.Fatalf("opened event = %#v", opened)
	}
	closed := waitForTerminalClosed(t, events)
	if closed.Type != TerminalEventClosed ||
		closed.Reason != TerminalCloseOutputOverflow ||
		!closed.TerminationConfirmed {
		t.Fatalf("overflow outcome = %#v", closed)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestAuthorityBrokerTerminalWorkerCancellationDrainsBlockedOutput(t *testing.T) {
	runner := testAuthorityBrokerProcessRunner(t)
	controls := make(chan TerminalBrokerControl)
	events := make(chan TerminalBrokerEvent, 1)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- runner.Terminal(
			ctx,
			testPreparedAuthorityBrokerTerminal(t),
			testAuthorityBrokerTerminalRequest(),
			"terminal_blocked_output",
			controls,
			events,
		)
	}()
	if opened := receiveTerminalWorkerEvent(t, events); opened.Type != TerminalEventOpened {
		t.Fatalf("opened event = %#v", opened)
	}
	controls <- terminalInputControl(1, "input-1", "yes terminal-output\n")
	waitForTerminalEventBuffer(t, events)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("terminal worker cancellation blocked behind output")
	}
}

func TestTerminalPTYBufferAccountsBytesAndSignalsOverflowCompletion(t *testing.T) {
	buffer := newTerminalPTYBuffer(4)
	if !buffer.push(terminalPTYRead{data: []byte("abc")}) {
		t.Fatal("in-bounds output was rejected")
	}
	if buffer.push(terminalPTYRead{data: []byte("de")}) {
		t.Fatal("byte-overflowing output was accepted")
	}
	frame, ok, overflow, done := buffer.pop()
	if !ok || string(frame.data) != "abc" || !overflow || !done {
		t.Fatalf("buffer outcome = (%#v, %v, %v, %v)", frame, ok, overflow, done)
	}
}

func TestAuthorityBrokerTerminalWorkerReapsSetsidDescendantOnClose(t *testing.T) {
	if _, err := os.Stat("/usr/bin/setsid"); err != nil {
		t.Skip("setsid unavailable")
	}
	pidFile := t.TempDir() + "/descendant.pid"
	runner := testAuthorityBrokerProcessRunner(t)
	controls := make(chan TerminalBrokerControl)
	events := make(chan TerminalBrokerEvent)
	done := make(chan error, 1)
	go func() {
		done <- runner.Terminal(
			t.Context(),
			testPreparedAuthorityBrokerTerminal(t),
			testAuthorityBrokerTerminalRequest(),
			"terminal_descendant",
			controls,
			events,
		)
	}()
	if opened := receiveTerminalWorkerEvent(t, events); opened.Type != TerminalEventOpened {
		t.Fatalf("opened event = %#v", opened)
	}
	command := "/usr/bin/setsid /bin/sh -c 'echo $$ > " + pidFile +
		"; trap \"\" TERM; while :; do sleep 1; done' &\n"
	controls <- terminalInputControl(1, "input-1", command)
	waitForTerminalAck(t, events, 1)
	waitForAuthorityBrokerFile(t, pidFile)
	controls <- TerminalBrokerControl{
		Version: AuthorityBrokerProtocolVersion, Sequence: 2,
		IdempotencyKey: "close-2", Close: true,
	}
	waitForTerminalAck(t, events, 2)
	waitForTerminalClosed(t, events)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	rawPID, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(rawPID)))
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Kill(pid, 0); !errors.Is(err, unix.ESRCH) {
		t.Fatalf("setsid terminal descendant %d remains alive: %v", pid, err)
	}
}

func TestAuthorityBrokerTerminalWorkerConfirmsDisconnectCleanup(t *testing.T) {
	runner := testAuthorityBrokerProcessRunner(t)
	controls := make(chan TerminalBrokerControl)
	events := make(chan TerminalBrokerEvent)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- runner.Terminal(
			ctx,
			testPreparedAuthorityBrokerTerminal(t),
			testAuthorityBrokerTerminalRequest(),
			"terminal_disconnect",
			controls,
			events,
		)
	}()
	if opened := receiveTerminalWorkerEvent(t, events); opened.Type != TerminalEventOpened {
		t.Fatalf("opened event = %#v", opened)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("terminal worker did not complete disconnect cleanup")
	}
}

func testPreparedAuthorityBrokerTerminal(t *testing.T) preparedAuthorityBrokerTerminal {
	t.Helper()
	return preparedAuthorityBrokerTerminal{
		profile: normalizedAuthorityBrokerProfile{
			AuthorityBrokerProfile: AuthorityBrokerProfile{
				UID: uint32(os.Getuid()), GID: uint32(os.Getgid()),
			},
		},
		shellPath:        "/bin/sh",
		shellArguments:   []string{"-i"},
		workingDirectory: t.TempDir(),
		environment:      []string{"PATH=/usr/bin:/bin", "TERM=xterm"},
	}
}

func testAuthorityBrokerTerminalRequest() TerminalBrokerOpenRequest {
	return TerminalBrokerOpenRequest{
		OpenID: "open_test", PlanHash: strings.Repeat("a", 64),
		Profile: "owner-root", ProfileRevision: "profile-v1",
		WorkingScope: "workspace", Environment: map[string]string{},
		Columns: DefaultTerminalColumns, Rows: DefaultTerminalRows,
		IdleSeconds: 30, LifetimeSeconds: 60,
		BufferBytes: DefaultTerminalBufferBytes,
	}
}

func terminalInputControl(sequence uint64, key, input string) TerminalBrokerControl {
	return TerminalBrokerControl{
		Version: AuthorityBrokerProtocolVersion, Sequence: sequence,
		IdempotencyKey: key,
		InputBase64:    base64.StdEncoding.EncodeToString([]byte(input)),
	}
}

func receiveTerminalWorkerEvent(
	t *testing.T,
	events <-chan TerminalBrokerEvent,
) TerminalBrokerEvent {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(5 * time.Second):
		t.Fatal("terminal worker event timed out")
		return TerminalBrokerEvent{}
	}
}

func waitForTerminalAck(
	t *testing.T,
	events <-chan TerminalBrokerEvent,
	sequence uint64,
) {
	t.Helper()
	for {
		event := receiveTerminalWorkerEvent(t, events)
		if event.Type == TerminalEventAck && event.AcceptedSequence == sequence {
			return
		}
	}
}

func waitForTerminalClosed(
	t *testing.T,
	events <-chan TerminalBrokerEvent,
) TerminalBrokerEvent {
	t.Helper()
	for {
		event := receiveTerminalWorkerEvent(t, events)
		if event.Type == TerminalEventClosed || event.Type == TerminalEventUnknown {
			return event
		}
	}
}

func waitForTerminalDenied(
	t *testing.T,
	events <-chan TerminalBrokerEvent,
	reason string,
) {
	t.Helper()
	for {
		event := receiveTerminalWorkerEvent(t, events)
		if event.Type == TerminalEventDenied && event.Reason == reason {
			return
		}
	}
}

func waitForTerminalOutput(
	t *testing.T,
	events <-chan TerminalBrokerEvent,
	marker string,
) {
	t.Helper()
	var output strings.Builder
	for {
		event := receiveTerminalWorkerEvent(t, events)
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

func assertNoTerminalOutput(
	t *testing.T,
	events <-chan TerminalBrokerEvent,
	marker string,
) {
	t.Helper()
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case event := <-events:
			if event.Type != TerminalEventOutput {
				continue
			}
			data, err := base64.StdEncoding.DecodeString(event.DataBase64)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(data), marker) {
				t.Fatal("duplicate input produced duplicate terminal output")
			}
		case <-timer.C:
			return
		}
	}
}

func drainTerminalEvents(events <-chan TerminalBrokerEvent) {
	for {
		select {
		case <-events:
		default:
			return
		}
	}
}

func waitForTerminalEventBuffer(
	t *testing.T,
	events <-chan TerminalBrokerEvent,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for len(events) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("terminal event buffer was not filled")
		}
		time.Sleep(time.Millisecond)
	}
}
