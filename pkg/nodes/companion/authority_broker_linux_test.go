//go:build linux

package companion

import (
	"context"
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
	client, stop := startTestAuthorityBrokerServer(t, runner)
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

func startTestAuthorityBrokerServer(
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
	server, err := newAuthorityBrokerServer(config, runner)
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
	if _, err := client.Snapshot(t.Context()); err != nil {
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
