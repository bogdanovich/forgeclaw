package companion

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

type fakeShellBroker struct {
	mu       sync.Mutex
	requests []ShellBrokerRequest
	started  chan struct{}
	block    bool
	result   ShellBrokerResult
	err      error
}

func (broker *fakeShellBroker) Execute(
	ctx context.Context,
	request ShellBrokerRequest,
) (ShellBrokerResult, error) {
	broker.mu.Lock()
	broker.requests = append(broker.requests, request)
	started := broker.started
	block := broker.block
	result := broker.result
	err := broker.err
	broker.mu.Unlock()
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
	return result, err
}

func (broker *fakeShellBroker) calls() []ShellBrokerRequest {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	return append([]ShellBrokerRequest(nil), broker.requests...)
}

func TestRuntimeRegistersShellOnlyWithBrokerProjection(t *testing.T) {
	broker := successfulFakeShellBroker()
	runtime := newShellRuntime(t, broker)
	descriptor := shellRuntimeDescriptor(t, runtime)
	contract := descriptor.ModelContract
	if contract == nil ||
		contract.Availability != nodes.ModelAvailable ||
		contract.ApprovalMode != "each_command" ||
		!slices.Equal(contract.Constraints.ProfileAliases, []string{"owner"}) ||
		!slices.Equal(contract.Constraints.WorkingScopes, []string{"workspace"}) ||
		!slices.Equal(contract.Constraints.EnvironmentNames, []string{"LANG"}) ||
		!descriptor.SupportsCancel {
		t.Fatalf("shell descriptor = %#v", descriptor)
	}
	encoded, err := json.Marshal(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	for _, hidden := range []string{"/bin/sh", `"uid"`, "broker_socket", "/private/root"} {
		if strings.Contains(string(encoded), hidden) {
			t.Fatalf("shell descriptor leaked %q: %s", hidden, encoded)
		}
	}
}

func TestShellExecBindsBrokerRequestToPreparedAuthority(t *testing.T) {
	broker := successfulFakeShellBroker()
	runtime := newShellRuntime(t, broker)
	plan := testRuntimePlan(t, runtime, "shell.exec.v1", validShellInput())
	result, err := runtime.Invoke(t.Context(), plan)
	if err != nil {
		t.Fatal(err)
	}
	var output ShellBrokerResult
	if err := json.Unmarshal(result, &output); err != nil {
		t.Fatal(err)
	}
	if output.ExitCode != 7 || output.Stdout != "out" || output.Stderr != "err" {
		t.Fatalf("shell result = %#v", output)
	}
	requests := broker.calls()
	if len(requests) != 1 {
		t.Fatalf("broker calls = %d", len(requests))
	}
	request := requests[0]
	if request.InvocationID != plan.InvocationID ||
		request.PlanHash != plan.PlanHash ||
		request.Profile != "owner" ||
		request.ProfileRevision != "profile-v1" ||
		request.Script != "printf out; printf err >&2; exit 7" ||
		request.WorkingScope != "workspace" ||
		request.Environment["LANG"] != "C" ||
		request.TimeoutSeconds != 5 ||
		request.OutputBytesMax != plan.OutputLimitBytes {
		t.Fatalf("broker request = %#v", request)
	}
}

func TestShellExecRejectsAlteredAuthorityBeforeBroker(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{
			name: "profile",
			mutate: func(input map[string]any) {
				input["profile"] = "root"
			},
		},
		{
			name: "working scope",
			mutate: func(input map[string]any) {
				input["cwd"] = "/private/root"
			},
		},
		{
			name: "environment",
			mutate: func(input map[string]any) {
				input["env"] = map[string]any{"SECRET": "value"}
			},
		},
		{
			name: "raw uid",
			mutate: func(input map[string]any) {
				input["uid"] = 0
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			broker := successfulFakeShellBroker()
			runtime := newShellRuntime(t, broker)
			var input map[string]any
			if err := json.Unmarshal(validShellInput(), &input); err != nil {
				t.Fatal(err)
			}
			test.mutate(input)
			raw, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			descriptor := shellRuntimeDescriptor(t, runtime)
			catalogHash, err := runtime.Catalog().Hash()
			if err != nil {
				t.Fatal(err)
			}
			plan, err := nodes.PrepareExecutionPlan(nodes.InvocationRequest{
				InvocationID: "inv_shell", IdempotencyKey: "idem_shell",
				NodeID: runtime.nodeID, CatalogHash: catalogHash,
				Command: "shell.exec.v1", Input: raw,
				AgentID: "agent_test", SessionID: "session_test", ActorID: "actor_test",
				TimeoutSeconds: 5, OutputLimitBytes: 4096,
			}, descriptor, LocalExecutor, runtime.policy.Revision, time.Now(), time.Minute)
			if err == nil {
				if _, invokeErr := runtime.Invoke(t.Context(), plan); invokeErr == nil {
					t.Fatal("altered shell authority was invoked")
				}
			}
			if len(broker.calls()) != 0 {
				t.Fatal("altered shell authority reached broker")
			}
		})
	}
}

func TestShellExecCancellationRequiresBrokerTerminationProof(t *testing.T) {
	broker := &fakeShellBroker{started: make(chan struct{}), block: true}
	runtime := newShellRuntime(t, broker)
	plan := testRuntimePlan(t, runtime, "shell.exec.v1", validShellInput())
	invokeDone := make(chan error, 1)
	go func() {
		_, err := runtime.Invoke(t.Context(), plan)
		invokeDone <- err
	}()
	<-broker.started
	record, err := runtime.Cancel(nodes.InvocationCancelRequest{InvocationID: plan.InvocationID})
	if err != nil {
		t.Fatal(err)
	}
	if record.State != nodes.InvocationRunning ||
		record.Cancellation == nil ||
		record.Cancellation.TerminationConfirmed {
		t.Fatalf("cancel request = %#v", record)
	}
	if err := <-invokeDone; !errors.Is(err, ErrInvocationCanceled) {
		t.Fatalf("Invoke() error = %v", err)
	}
	record, found, err := runtime.Invocation(plan.InvocationID)
	if err != nil || !found ||
		record.State != nodes.InvocationCanceled ||
		record.Cancellation == nil ||
		!record.Cancellation.TerminationConfirmed {
		t.Fatalf("terminal cancellation = (%#v, %v, %v)", record, found, err)
	}
}

func TestShellBrokerSnapshotFailsClosed(t *testing.T) {
	tests := []ShellBrokerSnapshot{
		{},
		{Revision: "broker-v1"},
		{
			Revision: "broker-v1",
			Profiles: []ShellBrokerProfile{
				{Alias: "owner", Revision: "profile-v1", WorkingScopes: []string{"workspace"}},
				{Alias: "other", Revision: "profile-v1", WorkingScopes: []string{"workspace"}},
			},
		},
	}
	for _, snapshot := range tests {
		if _, err := newShellExecRuntime(snapshot, successfulFakeShellBroker()); err == nil {
			t.Fatalf("invalid snapshot accepted: %#v", snapshot)
		}
	}
	if _, err := newShellExecRuntime(validShellBrokerSnapshot(), nil); err == nil {
		t.Fatal("nil broker accepted")
	}
}

func TestShellBrokerAuthorityDigestBindsSafeProjection(t *testing.T) {
	first := validShellBrokerSnapshot()
	second := validShellBrokerSnapshot()
	second.Profiles[0].OutputBytesMax++
	firstRuntime, err := newShellExecRuntime(first, successfulFakeShellBroker())
	if err != nil {
		t.Fatal(err)
	}
	secondRuntime, err := newShellExecRuntime(second, successfulFakeShellBroker())
	if err != nil {
		t.Fatal(err)
	}
	policy := testRuntimePolicy([]string{"shell.exec.v1"})
	policy.MaximumRisk = nodes.RiskPrivileged
	policy.MaxTimeoutSeconds = 30
	policy.MaxOutputBytes = 8192
	firstContract, err := firstRuntime.handler.modelContract(policy)
	if err != nil {
		t.Fatal(err)
	}
	secondContract, err := secondRuntime.handler.modelContract(policy)
	if err != nil {
		t.Fatal(err)
	}
	if firstContract.AuthorityDigest == secondContract.AuthorityDigest {
		t.Fatal("authority projection change retained the same digest")
	}
}

func successfulFakeShellBroker() *fakeShellBroker {
	return &fakeShellBroker{
		result: ShellBrokerResult{
			ExitCode: 7, Stdout: "out", Stderr: "err",
			StartedAt: 1, CompletedAt: 2,
		},
	}
}

func validShellBrokerSnapshot() ShellBrokerSnapshot {
	return ShellBrokerSnapshot{
		Revision: "broker-v1",
		Profiles: []ShellBrokerProfile{
			{
				Alias: "owner", Revision: "profile-v1",
				WorkingScopes: []string{"workspace"}, EnvironmentNames: []string{"LANG"},
				TimeoutSecondsMax: 30, OutputBytesMax: 8192, ConcurrentCommands: 1,
			},
		},
	}
}

func newShellRuntime(t *testing.T, broker ShellBroker) *Runtime {
	t.Helper()
	policy := testRuntimePolicy([]string{"shell.exec.v1"})
	policy.MaximumRisk = nodes.RiskPrivileged
	policy.MaxTimeoutSeconds = 30
	policy.MaxOutputBytes = 8192
	runtime, err := NewRuntime(
		nodes.ID("node_test"),
		"test",
		policy,
		newMemoryInvocationLedger(),
		WithShellBroker(validShellBrokerSnapshot(), broker),
	)
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func shellRuntimeDescriptor(t *testing.T, runtime *Runtime) nodes.CommandDescriptor {
	t.Helper()
	for _, descriptor := range runtime.Catalog().Commands {
		if descriptor.Name == "shell.exec.v1" {
			return descriptor
		}
	}
	t.Fatal("shell.exec.v1 descriptor is missing")
	return nodes.CommandDescriptor{}
}

func validShellInput() json.RawMessage {
	return json.RawMessage(
		`{"profile":"owner","script":"printf out; printf err >&2; exit 7","cwd":"workspace","env":{"LANG":"C"},"timeout_seconds":5}`,
	)
}
