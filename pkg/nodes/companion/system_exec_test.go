package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/nodes"
)

const (
	systemExecHelperEnabled = "PICOCLAW_SYSTEM_EXEC_HELPER"
	systemExecHelperAction  = "PICOCLAW_SYSTEM_EXEC_ACTION"
	systemExecHelperMarker  = "PICOCLAW_SYSTEM_EXEC_MARKER"
	systemExecVisibleEnv    = "PICOCLAW_SYSTEM_EXEC_VISIBLE"
	systemExecHiddenEnv     = "PICOCLAW_SYSTEM_EXEC_HIDDEN"
)

var systemExecPlanSequence atomic.Uint64

func TestSystemExecHelperProcess(t *testing.T) {
	if os.Getenv(systemExecHelperEnabled) != "1" {
		return
	}
	switch os.Getenv(systemExecHelperAction) {
	case "environment":
		_, _ = fmt.Fprintf(
			os.Stdout,
			"visible=%s hidden=%s",
			os.Getenv(systemExecVisibleEnv),
			os.Getenv(systemExecHiddenEnv),
		)
		_, _ = os.Stderr.WriteString("helper-stderr")
	case "exit":
		os.Exit(7)
	case "large":
		_, _ = os.Stdout.WriteString(strings.Repeat("x", 16*1024))
	case "sleep":
		if marker := os.Getenv(systemExecHelperMarker); marker != "" {
			_ = os.WriteFile(marker, []byte("started"), 0o600)
		}
		time.Sleep(30 * time.Second)
	default:
		os.Exit(64)
	}
}

func TestSystemExecIsOptIn(t *testing.T) {
	runtime, err := NewRuntime(
		nodes.ID("node_test"),
		"test",
		testRuntimePolicy([]string{"node.info.v1"}),
		newMemoryInvocationLedger(),
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, descriptor := range runtime.Catalog().Commands {
		if descriptor.Name == "system.exec.v1" {
			t.Fatal("system.exec.v1 advertised without explicit configuration")
		}
	}
}

func TestSystemExecCapturesEnvironmentAndNonzeroExit(t *testing.T) {
	t.Setenv(systemExecVisibleEnv, "parent")
	t.Setenv(systemExecHiddenEnv, "secret")
	runtime, _, root, executable := newSystemExecRuntime(t)

	result := invokeSystemExec(t, runtime, systemExecInput{
		Argv: []string{executable, "-test.run=^TestSystemExecHelperProcess$"},
		CWD:  root,
		Env: map[string]string{
			systemExecHelperEnabled: "1",
			systemExecHelperAction:  "environment",
			systemExecVisibleEnv:    "override",
		},
		TimeoutSeconds: 2,
	}, 3, 4096)
	if result.ExitCode != 0 || !strings.HasPrefix(result.Stdout, "visible=override hidden=") ||
		result.Stderr != "helper-stderr" || result.Truncated {
		t.Fatalf("environment result = %+v", result)
	}

	result = invokeSystemExec(t, runtime, systemExecInput{
		Argv: []string{executable, "-test.run=^TestSystemExecHelperProcess$"},
		CWD:  root,
		Env: map[string]string{
			systemExecHelperEnabled: "1",
			systemExecHelperAction:  "exit",
		},
		TimeoutSeconds: 2,
	}, 3, 4096)
	if result.ExitCode != 7 {
		t.Fatalf("nonzero exit result = %+v", result)
	}
}

func TestSystemExecPolicyDeniesBeforeAccept(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(systemExecInput, string) systemExecInput
	}{
		{
			name: "executable",
			mutate: func(input systemExecInput, root string) systemExecInput {
				input.Argv[0] = filepath.Join(root, "not-allowed")
				return input
			},
		},
		{
			name: "environment",
			mutate: func(input systemExecInput, _ string) systemExecInput {
				input.Env[systemExecHiddenEnv] = "secret"
				return input
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, ledger, root, executable := newSystemExecRuntime(t)
			input := test.mutate(systemExecInput{
				Argv:           []string{executable, "-test.run=^TestSystemExecHelperProcess$"},
				CWD:            root,
				Env:            map[string]string{},
				TimeoutSeconds: 1,
			}, root)
			plan := prepareSystemExecPlan(t, runtime, input, 2, 4096)
			if _, err := runtime.Invoke(t.Context(), plan); !errors.Is(err, nodes.ErrCommandDenied) {
				t.Fatalf("Invoke() error = %v", err)
			}
			if record, found := ledger.Get(plan.InvocationID); found {
				t.Fatalf("denied invocation was durably accepted: %+v", record)
			}
		})
	}
}

func TestSystemExecRejectsWorkingDirectorySymlinkEscape(t *testing.T) {
	runtime, ledger, root, executable := newSystemExecRuntime(t)
	outside := t.TempDir()
	link := filepath.Join(root, "outside")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	input := systemExecInput{
		Argv:           []string{executable, "-test.run=^TestSystemExecHelperProcess$"},
		CWD:            link,
		Env:            map[string]string{},
		TimeoutSeconds: 1,
	}
	plan := prepareSystemExecPlan(t, runtime, input, 2, 4096)
	if _, err := runtime.Invoke(t.Context(), plan); !errors.Is(err, nodes.ErrCommandDenied) {
		t.Fatalf("Invoke() error = %v", err)
	}
	if _, found := ledger.Get(plan.InvocationID); found {
		t.Fatal("symlink escape was accepted")
	}
}

func TestSystemExecBoundsOutput(t *testing.T) {
	runtime, _, root, executable := newSystemExecRuntime(t)
	plan := prepareSystemExecPlan(t, runtime, systemExecInput{
		Argv: []string{executable, "-test.run=^TestSystemExecHelperProcess$"},
		CWD:  root,
		Env: map[string]string{
			systemExecHelperEnabled: "1",
			systemExecHelperAction:  "large",
		},
		TimeoutSeconds: 2,
	}, 3, 256)
	raw, err := runtime.Invoke(t.Context(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > plan.OutputLimitBytes {
		t.Fatalf("output length = %d, limit = %d", len(raw), plan.OutputLimitBytes)
	}
	var result systemExecOutput
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	if !result.Truncated || len(result.Stdout) >= 16*1024 {
		t.Fatalf("bounded result = %+v", result)
	}
}

func TestSystemExecTimeoutIsDurableFailure(t *testing.T) {
	runtime, ledger, root, executable := newSystemExecRuntime(t)
	plan := prepareSystemExecPlan(t, runtime, systemExecInput{
		Argv: []string{executable, "-test.run=^TestSystemExecHelperProcess$"},
		CWD:  root,
		Env: map[string]string{
			systemExecHelperEnabled: "1",
			systemExecHelperAction:  "sleep",
		},
		TimeoutSeconds: 1,
	}, 2, 4096)
	if _, err := runtime.Invoke(t.Context(), plan); err == nil {
		t.Fatal("timed out invocation succeeded")
	}
	record, found := ledger.Get(plan.InvocationID)
	if !found || record.State != nodes.InvocationFailed || record.Failure == nil ||
		record.Failure.Code != "TIMEOUT" {
		t.Fatalf("timeout record = %+v, found = %v", record, found)
	}
}

func TestSystemExecCancellationConfirmsTermination(t *testing.T) {
	runtime, ledger, root, executable := newSystemExecRuntime(t)
	marker := filepath.Join(root, "started")
	plan := prepareSystemExecPlan(t, runtime, systemExecInput{
		Argv: []string{executable, "-test.run=^TestSystemExecHelperProcess$"},
		CWD:  root,
		Env: map[string]string{
			systemExecHelperEnabled: "1",
			systemExecHelperAction:  "sleep",
			systemExecHelperMarker:  marker,
		},
		TimeoutSeconds: 10,
	}, 15, 4096)
	result := make(chan error, 1)
	go func() {
		_, err := runtime.Invoke(context.Background(), plan)
		result <- err
	}()
	waitForSystemExecMarker(t, marker, result)
	if _, err := runtime.Cancel(nodes.InvocationCancelRequest{
		InvocationID: plan.InvocationID,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrInvocationCanceled) {
			t.Fatalf("Invoke() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled process did not terminate")
	}
	record, found := ledger.Get(plan.InvocationID)
	if !found || record.State != nodes.InvocationCanceled || record.Cancellation == nil ||
		!record.Cancellation.TerminationConfirmed {
		t.Fatalf("cancellation record = %+v, found = %v", record, found)
	}
}

func newSystemExecRuntime(
	t *testing.T,
) (*Runtime, *InvocationLedger, string, string) {
	t.Helper()
	root := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	policy, err := normalizeSystemExecPolicy(SystemExecPolicy{
		WorkingRoots: []string{root},
		Executables:  []string{executable},
		Environment: []string{
			systemExecHelperEnabled,
			systemExecHelperAction,
			systemExecHelperMarker,
			systemExecVisibleEnv,
		},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	ledger := newMemoryInvocationLedger()
	localPolicy := testRuntimePolicy([]string{"system.exec.v1"})
	localPolicy.MaximumRisk = nodes.RiskWrite
	localPolicy.MaxTimeoutSeconds = 30
	runtime, err := NewRuntime(
		nodes.ID("node_test"),
		"test",
		localPolicy,
		ledger,
		WithSystemExec(policy),
	)
	if err != nil {
		t.Fatal(err)
	}
	return runtime, ledger, root, policy.Executables[0]
}

func invokeSystemExec(
	t *testing.T,
	runtime *Runtime,
	input systemExecInput,
	planTimeout int,
	outputLimit int,
) systemExecOutput {
	t.Helper()
	raw, err := runtime.Invoke(
		t.Context(),
		prepareSystemExecPlan(t, runtime, input, planTimeout, outputLimit),
	)
	if err != nil {
		t.Fatal(err)
	}
	var result systemExecOutput
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func prepareSystemExecPlan(
	t *testing.T,
	runtime *Runtime,
	input systemExecInput,
	timeoutSeconds int,
	outputLimit int,
) nodes.ExecutionPlan {
	t.Helper()
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	catalog := runtime.Catalog()
	catalogHash, err := catalog.Hash()
	if err != nil {
		t.Fatal(err)
	}
	var descriptor nodes.CommandDescriptor
	for _, candidate := range catalog.Commands {
		if candidate.Name == "system.exec.v1" {
			descriptor = candidate
			break
		}
	}
	if descriptor.Name == "" {
		t.Fatal("system.exec.v1 missing from catalog")
	}
	sequence := systemExecPlanSequence.Add(1)
	plan, err := nodes.PrepareExecutionPlan(nodes.InvocationRequest{
		InvocationID:     fmt.Sprintf("inv_system_exec_%d", sequence),
		IdempotencyKey:   fmt.Sprintf("idem_system_exec_%d", sequence),
		NodeID:           runtime.nodeID,
		CatalogHash:      catalogHash,
		Command:          descriptor.Name,
		Input:            raw,
		AgentID:          "agent_test",
		SessionID:        "session_test",
		ActorID:          "actor_test",
		TimeoutSeconds:   timeoutSeconds,
		OutputLimitBytes: outputLimit,
	}, descriptor, LocalExecutor, runtime.policy.Revision, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func waitForSystemExecMarker(t *testing.T, marker string, result <-chan error) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(marker); err == nil {
			return
		}
		select {
		case err := <-result:
			t.Fatalf("system.exec helper exited before marker: %v", err)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("system.exec helper did not start")
}
