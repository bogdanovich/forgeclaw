package companion

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

var shellExecPlanSequence atomic.Uint64

func TestOwnerShellIsAbsentAndDisabledByDefault(t *testing.T) {
	for _, ownerShell := range []*OwnerShellPolicy{
		nil,
		{Enabled: false},
	} {
		cfg := Config{
			GatewayURL: "wss://gateway.example",
			StateDir:   t.TempDir(),
			OwnerShell: ownerShell,
		}
		normalized, err := cfg.Normalize("")
		if err != nil {
			t.Fatal(err)
		}
		options := []RuntimeOption{}
		if normalized.OwnerShell != nil && normalized.OwnerShell.Enabled {
			options = append(options, WithOwnerShell(*normalized.OwnerShell))
		}
		runtime, err := NewRuntime(
			nodes.ID("node_test"),
			"test",
			normalized.Policy,
			newMemoryInvocationLedger(),
			options...,
		)
		if err != nil {
			t.Fatal(err)
		}
		for _, descriptor := range runtime.Catalog().Commands {
			if descriptor.Name == "shell.exec.v1" {
				t.Fatal("shell.exec.v1 advertised without an enabled profile")
			}
		}
	}
}

func TestOwnerShellRejectsBrokerUntilBrokerBoundaryExists(t *testing.T) {
	root := t.TempDir()
	_, err := normalizeOwnerShellPolicy(OwnerShellPolicy{
		Enabled:  true,
		Revision: "owner-v1",
		Profiles: map[string]OwnerShellProfile{
			"owner-root": {
				Executor:            ownerShellExecutorBroker,
				Shell:               OwnerShellExecutable{Path: testShellPath(t)},
				Identity:            &OwnerShellIdentity{UID: 0, GID: 0},
				WorkingRoots:        []string{root},
				InitialDirectory:    root,
				WorkingScopeAliases: map[string]string{"workspace": root},
				Network:             ownerShellNetworkInherit,
				Approval: OwnerShellApproval{
					ShellExec: "each_command", TerminalOpen: "session_start",
				},
			},
		},
	}, "")
	if err == nil || !strings.Contains(err.Error(), "authority broker") {
		t.Fatalf("broker profile error = %v", err)
	}
}

func TestShellExecRunsRealShellSemantics(t *testing.T) {
	runtime, _, root := newShellExecRuntime(t)
	result := invokeShellExec(t, runtime, shellExecInput{
		Profile: "owner",
		Script: "value=alpha; printf '%s\\n' \"$value\" | tr a-z A-Z > result.txt; " +
			"if test \"$(cat result.txt)\" = ALPHA; then printf condition-ok; fi; " +
			"printf shell-stderr >&2; exit 7",
		CWD:            "workspace",
		Env:            map[string]string{},
		TimeoutSeconds: 5,
	}, 6, 4096)
	if result.ExitCode != 7 ||
		result.Signal != "" ||
		result.Stdout != "condition-ok" ||
		result.Stderr != "shell-stderr" ||
		result.Truncated {
		t.Fatalf("shell result = %+v", result)
	}
	data, err := os.ReadFile(filepath.Join(root, "result.txt"))
	if err != nil || string(data) != "ALPHA\n" {
		t.Fatalf("redirected output = %q, err = %v", data, err)
	}
}

func TestShellExecDeniesUnknownProfileScopeAndEnvironmentBeforeAccept(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(shellExecInput) shellExecInput
	}{
		{
			name: "profile",
			mutate: func(input shellExecInput) shellExecInput {
				input.Profile = "invented"
				return input
			},
		},
		{
			name: "scope",
			mutate: func(input shellExecInput) shellExecInput {
				input.CWD = "invented"
				return input
			},
		},
		{
			name: "environment",
			mutate: func(input shellExecInput) shellExecInput {
				input.Env["SECRET"] = "value"
				return input
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, ledger, _ := newShellExecRuntime(t)
			input := test.mutate(shellExecInput{
				Profile:        "owner",
				Script:         "true",
				CWD:            "workspace",
				Env:            map[string]string{},
				TimeoutSeconds: 2,
			})
			plan := prepareShellExecPlan(t, runtime, input, 3, 4096)
			if _, err := runtime.Invoke(t.Context(), plan); !errors.Is(err, nodes.ErrCommandDenied) {
				t.Fatalf("Invoke() error = %v", err)
			}
			if _, found := ledger.Get(plan.InvocationID); found {
				t.Fatal("denied shell invocation was durably accepted")
			}
		})
	}
}

func TestShellExecBoundsOutput(t *testing.T) {
	runtime, _, _ := newShellExecRuntime(t)
	result := invokeShellExec(t, runtime, shellExecInput{
		Profile:        "owner",
		Script:         "i=0; while test \"$i\" -lt 5000; do printf x; i=$((i+1)); done",
		CWD:            "workspace",
		Env:            map[string]string{},
		TimeoutSeconds: 1,
	}, 2, 256)
	if !result.Truncated || len(result.Stdout) >= 4096 {
		t.Fatalf("bounded output = %+v", result)
	}
}

func TestShellExecCatalogHidesAuthorityAndBindsChanges(t *testing.T) {
	first, _, firstRoot := newShellExecRuntime(t)
	secondRoot := t.TempDir()
	secondPolicy := testOwnerShellPolicy(t, secondRoot)
	second, err := NewRuntime(
		nodes.ID("node_test"),
		"test",
		testOwnerLocalPolicy(),
		newMemoryInvocationLedger(),
		WithOwnerShell(secondPolicy),
	)
	if err != nil {
		t.Fatal(err)
	}
	firstDescriptor := shellExecDescriptor(t, first)
	secondDescriptor := shellExecDescriptor(t, second)
	contract := firstDescriptor.ModelContract
	if contract == nil ||
		contract.ApprovalMode != "each_command" ||
		!slices.Equal(contract.Constraints.ProfileAliases, []string{"owner"}) ||
		!slices.Equal(contract.Constraints.WorkingScopes, []string{"workspace"}) ||
		!slices.Equal(contract.Constraints.EnvironmentNames, []string{"VISIBLE"}) {
		t.Fatalf("shell model contract = %#v", contract)
	}
	projected, err := nodes.ShellExecModelInputSchema(*contract)
	if err != nil {
		t.Fatal(err)
	}
	for _, hidden := range []string{
		firstRoot,
		testShellPath(t),
		"FIXED_SECRET",
		"fixed-value",
		"companion_user",
	} {
		if strings.Contains(string(projected), hidden) {
			t.Fatalf("model projection leaked %q: %s", hidden, projected)
		}
	}
	if contract.AuthorityDigest == secondDescriptor.ModelContract.AuthorityDigest {
		t.Fatal("hidden working-root change did not change the authority digest")
	}
}

func newShellExecRuntime(
	t *testing.T,
) (*Runtime, *InvocationLedger, string) {
	t.Helper()
	root := t.TempDir()
	ledger := newMemoryInvocationLedger()
	runtime, err := NewRuntime(
		nodes.ID("node_test"),
		"test",
		testOwnerLocalPolicy(),
		ledger,
		WithOwnerShell(testOwnerShellPolicy(t, root)),
	)
	if err != nil {
		t.Fatal(err)
	}
	return runtime, ledger, root
}

func testOwnerLocalPolicy() nodes.LocalCommandPolicy {
	policy := testRuntimePolicy([]string{"shell.exec.v1"})
	policy.MaximumRisk = nodes.RiskPrivileged
	policy.MaxTimeoutSeconds = 30
	policy.MaxOutputBytes = maxOwnerShellOutputBytes
	return policy
}

func testOwnerShellPolicy(t *testing.T, root string) OwnerShellPolicy {
	t.Helper()
	policy, err := normalizeOwnerShellPolicy(OwnerShellPolicy{
		Enabled:  true,
		Revision: "owner-v1",
		Profiles: map[string]OwnerShellProfile{
			"owner": {
				Executor:                  ownerShellExecutorCompanion,
				Label:                     "Owner shell",
				Shell:                     OwnerShellExecutable{Path: testShellPath(t)},
				WorkingRoots:              []string{root},
				InitialDirectory:          root,
				WorkingScopeAliases:       map[string]string{"workspace": root},
				FixedEnvironment:          map[string]string{"FIXED_SECRET": "fixed-value"},
				PermittedEnvironmentNames: []string{"VISIBLE"},
				Network:                   ownerShellNetworkInherit,
				Approval: OwnerShellApproval{
					ShellExec: "each_command", TerminalOpen: "session_start",
				},
				Limits: OwnerShellLimits{
					CommandBytes:       maxOwnerShellScriptBytes,
					TimeoutSeconds:     20,
					OutputBytes:        maxOwnerShellOutputBytes,
					ConcurrentCommands: 2,
				},
			},
		},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func testShellPath(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("sh")
	if err != nil {
		t.Fatal(err)
	}
	path, err = filepath.Abs(path)
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func invokeShellExec(
	t *testing.T,
	runtime *Runtime,
	input shellExecInput,
	planTimeout int,
	outputLimit int,
) shellExecOutput {
	t.Helper()
	raw, err := runtime.Invoke(
		t.Context(),
		prepareShellExecPlan(t, runtime, input, planTimeout, outputLimit),
	)
	if err != nil {
		t.Fatal(err)
	}
	var result shellExecOutput
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func prepareShellExecPlan(
	t *testing.T,
	runtime *Runtime,
	input shellExecInput,
	timeoutSeconds int,
	outputLimit int,
) nodes.ExecutionPlan {
	t.Helper()
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := shellExecDescriptor(t, runtime)
	catalogHash, err := runtime.Catalog().Hash()
	if err != nil {
		t.Fatal(err)
	}
	sequence := shellExecPlanSequence.Add(1)
	plan, err := nodes.PrepareExecutionPlan(nodes.InvocationRequest{
		InvocationID:     fmt.Sprintf("inv_shell_exec_%d", sequence),
		IdempotencyKey:   fmt.Sprintf("idem_shell_exec_%d", sequence),
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

func shellExecDescriptor(t *testing.T, runtime *Runtime) nodes.CommandDescriptor {
	t.Helper()
	for _, descriptor := range runtime.Catalog().Commands {
		if descriptor.Name == "shell.exec.v1" {
			return descriptor
		}
	}
	t.Fatal("shell.exec.v1 is missing")
	return nodes.CommandDescriptor{}
}
