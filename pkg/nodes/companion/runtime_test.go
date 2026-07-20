package companion

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/nodes"
)

func TestRuntimeRequiresLocalCommandApproval(t *testing.T) {
	policy := testRuntimePolicy(nil)
	runtime, err := NewRuntime(nodes.ID("node_test"), "test", policy)
	if err != nil {
		t.Fatal(err)
	}
	plan := testRuntimePlan(t, runtime, "node.info.v1", json.RawMessage(`{}`))
	if _, err := runtime.Invoke(t.Context(), plan); !errors.Is(err, nodes.ErrCommandDenied) {
		t.Fatalf("Invoke() error = %v", err)
	}
}

func TestRuntimeExecutesReadOnlyBuiltins(t *testing.T) {
	policy := testRuntimePolicy([]string{"node.info.v1", "system.which.v1"})
	runtime, err := NewRuntime(nodes.ID("node_test"), "v-test", policy)
	if err != nil {
		t.Fatal(err)
	}

	info, err := runtime.Invoke(
		t.Context(),
		testRuntimePlan(t, runtime, "node.info.v1", json.RawMessage(`{}`)),
	)
	if err != nil {
		t.Fatal(err)
	}
	var infoResult struct {
		NodeID  nodes.ID `json:"node_id"`
		Version string   `json:"version"`
	}
	if decodeErr := json.Unmarshal(info, &infoResult); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if infoResult.NodeID != "node_test" || infoResult.Version != "v-test" {
		t.Fatalf("node.info result = %+v", infoResult)
	}

	which, err := runtime.Invoke(t.Context(), testRuntimePlan(
		t,
		runtime,
		"system.which.v1",
		json.RawMessage(`{"name":"go"}`),
	))
	if err != nil {
		t.Fatal(err)
	}
	var whichResult struct {
		Found bool   `json:"found"`
		Path  string `json:"path"`
	}
	if decodeErr := json.Unmarshal(which, &whichResult); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if !whichResult.Found || whichResult.Path == "" {
		t.Fatalf("system.which result = %+v", whichResult)
	}
}

func testRuntimePolicy(commands []string) nodes.LocalCommandPolicy {
	return nodes.LocalCommandPolicy{
		Revision:          "policy-test",
		AllowedCommands:   commands,
		MaximumRisk:       nodes.RiskRead,
		MaxTimeoutSeconds: 30,
		MaxOutputBytes:    64 * 1024,
	}
}

func testRuntimePlan(
	t *testing.T,
	runtime *Runtime,
	command string,
	input json.RawMessage,
) nodes.ExecutionPlan {
	t.Helper()
	catalog := runtime.Catalog()
	catalogHash, err := catalog.Hash()
	if err != nil {
		t.Fatal(err)
	}
	var descriptor nodes.CommandDescriptor
	for _, candidate := range catalog.Commands {
		if candidate.Name == command {
			descriptor = candidate
			break
		}
	}
	plan, err := nodes.PrepareExecutionPlan(nodes.InvocationRequest{
		InvocationID:     "inv_test",
		IdempotencyKey:   "idem_test",
		NodeID:           runtime.nodeID,
		CatalogHash:      catalogHash,
		Command:          command,
		Input:            input,
		AgentID:          "agent_test",
		SessionID:        "session_test",
		ActorID:          "actor_test",
		TimeoutSeconds:   5,
		OutputLimitBytes: 4096,
	}, descriptor, LocalExecutor, runtime.policy.Revision, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}
