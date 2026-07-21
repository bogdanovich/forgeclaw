package companion

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/nodes"
)

func TestRuntimeRequiresLocalCommandApproval(t *testing.T) {
	policy := testRuntimePolicy(nil)
	runtime, err := NewRuntime(nodes.ID("node_test"), "test", policy, newMemoryInvocationLedger())
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
	runtime, newErr := NewRuntime(
		nodes.ID("node_test"),
		"v-test",
		policy,
		newMemoryInvocationLedger(),
	)
	if newErr != nil {
		t.Fatal(newErr)
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

func TestRuntimeReturnsRecordedResultAfterPlanExpires(t *testing.T) {
	clock := time.Unix(100, 0)
	ledger := newInvocationLedger(
		"",
		DefaultInvocationLedgerLimit,
		DefaultInvocationLedgerBytes,
		func() time.Time { return clock },
	)
	runtime, newErr := NewRuntime(
		nodes.ID("node_test"),
		"test",
		testRuntimePolicy([]string{"node.info.v1"}),
		ledger,
	)
	if newErr != nil {
		t.Fatal(newErr)
	}
	plan := testRuntimePlanAt(
		t,
		runtime,
		"node.info.v1",
		json.RawMessage(`{}`),
		clock,
		time.Second,
	)
	if _, _, acceptErr := ledger.Accept(plan); acceptErr != nil {
		t.Fatal(acceptErr)
	}
	if _, markErr := ledger.MarkRunning(plan.InvocationID); markErr != nil {
		t.Fatal(markErr)
	}
	want := json.RawMessage(
		`{"node_id":"node_test","platform":"linux","architecture":"amd64","version":"test"}`,
	)
	if _, completeErr := ledger.CompleteSuccess(plan.InvocationID, want); completeErr != nil {
		t.Fatal(completeErr)
	}
	clock = clock.Add(2 * time.Second)
	got, err := runtime.Invoke(t.Context(), plan)
	if err != nil {
		t.Fatalf("expired duplicate Invoke() error = %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("expired duplicate result = %s", got)
	}
}

func TestRuntimeDoesNotReplayUnfinishedInvocation(t *testing.T) {
	ledger := newMemoryInvocationLedger()
	runtime, newErr := NewRuntime(
		nodes.ID("node_test"),
		"test",
		testRuntimePolicy([]string{"node.info.v1"}),
		ledger,
	)
	if newErr != nil {
		t.Fatal(newErr)
	}
	plan := testRuntimePlan(t, runtime, "node.info.v1", json.RawMessage(`{}`))
	if _, _, acceptErr := ledger.Accept(plan); acceptErr != nil {
		t.Fatal(acceptErr)
	}
	if _, markErr := ledger.MarkRunning(plan.InvocationID); markErr != nil {
		t.Fatal(markErr)
	}
	if _, err := runtime.Invoke(t.Context(), plan); !errors.Is(
		err,
		ErrInvocationOutcomeUnknown,
	) {
		t.Fatalf("unfinished duplicate Invoke() error = %v", err)
	}
}

func TestRuntimeReturnsRecordedResultWithoutExecutingAgain(t *testing.T) {
	ledger := newMemoryInvocationLedger()
	runtime, newErr := NewRuntime(
		nodes.ID("node_test"),
		"test",
		testRuntimePolicy([]string{"test.count.v1"}),
		ledger,
	)
	if newErr != nil {
		t.Fatal(newErr)
	}
	handler := &countingHandler{}
	descriptor := handler.descriptor()
	runtime.handlers[descriptor.Name] = handler
	runtime.catalog.Commands = append(runtime.catalog.Commands, descriptor)
	plan := testRuntimePlan(t, runtime, descriptor.Name, json.RawMessage(`{}`))

	first, firstErr := runtime.Invoke(t.Context(), plan)
	second, secondErr := runtime.Invoke(t.Context(), plan)
	if firstErr != nil || secondErr != nil {
		t.Fatalf("Invoke() errors = %v, %v", firstErr, secondErr)
	}
	if string(first) != string(second) || handler.executions != 1 {
		t.Fatalf("results = %s, %s; executions = %d", first, second, handler.executions)
	}
}

type countingHandler struct {
	executions int
}

func (*countingHandler) descriptor() nodes.CommandDescriptor {
	return nodes.CommandDescriptor{
		Name:         "test.count.v1",
		InputSchema:  json.RawMessage(`{"type":"object","additionalProperties":false}`),
		OutputSchema: json.RawMessage(`{"type":"object"}`),
		Risk:         nodes.RiskRead,
	}
}

func (handler *countingHandler) execute(context.Context, json.RawMessage) (any, error) {
	handler.executions++
	return map[string]int{"executions": handler.executions}, nil
}

func TestRuntimePersistsOutputEncodingFailure(t *testing.T) {
	ledger := newMemoryInvocationLedger()
	runtime, newErr := NewRuntime(
		nodes.ID("node_test"),
		"test",
		testRuntimePolicy([]string{"test.invalid-output.v1"}),
		ledger,
	)
	if newErr != nil {
		t.Fatal(newErr)
	}
	handler := invalidOutputHandler{}
	descriptor := handler.descriptor()
	runtime.handlers[descriptor.Name] = handler
	runtime.catalog.Commands = append(runtime.catalog.Commands, descriptor)
	plan := testRuntimePlan(t, runtime, descriptor.Name, json.RawMessage(`{}`))

	if _, err := runtime.Invoke(t.Context(), plan); err == nil {
		t.Fatal("Invoke() accepted an unencodable command result")
	}
	record, found := ledger.Get(plan.InvocationID)
	if !found || record.State != nodes.InvocationFailed || record.Failure == nil ||
		record.Failure.Code != "INVALID_OUTPUT" {
		t.Fatalf("encoding failure record = %#v, found %v", record, found)
	}
}

type invalidOutputHandler struct{}

func (invalidOutputHandler) descriptor() nodes.CommandDescriptor {
	return nodes.CommandDescriptor{
		Name:         "test.invalid-output.v1",
		InputSchema:  json.RawMessage(`{"type":"object","additionalProperties":false}`),
		OutputSchema: json.RawMessage(`{"type":"object"}`),
		Risk:         nodes.RiskRead,
	}
}

func (invalidOutputHandler) execute(context.Context, json.RawMessage) (any, error) {
	return make(chan int), nil
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
	return testRuntimePlanAt(t, runtime, command, input, time.Now(), time.Minute)
}

func testRuntimePlanAt(
	t *testing.T,
	runtime *Runtime,
	command string,
	input json.RawMessage,
	preparedAt time.Time,
	ttl time.Duration,
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
		InvocationID:     "inv_" + strings.ReplaceAll(command, ".", "_"),
		IdempotencyKey:   "idem_" + strings.ReplaceAll(command, ".", "_"),
		NodeID:           runtime.nodeID,
		CatalogHash:      catalogHash,
		Command:          command,
		Input:            input,
		AgentID:          "agent_test",
		SessionID:        "session_test",
		ActorID:          "actor_test",
		TimeoutSeconds:   5,
		OutputLimitBytes: 4096,
	}, descriptor, LocalExecutor, runtime.policy.Revision, preparedAt, ttl)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}
