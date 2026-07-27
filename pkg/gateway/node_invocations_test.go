package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/tools"
)

type fakeNodeAdmissionHandler struct {
	beforeCommit *sync.WaitGroup
	invocation   nodes.InvocationRecord
	invokeCalls  atomic.Int32
	writeCalls   atomic.Int32
	queryCalls   atomic.Int32
}

func (*fakeNodeAdmissionHandler) ServeHTTP(http.ResponseWriter, *http.Request) {}

func (*fakeNodeAdmissionHandler) Close(context.Context) error { return nil }

func (*fakeNodeAdmissionHandler) WithResolvedApprovedCommand(
	_ string,
	_ string,
	operation func(nodes.Registration, nodes.CommandApproval) error,
) (nodes.CommandApproval, error) {
	approval := nodes.CommandApproval{}
	return approval, operation(nodes.Registration{}, approval)
}

func (handler *fakeNodeAdmissionHandler) Invoke(
	_ context.Context,
	_ nodes.ID,
	_ nodes.ExecutionPlan,
	commit func() error,
) (json.RawMessage, bool, error) {
	handler.invokeCalls.Add(1)
	if handler.beforeCommit != nil {
		handler.beforeCommit.Done()
		handler.beforeCommit.Wait()
	}
	if err := commit(); err != nil {
		return nil, false, err
	}
	handler.writeCalls.Add(1)
	return json.RawMessage(`{"value":"ok"}`), true, nil
}

func (handler *fakeNodeAdmissionHandler) Invocation(
	context.Context,
	nodes.ID,
	string,
) (nodes.InvocationRecord, error) {
	handler.queryCalls.Add(1)
	return handler.invocation, nil
}

func TestNodeInvocationSourceGrantsOneDispatchWinner(t *testing.T) {
	var beforeCommit sync.WaitGroup
	beforeCommit.Add(2)
	handler := &fakeNodeAdmissionHandler{beforeCommit: &beforeCommit}
	source := newTestNodeInvocationSource(t, handler)
	descriptor, plan, owner := testGatewayInvocation(t)
	if _, _, err := source.PrepareInvocation(
		"builder-node",
		"build",
		owner.ToolCallID,
		nodes.GatewayInvocationPrincipal{
			AgentID: plan.AgentID, SessionID: plan.SessionID, ActorID: plan.ActorID,
		},
		plan,
		descriptor,
		true,
		func(tools.NodeDiscoveryRecord) error { return nil },
	); err != nil {
		t.Fatal(err)
	}

	type result struct {
		dispatched bool
		err        error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			_, dispatched, err := source.DispatchInvocation(
				t.Context(),
				owner,
				plan.InvocationID,
				plan.PlanHash,
			)
			results <- result{dispatched: dispatched, err: err}
		}()
	}

	var successes, duplicates int
	for range 2 {
		got := <-results
		switch {
		case got.err == nil && got.dispatched:
			successes++
		case errors.Is(got.err, nodes.ErrGatewayInvocationDispatched) && !got.dispatched:
			duplicates++
		default:
			t.Fatalf("unexpected dispatch result = %#v", got)
		}
	}
	if successes != 1 || duplicates != 1 ||
		handler.invokeCalls.Load() != 2 || handler.writeCalls.Load() != 1 {
		t.Fatalf(
			"dispatches = successes %d, duplicates %d, invokes %d, writes %d",
			successes,
			duplicates,
			handler.invokeCalls.Load(),
			handler.writeCalls.Load(),
		)
	}
	record, found, err := source.LookupInvocation(
		nodes.GatewayInvocationPrincipal{
			AgentID: plan.AgentID, SessionID: plan.SessionID, ActorID: plan.ActorID,
		},
		plan.InvocationID,
	)
	if err != nil || !found || record.State != nodes.GatewayInvocationDispatched {
		t.Fatalf("durable dispatch record = %#v, found %v, error %v", record, found, err)
	}
}

func TestNodeInvocationSourceRecoversOnlyBoundDispatchedResult(t *testing.T) {
	handler := &fakeNodeAdmissionHandler{}
	source := newTestNodeInvocationSource(t, handler)
	descriptor, plan, owner := testGatewayInvocation(t)
	if _, _, err := source.PrepareInvocation(
		"builder-node",
		"build",
		owner.ToolCallID,
		nodes.GatewayInvocationPrincipal{
			AgentID: plan.AgentID, SessionID: plan.SessionID, ActorID: plan.ActorID,
		},
		plan,
		descriptor,
		true,
		func(tools.NodeDiscoveryRecord) error { return nil },
	); err != nil {
		t.Fatal(err)
	}
	principal := nodes.GatewayInvocationPrincipal{
		AgentID: plan.AgentID, SessionID: plan.SessionID, ActorID: plan.ActorID,
	}
	if _, err := source.QueryInvocation(
		t.Context(),
		principal,
		owner.Target,
		plan.NodeID,
		plan.InvocationID,
	); !errors.Is(err, nodes.ErrGatewayInvocationConflict) {
		t.Fatalf("prepared recovery error = %v", err)
	}
	if handler.queryCalls.Load() != 0 {
		t.Fatal("prepared invocation queried the companion")
	}
	if _, transitioned, err := source.store.MarkDispatched(
		owner,
		plan.InvocationID,
		plan.PlanHash,
	); err != nil || !transitioned {
		t.Fatalf("mark dispatched = transitioned %v, error %v", transitioned, err)
	}

	now := time.Now()
	handler.invocation = nodes.InvocationRecord{
		InvocationID:   plan.InvocationID,
		IdempotencyKey: plan.IdempotencyKey,
		PlanHash:       plan.PlanHash,
		NodeID:         plan.NodeID,
		CatalogHash:    plan.CatalogHash,
		Command:        plan.Command,
		Risk:           plan.Risk,
		State:          nodes.InvocationSucceeded,
		AcceptedAt:     now.Add(-time.Second).UnixNano(),
		UpdatedAt:      now.UnixNano(),
		ExpiresAt:      now.Add(time.Minute).Unix(),
		CompletedAt:    now.UnixNano(),
		Result:         json.RawMessage(`{ "value": "ok" }`),
	}
	handler.invocation.CompletedAt = handler.invocation.UpdatedAt
	recovered, err := source.QueryInvocation(
		t.Context(),
		principal,
		owner.Target,
		plan.NodeID,
		plan.InvocationID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(recovered.Result) != `{"value":"ok"}` {
		t.Fatalf("recovered canonical result = %s", recovered.Result)
	}

	handler.invocation.Result = json.RawMessage(`{"other":"wrong schema"}`)
	if _, err := source.QueryInvocation(
		t.Context(),
		principal,
		owner.Target,
		plan.NodeID,
		plan.InvocationID,
	); !errors.Is(err, nodes.ErrGatewayInvocationConflict) {
		t.Fatalf("schema-invalid recovery error = %v", err)
	}
}

func TestNodeInvocationSourceRejectsStaleRuntimeGeneration(t *testing.T) {
	handler := &fakeNodeAdmissionHandler{}
	source := newTestNodeInvocationSource(t, handler)
	source.runtime.registryMu.Lock()
	source.runtime.generation++
	source.runtime.registryMu.Unlock()
	descriptor, plan, owner := testGatewayInvocation(t)

	if _, _, err := source.PrepareInvocation(
		"builder-node",
		"build",
		owner.ToolCallID,
		nodes.GatewayInvocationPrincipal{
			AgentID: plan.AgentID, SessionID: plan.SessionID, ActorID: plan.ActorID,
		},
		plan,
		descriptor,
		true,
		func(tools.NodeDiscoveryRecord) error { return nil },
	); !errors.Is(err, errNodeDiscoveryAuthorityUnavailable) {
		t.Fatalf("stale prepare error = %v", err)
	}
	if _, dispatched, err := source.DispatchInvocation(
		t.Context(),
		owner,
		plan.InvocationID,
		plan.PlanHash,
	); !errors.Is(err, errNodeDiscoveryAuthorityUnavailable) || dispatched {
		t.Fatalf("stale dispatch = dispatched %v, error %v", dispatched, err)
	}
}

func newTestNodeInvocationSource(
	t *testing.T,
	handler nodeAdmissionHandler,
) *nodeInvocationSource {
	t.Helper()
	store, err := nodes.NewGatewayInvocationStore(
		nodes.GatewayInvocationStorePath(t.TempDir()),
		8,
		1024*1024,
	)
	if err != nil {
		t.Fatal(err)
	}
	registryPath := filepath.Join(t.TempDir(), "registry.json")
	runtime := &nodeAdmissionRuntime{
		registryPath: registryPath,
		handler:      handler,
		generation:   1,
		mounted:      true,
	}
	return &nodeInvocationSource{
		nodeDiscoverySource: nodeDiscoverySource{
			runtime: runtime, registryPath: registryPath,
		},
		store:      store,
		generation: runtime.generation,
	}
}

func testGatewayInvocation(
	t *testing.T,
) (nodes.CommandDescriptor, nodes.ExecutionPlan, nodes.GatewayInvocationOwner) {
	t.Helper()
	descriptor := nodes.CommandDescriptor{
		Name:        "node.info.v1",
		InputSchema: json.RawMessage(`{"type":"object","additionalProperties":false}`),
		OutputSchema: json.RawMessage(
			`{"type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}`,
		),
		Risk: nodes.RiskRead,
	}
	catalogHash, err := (nodes.CapabilityCatalog{
		Commands: []nodes.CommandDescriptor{descriptor},
	}).Hash()
	if err != nil {
		t.Fatal(err)
	}
	request := nodes.InvocationRequest{
		InvocationID:     "inv_1",
		IdempotencyKey:   "idem_1",
		NodeID:           "node_test",
		CatalogHash:      catalogHash,
		Command:          descriptor.Name,
		Input:            json.RawMessage(`{}`),
		AgentID:          "agent_1",
		SessionID:        "session_1",
		ActorID:          "actor_1",
		TimeoutSeconds:   30,
		OutputLimitBytes: 1024,
	}
	plan, err := nodes.PrepareExecutionPlan(
		request,
		descriptor,
		"builtin",
		"policy_1",
		time.Now(),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	return descriptor, plan, nodes.GatewayInvocationOwner{
		Target:     "build",
		AgentID:    plan.AgentID,
		SessionID:  plan.SessionID,
		ActorID:    plan.ActorID,
		ToolCallID: "call_1",
	}
}
