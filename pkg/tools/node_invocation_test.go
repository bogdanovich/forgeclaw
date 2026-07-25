package tools

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/nodes"
	"github.com/sipeed/picoclaw/pkg/tools/loopguard"
)

type fakeNodeInvocationSource struct {
	*fakeNodeDiscoverySource
	store          *nodes.GatewayInvocationStore
	preDispatchErr error
	dispatchErr    error
	queryErr       error
	remote         nodes.InvocationRecord
	lookupMiss     bool
	prepareCalls   int
}

func (source *fakeNodeInvocationSource) PrepareInvocation(
	target string,
	toolCallID string,
	plan nodes.ExecutionPlan,
) (nodes.GatewayInvocationRecord, error) {
	source.prepareCalls++
	return source.store.Prepare(target, toolCallID, plan)
}

func (source *fakeNodeInvocationSource) LookupInvocationByToolCall(
	principal nodes.GatewayInvocationPrincipal,
	toolCallID string,
) (nodes.GatewayInvocationRecord, bool, error) {
	if source.lookupMiss {
		return nodes.GatewayInvocationRecord{}, false, nil
	}
	return source.store.ByToolCall(principal, toolCallID)
}

func (source *fakeNodeInvocationSource) LookupInvocation(
	principal nodes.GatewayInvocationPrincipal,
	invocationID string,
) (nodes.GatewayInvocationRecord, bool, error) {
	return source.store.Lookup(principal, invocationID)
}

func (source *fakeNodeInvocationSource) DispatchInvocation(
	_ context.Context,
	owner nodes.GatewayInvocationOwner,
	invocationID string,
	expectedPlanHash string,
) (json.RawMessage, bool, error) {
	if source.preDispatchErr != nil {
		return nil, false, source.preDispatchErr
	}
	if _, err := source.store.MarkDispatched(owner, invocationID, expectedPlanHash); err != nil {
		return nil, false, err
	}
	if source.dispatchErr != nil {
		return nil, true, source.dispatchErr
	}
	return json.RawMessage(`{"stdout":"ok","exit_code":0}`), true, nil
}

func (source *fakeNodeInvocationSource) QueryInvocation(
	_ context.Context,
	principal nodes.GatewayInvocationPrincipal,
	target string,
	nodeID nodes.ID,
	invocationID string,
) (nodes.InvocationRecord, error) {
	record, found, err := source.store.Lookup(principal, invocationID)
	if err != nil {
		return nodes.InvocationRecord{}, err
	}
	if !found || record.Target != target || record.Plan.NodeID != nodeID {
		return nodes.InvocationRecord{}, nodes.ErrGatewayInvocationConflict
	}
	if source.queryErr != nil {
		return nodes.InvocationRecord{}, source.queryErr
	}
	return source.remote, nil
}

func TestNodeInvokeToolReusesApprovalPlanAndDispatches(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	args := nodeInvocationTestArgs()

	first, err := tool.ApprovalArguments(ctx, args)
	if err != nil {
		t.Fatal(err)
	}
	second, err := tool.ApprovalArguments(ctx, args)
	if err != nil {
		t.Fatal(err)
	}
	if first["plan_hash"] != second["plan_hash"] ||
		first["invocation_id"] != second["invocation_id"] {
		t.Fatalf("approval bindings changed = %#v, %#v", first, second)
	}
	otherActor, err := tool.ApprovalArguments(
		nodeInvocationTestContext("actor-2", "call-1"),
		args,
	)
	if err != nil {
		t.Fatal(err)
	}
	if otherActor["invocation_id"] == first["invocation_id"] {
		t.Fatal("different actors shared invocation authority")
	}
	result := tool.Execute(ctx, args)
	if result.IsError {
		t.Fatalf("nodes_invoke failed: %s", result.ForLLM)
	}
	payload := decodeNodeResult(t, result)
	if payload["state"] != string(nodes.InvocationSucceeded) ||
		payload["target"] != "build" ||
		payload["invocation_id"] != first["invocation_id"] {
		t.Fatalf("invoke result = %#v", payload)
	}
	if strings.Contains(result.ForLLM, "private-node-id") ||
		strings.Contains(result.ForLLM, "plan_hash") {
		t.Fatalf("invoke result leaked internal authority: %s", result.ForLLM)
	}
}

func TestNodeInvokeToolRejectsChangedArgumentsAfterPreparation(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	args := nodeInvocationTestArgs()
	if _, err := tool.ApprovalArguments(ctx, args); err != nil {
		t.Fatal(err)
	}
	changed := nodeInvocationTestArgs()
	changed["input"] = map[string]any{"argv": []any{"git", "diff"}}
	if _, err := tool.ApprovalArguments(ctx, changed); err == nil ||
		!strings.Contains(err.Error(), "retained invocation") {
		t.Fatalf("changed approval error = %v", err)
	}
}

func TestNodeInvokeToolDoesNotReplaceExpiredAuthorityOnApprovalResume(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	if _, err := tool.ApprovalArguments(ctx, nodeInvocationTestArgs()); err != nil {
		t.Fatal(err)
	}
	if source.prepareCalls != 1 {
		t.Fatalf("initial prepare calls = %d", source.prepareCalls)
	}
	source.lookupMiss = true
	ctx = WithToolApprovalContinuation(ctx, true)
	if _, err := tool.ApprovalArguments(ctx, nodeInvocationTestArgs()); err == nil ||
		!strings.Contains(err.Error(), "expired before approval resumed") {
		t.Fatalf("approval resume error = %v", err)
	}
	if source.prepareCalls != 1 {
		t.Fatalf("approval resume minted new authority; prepare calls = %d", source.prepareCalls)
	}
}

func TestNodeInvokeToolReportsDispatchUncertaintyWithoutReplay(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	source.dispatchErr = errors.New("transport closed")
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	result := tool.Execute(
		nodeInvocationTestContext("actor-1", "call-1"),
		nodeInvocationTestArgs(),
	)
	if !result.IsError ||
		!strings.Contains(result.ForLLM, "DISPATCH_UNCERTAIN") ||
		!strings.Contains(result.ForLLM, "nodes_status") {
		t.Fatalf("uncertain dispatch = %#v", result)
	}
}

func TestNodeInvokeToolDistinguishesPreDispatchRejection(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	source.preDispatchErr = errors.New("durable authority unavailable")
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	result := tool.Execute(
		nodeInvocationTestContext("actor-1", "call-1"),
		nodeInvocationTestArgs(),
	)
	if !result.IsError ||
		!strings.Contains(result.ForLLM, "DISPATCH_DENIED") ||
		strings.Contains(result.ForLLM, "nodes_status") {
		t.Fatalf("pre-dispatch rejection = %#v", result)
	}
}

func TestNodeStatusToolIsActorScopedAndRecoversResult(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	invoke := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	invocationID := decodeNodeResult(
		t,
		invoke.Execute(ctx, nodeInvocationTestArgs()),
	)["invocation_id"].(string)
	record := mustFakeGatewayInvocation(t, source, ctx, invocationID)
	source.remote = successfulRemoteInvocation(record)

	status := NewNodeStatusTool(nodeDiscoveryTestConfig(), source)
	payload := decodeNodeResult(
		t,
		status.Execute(ctx, map[string]any{"invocation_id": invocationID}),
	)
	if payload["state"] != string(nodes.InvocationSucceeded) {
		t.Fatalf("status = %#v", payload)
	}
	denied := status.Execute(
		nodeInvocationTestContext("actor-2", "status-call"),
		map[string]any{"invocation_id": invocationID},
	)
	if !denied.IsError || !strings.Contains(denied.ForLLM, "not found in this scope") {
		t.Fatalf("cross-actor status = %#v", denied)
	}
}

func TestNodeStatusToolReportsDisconnectedOutcomeAsUnknown(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	invoke := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	invocationID := decodeNodeResult(
		t,
		invoke.Execute(ctx, nodeInvocationTestArgs()),
	)["invocation_id"].(string)
	source.connected = map[nodes.ID]bool{}

	status := NewNodeStatusTool(nodeDiscoveryTestConfig(), source)
	payload := decodeNodeResult(
		t,
		status.Execute(ctx, map[string]any{"invocation_id": invocationID}),
	)
	if payload["state"] != string(nodes.InvocationUnknown) ||
		payload["error_code"] != "NODE_UNAVAILABLE" ||
		payload["node_available"] != false {
		t.Fatalf("offline status = %#v", payload)
	}
}

func TestNodeInvokeToolRuntimeSemantics(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	if got := NewNodeInvokeTool(nil, source).ToolLoopSemantics(); got != loopguard.SemanticsMutating {
		t.Fatalf("invoke semantics = %q", got)
	}
	if got := NewNodeStatusTool(nil, source).ToolLoopSemantics(); got !=
		loopguard.SemanticsReadOnlyIdempotent {
		t.Fatalf("status semantics = %q", got)
	}
}

func newFakeNodeInvocationSource(t *testing.T) *fakeNodeInvocationSource {
	t.Helper()
	command := testNodeCommand("system.exec.v1", nodes.RiskWrite, false, true)
	command.InputSchema = json.RawMessage(
		`{"type":"object","properties":{"argv":{"type":"array","items":{"type":"string"}}},"required":["argv"],"additionalProperties":false}`,
	)
	catalog := nodes.CapabilityCatalog{Commands: []nodes.CommandDescriptor{command}}
	catalogHash := mustCatalogHash(t, catalog)
	snapshot := nodes.Snapshot{
		ID:             "private-node-id",
		State:          nodes.StateConnected,
		Catalog:        catalog,
		CatalogHash:    catalogHash,
		Executor:       "local",
		PolicyRevision: "policy-1",
	}
	discovery := &fakeNodeDiscoverySource{
		byRef: map[string]nodes.Snapshot{"builder-node": snapshot},
		registrations: map[nodes.ID]nodes.Registration{
			snapshot.ID: {
				Snapshot:            snapshot,
				AllowedCommands:     []string{command.Name},
				ApprovedCatalogHash: catalogHash,
				ApprovedAt:          1,
			},
		},
		connected: map[nodes.ID]bool{snapshot.ID: true},
	}
	store, err := nodes.NewGatewayInvocationStore(
		filepath.Join(t.TempDir(), "node_invocations.json"),
		8,
		1024*1024,
	)
	if err != nil {
		t.Fatal(err)
	}
	return &fakeNodeInvocationSource{
		fakeNodeDiscoverySource: discovery,
		store:                   store,
	}
}

func nodeInvocationTestContext(actorID string, toolCallID string) context.Context {
	ctx := WithToolSessionContext(context.Background(), "main", "history-session", nil)
	ctx = WithToolRouteSessionKey(ctx, "route-session")
	ctx = WithToolInboundMetadata(ctx, bus.InboundContext{
		Channel: "telegram", ChatID: "chat-1", SenderID: actorID, ActorID: actorID,
	})
	return WithToolCallID(ctx, toolCallID)
}

func nodeInvocationTestArgs() map[string]any {
	return map[string]any{
		"target":  "build",
		"command": "system.exec.v1",
		"input":   map[string]any{"argv": []any{"git", "status"}},
	}
}

func mustFakeGatewayInvocation(
	t *testing.T,
	source *fakeNodeInvocationSource,
	ctx context.Context,
	invocationID string,
) nodes.GatewayInvocationRecord {
	t.Helper()
	principal, err := nodeInvocationIdentityWithoutCall(ctx)
	if err != nil {
		t.Fatal(err)
	}
	record, found, err := source.store.Lookup(principal, invocationID)
	if err != nil || !found {
		t.Fatalf("gateway invocation = (%#v, %v, %v)", record, found, err)
	}
	return record
}

func successfulRemoteInvocation(
	gateway nodes.GatewayInvocationRecord,
) nodes.InvocationRecord {
	now := time.Now().UnixNano()
	return nodes.InvocationRecord{
		InvocationID: gateway.Plan.InvocationID, IdempotencyKey: gateway.Plan.IdempotencyKey,
		PlanHash: gateway.ExpectedPlanHash, NodeID: gateway.Plan.NodeID,
		CatalogHash: gateway.Plan.CatalogHash, Command: gateway.Plan.Command,
		Risk: gateway.Plan.Risk, State: nodes.InvocationSucceeded,
		AcceptedAt: now, UpdatedAt: now, CompletedAt: now, ExpiresAt: gateway.Plan.ExpiresAt,
		Result: json.RawMessage(`{"stdout":"ok","exit_code":0}`),
	}
}
