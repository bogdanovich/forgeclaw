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
	rejection      *nodes.InvocationFailure
	queryErr       error
	remote         nodes.InvocationRecord
	lookupMiss     bool
	prepareCalls   int
	dispatchCalls  int
	queryCalls     int
}

func (source *fakeNodeInvocationSource) PrepareInvocation(
	target string,
	toolCallID string,
	plan nodes.ExecutionPlan,
	descriptor nodes.CommandDescriptor,
) (nodes.GatewayInvocationRecord, error) {
	source.prepareCalls++
	return source.store.Prepare(target, toolCallID, plan, descriptor)
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
	principal := nodes.GatewayInvocationPrincipal{
		AgentID: owner.AgentID, SessionID: owner.SessionID, ActorID: owner.ActorID,
	}
	record, found, err := source.store.Lookup(principal, invocationID)
	if err != nil || !found {
		return nil, false, nodes.ErrGatewayInvocationConflict
	}
	if record.State == nodes.GatewayInvocationDispatched {
		return nil, true, nodes.ErrGatewayInvocationDispatched
	}
	if record.State == nodes.GatewayInvocationRejected && record.Rejection != nil {
		return nil, true, &nodes.GatewayInvocationRejectedError{
			Failure: *record.Rejection,
		}
	}
	if _, transitioned, err := source.store.MarkDispatched(
		owner,
		invocationID,
		expectedPlanHash,
	); err != nil {
		return nil, false, err
	} else if !transitioned {
		return nil, true, nodes.ErrGatewayInvocationDispatched
	}
	source.dispatchCalls++
	if source.rejection != nil {
		rejection := *source.rejection
		if _, _, err := source.store.MarkRejected(
			owner,
			invocationID,
			expectedPlanHash,
			rejection,
		); err != nil {
			return nil, true, err
		}
		return nil, true, &nodes.GatewayInvocationRejectedError{
			Failure: rejection,
		}
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
	source.queryCalls++
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

func TestNodeInvokeToolNamespacesProviderCallByTurnAndWorkspace(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	args := nodeInvocationTestArgs()
	firstCtx := nodeInvocationTestContext("actor-1", "reused-call")
	first, err := tool.ApprovalArguments(firstCtx, args)
	if err != nil {
		t.Fatal(err)
	}

	nextTurnCtx := WithToolExecutionIdentity(
		nodeInvocationTestContext("actor-1", "reused-call"),
		"/workspace/main",
		"turn-2",
	)
	nextTurn, err := tool.ApprovalArguments(nextTurnCtx, args)
	if err != nil {
		t.Fatal(err)
	}
	otherWorkspaceCtx := WithToolExecutionIdentity(
		nodeInvocationTestContext("actor-1", "reused-call"),
		"/workspace/other",
		"turn-1",
	)
	otherWorkspace, err := tool.ApprovalArguments(otherWorkspaceCtx, args)
	if err != nil {
		t.Fatal(err)
	}
	if first["invocation_id"] == nextTurn["invocation_id"] ||
		first["invocation_id"] == otherWorkspace["invocation_id"] ||
		nextTurn["invocation_id"] == otherWorkspace["invocation_id"] {
		t.Fatalf(
			"execution namespaces collided: first=%v next=%v workspace=%v",
			first["invocation_id"],
			nextTurn["invocation_id"],
			otherWorkspace["invocation_id"],
		)
	}
}

func TestNodeInvokeToolApprovalResumeRetainsOriginExecutionIdentity(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	first, err := tool.ApprovalArguments(ctx, nodeInvocationTestArgs())
	if err != nil {
		t.Fatal(err)
	}
	resumedCtx := WithToolApprovalContinuation(
		WithToolExecutionIdentity(ctx, "/workspace/main", "turn-1"),
		true,
	)
	resumed, err := tool.ApprovalArguments(resumedCtx, nodeInvocationTestArgs())
	if err != nil {
		t.Fatal(err)
	}
	if resumed["invocation_id"] != first["invocation_id"] || source.prepareCalls != 1 {
		t.Fatalf("approval resume changed authority: first=%#v resumed=%#v", first, resumed)
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

func TestNodeInvokeToolPersistsDefinitiveRejectionWithoutStatusQuery(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	source.rejection = &nodes.InvocationFailure{
		Code:    "NODE_BUSY",
		Message: "node invocation ledger is full",
	}
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	result := tool.Execute(ctx, nodeInvocationTestArgs())
	if !result.IsError ||
		!strings.Contains(result.ForLLM, "NOT_ACCEPTED") ||
		strings.Contains(result.ForLLM, "nodes_status") {
		t.Fatalf("definitive rejection = %#v", result)
	}
	var errorPayload struct {
		Invocation nodeInvokeResult `json:"invocation"`
	}
	if err := json.Unmarshal([]byte(result.ForLLM), &errorPayload); err != nil {
		t.Fatalf("decode rejection %q: %v", result.ForLLM, err)
	}
	invocationID := errorPayload.Invocation.InvocationID
	if errorPayload.Invocation.State != "rejected" ||
		errorPayload.Invocation.GatewayState != nodes.GatewayInvocationRejected {
		t.Fatalf("rejection payload = %#v", errorPayload)
	}

	status := NewNodeStatusTool(nodeDiscoveryTestConfig(), source)
	statusPayload := decodeNodeResult(
		t,
		status.Execute(ctx, map[string]any{"invocation_id": invocationID}),
	)
	if statusPayload["state"] != "rejected" ||
		statusPayload["gateway_state"] != string(nodes.GatewayInvocationRejected) ||
		source.queryCalls != 0 {
		t.Fatalf("rejected status = %#v, query calls = %d", statusPayload, source.queryCalls)
	}

	repeated := tool.Execute(ctx, nodeInvocationTestArgs())
	if !repeated.IsError ||
		!strings.Contains(repeated.ForLLM, "NOT_ACCEPTED") ||
		source.dispatchCalls != 1 {
		t.Fatalf("repeated rejection = %#v, dispatch calls = %d", repeated, source.dispatchCalls)
	}
}

func TestNodeInvokeToolDistinguishesPreDispatchRejection(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	source.preDispatchErr = errors.New("durable authority unavailable")
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	result := tool.Execute(
		ctx,
		nodeInvocationTestArgs(),
	)
	if !result.IsError ||
		!strings.Contains(result.ForLLM, "DISPATCH_DENIED") ||
		strings.Contains(result.ForLLM, "nodes_status") {
		t.Fatalf("pre-dispatch rejection = %#v", result)
	}
	_, executionCallID, err := nodeInvocationIdentity(ctx)
	if err != nil {
		t.Fatal(err)
	}
	record := mustFakeGatewayInvocation(
		t,
		source,
		ctx,
		stableNodeInvocationID(
			"inv",
			stableNodeInvocationID("agent", "main"),
			stableNodeInvocationID("session", "route-session"),
			stableNodeInvocationID("actor", "actor-1"),
			executionCallID,
		),
	)
	if record.State != nodes.GatewayInvocationPrepared {
		t.Fatalf("pre-dispatch rejection state = %q", record.State)
	}
}

func TestNodeInvokeToolDoesNotReplayDispatchedInvocation(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	if result := tool.Execute(ctx, nodeInvocationTestArgs()); result.IsError {
		t.Fatalf("first invoke = %#v", result)
	}
	result := tool.Execute(ctx, nodeInvocationTestArgs())
	if !result.IsError ||
		!strings.Contains(result.ForLLM, "DISPATCH_UNCERTAIN") ||
		!strings.Contains(result.ForLLM, "nodes_status") {
		t.Fatalf("repeated invoke = %#v", result)
	}
	if source.dispatchCalls != 1 {
		t.Fatalf("dispatch calls = %d", source.dispatchCalls)
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
	ctx = WithToolExecutionIdentity(ctx, "/workspace/main", "turn-1")
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
