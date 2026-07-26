package tools

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/nodes"
	"github.com/sipeed/picoclaw/pkg/tools/loopguard"
)

type recordingNodeEventBus struct {
	mu     sync.Mutex
	events []runtimeevents.Event
}

func (bus *recordingNodeEventBus) Publish(
	_ context.Context,
	event runtimeevents.Event,
) runtimeevents.PublishResult {
	return bus.record(event)
}

func (bus *recordingNodeEventBus) PublishNonBlocking(
	event runtimeevents.Event,
) runtimeevents.PublishResult {
	return bus.record(event)
}

func (bus *recordingNodeEventBus) record(
	event runtimeevents.Event,
) runtimeevents.PublishResult {
	bus.mu.Lock()
	bus.events = append(bus.events, event)
	bus.mu.Unlock()
	return runtimeevents.PublishResult{Matched: 1, Delivered: 1}
}

func (*recordingNodeEventBus) Channel() runtimeevents.EventChannel { return nil }
func (*recordingNodeEventBus) Close() error                        { return nil }
func (*recordingNodeEventBus) Stats() runtimeevents.Stats          { return runtimeevents.Stats{} }

func (bus *recordingNodeEventBus) snapshot() []runtimeevents.Event {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	return append([]runtimeevents.Event(nil), bus.events...)
}

type fakeNodeInvocationSource struct {
	*fakeNodeDiscoverySource
	store          *nodes.GatewayInvocationStore
	preDispatchErr error
	dispatchErr    error
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
		return nil, false, nodes.ErrGatewayInvocationDispatched
	}
	if _, transitioned, err := source.store.MarkDispatched(
		owner,
		invocationID,
		expectedPlanHash,
	); err != nil {
		return nil, false, err
	} else if !transitioned {
		return nil, false, nodes.ErrGatewayInvocationDispatched
	}
	source.dispatchCalls++
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

func TestNodeInvokeToolReusesPreparedAuthorityAndDispatches(t *testing.T) {
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
		first["invocation_id"] != second["invocation_id"] ||
		source.prepareCalls != 1 {
		t.Fatalf("approval binding changed = %#v, %#v", first, second)
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

func TestNodeInvokeToolNamespacesProviderCallByExecutionAndWorkspace(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	args := nodeInvocationTestArgs()
	firstCtx := nodeInvocationTestContext("actor-1", "reused-call")
	first, err := tool.ApprovalArguments(firstCtx, args)
	if err != nil {
		t.Fatal(err)
	}

	nextExecutionCtx := WithToolExecutionIdentity(
		nodeInvocationTestContext("actor-1", "reused-call"),
		"/workspace/main",
		"execution-2",
	)
	nextExecution, err := tool.ApprovalArguments(nextExecutionCtx, args)
	if err != nil {
		t.Fatal(err)
	}
	otherWorkspaceCtx := WithToolExecutionIdentity(
		nodeInvocationTestContext("actor-1", "reused-call"),
		"/workspace/other",
		"execution-1",
	)
	otherWorkspace, err := tool.ApprovalArguments(otherWorkspaceCtx, args)
	if err != nil {
		t.Fatal(err)
	}
	if first["invocation_id"] == nextExecution["invocation_id"] ||
		first["invocation_id"] == otherWorkspace["invocation_id"] ||
		nextExecution["invocation_id"] == otherWorkspace["invocation_id"] {
		t.Fatalf(
			"execution namespaces collided: first=%v next=%v workspace=%v",
			first["invocation_id"],
			nextExecution["invocation_id"],
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
		WithToolExecutionIdentity(ctx, "/workspace/main", "execution-1"),
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
	if _, err := tool.ApprovalArguments(ctx, nodeInvocationTestArgs()); err != nil {
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

func TestNodeInvokeToolReportsPostDispatchUncertaintyWithoutReplay(t *testing.T) {
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

func TestNodeInvocationEventsUseProvenStatesAndRedactPayloads(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	eventBus := &recordingNodeEventBus{}
	tool.SetEventPublisher(eventBus)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	args := nodeInvocationTestArgs()
	args["input"] = map[string]any{
		"argv": []any{"git", "status", "super-secret-command-input"},
	}

	if _, err := tool.ApprovalArguments(ctx, args); err != nil {
		t.Fatal(err)
	}
	if result := tool.Execute(ctx, args); result.IsError {
		t.Fatalf("nodes_invoke failed: %s", result.ForLLM)
	}

	events := eventBus.snapshot()
	wantKinds := []runtimeevents.Kind{
		runtimeevents.KindNodeInvocationPrepared,
		runtimeevents.KindNodeInvocationDispatched,
		runtimeevents.KindNodeInvocationCompleted,
	}
	if len(events) != len(wantKinds) {
		t.Fatalf("event count = %d, want %d: %#v", len(events), len(wantKinds), events)
	}
	for index, event := range events {
		if event.Kind != wantKinds[index] {
			t.Fatalf("event[%d].Kind = %q, want %q", index, event.Kind, wantKinds[index])
		}
		payload, ok := event.Payload.(NodeInvocationEventPayload)
		if !ok {
			t.Fatalf("event[%d].Payload = %T", index, event.Payload)
		}
		if payload.Target != "build" || payload.Command != "system.exec.v1" ||
			payload.InvocationID == "" {
			t.Fatalf("event[%d] payload = %#v", index, payload)
		}
		if event.Scope.Workspace != "/workspace/main" ||
			event.Scope.TurnID != "execution-1" ||
			event.Scope.AgentID != "main" ||
			event.Scope.SessionKey != "route-session" ||
			event.Scope.Channel != "telegram" ||
			event.Scope.ChatID != "chat-1" ||
			event.Scope.SenderID != "actor-1" ||
			event.Correlation.RequestID != "call-1" {
			t.Fatalf("event[%d] scope = %#v correlation = %#v", index, event.Scope, event.Correlation)
		}
		wantGatewayState := nodes.GatewayInvocationPrepared
		if index > 0 {
			wantGatewayState = nodes.GatewayInvocationDispatched
		}
		if payload.GatewayState != wantGatewayState {
			t.Fatalf(
				"event[%d] gateway state = %q, want %q",
				index,
				payload.GatewayState,
				wantGatewayState,
			)
		}
	}
	encoded, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{
		"super-secret-command-input",
		"private-node-id",
		"plan_hash",
		"policy_revision",
		`\"stdout\"`,
	} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("audit events leaked %q: %s", forbidden, encoded)
		}
	}
}

func TestNodeInvocationEventsReportUncertainThenObservedFailure(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	source.dispatchErr = errors.New("sensitive transport endpoint disconnected")
	eventBus := &recordingNodeEventBus{}
	invoke := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	invoke.SetEventPublisher(eventBus)
	ctx := nodeInvocationTestContext("actor-1", "call-1")

	result := invoke.Execute(ctx, nodeInvocationTestArgs())
	if !result.IsError {
		t.Fatalf("nodes_invoke = %#v, want uncertain error", result)
	}
	invocationID := invocationIDFromError(t, result)
	record := mustFakeGatewayInvocation(t, source, ctx, invocationID)
	source.remote = failedRemoteInvocation(record)

	status := NewNodeStatusTool(nodeDiscoveryTestConfig(), source)
	status.SetEventPublisher(eventBus)
	statusResult := status.Execute(ctx, map[string]any{"invocation_id": invocationID})
	if statusResult.IsError {
		t.Fatalf("nodes_status failed: %#v", statusResult)
	}

	events := eventBus.snapshot()
	wantKinds := []runtimeevents.Kind{
		runtimeevents.KindNodeInvocationPrepared,
		runtimeevents.KindNodeInvocationDispatched,
		runtimeevents.KindNodeInvocationUncertain,
		runtimeevents.KindNodeInvocationCompleted,
	}
	if len(events) != len(wantKinds) {
		t.Fatalf("event count = %d, want %d: %#v", len(events), len(wantKinds), events)
	}
	for index, kind := range wantKinds {
		if events[index].Kind != kind {
			t.Fatalf("event[%d].Kind = %q, want %q", index, events[index].Kind, kind)
		}
	}
	uncertain := events[2].Payload.(NodeInvocationEventPayload)
	if uncertain.State != string(nodes.InvocationUnknown) ||
		uncertain.ErrorCode != "DISPATCH_UNCERTAIN" ||
		events[2].Severity != runtimeevents.SeverityWarn {
		t.Fatalf("uncertain event = %#v", events[2])
	}
	completed := events[3].Payload.(NodeInvocationEventPayload)
	if completed.State != string(nodes.InvocationFailed) ||
		completed.ErrorCode != "" {
		t.Fatalf("completed event = %#v", events[3])
	}
	encoded, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "sensitive transport endpoint") ||
		strings.Contains(string(encoded), "remote failure detail") {
		t.Fatalf("audit events leaked errors: %s", encoded)
	}
}

func TestNodeInvokeToolTreatsAlreadyDispatchedAsUncertain(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	if result := tool.Execute(ctx, nodeInvocationTestArgs()); result.IsError {
		t.Fatalf("first invoke = %#v", result)
	}
	result := tool.Execute(ctx, nodeInvocationTestArgs())
	if !result.IsError ||
		!strings.Contains(result.ForLLM, "DISPATCH_UNCERTAIN") ||
		!strings.Contains(result.ForLLM, "nodes_status") ||
		source.dispatchCalls != 1 {
		t.Fatalf("repeated invoke = %#v, dispatch calls = %d", result, source.dispatchCalls)
	}
}

func TestNodeInvokeToolDistinguishesPreDispatchDenial(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	source.preDispatchErr = errors.New("durable authority unavailable")
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	result := tool.Execute(ctx, nodeInvocationTestArgs())
	if !result.IsError ||
		!strings.Contains(result.ForLLM, "DISPATCH_DENIED") ||
		strings.Contains(result.ForLLM, "nodes_status") {
		t.Fatalf("pre-dispatch denial = %#v", result)
	}
	record := mustFakeGatewayInvocation(t, source, ctx, invocationIDFromError(t, result))
	if record.State != nodes.GatewayInvocationPrepared {
		t.Fatalf("pre-dispatch state = %q", record.State)
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

func TestNodeStatusToolReportsDisconnectedDispatchedInvocationAsUnknown(t *testing.T) {
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
		payload["node_available"] != false ||
		source.queryCalls != 0 {
		t.Fatalf("offline status = %#v, query calls = %d", payload, source.queryCalls)
	}
}

func TestNodeStatusToolReturnsPreparedStateWithoutQuery(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	invoke := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	approval, err := invoke.ApprovalArguments(ctx, nodeInvocationTestArgs())
	if err != nil {
		t.Fatal(err)
	}
	status := NewNodeStatusTool(nodeDiscoveryTestConfig(), source)
	payload := decodeNodeResult(t, status.Execute(ctx, map[string]any{
		"invocation_id": approval["invocation_id"],
	}))
	if payload["state"] != string(nodes.GatewayInvocationPrepared) || source.queryCalls != 0 {
		t.Fatalf("prepared status = %#v, query calls = %d", payload, source.queryCalls)
	}
}

func TestNodeInvocationToolRuntimeSemantics(t *testing.T) {
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
	ctx = WithToolExecutionIdentity(ctx, "/workspace/main", "execution-1")
	ctx = WithToolInboundContext(ctx, "telegram", "chat-1", "", "")
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

func invocationIDFromError(t *testing.T, result *ToolResult) string {
	t.Helper()
	var payload struct {
		Invocation nodeInvokeResult `json:"invocation"`
	}
	if err := json.Unmarshal([]byte(result.ForLLM), &payload); err != nil {
		t.Fatalf("decode invocation error %q: %v", result.ForLLM, err)
	}
	return payload.Invocation.InvocationID
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

func failedRemoteInvocation(
	gateway nodes.GatewayInvocationRecord,
) nodes.InvocationRecord {
	now := time.Now().UnixNano()
	return nodes.InvocationRecord{
		InvocationID: gateway.Plan.InvocationID, IdempotencyKey: gateway.Plan.IdempotencyKey,
		PlanHash: gateway.ExpectedPlanHash, NodeID: gateway.Plan.NodeID,
		CatalogHash: gateway.Plan.CatalogHash, Command: gateway.Plan.Command,
		Risk: nodes.RiskWrite, State: nodes.InvocationFailed,
		AcceptedAt: now, UpdatedAt: now, CompletedAt: now, ExpiresAt: gateway.Plan.ExpiresAt,
		Failure: &nodes.InvocationFailure{
			Code: "REMOTE_FAILED", Message: "remote failure detail",
		},
	}
}
