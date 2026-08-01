package tools

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

type fakeNodeTerminalSource struct {
	*fakeNodeDiscoverySource
	mu              sync.Mutex
	record          nodes.GatewayTerminalRecord
	metadata        nodes.TerminalMetadata
	prepared        int
	opened          int
	bound           int
	signaled        int
	closed          int
	operatorSession string
}

func (source *fakeNodeTerminalSource) PrepareTerminal(
	nodeID nodes.ID,
	_ string,
	openID string,
	idempotencyKey string,
	owner nodes.TerminalOwner,
	workingScope string,
	columns int,
	rows int,
	allowCreate bool,
) (nodes.GatewayTerminalRecord, bool, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.record.Plan.OpenID != "" {
		candidate, err := nodes.PrepareTerminalOpenPlan(nodes.TerminalOpenPlan{
			OpenID: openID, IdempotencyKey: idempotencyKey, NodeID: nodeID, Owner: owner,
			CatalogHash: source.record.Plan.CatalogHash, AuthorityDigest: source.record.Plan.AuthorityDigest,
			WorkingScope: workingScope, Columns: columns, Rows: rows, ApprovalMode: "session_start",
		}, time.Unix(source.record.Plan.PreparedAt, 0), time.Minute)
		if err != nil || candidate.PlanHash != source.record.ExpectedPlanHash {
			return nodes.GatewayTerminalRecord{}, false, nodes.ErrGatewayTerminalConflict
		}
		return source.record, false, nil
	}
	if !allowCreate {
		return nodes.GatewayTerminalRecord{}, false, nodes.ErrGatewayTerminalNotFound
	}
	record, err := nodes.PrepareTerminalOpenPlan(nodes.TerminalOpenPlan{
		OpenID: openID, IdempotencyKey: idempotencyKey, NodeID: nodeID, Owner: owner,
		CatalogHash: strings.Repeat("a", 64), AuthorityDigest: strings.Repeat("b", 64),
		WorkingScope: workingScope, Columns: columns, Rows: rows, ApprovalMode: "session_start",
	}, time.Now(), time.Minute)
	if err != nil {
		return nodes.GatewayTerminalRecord{}, false, err
	}
	source.record = nodes.GatewayTerminalRecord{
		Plan: record, ExpectedPlanHash: record.PlanHash, State: nodes.GatewayTerminalPrepared,
		CreatedAt: time.Now().UnixNano(), UpdatedAt: time.Now().UnixNano(),
	}
	source.prepared++
	return source.record, true, nil
}

func (source *fakeNodeTerminalSource) OpenTerminal(
	_ context.Context,
	owner nodes.TerminalOwner,
	openID string,
	expectedPlanHash string,
) (nodes.TerminalMetadata, bool, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.record.Plan.Owner != owner ||
		source.record.Plan.OpenID != openID ||
		source.record.ExpectedPlanHash != expectedPlanHash {
		return nodes.TerminalMetadata{}, false, nodes.ErrGatewayTerminalConflict
	}
	source.opened++
	source.metadata = nodes.TerminalMetadata{
		TerminalID: "terminal_test", Owner: owner,
		State: string(nodes.GatewayTerminalPendingAttach), StartedAt: time.Now().Unix(),
	}
	return source.metadata, true, nil
}

func (source *fakeNodeTerminalSource) TerminalStatus(
	_ context.Context,
	owner nodes.TerminalOwner,
	terminalID string,
) (nodes.TerminalMetadata, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.metadata.Owner != owner || source.metadata.TerminalID != terminalID {
		return nodes.TerminalMetadata{}, nodes.ErrGatewayTerminalConflict
	}
	return source.metadata, nil
}

func (source *fakeNodeTerminalSource) SignalTerminal(
	_ context.Context,
	owner nodes.TerminalOwner,
	terminalID string,
	signal string,
) (nodes.TerminalMetadata, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.metadata.Owner != owner ||
		source.metadata.TerminalID != terminalID ||
		!slicesContains([]string{"INT", "TERM", "HUP"}, signal) {
		return nodes.TerminalMetadata{}, nodes.ErrGatewayTerminalConflict
	}
	source.signaled++
	return source.metadata, nil
}

func (source *fakeNodeTerminalSource) CloseTerminal(
	_ context.Context,
	owner nodes.TerminalOwner,
	terminalID string,
) (nodes.TerminalMetadata, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.metadata.Owner != owner || source.metadata.TerminalID != terminalID {
		return nodes.TerminalMetadata{}, nodes.ErrGatewayTerminalConflict
	}
	source.closed++
	source.metadata.State = string(nodes.GatewayTerminalClosed)
	source.metadata.Reason = "close"
	source.metadata.CompletedAt = source.metadata.StartedAt + 1
	source.metadata.TerminationConfirmed = true
	return source.metadata, nil
}

func (source *fakeNodeTerminalSource) BindTerminalOperator(
	owner nodes.TerminalOwner,
	terminalID string,
	operatorSessionID string,
) error {
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.metadata.Owner != owner || source.metadata.TerminalID != terminalID {
		return nodes.ErrGatewayTerminalConflict
	}
	source.bound++
	source.operatorSession = operatorSessionID
	return nil
}

func TestNodeTerminalToolDiscoversOnlySafeOwnerAliases(t *testing.T) {
	source := newFakeNodeTerminalSource(t)
	tool := NewNodeTerminalTool(nodeDiscoveryTestConfig(), source)
	parameters, err := json.Marshal(tool.Parameters())
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"input_base64", "data_base64", "transcript", "environment"} {
		if strings.Contains(string(parameters), forbidden) {
			t.Fatalf("model terminal schema exposed %q: %s", forbidden, parameters)
		}
	}
	result := tool.Execute(
		nodeTerminalTestContext("actor-1", "call-discover"),
		map[string]any{"action": "discover", "target": "build"},
	)
	payload := decodeNodeResult(t, result)
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if payload["approval"] != "session_start" ||
		!strings.Contains(string(raw), `"owner"`) ||
		!strings.Contains(string(raw), `"workspace"`) {
		t.Fatalf("terminal discovery = %s", raw)
	}
	for _, forbidden := range []string{
		"authority_digest", "policy_revision", "node_id", "shell", "uid", "broker", "environment",
	} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("terminal discovery exposed %q: %s", forbidden, raw)
		}
	}
}

func TestNodeTerminalToolBindsApprovalAndAuthenticatedOperatorSession(t *testing.T) {
	source := newFakeNodeTerminalSource(t)
	tool := NewNodeTerminalTool(nodeDiscoveryTestConfig(), source)
	ctx := nodeTerminalTestContext("actor-1", "call-open")
	args := nodeTerminalOpenArgs(t, tool, ctx)
	approval, err := tool.ApprovalArguments(ctx, args)
	if err != nil {
		t.Fatal(err)
	}
	approvalJSON, err := json.Marshal(approval)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"plan_hash", "authority", "node_id", "route_id", "actor_id"} {
		if strings.Contains(string(approvalJSON), forbidden) {
			t.Fatalf("approval exposed %q: %s", forbidden, approvalJSON)
		}
	}
	denied := decodeNodeTerminalResult(t, tool.Execute(ctx, args))
	if denied["code"] != nodeDenialApprovalRequired || source.opened != 0 {
		t.Fatalf("unapproved terminal open = %#v; opened=%d", denied, source.opened)
	}
	resumed := WithToolApprovalContinuation(ctx, true)
	opened := decodeNodeResult(t, tool.Execute(resumed, args))
	if opened["state"] != string(nodes.GatewayTerminalPendingAttach) ||
		source.prepared != 1 ||
		source.opened != 1 ||
		source.bound != 1 ||
		source.operatorSession != "operator-1" {
		t.Fatalf(
			"approved terminal open = %#v; prepared=%d opened=%d bound=%d session=%q",
			opened,
			source.prepared,
			source.opened,
			source.bound,
			source.operatorSession,
		)
	}

	bypassSource := newFakeNodeTerminalSource(t)
	bypassTool := NewNodeTerminalTool(nodeDiscoveryTestConfig(), bypassSource)
	bypassCtx := nodeTerminalTestContext("actor-1", "call-open-bypass")
	bypassArgs := nodeTerminalOpenArgs(t, bypassTool, bypassCtx)
	bypassed := decodeNodeResult(
		t,
		bypassTool.Execute(WithToolApprovalBypass(bypassCtx, true), bypassArgs),
	)
	if bypassed["state"] != string(nodes.GatewayTerminalPendingAttach) ||
		bypassSource.prepared != 1 ||
		bypassSource.opened != 1 {
		t.Fatalf(
			"allow-all terminal open = %#v; prepared=%d opened=%d",
			bypassed,
			bypassSource.prepared,
			bypassSource.opened,
		)
	}
}

func TestNodeTerminalOperatorOpensWithSharedAuthorityChecks(t *testing.T) {
	source := newFakeNodeTerminalSource(t)
	operator := NewNodeTerminalOperator(nodeDiscoveryTestConfig(), source)
	owner := nodes.TerminalOwner{
		ActorID: "operator_test", AgentID: "agent_test", RouteID: "route_test",
		SessionID: "session_test", WorkspaceID: "workspace_test",
		Target: "build", Profile: "owner",
	}
	result, err := operator.Open(t.Context(), NodeTerminalOperatorOpenRequest{
		AgentID: "main", OperatorSessionID: "operator-session", RequestID: "request-one",
		Owner: owner, Target: "build", Profile: "owner", WorkingScope: "workspace",
		Columns: 100, Rows: 40,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.TerminalID != "terminal_test" ||
		result.State != string(nodes.GatewayTerminalPendingAttach) ||
		source.prepared != 1 || source.opened != 1 || source.bound != 1 ||
		source.operatorSession != "operator-session" {
		t.Fatalf("operator open = %#v, source = %#v", result, source)
	}
}

func TestNodeTerminalOperatorDeniesInvisibleTargetAndProfile(t *testing.T) {
	for _, test := range []struct {
		name    string
		target  string
		profile string
	}{
		{name: "target", target: "private", profile: "owner"},
		{name: "profile", target: "build", profile: "root"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := newFakeNodeTerminalSource(t)
			operator := NewNodeTerminalOperator(nodeDiscoveryTestConfig(), source)
			owner := nodes.TerminalOwner{
				ActorID: "operator_test", AgentID: "agent_test", RouteID: "route_test",
				SessionID: "session_test", WorkspaceID: "workspace_test",
				Target: test.target, Profile: test.profile,
			}
			_, err := operator.Open(t.Context(), NodeTerminalOperatorOpenRequest{
				AgentID: "main", OperatorSessionID: "operator-session", RequestID: "request-one",
				Owner: owner, Target: test.target, Profile: test.profile, WorkingScope: "workspace",
				Columns: 100, Rows: 40,
			})
			if err == nil || source.opened != 0 || source.bound != 0 {
				t.Fatalf("denied operator open error = %v, source = %#v", err, source)
			}
		})
	}
}

func TestNodeTerminalToolDeniesDifferentOwnerAndNonOperatorRoute(t *testing.T) {
	source := newFakeNodeTerminalSource(t)
	tool := NewNodeTerminalTool(nodeDiscoveryTestConfig(), source)
	ctx := nodeTerminalTestContext("actor-1", "call-open")
	args := nodeTerminalOpenArgs(t, tool, ctx)
	if _, err := tool.ApprovalArguments(ctx, args); err != nil {
		t.Fatal(err)
	}
	if result := tool.Execute(WithToolApprovalContinuation(ctx, true), args); result.IsError {
		t.Fatalf("open failed: %s", result.ForLLM)
	}
	statusArgs := map[string]any{
		"action": "status", "target": "build", "profile": "owner",
		"terminal_id": "terminal_test",
	}
	otherActor := nodeTerminalTestContext("actor-2", "call-status")
	denied := decodeNodeTerminalResult(t, tool.Execute(otherActor, statusArgs))
	if denied["state"] != "denied" || source.signaled != 0 || source.closed != 0 {
		t.Fatalf("cross-owner status = %#v", denied)
	}
	for _, test := range []struct {
		name string
		ctx  context.Context
		args map[string]any
	}{
		{
			name: "agent",
			ctx: WithToolSessionContext(
				ctx,
				"secondary",
				"history-session",
				nil,
			),
			args: statusArgs,
		},
		{name: "route session", ctx: WithToolRouteSessionKey(ctx, "other-route"), args: statusArgs},
		{
			name: "workspace",
			ctx:  WithToolExecutionIdentity(ctx, "/workspace/other", "execution-1"),
			args: statusArgs,
		},
		{
			name: "target",
			ctx:  ctx,
			args: map[string]any{
				"action": "status", "target": "other", "profile": "owner",
				"terminal_id": "terminal_test",
			},
		},
		{
			name: "profile",
			ctx:  ctx,
			args: map[string]any{
				"action": "status", "target": "build", "profile": "other",
				"terminal_id": "terminal_test",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := decodeNodeTerminalResult(t, tool.Execute(test.ctx, test.args))
			if result["state"] != "denied" {
				t.Fatalf("cross-owner status = %#v", result)
			}
		})
	}

	nonOperator := nodeInvocationTestContext("actor-1", "call-telegram")
	nonOperatorDiscovery := decodeNodeTerminalResult(t, tool.Execute(
		nonOperator,
		map[string]any{"action": "discover", "target": "build"},
	))
	if nonOperatorDiscovery["state"] != "denied" {
		t.Fatalf("non-MintClaw discovery = %#v", nonOperatorDiscovery)
	}
	if _, err := tool.ApprovalArguments(nonOperator, args); err == nil {
		t.Fatal("non-MintClaw route prepared a terminal")
	}
}

func TestNodeTerminalEventsAreRedacted(t *testing.T) {
	source := newFakeNodeTerminalSource(t)
	tool := NewNodeTerminalTool(nodeDiscoveryTestConfig(), source)
	eventBus := &recordingNodeEventBus{}
	tool.SetEventPublisher(eventBus)
	ctx := nodeTerminalTestContext("actor-1", "call-events")
	args := nodeTerminalOpenArgs(t, tool, ctx)
	if _, err := tool.ApprovalArguments(ctx, args); err != nil {
		t.Fatal(err)
	}
	_ = tool.Execute(WithToolApprovalContinuation(ctx, true), args)
	events := eventBus.snapshot()
	if len(events) == 0 {
		t.Fatal("terminal lifecycle event was not published")
	}
	raw, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{
		"input_base64", "data_base64", "transcript", "authority_digest",
		"script", "environment", "credential", "broker",
	} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("terminal events exposed %q: %s", forbidden, raw)
		}
	}
}

func TestNodeTerminalMetadataViewDropsUntrustedReasonAndSignal(t *testing.T) {
	owner := nodes.TerminalOwner{Target: "build", Profile: "owner"}
	view := nodeTerminalMetadataView(owner, nodes.TerminalMetadata{
		TerminalID: "terminal_test", State: "unknown",
		Reason: "SECRET_ENV_VALUE", Signal: "private credential",
	})
	raw, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "SECRET_ENV_VALUE") ||
		strings.Contains(string(raw), "private credential") {
		t.Fatalf("terminal metadata exposed untrusted node text: %s", raw)
	}
}

func newFakeNodeTerminalSource(t *testing.T) *fakeNodeTerminalSource {
	t.Helper()
	command := shellNodeInvocationTestDescriptor()
	catalog := nodes.CapabilityCatalog{Commands: []nodes.CommandDescriptor{command}}
	catalogHash := mustCatalogHash(t, catalog)
	snapshot := nodes.Snapshot{
		ID: "private-node-id", State: nodes.StateConnected, Catalog: catalog,
		CatalogHash: catalogHash, Executor: "local", PolicyRevision: "policy-1",
	}
	return &fakeNodeTerminalSource{
		fakeNodeDiscoverySource: &fakeNodeDiscoverySource{
			byRef: map[string]nodes.Snapshot{"builder-node": snapshot},
			registrations: map[nodes.ID]nodes.Registration{
				snapshot.ID: {
					Snapshot: snapshot, AllowedCommands: []string{command.Name},
					ApprovedCatalogHash: catalogHash, ApprovedAt: 1,
				},
			},
			connected: map[nodes.ID]bool{snapshot.ID: true},
		},
	}
}

func nodeTerminalTestContext(actorID, toolCallID string) context.Context {
	ctx := WithToolSessionContext(context.Background(), "main", "history-session", nil)
	ctx = WithToolRouteSessionKey(ctx, "route-session")
	ctx = WithToolExecutionIdentity(ctx, "/workspace/main", "execution-1")
	ctx = WithToolInboundContext(ctx, "mintclaw", "mintclaw:operator-1", "", "")
	ctx = WithToolInboundMetadata(ctx, bus.InboundContext{
		Channel: "mintclaw", ChatID: "mintclaw:operator-1",
		SenderID: actorID, ActorID: actorID,
	})
	return WithToolCallID(ctx, toolCallID)
}

func nodeTerminalOpenArgs(
	t *testing.T,
	tool *NodeTerminalTool,
	ctx context.Context,
) map[string]any {
	t.Helper()
	discovered := decodeNodeResult(t, tool.Execute(
		ctx,
		map[string]any{"action": "discover", "target": "build"},
	))
	return map[string]any{
		"action": "open", "target": "build", "profile": "owner",
		"working_scope": "workspace", "columns": 100, "rows": 40,
		"discovery_revision": discovered["discovery_revision"],
	}
}

func slicesContains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func decodeNodeTerminalResult(t *testing.T, result *ToolResult) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(result.ForLLM), &payload); err != nil {
		t.Fatalf("decode terminal result %q: %v", result.ForLLM, err)
	}
	return payload
}

var _ NodeTerminalSource = (*fakeNodeTerminalSource)(nil)
