package agent

import (
	"context"
	"testing"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/memory"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/session"
	"github.com/sipeed/picoclaw/pkg/tools"
)

type resetTrackingMessageTool struct {
	resetSessions []string
}

func (t *resetTrackingMessageTool) Name() string        { return "message" }
func (t *resetTrackingMessageTool) Description() string { return "test message tool" }
func (t *resetTrackingMessageTool) Parameters() map[string]any {
	return map[string]any{"type": "object"}
}

func (t *resetTrackingMessageTool) Execute(context.Context, map[string]any) *tools.ToolResult {
	return &tools.ToolResult{}
}

func (t *resetTrackingMessageTool) ResetSentInRound(sessionKey string) {
	t.resetSessions = append(t.resetSessions, sessionKey)
}

func testSessionScope(sessionKey string) runtimeSessionScope {
	return newRuntimeSessionScope("/test/workspace", sessionKey)
}

func testRuntimeSessionScope(al *AgentLoop, sessionKey string) runtimeSessionScope {
	if al != nil {
		if turn, ambiguous := al.uniqueActiveTurnForSession(sessionKey); turn != nil && !ambiguous {
			return turn.runtimeSessionScope()
		}
		if al.steering != nil {
			if scope, found, ambiguous := al.steering.uniqueScopeForSession(sessionKey); found && !ambiguous {
				return scope
			}
		}
		if registry := al.GetRegistry(); registry != nil {
			var found runtimeSessionScope
			for _, agentID := range registry.ListAgentIDs() {
				agent, ok := registry.GetAgent(agentID)
				if !ok || session.ResolveAgentID(agent.Sessions, sessionKey) == "" {
					continue
				}
				candidate := newRuntimeSessionScope(agent.Workspace, sessionKey)
				if found.complete() && found != candidate {
					return runtimeSessionScope{sessionKey: sessionKey}
				}
				found = candidate
			}
			if found.complete() {
				return found
			}
		}
	}
	return runtimeSessionScope{sessionKey: sessionKey}
}

func steerActiveForTest(al *AgentLoop, msg providers.Message) error {
	active := al.GetActiveTurn()
	if active == nil {
		return al.Steer("", "", "", msg)
	}
	agent := al.agentByRuntimeIDForTest(active.AgentID)
	workspace := ""
	if agent != nil {
		workspace = agent.Workspace
	}
	return al.Steer(workspace, active.SessionKey, active.AgentID, msg)
}

func (al *AgentLoop) agentByRuntimeIDForTest(agentID string) *AgentInstance {
	if al == nil || al.GetRegistry() == nil {
		return nil
	}
	agent, _ := al.GetRegistry().GetAgent(agentID)
	return agent
}

func TestRuntimeSessionClaimsAreWorkspaceScoped(t *testing.T) {
	al := &AgentLoop{}
	first := newRuntimeSessionScope("/workspace/first", "shared-session")
	second := newRuntimeSessionScope("/workspace/second", "shared-session")

	firstClaim, firstClaimed := al.claimRuntimeSession(first, "pending-first")
	secondClaim, secondClaimed := al.claimRuntimeSession(second, "pending-second")
	if !firstClaimed || !secondClaimed {
		t.Fatalf("claims = (%t, %t), want both workspaces claimed", firstClaimed, secondClaimed)
	}
	t.Cleanup(firstClaim.releaseIfOwned)
	t.Cleanup(secondClaim.releaseIfOwned)

	if got := al.getActiveTurnState(first); got != firstClaim.placeholder {
		t.Fatalf("first active turn = %p, want %p", got, firstClaim.placeholder)
	}
	if got := al.getActiveTurnState(second); got != secondClaim.placeholder {
		t.Fatalf("second active turn = %p, want %p", got, secondClaim.placeholder)
	}
	if _, ambiguous := al.uniqueActiveTurnForSession("shared-session"); !ambiguous {
		t.Fatal("session-only lookup did not fail closed across workspaces")
	}
}

func TestRuntimeRouteClaimsAreWorkspaceScoped(t *testing.T) {
	al := &AgentLoop{}
	first := &inboundDispatchTarget{
		Agent:      &AgentInstance{ID: "first", Workspace: "/workspace/first"},
		SessionKey: "shared-session", RouteClaimKey: "route:shared",
	}
	second := &inboundDispatchTarget{
		Agent:      &AgentInstance{ID: "second", Workspace: "/workspace/second"},
		SessionKey: "shared-session", RouteClaimKey: "route:shared",
	}

	firstClaim, _, firstClaimed := al.claimRuntimeRouteSession(first, "pending-first")
	secondClaim, _, secondClaimed := al.claimRuntimeRouteSession(second, "pending-second")
	if !firstClaimed || !secondClaimed {
		t.Fatalf("route claims = (%t, %t), want both workspaces claimed", firstClaimed, secondClaimed)
	}
	t.Cleanup(firstClaim.releaseIfOwned)
	t.Cleanup(secondClaim.releaseIfOwned)
}

func TestSteeringQueueSeparatesIdenticalSessionsAcrossWorkspaces(t *testing.T) {
	queue := newSteeringQueue(SteeringAll)
	first := newRuntimeSessionScope("/workspace/first", "shared-session")
	second := newRuntimeSessionScope("/workspace/second", "shared-session")
	if err := queue.pushScopeWithSender(first, providers.Message{Content: "first"}, "user"); err != nil {
		t.Fatal(err)
	}
	if err := queue.pushScopeWithSender(second, providers.Message{Content: "second"}, "user"); err != nil {
		t.Fatal(err)
	}

	firstBatch := queue.dequeueScopeForContinuationBatch(first)
	if len(firstBatch.entries) != 1 || firstBatch.entries[0].msg.Content != "first" {
		t.Fatalf("first batch = %#v", firstBatch)
	}
	if got := queue.lenScope(second); got != 1 {
		t.Fatalf("second queue depth = %d, want 1", got)
	}
	secondBatch := queue.dequeueScopeForContinuationBatch(second)
	if len(secondBatch.entries) != 1 || secondBatch.entries[0].msg.Content != "second" {
		t.Fatalf("second batch = %#v", secondBatch)
	}
}

func TestPendingStopsAreWorkspaceScoped(t *testing.T) {
	al := &AgentLoop{}
	first := newRuntimeSessionScope("/workspace/first", "shared-session")
	second := newRuntimeSessionScope("/workspace/second", "shared-session")
	al.markPendingStop(first)

	if al.takePendingStop(second) {
		t.Fatal("second workspace consumed the first workspace stop")
	}
	if !al.takePendingStop(first) {
		t.Fatal("first workspace did not retain its pending stop")
	}
}

func TestPendingSkillsAreWorkspaceScoped(t *testing.T) {
	al := &AgentLoop{}
	first := newRuntimeSessionScope("/workspace/first", "shared-session")
	second := newRuntimeSessionScope("/workspace/second", "shared-session")
	al.setPendingSkills(first, []string{"first-skill"})
	al.setPendingSkills(second, []string{"second-skill"})

	if got := al.takePendingSkills(first); len(got) != 1 || got[0] != "first-skill" {
		t.Fatalf("first pending skills = %#v", got)
	}
	if got := al.takePendingSkills(second); len(got) != 1 || got[0] != "second-skill" {
		t.Fatalf("second pending skills = %#v", got)
	}
}

func TestContinuationConsumesOnlyItsWorkspaceQueue(t *testing.T) {
	al, cfg, msgBus, provider, cleanup := newTestAgentLoop(t)
	defer cleanup()
	_ = cfg
	_ = msgBus
	_ = provider
	agent := al.GetRegistry().GetDefaultAgent()
	first := newRuntimeSessionScope(agent.Workspace, "shared-session")
	second := newRuntimeSessionScope(t.TempDir(), "shared-session")
	if err := al.enqueueSteeringMessageWithSender(
		first, agent.ID, "user", providers.Message{Role: "user", Content: "first"},
	); err != nil {
		t.Fatal(err)
	}
	if err := al.steering.pushScopeWithSender(
		second, providers.Message{Role: "user", Content: "second"}, "user",
	); err != nil {
		t.Fatal(err)
	}

	response, err := al.continueRuntimeSession(t.Context(), &continuationTarget{
		AgentID: agent.ID, Workspace: first.workspace, SessionKey: first.sessionKey,
		Channel: "test", ChatID: "chat-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if response == "" {
		t.Fatal("continuation returned an empty response")
	}
	if got := al.steering.lenScope(first); got != 0 {
		t.Fatalf("first queue depth = %d, want 0", got)
	}
	if got := al.steering.lenScope(second); got != 1 {
		t.Fatalf("second queue depth = %d, want 1", got)
	}
}

func TestMessageToolResetUsesScopedAgent(t *testing.T) {
	firstTool := &resetTrackingMessageTool{}
	secondTool := &resetTrackingMessageTool{}
	firstRegistry := tools.NewToolRegistry()
	secondRegistry := tools.NewToolRegistry()
	firstRegistry.Register(firstTool)
	secondRegistry.Register(secondTool)
	al := &AgentLoop{registry: &AgentRegistry{agents: map[string]*AgentInstance{
		"first": {
			ID: "first", Workspace: "/workspace/first", Tools: firstRegistry,
		},
		"second": {
			ID: "second", Workspace: "/workspace/second", Tools: secondRegistry,
		},
	}}}

	al.resetMessageToolRound(
		newRuntimeSessionScope("/workspace/second", "shared-session"), "second",
	)
	if len(firstTool.resetSessions) != 0 {
		t.Fatalf("default workspace resets = %#v, want none", firstTool.resetSessions)
	}
	if len(secondTool.resetSessions) != 1 || secondTool.resetSessions[0] != "shared-session" {
		t.Fatalf("target workspace resets = %#v", secondTool.resetSessions)
	}
}

func TestInboundRecoveryBlockScopeIncludesRoutedWorkspace(t *testing.T) {
	firstWorkspace := t.TempDir()
	secondWorkspace := t.TempDir()
	cfg := &config.Config{Agents: config.AgentsConfig{
		Defaults: config.AgentDefaults{Workspace: firstWorkspace, ModelName: "test-model"},
		List: []config.AgentConfig{
			{ID: "first", Default: true},
			{ID: "second", Workspace: secondWorkspace},
		},
		Dispatch: &config.DispatchConfig{Rules: []config.DispatchRule{{
			Name: "second-channel", Agent: "second",
			When: config.DispatchSelector{Channel: "second"},
		}}},
	}}
	al := NewAgentLoop(cfg, bus.NewMessageBus(), &simpleMockProvider{response: "ok"})
	defer al.Close()

	first := al.runtimeScopeForInboundRecoveryBlock(bus.InboundMessage{
		Context: bus.InboundContext{Channel: "first"}, SessionKey: "shared-session",
	})
	second := al.runtimeScopeForInboundRecoveryBlock(bus.InboundMessage{
		Context: bus.InboundContext{Channel: "second"}, SessionKey: "shared-session",
	})
	if first.sessionKey != second.sessionKey || first.workspace == second.workspace {
		t.Fatalf("recovery scopes = %#v and %#v", first, second)
	}
	if first.workspace != normalizeRuntimeWorkspace(firstWorkspace) ||
		second.workspace != normalizeRuntimeWorkspace(secondWorkspace) {
		t.Fatalf("recovery workspaces = %q and %q", first.workspace, second.workspace)
	}
}

func TestLegacyContextManagerUsesExplicitSessionOwner(t *testing.T) {
	firstStore := session.NewSessionManager("")
	secondStore := session.NewSessionManager("")
	firstStore.GetOrCreate("shared-session")
	secondStore.GetOrCreate("shared-session")
	firstStore.SetHistory("shared-session", []providers.Message{{Role: "user", Content: "first"}})
	secondStore.SetHistory("shared-session", []providers.Message{{Role: "user", Content: "second"}})
	first := &AgentInstance{ID: "first", Workspace: "/workspace/first", Sessions: firstStore}
	second := &AgentInstance{ID: "second", Workspace: "/workspace/second", Sessions: secondStore}
	mgr := &legacyContextManager{al: &AgentLoop{registry: &AgentRegistry{agents: map[string]*AgentInstance{
		"first": first, "second": second,
	}}}}

	resp, err := mgr.Assemble(context.Background(), &AssembleRequest{
		Agent: second, SessionKey: "shared-session",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.History) != 1 || resp.History[0].Content != "second" {
		t.Fatalf("history = %#v, want second workspace history", resp.History)
	}
}

func TestMessageToolSuppressionUsesExplicitSessionOwner(t *testing.T) {
	first := tools.NewMessageTool()
	second := tools.NewMessageTool()
	first.SetSendCallback(func(
		context.Context, string, string, string, string, []bus.MediaPart,
	) error {
		return nil
	})
	result := first.Execute(
		tools.WithToolSessionContext(context.Background(), "first", "shared-session", nil),
		map[string]any{"content": "sent", "channel": "test", "chat_id": "same"},
	)
	if result == nil || result.IsError {
		t.Fatalf("message execute = %#v", result)
	}
	firstRegistry := tools.NewToolRegistry()
	secondRegistry := tools.NewToolRegistry()
	firstRegistry.Register(first)
	secondRegistry.Register(second)
	firstAgent := &AgentInstance{ID: "first", Tools: firstRegistry}
	secondAgent := &AgentInstance{ID: "second", Tools: secondRegistry}

	if !messageToolSentToSameChat(firstAgent, "shared-session", "test", "same") {
		t.Fatal("first owner should report its sent message")
	}
	if messageToolSentToSameChat(secondAgent, "shared-session", "test", "same") {
		t.Fatal("second owner inherited first owner message state")
	}
}

func TestAgentForRuntimeScopeFailsClosedOnAmbiguousStoredOwners(t *testing.T) {
	firstJSONL, err := memory.NewJSONLStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	secondJSONL, err := memory.NewJSONLStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	firstStore := session.NewJSONLBackend(firstJSONL)
	secondStore := session.NewJSONLBackend(secondJSONL)
	key := "shared-session"
	firstStore.EnsureSessionMetadata(key, &session.SessionScope{
		Version: session.ScopeVersionV1, AgentID: "first",
	}, nil)
	secondStore.EnsureSessionMetadata(key, &session.SessionScope{
		Version: session.ScopeVersionV1, AgentID: "second",
	}, nil)
	al := &AgentLoop{registry: &AgentRegistry{agents: map[string]*AgentInstance{
		"first": {
			ID: "first", Workspace: "/workspace/shared", Sessions: firstStore,
		},
		"second": {
			ID: "second", Workspace: "/workspace/shared", Sessions: secondStore,
		},
	}}}

	if got := al.agentForRuntimeScope(
		newRuntimeSessionScope("/workspace/shared", key), "",
	); got != nil {
		t.Fatalf("ambiguous owner = %q, want nil", got.ID)
	}
}
