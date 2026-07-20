package agent

import (
	"testing"

	"github.com/sipeed/picoclaw/pkg/providers"
)

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
		if agent := al.agentForSession(sessionKey); agent != nil {
			return newRuntimeSessionScope(agent.Workspace, sessionKey)
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
