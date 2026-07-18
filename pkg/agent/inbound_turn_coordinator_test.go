// PicoClaw - Ultra-lightweight personal AI agent

package agent

import (
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/session"
)

func coordinatorTestTarget(routeScopeKey, sessionKey string) *inboundDispatchTarget {
	return &inboundDispatchTarget{
		Agent:         &AgentInstance{ID: "main"},
		RouteClaimKey: runtimeRouteClaimKey(routeScopeKey, ""),
		Allocation: session.Allocation{
			RouteScopeKey: routeScopeKey,
		},
		SessionKey: sessionKey,
	}
}

func TestInboundTurnCoordinatorClaimSessionSerializesSession(t *testing.T) {
	al := &AgentLoop{}
	coord := newInboundTurnCoordinator(al)

	firstTarget := coordinatorTestTarget("route-1", "session-1")
	claim, _, claimed := coord.claimSession(firstTarget)
	if !claimed {
		t.Fatal("expected first claim to succeed")
	}
	if claim == nil || claim.placeholder == nil {
		t.Fatal("expected claim with placeholder")
	}
	if claim.sessionKey != "session-1" {
		t.Fatalf("claim session key = %q, want session-1", claim.sessionKey)
	}
	if !isPendingTurnState(claim.placeholder) {
		t.Fatalf("placeholder turn id = %q, want pending turn", claim.placeholder.turnID)
	}
	if got := al.getActiveTurnState("session-1"); got != claim.placeholder {
		t.Fatalf("active turn = %p, want placeholder %p", got, claim.placeholder)
	}

	second, activeTarget, claimed := coord.claimSession(coordinatorTestTarget("route-1", "session-2"))
	if claimed {
		t.Fatalf("expected second claim to fail, got placeholder %p", second)
	}
	if activeTarget.SessionKey != "session-1" {
		t.Fatalf("active session key = %q, want session-1", activeTarget.SessionKey)
	}
	if activeTarget != firstTarget {
		t.Fatal("route claim did not retain the original dispatch target")
	}
	if got := al.getActiveTurnState("session-1"); got != claim.placeholder {
		t.Fatalf("active turn changed after rejected claim: got %p, want %p", got, claim.placeholder)
	}
}

func TestInboundTurnCoordinatorCleanupOnlyClearsOwnedPlaceholder(t *testing.T) {
	al := &AgentLoop{}
	coord := newInboundTurnCoordinator(al)

	first, _, claimed := coord.claimSession(coordinatorTestTarget("route-1", "session-1"))
	if !claimed {
		t.Fatal("expected first claim")
	}

	replacement := &turnState{
		turnID: makePendingTurnID("session-1", al.turnSeq.Add(1)),
		phase:  TurnPhaseSetup,
	}
	al.activeTurnStates.Store("session-1", replacement)

	first.releaseIfOwned()
	if got := al.getActiveTurnState("session-1"); got != replacement {
		t.Fatalf("cleanup removed unowned placeholder: got %p, want replacement %p", got, replacement)
	}

	replacementClaim := &runtimeSessionClaim{
		al:          al,
		sessionKey:  "session-1",
		placeholder: replacement,
	}
	replacementClaim.releaseIfOwned()
	if got := al.getActiveTurnState("session-1"); got != nil {
		t.Fatalf("cleanup left owned placeholder active: got %p", got)
	}
}

func TestInboundTurnCoordinatorPinsFollowUpAcrossCalendarBoundary(t *testing.T) {
	al, cfg, _, _, cleanup := newTestAgentLoop(t)
	defer cleanup()
	cfg.Session.Lifecycle = &config.SessionLifecycleConfig{
		Strategy: "calendar",
		Period:   "day",
		Timezone: "UTC",
	}
	now := time.Date(2026, 7, 17, 23, 59, 0, 0, time.UTC)
	al.sessionNow = func() time.Time { return now }
	msg := bus.NormalizeInboundMessage(bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  "telegram",
			ChatID:   "chat-1",
			ChatType: "direct",
			SenderID: "telegram:42",
		},
		Content: "first",
	})

	initial, ok := al.resolveSteeringTarget(msg)
	if !ok {
		t.Fatal("resolveSteeringTarget() failed for initial message")
	}
	coord := newInboundTurnCoordinator(al)
	claim, _, claimed := coord.claimSession(initial)
	if !claimed {
		t.Fatal("initial route claim failed")
	}
	defer claim.releaseIfOwned()

	now = now.Add(2 * time.Minute)
	followUp, ok := al.resolveSteeringTarget(msg)
	if !ok {
		t.Fatal("resolveSteeringTarget() failed for follow-up")
	}
	if followUp != initial || followUp.SessionKey != initial.SessionKey {
		t.Fatal("follow-up escaped the active epoch at calendar boundary")
	}
}

func TestInboundTurnCoordinatorFollowUpExtendsIdleEpoch(t *testing.T) {
	al, cfg, _, _, cleanup := newTestAgentLoop(t)
	defer cleanup()
	cfg.Session.Lifecycle = &config.SessionLifecycleConfig{
		Strategy:           "idle",
		IdleTimeoutMinutes: 30,
	}
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	al.sessionNow = func() time.Time { return now }
	msg := bus.NormalizeInboundMessage(bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  "telegram",
			ChatID:   "chat-1",
			ChatType: "direct",
			SenderID: "telegram:42",
		},
		Content: "first",
	})

	initial, ok := al.resolveSteeringTarget(msg)
	if !ok {
		t.Fatal("resolveSteeringTarget() failed for initial message")
	}
	coord := newInboundTurnCoordinator(al)
	claim, _, claimed := coord.claimSession(initial)
	if !claimed {
		t.Fatal("initial route claim failed")
	}

	now = now.Add(20 * time.Minute)
	followUp, ok := al.resolveSteeringTarget(msg)
	if !ok || followUp.SessionKey != initial.SessionKey {
		t.Fatal("follow-up did not remain in the active idle epoch")
	}
	claim.releaseIfOwned()

	now = now.Add(20 * time.Minute)
	next, ok := al.resolveSteeringTarget(msg)
	if !ok {
		t.Fatal("resolveSteeringTarget() failed after active turn")
	}
	if next.SessionKey != initial.SessionKey {
		t.Fatal("idle epoch rotated relative to initial activity instead of follow-up activity")
	}
}
