// PicoClaw - Ultra-lightweight personal AI agent

package agent

import "testing"

func TestInboundTurnCoordinatorClaimSessionSerializesSession(t *testing.T) {
	al := &AgentLoop{}
	coord := newInboundTurnCoordinator(al)

	claim, claimed := coord.claimSession("session-1")
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

	second, claimed := coord.claimSession("session-1")
	if claimed {
		t.Fatalf("expected second claim to fail, got placeholder %p", second)
	}
	if got := al.getActiveTurnState("session-1"); got != claim.placeholder {
		t.Fatalf("active turn changed after rejected claim: got %p, want %p", got, claim.placeholder)
	}
}

func TestInboundTurnCoordinatorCleanupOnlyClearsOwnedPlaceholder(t *testing.T) {
	al := &AgentLoop{}
	coord := newInboundTurnCoordinator(al)

	first, claimed := coord.claimSession("session-1")
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

	replacementClaim := &inboundSessionClaim{
		coordinator: coord,
		sessionKey:  "session-1",
		placeholder: replacement,
	}
	replacementClaim.releaseIfOwned()
	if got := al.getActiveTurnState("session-1"); got != nil {
		t.Fatalf("cleanup left owned placeholder active: got %p", got)
	}
}
