// PicoClaw - Ultra-lightweight personal AI agent

package agent

import "testing"

func TestInboundTurnCoordinatorClaimSessionSerializesSession(t *testing.T) {
	al := &AgentLoop{}
	coord := newInboundTurnCoordinator(al)

	placeholder, claimed := coord.claimSession("session-1")
	if !claimed {
		t.Fatal("expected first claim to succeed")
	}
	if placeholder == nil {
		t.Fatal("expected placeholder")
	}
	if !isPendingTurnState(placeholder) {
		t.Fatalf("placeholder turn id = %q, want pending turn", placeholder.turnID)
	}
	if got := al.getActiveTurnState("session-1"); got != placeholder {
		t.Fatalf("active turn = %p, want placeholder %p", got, placeholder)
	}

	second, claimed := coord.claimSession("session-1")
	if claimed {
		t.Fatalf("expected second claim to fail, got placeholder %p", second)
	}
	if got := al.getActiveTurnState("session-1"); got != placeholder {
		t.Fatalf("active turn changed after rejected claim: got %p, want %p", got, placeholder)
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

	coord.cleanupPlaceholder("session-1", first)
	if got := al.getActiveTurnState("session-1"); got != replacement {
		t.Fatalf("cleanup removed unowned placeholder: got %p, want replacement %p", got, replacement)
	}

	coord.cleanupPlaceholder("session-1", replacement)
	if got := al.getActiveTurnState("session-1"); got != nil {
		t.Fatalf("cleanup left owned placeholder active: got %p", got)
	}
}
