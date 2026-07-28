package ws

import (
	"context"
	"errors"
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

func TestTerminalEventSubscriptionAppliesByteAccurateBackpressure(t *testing.T) {
	subscription := &terminalEventSubscription{
		request: nodes.TerminalSessionRequest{
			TerminalID: "terminal_test",
			Owner:      testTerminalOwner(),
		},
		events: make(
			chan terminalEventResult,
			nodes.MaxTerminalTransportBuffer/nodes.MinTerminalTransportEventCharge,
		),
	}
	frameSize := nodes.MaxTerminalTransportFrameBytes
	for index := 0; index < nodes.MaxTerminalTransportBuffer/frameSize; index++ {
		if err := subscription.offer(nodes.TerminalEvent{
			TerminalID: "terminal_test",
			Type:       "output",
		}, frameSize); err != nil {
			t.Fatalf("offer frame %d: %v", index, err)
		}
	}
	if err := subscription.offer(nodes.TerminalEvent{
		TerminalID: "terminal_test",
		Type:       "ack",
	}, 1); !errors.Is(err, ErrTerminalEventBackpressure) {
		t.Fatalf("overflow offer error = %v", err)
	}
	if _, err := subscription.receive(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := subscription.offer(nodes.TerminalEvent{
		TerminalID: "terminal_test",
		Type:       "ack",
	}, 1); err != nil {
		t.Fatalf("released bytes did not admit bounded event: %v", err)
	}
}

func TestTerminalSubscriptionFailureDoesNotBlockOrDropQueuedEvents(t *testing.T) {
	subscription := &terminalEventSubscription{
		request: nodes.TerminalSessionRequest{
			TerminalID: "terminal_test",
			Owner:      testTerminalOwner(),
		},
		events: make(chan terminalEventResult, 2),
	}
	want := nodes.TerminalEvent{TerminalID: "terminal_test", Type: "ack", AcceptedSequence: 1}
	if err := subscription.offer(want, 1); err != nil {
		t.Fatal(err)
	}
	subscription.fail(ErrNodeDisconnected)
	got, err := subscription.receive(t.Context())
	if err != nil || got != want {
		t.Fatalf("queued event = (%#v, %v)", got, err)
	}
	if _, err := subscription.receive(context.Background()); !errors.Is(err, ErrNodeDisconnected) {
		t.Fatalf("terminal failure = %v", err)
	}
}

func testTerminalOwner() nodes.TerminalOwner {
	return nodes.TerminalOwner{
		ActorID:     "actor_test",
		AgentID:     "agent_test",
		RouteID:     "route_test",
		SessionID:   "session_test",
		WorkspaceID: "workspace_test",
		Target:      "target_test",
		Profile:     "owner",
	}
}
