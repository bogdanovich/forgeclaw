package agent

import (
	"context"
	"testing"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/session"
)

func newInboundDispatchTestLoop(t *testing.T) (*AgentLoop, func()) {
	t.Helper()
	al, _, _, _, cleanup := newTestAgentLoop(t) //nolint:dogsled
	return al, cleanup
}

func TestBuildInboundMessageTurn_ConstructsDispatchEnvelope(t *testing.T) {
	al, cleanup := newInboundDispatchTestLoop(t)
	defer cleanup()

	msg := bus.NormalizeInboundMessage(bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:          "telegram",
			ChatID:           "-100123",
			ChatType:         "group",
			SenderID:         "telegram:42",
			MessageID:        "msg-1",
			ReplyToMessageID: "reply-1",
		},
		Sender: bus.SenderInfo{
			DisplayName: "Anton",
		},
		Content: "hello",
		Media:   []string{"media://one"},
	})

	turn, err := al.buildInboundMessageTurn(context.Background(), msg)
	if err != nil {
		t.Fatalf("buildInboundMessageTurn() error = %v", err)
	}
	defer turn.Cleanup()

	if turn.Agent == nil {
		t.Fatal("Agent is nil")
	}
	if turn.SessionKey == "" {
		t.Fatal("SessionKey is empty")
	}
	if turn.ScopeKey != turn.SessionKey {
		t.Fatalf("ScopeKey = %q, SessionKey = %q, want equal", turn.ScopeKey, turn.SessionKey)
	}
	if turn.Options.Dispatch.RouteSessionKey != turn.SessionKey {
		t.Fatalf(
			"Dispatch.RouteSessionKey = %q, want %q",
			turn.Options.Dispatch.RouteSessionKey,
			turn.SessionKey,
		)
	}
	if turn.Options.Dispatch.SessionKey != turn.SessionKey {
		t.Fatalf("Dispatch.SessionKey = %q, want %q", turn.Options.Dispatch.SessionKey, turn.SessionKey)
	}
	if turn.Options.Dispatch.Channel() != "telegram" || turn.Options.Dispatch.ChatID() != "-100123" {
		t.Fatalf(
			"dispatch addressing = (%q,%q), want (telegram,-100123)",
			turn.Options.Dispatch.Channel(),
			turn.Options.Dispatch.ChatID(),
		)
	}
	if turn.Options.Dispatch.MessageID() != "msg-1" ||
		turn.Options.Dispatch.ReplyToMessageID() != "reply-1" {
		t.Fatalf(
			"dispatch message ids = (%q,%q), want (msg-1,reply-1)",
			turn.Options.Dispatch.MessageID(),
			turn.Options.Dispatch.ReplyToMessageID(),
		)
	}
	if turn.Options.Dispatch.UserMessage != "hello" {
		t.Fatalf("Dispatch.UserMessage = %q, want hello", turn.Options.Dispatch.UserMessage)
	}
	if len(turn.Options.Dispatch.Media) != 1 || turn.Options.Dispatch.Media[0] != "media://one" {
		t.Fatalf("Dispatch.Media = %v, want [media://one]", turn.Options.Dispatch.Media)
	}
	if turn.Options.SenderID != "telegram:42" || turn.Options.SenderDisplayName != "Anton" {
		t.Fatalf(
			"sender fields = (%q,%q), want (telegram:42,Anton)",
			turn.Options.SenderID,
			turn.Options.SenderDisplayName,
		)
	}
	if turn.Options.ModelBinding.RouteSessionKey != turn.Options.Dispatch.RouteSessionKey {
		t.Fatalf(
			"ModelBinding.RouteSessionKey = %q, want %q",
			turn.Options.ModelBinding.RouteSessionKey,
			turn.Options.Dispatch.RouteSessionKey,
		)
	}
	if turn.Options.ModelBinding.WorkspaceAgent != turn.Agent {
		t.Fatal("ModelBinding.WorkspaceAgent does not match routed agent")
	}
}

func TestBuildInboundMessageTurn_PreservesExplicitSessionKey(t *testing.T) {
	al, cleanup := newInboundDispatchTestLoop(t)
	defer cleanup()

	explicitSessionKey := "agent:main:manual-session"
	msg := bus.NormalizeInboundMessage(bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  "telegram",
			ChatID:   "chat-1",
			ChatType: "private",
			SenderID: "telegram:42",
		},
		Content:    "hello",
		SessionKey: explicitSessionKey,
	})

	turn, err := al.buildInboundMessageTurn(context.Background(), msg)
	if err != nil {
		t.Fatalf("buildInboundMessageTurn() error = %v", err)
	}
	defer turn.Cleanup()

	if turn.SessionKey != explicitSessionKey {
		t.Fatalf("SessionKey = %q, want %q", turn.SessionKey, explicitSessionKey)
	}
	if turn.Options.Dispatch.SessionKey != explicitSessionKey {
		t.Fatalf("Dispatch.SessionKey = %q, want %q", turn.Options.Dispatch.SessionKey, explicitSessionKey)
	}
	if turn.Options.Dispatch.RouteSessionKey == explicitSessionKey {
		t.Fatalf("RouteSessionKey = explicit key %q, want routed session key", explicitSessionKey)
	}
	if len(turn.Options.Dispatch.SessionAliases) != 0 {
		t.Fatalf("SessionAliases = %v, want none for explicit canonical session", turn.Options.Dispatch.SessionAliases)
	}
}

func TestBuildInboundMessageTurn_UsesSessionOverrideAsEffectiveSession(t *testing.T) {
	al, cleanup := newInboundDispatchTestLoop(t)
	defer cleanup()

	msg := bus.NormalizeInboundMessage(bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  "telegram",
			ChatID:   "chat-1",
			ChatType: "private",
			SenderID: "telegram:42",
		},
		Content: "hello",
	})

	route, _, err := al.resolveMessageRoute(msg)
	if err != nil {
		t.Fatalf("resolveMessageRoute() error = %v", err)
	}
	allocation := al.allocateRouteSession(route, msg)
	overrideKey := session.BuildMainSessionKey("manual")
	if setErr := al.setSessionOverride(allocation.SessionKey, overrideKey); setErr != nil {
		t.Fatalf("setSessionOverride() error = %v", setErr)
	}

	turn, err := al.buildInboundMessageTurn(context.Background(), msg)
	if err != nil {
		t.Fatalf("buildInboundMessageTurn() error = %v", err)
	}
	defer turn.Cleanup()

	if turn.Options.Dispatch.RouteSessionKey != allocation.SessionKey {
		t.Fatalf(
			"RouteSessionKey = %q, want %q",
			turn.Options.Dispatch.RouteSessionKey,
			allocation.SessionKey,
		)
	}
	if turn.SessionKey != overrideKey {
		t.Fatalf("SessionKey = %q, want override %q", turn.SessionKey, overrideKey)
	}
	if len(turn.Options.Dispatch.SessionAliases) != 0 {
		t.Fatalf("SessionAliases = %v, want none when session override is active", turn.Options.Dispatch.SessionAliases)
	}
}

func TestBuildInboundMessageTurn_PreparesInboundMessage(t *testing.T) {
	al, cleanup := newInboundDispatchTestLoop(t)
	defer cleanup()

	msg := bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  "telegram",
			ChatID:   "chat-1",
			ChatType: "private",
			SenderID: "telegram:42",
		},
		Content: "hello",
	}

	turn, err := al.buildInboundMessageTurn(context.Background(), msg)
	if err != nil {
		t.Fatalf("buildInboundMessageTurn() error = %v", err)
	}
	defer turn.Cleanup()

	if turn.Message.Channel != "telegram" || turn.Message.ChatID != "chat-1" {
		t.Fatalf("prepared mirrors = (%q,%q), want (telegram,chat-1)", turn.Message.Channel, turn.Message.ChatID)
	}
	if turn.Options.Dispatch.Channel() != "telegram" || turn.Options.Dispatch.ChatID() != "chat-1" {
		t.Fatalf(
			"dispatch addressing = (%q,%q), want prepared context",
			turn.Options.Dispatch.Channel(),
			turn.Options.Dispatch.ChatID(),
		)
	}
}
