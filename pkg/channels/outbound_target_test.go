package channels

import (
	"testing"

	"github.com/sipeed/picoclaw/pkg/bus"
)

func TestEffectiveOutboundChatID(t *testing.T) {
	ctx := &bus.InboundContext{ChatID: "ctx-chat"}
	if got := EffectiveOutboundChatID("explicit-chat", ctx); got != "explicit-chat" {
		t.Fatalf("explicit chat id = %q, want explicit-chat", got)
	}
	if got := EffectiveOutboundChatID("", ctx); got != "ctx-chat" {
		t.Fatalf("fallback chat id = %q, want ctx-chat", got)
	}
	if got := EffectiveOutboundChatID("  ", nil); got != "" {
		t.Fatalf("empty chat id without context = %q, want empty", got)
	}
}

func TestEffectiveOutboundTopicID(t *testing.T) {
	ctx := &bus.InboundContext{TopicID: "ctx-topic"}
	if got := EffectiveOutboundTopicID("explicit-topic", ctx); got != "explicit-topic" {
		t.Fatalf("explicit topic id = %q, want explicit-topic", got)
	}
	if got := EffectiveOutboundTopicID("", ctx); got != "ctx-topic" {
		t.Fatalf("fallback topic id = %q, want ctx-topic", got)
	}
	if got := EffectiveOutboundTopicID("  ", nil); got != "" {
		t.Fatalf("empty topic id without context = %q, want empty", got)
	}
}

func TestEffectiveOutboundReplyToMessageID(t *testing.T) {
	ctx := &bus.InboundContext{ReplyToMessageID: "ctx-reply"}
	if got := EffectiveOutboundReplyToMessageID("explicit-reply", ctx); got != "explicit-reply" {
		t.Fatalf("explicit reply id = %q, want explicit-reply", got)
	}
	if got := EffectiveOutboundReplyToMessageID("", ctx); got != "ctx-reply" {
		t.Fatalf("fallback reply id = %q, want ctx-reply", got)
	}
	if got := EffectiveOutboundReplyToMessageID("  ", nil); got != "" {
		t.Fatalf("empty reply id without context = %q, want empty", got)
	}
}
