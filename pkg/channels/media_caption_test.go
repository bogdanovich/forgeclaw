package channels

import (
	"testing"

	"github.com/sipeed/picoclaw/pkg/bus"
)

func TestFirstMediaCaption(t *testing.T) {
	parts := []bus.MediaPart{
		{Caption: "   "},
		{Caption: "first"},
		{Caption: "second"},
	}
	if got := FirstMediaCaption(parts); got != "first" {
		t.Fatalf("FirstMediaCaption() = %q, want %q", got, "first")
	}
}

func TestClearMediaCaptions(t *testing.T) {
	msg := bus.OutboundMediaMessage{
		ChatID: "chat1",
		Parts: []bus.MediaPart{
			{Ref: "media://1", Caption: "one"},
			{Ref: "media://2", Caption: "two"},
		},
	}

	cleared := ClearMediaCaptions(msg)
	if cleared.ChatID != msg.ChatID {
		t.Fatalf("ChatID changed = %q, want %q", cleared.ChatID, msg.ChatID)
	}
	if got := cleared.Parts[0].Caption; got != "" {
		t.Fatalf("cleared.Parts[0].Caption = %q, want empty", got)
	}
	if got := cleared.Parts[1].Caption; got != "" {
		t.Fatalf("cleared.Parts[1].Caption = %q, want empty", got)
	}
	if msg.Parts[0].Caption != "one" || msg.Parts[1].Caption != "two" {
		t.Fatal("original message captions should remain unchanged")
	}
}
