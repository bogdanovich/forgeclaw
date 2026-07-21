package agent

import (
	"testing"

	"github.com/sipeed/picoclaw/pkg/bus"
)

func TestOutboundMessageForTurnCarriesTraceScopeWithoutSettlement(t *testing.T) {
	ts := &turnState{
		agent:     &AgentInstance{ID: "agent-1"},
		workspace: "/workspace/main",
		turnID:    "turn-1",
		channel:   "telegram",
		chatID:    "chat-1",
		opts: processOptions{Dispatch: DispatchRequest{
			InboundContext: &bus.InboundContext{Channel: "telegram", ChatID: "chat-1"},
		}},
	}

	msg := outboundMessageForTurn(ts, "working")
	if msg.TraceSettlement {
		t.Fatal("per-turn progress must not settle the trace")
	}
	if len(msg.TraceScopes) != 1 || msg.TraceScopes[0].Workspace != ts.workspace ||
		msg.TraceScopes[0].TurnID != ts.turnID {
		t.Fatalf("trace scopes = %+v, want current turn", msg.TraceScopes)
	}
}

func TestInferMediaType(t *testing.T) {
	tests := []struct {
		name        string
		filename    string
		contentType string
		want        string
	}{
		{
			name:        "png content type",
			filename:    "diagram",
			contentType: "image/png",
			want:        "image",
		},
		{
			name:        "jpeg extension fallback",
			filename:    "photo.JPG",
			contentType: "",
			want:        "image",
		},
		{
			name:        "svg content type is file",
			filename:    "diagram",
			contentType: "image/svg+xml",
			want:        "file",
		},
		{
			name:        "svg content type with parameters is file",
			filename:    "diagram",
			contentType: "image/svg+xml; charset=utf-8",
			want:        "file",
		},
		{
			name:        "svg extension fallback is file",
			filename:    "diagram.SVG",
			contentType: "",
			want:        "file",
		},
		{
			name:        "audio content type",
			filename:    "voice",
			contentType: "audio/ogg",
			want:        "audio",
		},
		{
			name:        "ogg application content type",
			filename:    "voice.ogg",
			contentType: "application/ogg",
			want:        "audio",
		},
		{
			name:        "video extension fallback",
			filename:    "clip.MP4",
			contentType: "",
			want:        "video",
		},
		{
			name:        "unknown type",
			filename:    "archive.bin",
			contentType: "application/octet-stream",
			want:        "file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := inferMediaType(tt.filename, tt.contentType)
			if got != tt.want {
				t.Fatalf("inferMediaType(%q, %q) = %q, want %q", tt.filename, tt.contentType, got, tt.want)
			}
		})
	}
}
