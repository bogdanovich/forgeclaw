package channels

import (
	"strings"

	"github.com/sipeed/picoclaw/pkg/bus"
)

// FirstMediaCaption returns the first non-empty caption from the media parts.
func FirstMediaCaption(parts []bus.MediaPart) string {
	for _, part := range parts {
		if caption := strings.TrimSpace(part.Caption); caption != "" {
			return caption
		}
	}
	return ""
}

// FirstPartCaption returns the trimmed caption from the first media part only.
// It preserves channel behaviors that intentionally ignore captions on later
// parts when deciding whether to emit a separate fallback text message.
func FirstPartCaption(parts []bus.MediaPart) string {
	if len(parts) == 0 {
		return ""
	}
	return strings.TrimSpace(parts[0].Caption)
}

// ClearMediaCaptions returns a copy of the outbound media message with all part
// captions removed. This is useful for channels that need to deliver the caption
// separately, e.g. when the platform caption limit is too small.
func ClearMediaCaptions(msg bus.OutboundMediaMessage) bus.OutboundMediaMessage {
	if len(msg.Parts) == 0 {
		return msg
	}
	cleared := msg
	cleared.Parts = make([]bus.MediaPart, len(msg.Parts))
	copy(cleared.Parts, msg.Parts)
	for i := range cleared.Parts {
		cleared.Parts[i].Caption = ""
	}
	return cleared
}
