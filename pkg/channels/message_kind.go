package channels

import "github.com/bogdanovich/mintclaw/pkg/bus"

// OutboundMessageFinalizesTrackedToolFeedback reports whether a normal
// user-visible outbound message may safely reuse the tracked tool-feedback
// carrier by editing it in-place.
//
// Terminal replies that must preserve chronology, such as final replies after
// steering/follow-up input, are expected to bypass tracked tool-feedback
// finalization and be sent as new messages instead.
func OutboundMessageFinalizesTrackedToolFeedback(msg bus.OutboundMessage) bool {
	metadata := bus.OutboundMetadataFromMessage(msg)
	if metadata.MessageKind == "" {
		return true
	}
	if metadata.IsToolFeedback() || metadata.IsThought() || metadata.IsToolCalls() || metadata.IsFinalReply() {
		return false
	}
	return true
}

// OutboundMessageDismissesTrackedToolFeedback reports whether the outgoing
// message is terminal user-facing content that should clear any previously
// tracked tool-feedback carrier after a fresh send.
func OutboundMessageDismissesTrackedToolFeedback(msg bus.OutboundMessage) bool {
	metadata := bus.OutboundMetadataFromMessage(msg)
	if metadata.IsToolFeedback() || metadata.IsThought() || metadata.IsToolCalls() {
		return false
	}
	return true
}
