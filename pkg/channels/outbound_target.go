package channels

import (
	"strings"

	"github.com/sipeed/picoclaw/pkg/bus"
)

// EffectiveOutboundChatID returns the explicit chat ID when provided, otherwise
// falls back to the normalized inbound context chat ID.
func EffectiveOutboundChatID(chatID string, outboundCtx *bus.InboundContext) string {
	deliveryChatID := strings.TrimSpace(chatID)
	if deliveryChatID == "" && outboundCtx != nil {
		deliveryChatID = strings.TrimSpace(outboundCtx.ChatID)
	}
	return deliveryChatID
}

// EffectiveOutboundTopicID returns the explicit topic/thread ID when provided,
// otherwise falls back to the normalized inbound context topic ID.
func EffectiveOutboundTopicID(topicID string, outboundCtx *bus.InboundContext) string {
	resolvedTopicID := strings.TrimSpace(topicID)
	if resolvedTopicID == "" && outboundCtx != nil {
		resolvedTopicID = strings.TrimSpace(outboundCtx.TopicID)
	}
	return resolvedTopicID
}

// EffectiveOutboundReplyToMessageID returns the explicit reply target when
// provided, otherwise falls back to the normalized inbound context reply target.
func EffectiveOutboundReplyToMessageID(replyToMessageID string, outboundCtx *bus.InboundContext) string {
	resolvedReplyToMessageID := strings.TrimSpace(replyToMessageID)
	if resolvedReplyToMessageID == "" && outboundCtx != nil {
		resolvedReplyToMessageID = strings.TrimSpace(outboundCtx.ReplyToMessageID)
	}
	return resolvedReplyToMessageID
}
