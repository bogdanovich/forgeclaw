package agent

import (
	"strings"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/routing"
)

func modelOverrideScopeKey(
	agentID string,
	inbound *bus.InboundContext,
	fallback string,
) string {
	if inbound == nil {
		return strings.TrimSpace(fallback)
	}

	channel := strings.ToLower(strings.TrimSpace(inbound.Channel))
	chatID := strings.ToLower(strings.TrimSpace(inbound.ChatID))
	if channel == "" || chatID == "" {
		return strings.TrimSpace(fallback)
	}

	chatType := strings.ToLower(strings.TrimSpace(inbound.ChatType))
	if chatType == "" {
		chatType = "direct"
	}
	if topicID := strings.ToLower(strings.TrimSpace(inbound.TopicID)); topicID != "" {
		chatID += "/" + topicID
	}

	parts := []string{
		"agent:" + routing.NormalizeAgentID(agentID),
		"channel:" + channel,
		"chat:" + chatType + ":" + chatID,
	}
	if account := routing.NormalizeAccountID(inbound.Account); account != "" {
		parts = append(parts, "account:"+account)
	}

	return strings.Join(parts, "|")
}
