package agent

import (
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/providers"
)

const adjacentMediaFollowupWindow = 2 * time.Minute

type currentTurnRelationKind string

const (
	currentTurnRelationStandalone            currentTurnRelationKind = "standalone"
	currentTurnRelationReplyToMessage        currentTurnRelationKind = "reply_to_message"
	currentTurnRelationAdjacentFollowupMedia currentTurnRelationKind = "adjacent_followup_media"
)

type currentTurnRelation struct {
	Kind      currentTurnRelationKind
	MediaOnly bool
}

type currentTurnRelationInput struct {
	Content          string
	Media            []string
	ReplyToMessageID string
	History          []providers.Message
	Now              time.Time
}

func classifyCurrentTurnRelation(input currentTurnRelationInput) currentTurnRelation {
	content := strings.TrimSpace(input.Content)
	mediaOnly := len(input.Media) > 0 && (content == "" || content == "[media only]")
	if !mediaOnly {
		return currentTurnRelation{Kind: currentTurnRelationStandalone, MediaOnly: false}
	}
	if strings.TrimSpace(input.ReplyToMessageID) != "" {
		return currentTurnRelation{Kind: currentTurnRelationReplyToMessage, MediaOnly: true}
	}
	if recentUserFollowupCandidate(input.History, input.Now, adjacentMediaFollowupWindow) {
		return currentTurnRelation{Kind: currentTurnRelationAdjacentFollowupMedia, MediaOnly: true}
	}
	return currentTurnRelation{Kind: currentTurnRelationStandalone, MediaOnly: true}
}

func recentUserFollowupCandidate(history []providers.Message, now time.Time, window time.Duration) bool {
	if len(history) == 0 || window <= 0 {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}

	lastUserIdx := -1
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].Role == "user" {
			lastUserIdx = i
			break
		}
	}
	if lastUserIdx < 0 {
		return false
	}

	lastUser := history[lastUserIdx]
	if lastUser.CreatedAt == nil || lastUser.CreatedAt.IsZero() {
		return false
	}
	if now.Sub(*lastUser.CreatedAt) > window {
		return false
	}

	for i := lastUserIdx + 1; i < len(history); i++ {
		if history[i].Role == "assistant" {
			return false
		}
	}

	return true
}
