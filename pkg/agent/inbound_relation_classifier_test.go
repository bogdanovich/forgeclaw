package agent

import (
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/providers"
)

func TestClassifyCurrentTurnRelation_ReplyWinsForMediaOnly(t *testing.T) {
	got := classifyCurrentTurnRelation(currentTurnRelationInput{
		Content:          "[media only]",
		Media:            []string{"media://image-1"},
		ReplyToMessageID: "reply-1",
	})

	if got.Kind != currentTurnRelationReplyToMessage {
		t.Fatalf("Kind = %q, want %q", got.Kind, currentTurnRelationReplyToMessage)
	}
	if !got.MediaOnly {
		t.Fatal("MediaOnly = false, want true")
	}
}

func TestClassifyCurrentTurnRelation_AdjacentMediaFollowup(t *testing.T) {
	ts := time.Now().Add(-time.Minute)

	got := classifyCurrentTurnRelation(currentTurnRelationInput{
		Content:                    "[media only]",
		Media:                      []string{"media://image-1"},
		AllowAdjacentMediaFollowup: true,
		History: []providers.Message{
			{Role: "user", Content: "Here is what I ate", CreatedAt: &ts},
		},
		Now: time.Now(),
	})

	if got.Kind != currentTurnRelationAdjacentFollowupMedia {
		t.Fatalf("Kind = %q, want %q", got.Kind, currentTurnRelationAdjacentFollowupMedia)
	}
}

func TestClassifyCurrentTurnRelation_AdjacentMediaFollowupRequiresExplicitAllow(t *testing.T) {
	ts := time.Now().Add(-time.Minute)

	got := classifyCurrentTurnRelation(currentTurnRelationInput{
		Content: "[media only]",
		Media:   []string{"media://image-1"},
		History: []providers.Message{
			{Role: "user", Content: "Here is what I ate", CreatedAt: &ts},
		},
		Now: time.Now(),
	})

	if got.Kind != currentTurnRelationStandalone {
		t.Fatalf("Kind = %q, want %q", got.Kind, currentTurnRelationStandalone)
	}
	if !got.MediaOnly {
		t.Fatal("MediaOnly = false, want true")
	}
}

func TestClassifyCurrentTurnRelation_AdjacentMediaFollowupWithoutTimestamp(t *testing.T) {
	got := classifyCurrentTurnRelation(currentTurnRelationInput{
		Content:                    "[media only]",
		Media:                      []string{"media://image-1"},
		AllowAdjacentMediaFollowup: true,
		History: []providers.Message{
			{Role: "user", Content: "Here is what I ate"},
		},
		Now: time.Now(),
	})

	if got.Kind != currentTurnRelationAdjacentFollowupMedia {
		t.Fatalf("Kind = %q, want %q", got.Kind, currentTurnRelationAdjacentFollowupMedia)
	}
}

func TestClassifyCurrentTurnRelation_StandaloneAfterAssistantReply(t *testing.T) {
	userTS := time.Now().Add(-time.Minute)
	assistantTS := time.Now().Add(-30 * time.Second)

	got := classifyCurrentTurnRelation(currentTurnRelationInput{
		Content: "[media only]",
		Media:   []string{"media://image-1"},
		History: []providers.Message{
			{Role: "user", Content: "Here is what I ate", CreatedAt: &userTS},
			{Role: "assistant", Content: "Saved.", CreatedAt: &assistantTS},
		},
		Now: time.Now(),
	})

	if got.Kind != currentTurnRelationStandalone {
		t.Fatalf("Kind = %q, want %q", got.Kind, currentTurnRelationStandalone)
	}
	if !got.MediaOnly {
		t.Fatal("MediaOnly = false, want true")
	}
}

func TestClassifyCurrentTurnRelation_TextMessageStaysStandalone(t *testing.T) {
	got := classifyCurrentTurnRelation(currentTurnRelationInput{
		Content: "this is plain text",
		Media:   []string{"media://image-1"},
	})

	if got.Kind != currentTurnRelationStandalone {
		t.Fatalf("Kind = %q, want %q", got.Kind, currentTurnRelationStandalone)
	}
	if got.MediaOnly {
		t.Fatal("MediaOnly = true, want false")
	}
}

func TestClassifyCurrentTurnRelation_KnownAttachmentPlaceholderCountsAsMediaOnly(t *testing.T) {
	got := classifyCurrentTurnRelation(currentTurnRelationInput{
		Content: "[image]",
		Media:   []string{"media://image-1"},
	})

	if got.Kind != currentTurnRelationStandalone {
		t.Fatalf("Kind = %q, want %q", got.Kind, currentTurnRelationStandalone)
	}
	if !got.MediaOnly {
		t.Fatal("MediaOnly = false, want true")
	}
}
