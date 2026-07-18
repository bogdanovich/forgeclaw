package seahorse

import (
	"context"
	"testing"
)

func TestCompactUntilAbsoluteBudgetsPreservesConfiguredRecentTurns(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	conversation, err := store.GetOrCreateConversation(ctx, "absolute-compaction")
	if err != nil {
		t.Fatal(err)
	}
	var recentMessageIDs []int64
	for turn := 0; turn < 10; turn++ {
		user, addErr := store.AddMessage(ctx, conversation.ConversationID, "user", "question", 20)
		if addErr != nil {
			t.Fatal(addErr)
		}
		assistant, addErr := store.AddMessage(ctx, conversation.ConversationID, "assistant", "answer", 20)
		if addErr != nil {
			t.Fatal(addErr)
		}
		if appendErr := store.AppendContextMessages(
			ctx,
			conversation.ConversationID,
			[]int64{user.ID, assistant.ID},
		); appendErr != nil {
			t.Fatal(appendErr)
		}
		if turn == 9 {
			recentMessageIDs = []int64{user.ID, assistant.ID}
		}
	}

	engine := &CompactionEngine{
		store: store,
		config: Config{
			HistoryMaxTokens: 80,
			SummaryMaxTokens: 200,
			RecentTailTurns:  1,
		},
		complete: func(context.Context, string, CompleteOptions) (string, error) {
			return "older turns summarized", nil
		},
	}
	if _, compactErr := engine.CompactUntilUnder(ctx, conversation.ConversationID, 500); compactErr != nil {
		t.Fatal(compactErr)
	}
	items, err := store.GetContextItems(ctx, conversation.ConversationID)
	if err != nil {
		t.Fatal(err)
	}
	found := make(map[int64]bool)
	for _, item := range items {
		if item.ItemType == "message" {
			found[item.MessageID] = true
		}
	}
	for _, messageID := range recentMessageIDs {
		if !found[messageID] {
			t.Errorf("configured recent-tail message %d was compacted", messageID)
		}
	}
	historyTokens, summaryTokens, err := store.GetContextTokenCounts(ctx, conversation.ConversationID)
	if err != nil {
		t.Fatal(err)
	}
	if historyTokens > 80 || summaryTokens > 200 || historyTokens+summaryTokens > 500 {
		t.Fatalf("compaction budgets not reached: history=%d summary=%d", historyTokens, summaryTokens)
	}
}

func TestCompactUntilAbsoluteBudgetsFailsWhenProtectedTailExceedsCap(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	conversation, err := store.GetOrCreateConversation(ctx, "absolute-no-progress")
	if err != nil {
		t.Fatal(err)
	}
	user, err := store.AddMessage(ctx, conversation.ConversationID, "user", "question", 100)
	if err != nil {
		t.Fatal(err)
	}
	assistant, err := store.AddMessage(ctx, conversation.ConversationID, "assistant", "answer", 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AppendContextMessages(
		ctx,
		conversation.ConversationID,
		[]int64{user.ID, assistant.ID},
	); err != nil {
		t.Fatal(err)
	}
	engine := &CompactionEngine{
		store: store,
		config: Config{
			HistoryMaxTokens: 50,
			RecentTailTurns:  1,
		},
	}
	if _, err := engine.CompactUntilUnder(ctx, conversation.ConversationID, 500); err == nil {
		t.Fatal("expected no-progress error for protected tail over absolute cap")
	}
}
