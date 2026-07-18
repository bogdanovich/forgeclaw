package seahorse

import (
	"context"
	"strings"
	"testing"
)

func TestAssemblerAbsoluteHistoryBudgetKeepsNewestCompleteTurns(t *testing.T) {
	store, convID := setupAssemblerStore(t)
	ctx := context.Background()
	items := make([]ContextItem, 0, 6)
	for turn := 1; turn <= 3; turn++ {
		user, err := store.AddMessage(ctx, convID, "user", strings.Repeat(string(rune('a'+turn)), 30), 20)
		if err != nil {
			t.Fatal(err)
		}
		assistant, err := store.AddMessage(ctx, convID, "assistant", "reply", 20)
		if err != nil {
			t.Fatal(err)
		}
		items = append(items,
			ContextItem{Ordinal: turn*200 - 100, ItemType: "message", MessageID: user.ID, TokenCount: 20},
			ContextItem{Ordinal: turn * 200, ItemType: "message", MessageID: assistant.ID, TokenCount: 20},
		)
	}
	if err := store.UpsertContextItems(ctx, convID, items); err != nil {
		t.Fatal(err)
	}

	assembler := &Assembler{store: store, config: Config{HistoryMaxTokens: 80, RecentTailTurns: 1}}
	result, err := assembler.Assemble(ctx, convID, AssembleInput{Budget: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 4 {
		t.Fatalf("selected messages = %d, want two complete turns", len(result.Messages))
	}
	if result.Messages[0].Role != "user" || result.Messages[2].Role != "user" {
		t.Fatalf("selected history does not preserve turn boundaries: %#v", result.Messages)
	}
	if result.Budget == nil || !result.Budget.NeedsCompaction ||
		result.Budget.SelectedHistoryTokens > result.Budget.HistoryBudget {
		t.Fatalf("unexpected budget report: %#v", result.Budget)
	}
}

func TestAssemblerAbsoluteSummaryBudgetUsesRenderedSummaryTokens(t *testing.T) {
	store, convID := setupAssemblerStore(t)
	ctx := context.Background()
	items := make([]ContextItem, 0, 3)
	for i := 0; i < 3; i++ {
		summary, err := store.CreateSummary(ctx, CreateSummaryInput{
			ConversationID: convID,
			Kind:           SummaryKindLeaf,
			Content:        strings.Repeat("summary detail ", 20),
			TokenCount:     1,
		})
		if err != nil {
			t.Fatal(err)
		}
		items = append(items, ContextItem{
			Ordinal:    (i + 1) * 100,
			ItemType:   "summary",
			SummaryID:  summary.SummaryID,
			TokenCount: 1,
		})
	}
	if err := store.UpsertContextItems(ctx, convID, items); err != nil {
		t.Fatal(err)
	}

	assembler := &Assembler{store: store, config: Config{SummaryMaxTokens: 220}}
	result, err := assembler.Assemble(ctx, convID, AssembleInput{Budget: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if result.Budget == nil || result.Budget.SelectedSummaryTokens > 220 {
		t.Fatalf("summary budget not enforced: %#v", result.Budget)
	}
	if !result.Budget.Truncated || !result.Budget.NeedsCompaction {
		t.Fatalf("expected summary pressure: %#v", result.Budget)
	}
}

func TestAssemblerRecentTailPreservesToolPairing(t *testing.T) {
	store, convID := setupAssemblerStore(t)
	ctx := context.Background()
	user, err := store.AddMessage(ctx, convID, "user", "run it", 10)
	if err != nil {
		t.Fatal(err)
	}
	assistant, err := store.AddMessageWithParts(ctx, convID, "assistant", []MessagePart{
		{Type: "tool_use", Name: "write", Arguments: `{}`, ToolCallID: "call-1"},
	}, 10)
	if err != nil {
		t.Fatal(err)
	}
	toolResult, err := store.AddMessageWithParts(ctx, convID, "tool", []MessagePart{
		{Type: "tool_result", Text: "ok", ToolCallID: "call-1"},
	}, 10)
	if err != nil {
		t.Fatal(err)
	}
	final, err := store.AddMessage(ctx, convID, "assistant", "done", 10)
	if err != nil {
		t.Fatal(err)
	}
	if upsertErr := store.UpsertContextItems(ctx, convID, []ContextItem{
		{Ordinal: 100, ItemType: "message", MessageID: user.ID, TokenCount: 10},
		{Ordinal: 200, ItemType: "message", MessageID: assistant.ID, TokenCount: 10},
		{Ordinal: 300, ItemType: "message", MessageID: toolResult.ID, TokenCount: 10},
		{Ordinal: 400, ItemType: "message", MessageID: final.ID, TokenCount: 10},
	}); upsertErr != nil {
		t.Fatal(upsertErr)
	}

	assembler := &Assembler{store: store, config: Config{HistoryMaxTokens: 100, RecentTailTurns: 1}}
	result, err := assembler.Assemble(ctx, convID, AssembleInput{Budget: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 4 || result.Messages[1].Parts[0].Type != "tool_use" ||
		result.Messages[2].Parts[0].Type != "tool_result" {
		t.Fatalf("tool pair was not preserved: %#v", result.Messages)
	}
}

func TestAssemblerRecentTailFailsClosedWhenMandatoryTurnCannotFit(t *testing.T) {
	store, convID := setupAssemblerStore(t)
	ctx := context.Background()
	user, err := store.AddMessage(ctx, convID, "user", strings.Repeat("oversized ", 100), 100)
	if err != nil {
		t.Fatal(err)
	}
	if upsertErr := store.UpsertContextItems(ctx, convID, []ContextItem{
		{Ordinal: 100, ItemType: "message", MessageID: user.ID, TokenCount: 1},
	}); upsertErr != nil {
		t.Fatal(upsertErr)
	}

	assembler := &Assembler{store: store, config: Config{HistoryMaxTokens: 50, RecentTailTurns: 1}}
	_, err = assembler.Assemble(ctx, convID, AssembleInput{Budget: 1000})
	if err == nil || !strings.Contains(err.Error(), "mandatory recent tail") {
		t.Fatalf("expected actionable recent-tail error, got %v", err)
	}
}

func TestNewEngineRejectsInvalidAbsoluteBudgets(t *testing.T) {
	_, err := NewEngine(Config{DBPath: t.TempDir() + "/test.db", HistoryMaxTokens: -1}, nil)
	if err == nil || !strings.Contains(err.Error(), "historyMaxTokens") {
		t.Fatalf("expected invalid history budget error, got %v", err)
	}
}
