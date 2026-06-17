package seahorse

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/tools"
)

func TestGrepSearchSummaries(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	conv, _ := s.GetOrCreateConversation(ctx, "test:grep-tool")

	s.CreateSummary(ctx, CreateSummaryInput{
		ConversationID: conv.ConversationID,
		Kind:           SummaryKindLeaf,
		Depth:          0,
		Content:        "database connection pool configuration",
		TokenCount:     50,
	})

	re := &RetrievalEngine{store: s}
	results, err := re.Grep(ctx, GrepInput{
		Pattern: "database",
	})
	if err != nil {
		t.Fatalf("Grep: %v", err)
	}
	if len(results.Summaries) == 0 {
		t.Error("expected at least 1 summary result")
	}
}

func TestGrepSearchMessages(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	conv, _ := s.GetOrCreateConversation(ctx, "test:grep-msg")

	s.AddMessage(ctx, conv.ConversationID, "user", "find this message about testing", 5)
	s.AddMessage(ctx, conv.ConversationID, "user", "unrelated content", 3)

	re := &RetrievalEngine{store: s}
	results, err := re.Grep(ctx, GrepInput{
		Pattern: "testing",
	})
	if err != nil {
		t.Fatalf("Grep messages: %v", err)
	}
	if len(results.Messages) == 0 {
		t.Error("expected at least 1 message result")
	}
}

func TestGrepMissingPattern(t *testing.T) {
	s := openTestStore(t)
	re := &RetrievalEngine{store: s}
	_, err := re.Grep(context.Background(), GrepInput{})
	if err == nil {
		t.Error("expected error for missing pattern")
	}
}

func TestGrepToolSupportsAllConversations(t *testing.T) {
	s := openTestStore(t)
	tool := NewGrepTool(&RetrievalEngine{store: s})
	params := tool.Parameters()
	props := params["properties"].(map[string]any)

	// GrepTool should accept all_conversations parameter
	if _, ok := props["all_conversations"]; !ok {
		t.Error("Parameters missing 'all_conversations' field")
	}
}

func TestGrepToolScopesToCurrentSessionByDefault(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	current, _ := s.GetOrCreateConversation(ctx, "session:current")
	other, _ := s.GetOrCreateConversation(ctx, "session:other")
	s.AddMessage(ctx, current.ConversationID, "user", "shared needle from current topic", 5)
	s.AddMessage(ctx, other.ConversationID, "user", "shared needle from other topic", 5)

	tool := NewGrepTool(&RetrievalEngine{store: s})
	toolCtx := tools.WithToolSessionContext(ctx, "agent", "session:current", nil)
	result := tool.Execute(toolCtx, map[string]any{"pattern": "needle"})
	if result.IsError {
		t.Fatalf("Execute returned error: %s", result.ContentForLLM())
	}

	var output struct {
		Messages []GrepMessageResult `json:"messages"`
	}
	if err := json.Unmarshal([]byte(result.ContentForLLM()), &output); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if len(output.Messages) != 1 {
		t.Fatalf("messages = %d, want 1: %#v", len(output.Messages), output.Messages)
	}
	if output.Messages[0].ConversationID != current.ConversationID {
		t.Fatalf("conversation id = %d, want %d", output.Messages[0].ConversationID, current.ConversationID)
	}
}

func TestGrepToolCanSearchAllConversations(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	current, _ := s.GetOrCreateConversation(ctx, "session:current")
	other, _ := s.GetOrCreateConversation(ctx, "session:other")
	s.AddMessage(ctx, current.ConversationID, "user", "shared needle from current topic", 5)
	s.AddMessage(ctx, other.ConversationID, "user", "shared needle from other topic", 5)

	tool := NewGrepTool(&RetrievalEngine{store: s})
	toolCtx := tools.WithToolSessionContext(ctx, "agent", "session:current", nil)
	result := tool.Execute(toolCtx, map[string]any{"pattern": "needle", "all_conversations": true})
	if result.IsError {
		t.Fatalf("Execute returned error: %s", result.ContentForLLM())
	}

	var output struct {
		Messages []GrepMessageResult `json:"messages"`
	}
	if err := json.Unmarshal([]byte(result.ContentForLLM()), &output); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if len(output.Messages) != 2 {
		t.Fatalf("messages = %d, want 2: %#v", len(output.Messages), output.Messages)
	}
}

func TestGrepToolUnknownSessionDoesNotSearchAllConversations(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	current, _ := s.GetOrCreateConversation(ctx, "session:current")
	other, _ := s.GetOrCreateConversation(ctx, "session:other")
	s.AddMessage(ctx, current.ConversationID, "user", "shared needle from current topic", 5)
	s.AddMessage(ctx, other.ConversationID, "user", "shared needle from other topic", 5)

	tool := NewGrepTool(&RetrievalEngine{store: s})
	toolCtx := tools.WithToolSessionContext(ctx, "agent", "session:missing", nil)
	result := tool.Execute(toolCtx, map[string]any{"pattern": "needle"})
	if result.IsError {
		t.Fatalf("Execute returned error: %s", result.ContentForLLM())
	}

	var output struct {
		Messages []GrepMessageResult `json:"messages"`
		Hint     string              `json:"hint"`
	}
	if err := json.Unmarshal([]byte(result.ContentForLLM()), &output); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if len(output.Messages) != 0 {
		t.Fatalf("messages = %d, want 0: %#v", len(output.Messages), output.Messages)
	}
	if output.Hint == "" {
		t.Fatal("expected hint for missing current conversation")
	}
}

func TestGrepToolEmptySessionDoesNotSearchAllConversations(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	conv, _ := s.GetOrCreateConversation(ctx, "session:current")
	s.AddMessage(ctx, conv.ConversationID, "user", "shared needle from current topic", 5)

	tool := NewGrepTool(&RetrievalEngine{store: s})
	result := tool.Execute(ctx, map[string]any{"pattern": "needle"})
	if result.IsError {
		t.Fatalf("Execute returned error: %s", result.ContentForLLM())
	}

	var output struct {
		Messages []GrepMessageResult `json:"messages"`
	}
	if err := json.Unmarshal([]byte(result.ContentForLLM()), &output); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if len(output.Messages) != 0 {
		t.Fatalf("messages = %d, want 0: %#v", len(output.Messages), output.Messages)
	}
}

func TestGrepJSONResultMarksTrimmedLargeContent(t *testing.T) {
	result := &GrepResult{
		Success: true,
		Summaries: []GrepSummaryResult{
			{
				ID:      "sum-1",
				Content: strings.Repeat("x", 5000),
			},
		},
		Messages: []GrepMessageResult{
			{
				ID:      1,
				Snippet: strings.Repeat("y", grepToolMaxMessageSnippetRunes+200),
			},
		},
	}

	toolResult := grepJSONResult(result)
	var output struct {
		Truncated        bool                `json:"truncated"`
		TruncationNotice string              `json:"truncation_notice"`
		Summaries        []GrepSummaryResult `json:"summaries"`
		Messages         []GrepMessageResult `json:"messages"`
	}
	if err := json.Unmarshal([]byte(toolResult.ContentForLLM()), &output); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if !output.Truncated {
		t.Fatal("expected truncated=true")
	}
	if output.TruncationNotice == "" {
		t.Fatal("expected truncation notice")
	}
	if got := output.Summaries[0].Content; strings.Contains(got, "[trimmed]") {
		t.Fatalf("summary content should remain intact when present: %q", got)
	}
	if got := output.Messages[0].Snippet; !strings.Contains(got, "[trimmed]") {
		t.Fatalf("message snippet was not marked trimmed: %q", got)
	}
}

func TestGrepJSONResultCapsOverallPayloadSize(t *testing.T) {
	summaries := make([]GrepSummaryResult, 0, 200)
	for i := 0; i < 200; i++ {
		summaries = append(summaries, GrepSummaryResult{
			ID:      strings.Repeat("s", 32) + string(rune('a'+(i%26))),
			Content: strings.Repeat("z", 5000),
		})
	}

	toolResult := grepJSONResult(&GrepResult{
		Success:        true,
		Summaries:      summaries,
		TotalSummaries: len(summaries),
	})

	if got := len(toolResult.ContentForLLM()); got > grepToolMaxForLLMBytes {
		t.Fatalf("tool result too large: got %d bytes", got)
	}

	var output struct {
		Truncated        bool                `json:"truncated"`
		OmittedSummaries int                 `json:"omitted_summaries"`
		Summaries        []GrepSummaryResult `json:"summaries"`
	}
	if err := json.Unmarshal([]byte(toolResult.ContentForLLM()), &output); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if !output.Truncated {
		t.Fatal("expected truncated=true")
	}
	if output.OmittedSummaries == 0 {
		t.Fatal("expected omitted_summaries > 0")
	}
	if len(output.Summaries) >= len(summaries) {
		t.Fatal("expected some summaries to be dropped")
	}
}

func TestGrepJSONResultPrefersDroppingLargeSummariesBeforeMessages(t *testing.T) {
	summaries := []GrepSummaryResult{
		{
			ID:      "sum-1",
			Content: strings.Repeat("s", grepToolMaxForLLMBytes),
		},
	}
	messages := []GrepMessageResult{
		{
			ID:      1,
			Snippet: "message hit one",
			Role:    "user",
		},
		{
			ID:      2,
			Snippet: "message hit two",
			Role:    "assistant",
		},
	}

	toolResult := grepJSONResult(&GrepResult{
		Success:        true,
		Summaries:      summaries,
		Messages:       messages,
		TotalSummaries: len(summaries),
		TotalMessages:  len(messages),
	})

	if got := len(toolResult.ContentForLLM()); got > grepToolMaxForLLMBytes {
		t.Fatalf("tool result too large: got %d bytes", got)
	}

	var output struct {
		Truncated        bool                `json:"truncated"`
		OmittedSummaries int                 `json:"omitted_summaries"`
		OmittedMessages  int                 `json:"omitted_messages"`
		Summaries        []GrepSummaryResult `json:"summaries"`
		Messages         []GrepMessageResult `json:"messages"`
	}
	if err := json.Unmarshal([]byte(toolResult.ContentForLLM()), &output); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if !output.Truncated {
		t.Fatal("expected truncated=true")
	}
	if output.OmittedSummaries == 0 {
		t.Fatal("expected at least one summary omission")
	}
	if output.OmittedMessages != 0 {
		t.Fatalf("expected to keep message hits, omitted_messages=%d", output.OmittedMessages)
	}
	if len(output.Messages) != len(messages) {
		t.Fatalf("expected all message hits to remain, got %d", len(output.Messages))
	}
}
