package seahorse

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/tools"
)

func TestExpandToolByMessageIDs(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	conv, _ := s.GetOrCreateConversation(ctx, "test:expand-tool")

	msg1, _ := s.AddMessage(ctx, conv.ConversationID, "user", "first message", 10)
	msg2, _ := s.AddMessage(ctx, conv.ConversationID, "assistant", "second message", 10)

	re := &RetrievalEngine{store: s}
	tool := NewExpandTool(re)
	toolCtx := tools.WithToolSessionContext(ctx, "agent", "test:expand-tool", nil)

	result := tool.Execute(toolCtx, map[string]any{
		"message_ids": []any{fmt.Sprintf("%d", msg1.ID), fmt.Sprintf("%d", msg2.ID)},
	})

	if result.IsError {
		t.Fatalf("Expand failed: %s", result.ForLLM)
	}

	// Parse result
	var output struct {
		Success    bool             `json:"success"`
		TokenCount int              `json:"tokenCount"`
		Messages   []map[string]any `json:"messages"`
	}
	if err := json.Unmarshal([]byte(result.ForLLM), &output); err != nil {
		t.Fatalf("Parse result: %v", err)
	}

	if !output.Success {
		t.Error("expected success=true")
	}
	if len(output.Messages) != 2 {
		t.Errorf("Messages = %d, want 2", len(output.Messages))
	}
	if output.TokenCount != 20 {
		t.Errorf("TokenCount = %d, want 20", output.TokenCount)
	}
}

func TestExpandToolMissingIDs(t *testing.T) {
	s := openTestStore(t)
	re := &RetrievalEngine{store: s}
	tool := NewExpandTool(re)

	result := tool.Execute(context.Background(), map[string]any{})

	if !result.IsError {
		t.Error("expected error for missing message_ids")
	}
}

func TestExpandToolWithParts(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	conv, _ := s.GetOrCreateConversation(ctx, "test:expand-parts")

	// Create message with parts
	parts := []MessagePart{
		{Type: "text", Text: "Hello"},
		{Type: "tool_use", Name: "bash", Arguments: `{"command":"ls"}`, ToolCallID: "call_123"},
		{Type: "tool_result", ToolCallID: "call_123", Text: "file1.txt\nfile2.txt"},
	}
	msg, _ := s.AddMessageWithParts(ctx, conv.ConversationID, "assistant", parts, 50)

	re := &RetrievalEngine{store: s}
	tool := NewExpandTool(re)
	toolCtx := tools.WithToolSessionContext(ctx, "agent", "test:expand-parts", nil)

	result := tool.Execute(toolCtx, map[string]any{
		"message_ids": []any{fmt.Sprintf("%d", msg.ID)},
	})

	if result.IsError {
		t.Fatalf("Expand failed: %s", result.ForLLM)
	}

	var output struct {
		Messages []struct {
			Parts []map[string]any `json:"parts"`
		} `json:"messages"`
	}
	if err := json.Unmarshal([]byte(result.ForLLM), &output); err != nil {
		t.Fatalf("Parse result: %v", err)
	}

	if len(output.Messages) != 1 {
		t.Fatalf("Messages = %d, want 1", len(output.Messages))
	}

	// Verify parts are filtered correctly
	foundText := false
	foundToolUse := false
	foundToolResult := false
	for _, p := range output.Messages[0].Parts {
		switch p["type"].(string) {
		case "text":
			foundText = true
			if p["text"] != "Hello" {
				t.Errorf("text = %v, want Hello", p["text"])
			}
		case "tool_use":
			foundToolUse = true
			if p["name"] != "bash" {
				t.Errorf("name = %v, want bash", p["name"])
			}
		case "tool_result":
			foundToolResult = true
			// tool_result should NOT have content
			if _, hasContent := p["content"]; hasContent {
				t.Error("tool_result should not have content field")
			}
			if p["toolCallId"] != "call_123" {
				t.Errorf("toolCallId = %v, want call_123", p["toolCallId"])
			}
		}
	}

	if !foundText {
		t.Error("missing text part")
	}
	if !foundToolUse {
		t.Error("missing tool_use part")
	}
	if !foundToolResult {
		t.Error("missing tool_result part")
	}
}

func TestExpandToolScopesToCurrentSession(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	current, _ := s.GetOrCreateConversation(ctx, "session:current")
	other, _ := s.GetOrCreateConversation(ctx, "session:other")
	currentMsg, _ := s.AddMessage(ctx, current.ConversationID, "user", "current message", 5)
	otherMsg, _ := s.AddMessage(ctx, other.ConversationID, "user", "other message", 5)

	tool := NewExpandTool(&RetrievalEngine{store: s})
	toolCtx := tools.WithToolSessionContext(ctx, "agent", "session:current", nil)
	result := tool.Execute(toolCtx, map[string]any{
		"message_ids": []any{
			float64(currentMsg.ID),
			float64(otherMsg.ID),
		},
	})
	if result.IsError {
		t.Fatalf("Execute returned error: %s", result.ContentForLLM())
	}

	var output struct {
		Messages []struct {
			Content string `json:"content"`
		} `json:"messages"`
		RejectedMessageIDs []int64 `json:"rejectedMessageIds"`
	}
	if err := json.Unmarshal([]byte(result.ContentForLLM()), &output); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if len(output.Messages) != 1 {
		t.Fatalf("messages = %d, want 1: %#v", len(output.Messages), output.Messages)
	}
	if output.Messages[0].Content != "current message" {
		t.Fatalf("content = %q, want current message", output.Messages[0].Content)
	}
	if len(output.RejectedMessageIDs) != 1 || output.RejectedMessageIDs[0] != otherMsg.ID {
		t.Fatalf("rejectedMessageIds = %#v, want [%d]", output.RejectedMessageIDs, otherMsg.ID)
	}
}

func TestExpandToolCanExpandRouteConversation(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	current, _ := s.GetOrCreateConversation(ctx, "session:current")
	other, _ := s.GetOrCreateConversation(ctx, "session:other")
	currentMsg, _ := s.AddMessage(ctx, current.ConversationID, "user", "current message", 5)
	otherMsg, _ := s.AddMessage(ctx, other.ConversationID, "user", "other message", 5)
	if err := s.SetConversationProvenance(ctx, "session:current", "route:a", "agent"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetConversationProvenance(ctx, "session:other", "route:a", "agent"); err != nil {
		t.Fatal(err)
	}

	tool := NewExpandTool(&RetrievalEngine{store: s})
	toolCtx := tools.WithToolSessionContext(
		ctx,
		"agent",
		"session:current",
		retrievalTestScope("route:a", "agent"),
	)
	result := tool.Execute(toolCtx, map[string]any{
		"message_ids": []any{
			float64(currentMsg.ID),
			float64(otherMsg.ID),
		},
		"retrieval_scope": "conversation",
	})
	if result.IsError {
		t.Fatalf("Execute returned error: %s", result.ContentForLLM())
	}

	var output struct {
		Messages []struct {
			Content string `json:"content"`
		} `json:"messages"`
		RejectedMessageIDs []int64 `json:"rejectedMessageIds"`
	}
	if err := json.Unmarshal([]byte(result.ContentForLLM()), &output); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if len(output.Messages) != 2 {
		t.Fatalf("messages = %d, want 2: %#v", len(output.Messages), output.Messages)
	}
	if len(output.RejectedMessageIDs) != 0 {
		t.Fatalf("rejectedMessageIds = %#v, want none", output.RejectedMessageIDs)
	}
}

func TestExpandToolUnknownSessionRejectsIDs(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	conv, _ := s.GetOrCreateConversation(ctx, "session:current")
	msg, _ := s.AddMessage(ctx, conv.ConversationID, "user", "current message", 5)

	tool := NewExpandTool(&RetrievalEngine{store: s})
	toolCtx := tools.WithToolSessionContext(ctx, "agent", "session:missing", nil)
	result := tool.Execute(toolCtx, map[string]any{
		"message_ids": []any{float64(msg.ID)},
	})
	if result.IsError {
		t.Fatalf("unexpected error: %s", result.ContentForLLM())
	}
	var output struct {
		Messages           []map[string]any `json:"messages"`
		RejectedMessageIDs []int64          `json:"rejectedMessageIds"`
	}
	if err := json.Unmarshal([]byte(result.ContentForLLM()), &output); err != nil {
		t.Fatal(err)
	}
	if len(output.Messages) != 0 || len(output.RejectedMessageIDs) != 1 {
		t.Fatalf("unexpected fail-closed result: %#v", output)
	}
}

func TestExpandToolSupportsRetrievalScopeParameter(t *testing.T) {
	s := openTestStore(t)
	tool := NewExpandTool(&RetrievalEngine{store: s})
	params := tool.Parameters()
	props := params["properties"].(map[string]any)

	if _, ok := props["retrieval_scope"]; !ok {
		t.Error("Parameters missing 'retrieval_scope' field")
	}
}

func TestExpandJSONResultCapsTokenHeavyPayload(t *testing.T) {
	result := expandJSONResult(&ExpandMessagesResult{
		Messages: []Message{
			{ID: 1, Content: "fits", TokenCount: retrievalToolMaxTokens},
			{ID: 2, Content: "does not fit", TokenCount: 1},
		},
		TokenCount: retrievalToolMaxTokens + 1,
	})
	var output struct {
		TokenCount      int  `json:"tokenCount"`
		Truncated       bool `json:"truncated"`
		OmittedMessages int  `json:"omitted_messages"`
	}
	if err := json.Unmarshal([]byte(result.ContentForLLM()), &output); err != nil {
		t.Fatal(err)
	}
	if output.TokenCount > retrievalToolMaxTokens || !output.Truncated || output.OmittedMessages != 1 {
		t.Fatalf("unexpected capped output: %#v", output)
	}
}

func TestExpandJSONResultCapsByteHeavyPayload(t *testing.T) {
	messages := make([]Message, 100)
	for i := range messages {
		messages[i] = Message{
			ID:         int64(i + 1),
			Content:    strings.Repeat("x", 2048),
			TokenCount: 1,
		}
	}
	result := expandJSONResult(&ExpandMessagesResult{Messages: messages, TokenCount: len(messages)})
	if len(result.ContentForLLM()) > expandToolMaxForLLMBytes {
		t.Fatalf("payload bytes = %d, want <= %d", len(result.ContentForLLM()), expandToolMaxForLLMBytes)
	}
	if got := estimateRetrievalResultTokens([]byte(result.ContentForLLM())); got > retrievalToolMaxTokens {
		t.Fatalf("payload tokens = %d, want <= %d", got, retrievalToolMaxTokens)
	}
	var output struct {
		Truncated       bool `json:"truncated"`
		OmittedMessages int  `json:"omitted_messages"`
	}
	if err := json.Unmarshal([]byte(result.ContentForLLM()), &output); err != nil {
		t.Fatal(err)
	}
	if !output.Truncated || output.OmittedMessages == 0 {
		t.Fatalf("expected explicit truncation metadata: %#v", output)
	}
}
