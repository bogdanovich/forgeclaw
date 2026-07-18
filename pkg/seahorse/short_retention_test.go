package seahorse

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	toolpolicy "github.com/sipeed/picoclaw/pkg/tools/policy"
)

func TestProjectToolResultMessagesAppliesAllRetentionModes(t *testing.T) {
	messages := retentionTestMessages([]retentionTestCall{
		{id: "preserve", name: "read", status: "success", content: "full read output"},
		{id: "receipt", name: "lookup", status: "success", content: "large lookup output"},
		{id: "transient", name: "daily_summary", status: "success", content: "routine summary"},
		{id: "durable", name: "log_meal", status: "success", content: "meal 42 with all fields"},
	})
	config := Config{ResultRetentionPolicy: toolpolicy.ResultRetentionPolicy{
		"read":          {Mode: toolpolicy.ResultRetentionPreserve},
		"lookup":        {Mode: toolpolicy.ResultRetentionCompactReceipt, Receipt: "Lookup completed."},
		"daily_summary": {Mode: toolpolicy.ResultRetentionTransient},
		"log_meal":      {Mode: toolpolicy.ResultRetentionDurable, Receipt: "Meal saved in Nutrition DB."},
	}}

	projected, report := projectToolResultMessages(messages, config)
	if report.Preserved != 1 || report.Receipted != 1 || report.Durable != 1 || report.Transient != 1 {
		t.Fatalf("unexpected projection report: %#v", report)
	}
	if got := toolUseNames(projected); strings.Join(got, ",") != "read,lookup,log_meal" {
		t.Fatalf("projected tool calls = %v", got)
	}
	if got := toolResultContent(projected, "preserve"); got != "full read output" {
		t.Fatalf("preserved result = %q", got)
	}
	if got := toolResultContent(projected, "receipt"); got != "Lookup completed." {
		t.Fatalf("compact receipt = %q", got)
	}
	if got := toolResultContent(projected, "durable"); got != "Meal saved in Nutrition DB." {
		t.Fatalf("durable receipt = %q", got)
	}
	if got := toolResultContent(projected, "transient"); got != "" {
		t.Fatalf("transient result was retained: %q", got)
	}
	if got := toolResultContent(messages, "receipt"); got != "large lookup output" {
		t.Fatalf("projection mutated canonical input: %q", got)
	}
}

func TestProjectToolResultMessagesPreservesUnsafeResults(t *testing.T) {
	messages := retentionTestMessages([]retentionTestCall{
		{id: "error", name: "routine", status: "error", content: "write failed"},
		{id: "unresolved", name: "routine", status: "unresolved", content: "still running"},
		{id: "unknown", name: "routine", content: "legacy unknown status"},
		{id: "media", name: "routine", status: "success", content: "image created", media: true},
	})
	config := Config{ResultRetentionPolicy: toolpolicy.ResultRetentionPolicy{
		"routine": {Mode: toolpolicy.ResultRetentionTransient},
	}}

	projected, report := projectToolResultMessages(messages, config)
	if report.SafetyPreserved != 4 || report.Transient != 0 {
		t.Fatalf("unsafe projection report: %#v", report)
	}
	for _, id := range []string{"error", "unresolved", "unknown", "media"} {
		if got := toolResultContent(projected, id); got == "" {
			t.Errorf("unsafe result %q was removed", id)
		}
	}
}

func TestProjectToolResultMessagesScopesReusedCallIDsToTheirRound(t *testing.T) {
	messages := []Message{
		{ID: 1, Role: "user", Content: "first"},
		retentionToolUseMessage(2, "reused", "preserve_tool"),
		retentionToolResultMessage(3, "reused", "success", "keep first result", false),
		{ID: 4, Role: "assistant", Content: "first done"},
		{ID: 5, Role: "user", Content: "second"},
		retentionToolUseMessage(6, "reused", "transient_tool"),
		retentionToolResultMessage(7, "reused", "success", "drop second result", false),
	}
	config := Config{ResultRetentionPolicy: toolpolicy.ResultRetentionPolicy{
		"preserve_tool":  {Mode: toolpolicy.ResultRetentionPreserve},
		"transient_tool": {Mode: toolpolicy.ResultRetentionTransient},
	}}

	projected, report := projectToolResultMessages(messages, config)
	if report.Preserved != 1 || report.Transient != 1 {
		t.Fatalf("projection report = %#v", report)
	}
	if got := toolResultContent(projected, "reused"); got != "keep first result" {
		t.Fatalf("remaining reused-ID result = %q", got)
	}
	if got := toolUseNames(projected); strings.Join(got, ",") != "preserve_tool" {
		t.Fatalf("remaining reused-ID call = %v", got)
	}
}

func TestAssemblerProjectsRetentionWithoutBreakingToolPairing(t *testing.T) {
	store, conversationID := setupAssemblerStore(t)
	ctx := context.Background()
	stored := []Message{
		{Role: "user", Content: "log and summarize", TokenCount: 20},
		{
			Role: "assistant", TokenCount: 20,
			Parts: []MessagePart{
				{Type: "tool_use", Name: "daily_summary", ToolCallID: "transient", Arguments: `{}`},
				{Type: "tool_use", Name: "log_meal", ToolCallID: "durable", Arguments: `{}`},
			},
		},
		retentionToolResultMessage(0, "transient", "success", "routine day totals", false),
		retentionToolResultMessage(0, "durable", "success", "meal 42 full payload", false),
		{Role: "assistant", Content: "done", TokenCount: 20},
	}
	items := make([]ContextItem, 0, len(stored))
	var durableResultID int64
	for i, message := range stored {
		added, err := addRetentionTestMessage(ctx, store, conversationID, message)
		if err != nil {
			t.Fatal(err)
		}
		items = append(items, ContextItem{
			Ordinal: (i + 1) * 100, ItemType: "message", MessageID: added.ID, TokenCount: 20,
		})
		if toolResultContent([]Message{*added}, "durable") != "" {
			durableResultID = added.ID
		}
	}
	if err := store.UpsertContextItems(ctx, conversationID, items); err != nil {
		t.Fatal(err)
	}

	assembler := &Assembler{store: store, config: Config{
		ResultRetentionPolicy: toolpolicy.ResultRetentionPolicy{
			"daily_summary": {Mode: toolpolicy.ResultRetentionTransient},
			"log_meal": {
				Mode: toolpolicy.ResultRetentionDurable, Receipt: "Meal saved in Nutrition DB.",
			},
		},
	}}
	result, err := assembler.Assemble(ctx, conversationID, AssembleInput{Budget: 10_000})
	if err != nil {
		t.Fatal(err)
	}
	if got := toolUseNames(result.Messages); strings.Join(got, ",") != "log_meal" {
		t.Fatalf("assembled calls = %v", got)
	}
	if got := toolResultContent(result.Messages, "durable"); got != "Meal saved in Nutrition DB." {
		t.Fatalf("assembled durable receipt = %q", got)
	}
	if got := toolResultContent(result.Messages, "transient"); got != "" {
		t.Fatalf("assembled transient output = %q", got)
	}
	raw, err := store.GetMessageByID(ctx, durableResultID)
	if err != nil {
		t.Fatal(err)
	}
	if got := toolResultContent([]Message{*raw}, "durable"); got != "meal 42 full payload" {
		t.Fatalf("assembly mutated durable audit row: %q", got)
	}
}

func TestCompactLeafUsesRetentionProjectionWithoutMutatingAuditRows(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	conversation, err := store.GetOrCreateConversation(ctx, "retention-compaction")
	if err != nil {
		t.Fatal(err)
	}
	messages := []Message{
		{Role: "user", Content: "log and report", TokenCount: 100},
		retentionToolUseMessage(0, "receipt", "lookup"),
		retentionToolResultMessage(0, "receipt", "success", "private large lookup output", false),
		retentionToolUseMessage(0, "transient", "daily_summary"),
		retentionToolResultMessage(0, "transient", "success", "routine daily summary", false),
		{Role: "assistant", Content: "done", TokenCount: 100},
		{Role: "user", Content: "thanks", TokenCount: 100},
		{Role: "assistant", Content: "you are welcome", TokenCount: 100},
	}
	contextIDs := make([]int64, 0, len(messages))
	var receiptResultID int64
	for _, message := range messages {
		added, addErr := addRetentionTestMessage(ctx, store, conversation.ConversationID, message)
		if addErr != nil {
			t.Fatal(addErr)
		}
		contextIDs = append(contextIDs, added.ID)
		if toolResultContent([]Message{*added}, "receipt") != "" {
			receiptResultID = added.ID
		}
	}
	if appendErr := store.AppendContextMessages(ctx, conversation.ConversationID, contextIDs); appendErr != nil {
		t.Fatal(appendErr)
	}

	var prompt string
	engine := &CompactionEngine{
		store: store,
		config: Config{ResultRetentionPolicy: toolpolicy.ResultRetentionPolicy{
			"lookup":        {Mode: toolpolicy.ResultRetentionCompactReceipt, Receipt: "Lookup completed."},
			"daily_summary": {Mode: toolpolicy.ResultRetentionTransient},
		}},
		complete: func(_ context.Context, input string, _ CompleteOptions) (string, error) {
			prompt = input
			return "Conversation actions summarized.", nil
		},
	}
	summaryID, err := engine.compactLeaf(ctx, conversation.ConversationID, true)
	if err != nil {
		t.Fatal(err)
	}
	if summaryID == nil {
		t.Fatal("expected compacted summary")
	}
	if !strings.Contains(prompt, "Lookup completed.") ||
		strings.Contains(prompt, "private large lookup output") ||
		strings.Contains(prompt, "routine daily summary") {
		t.Fatalf("unexpected compaction prompt: %s", prompt)
	}
	raw, err := store.GetMessageByID(ctx, receiptResultID)
	if err != nil {
		t.Fatal(err)
	}
	if got := toolResultContent([]Message{*raw}, "receipt"); got != "private large lookup output" {
		t.Fatalf("compaction mutated audit row: %q", got)
	}
	sources, err := store.GetSummarySourceMessages(ctx, *summaryID)
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != len(messages) {
		t.Fatalf("summary source messages = %d, want %d raw messages", len(sources), len(messages))
	}
}

func TestToolResultStatusPersistsAcrossStoreReopen(t *testing.T) {
	path := t.TempDir() + "/retention.db"
	store, err := openRetentionTestStore(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	conversation, err := store.GetOrCreateConversation(ctx, "restart")
	if err != nil {
		t.Fatal(err)
	}
	message, err := store.AddMessageWithParts(ctx, conversation.ConversationID, "tool", []MessagePart{{
		Type:             "tool_result",
		ToolCallID:       "call-1",
		ToolResultStatus: "error",
		Text:             "failed",
	}}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if closeErr := store.db.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}

	store, err = openRetentionTestStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.db.Close()
	reloaded, err := store.GetMessageByID(ctx, message.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(reloaded.Parts) != 1 || reloaded.Parts[0].ToolResultStatus != "error" {
		t.Fatalf("reloaded status = %#v", reloaded.Parts)
	}
}

func openRetentionTestStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if err := runSchema(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

type retentionTestCall struct {
	id      string
	name    string
	status  string
	content string
	media   bool
}

func retentionTestMessages(calls []retentionTestCall) []Message {
	assistant := Message{ID: 1, Role: "assistant"}
	for _, call := range calls {
		assistant.Parts = append(assistant.Parts, MessagePart{
			Type: "tool_use", Name: call.name, ToolCallID: call.id, Arguments: `{}`,
		})
	}
	assistant.Content = partsToReadableContent(assistant.Parts)
	messages := make([]Message, 0, 2+len(calls))
	messages = append(messages, Message{ID: 10, Role: "user", Content: "run tools"}, assistant)
	for i, call := range calls {
		messages = append(messages, retentionToolResultMessage(
			int64(i+2), call.id, call.status, call.content, call.media,
		))
	}
	return messages
}

func retentionToolUseMessage(id int64, callID, name string) Message {
	message := Message{ID: id, Role: "assistant", TokenCount: 100, Parts: []MessagePart{{
		Type: "tool_use", Name: name, ToolCallID: callID, Arguments: `{}`,
	}}}
	message.Content = partsToReadableContent(message.Parts)
	return message
}

func retentionToolResultMessage(id int64, callID, status, content string, media bool) Message {
	message := Message{ID: id, Role: "tool", TokenCount: 100, Parts: []MessagePart{{
		Type: "tool_result", ToolCallID: callID, ToolResultStatus: status, Text: content,
	}}}
	if media {
		message.Parts = append(message.Parts, MessagePart{Type: "media", MediaURI: "media://artifact"})
	}
	message.Content = partsToReadableContent(message.Parts)
	return message
}

func toolUseNames(messages []Message) []string {
	var names []string
	for _, message := range messages {
		for _, part := range message.Parts {
			if part.Type == "tool_use" {
				names = append(names, part.Name)
			}
		}
	}
	return names
}

func toolResultContent(messages []Message, callID string) string {
	for _, message := range messages {
		for _, part := range message.Parts {
			if part.Type == "tool_result" && part.ToolCallID == callID {
				return part.Text
			}
		}
	}
	return ""
}

func addRetentionTestMessage(
	ctx context.Context,
	store *Store,
	conversationID int64,
	message Message,
) (*Message, error) {
	if len(message.Parts) > 0 {
		return store.AddMessageWithParts(
			ctx,
			conversationID,
			message.Role,
			message.Parts,
			message.TokenCount,
		)
	}
	return store.AddMessage(ctx, conversationID, message.Role, message.Content, message.TokenCount)
}
