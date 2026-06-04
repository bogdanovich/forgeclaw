package integrationtools

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/media"
	"github.com/sipeed/picoclaw/pkg/session"
)

func TestMessageTool_Execute_Success(t *testing.T) {
	tool := NewMessageTool()

	ctx := WithToolContext(context.Background(), "test-channel", "test-chat-id")
	args := map[string]any{
		"content": "Hello, world!",
	}

	result := tool.Execute(ctx, args)

	if result.Outbound == nil {
		t.Fatal("expected declarative outbound delivery")
	}
	if result.Outbound.Channel != "test-channel" {
		t.Errorf("Expected channel 'test-channel', got '%s'", result.Outbound.Channel)
	}
	if result.Outbound.ChatID != "test-chat-id" {
		t.Errorf("Expected chatID 'test-chat-id', got '%s'", result.Outbound.ChatID)
	}
	if result.Outbound.Text != "Hello, world!" {
		t.Errorf("Expected content 'Hello, world!', got '%s'", result.Outbound.Text)
	}
	if len(result.Outbound.Media) != 0 {
		t.Fatalf("expected no media parts, got %d", len(result.Outbound.Media))
	}

	// Verify ToolResult meets US-011 criteria:
	// - Send success returns SilentResult (Silent=true)
	if !result.Silent {
		t.Error("Expected Silent=true for successful send")
	}
	if result.DeliveryIntent != DeliveryImmediateContinue {
		t.Fatalf("delivery intent = %q, want immediate_continue", result.DeliveryIntent)
	}

	// - ForLLM contains send status description
	if result.ForLLM != "Message sent to test-channel:test-chat-id" {
		t.Errorf("Expected ForLLM 'Message sent to test-channel:test-chat-id', got '%s'", result.ForLLM)
	}

	// - ForUser is empty (user already received message directly)
	if result.ForUser != "" {
		t.Errorf("Expected ForUser to be empty, got '%s'", result.ForUser)
	}

	// - IsError should be false
	if result.IsError {
		t.Error("Expected IsError=false for successful send")
	}
}

func TestMessageTool_Execute_WithCustomChannel(t *testing.T) {
	tool := NewMessageTool()

	ctx := WithToolContext(context.Background(), "default-channel", "default-chat-id")
	args := map[string]any{
		"content": "Test message",
		"channel": "custom-channel",
		"chat_id": "custom-chat-id",
	}

	result := tool.Execute(ctx, args)

	// Verify custom channel/chatID were used instead of defaults
	if result.Outbound == nil {
		t.Fatal("expected declarative outbound delivery")
	}
	if result.Outbound.Channel != "custom-channel" {
		t.Errorf("Expected channel 'custom-channel', got '%s'", result.Outbound.Channel)
	}
	if result.Outbound.ChatID != "custom-chat-id" {
		t.Errorf("Expected chatID 'custom-chat-id', got '%s'", result.Outbound.ChatID)
	}

	if !result.Silent {
		t.Error("Expected Silent=true")
	}
	if result.ForLLM != "Message sent to custom-channel:custom-chat-id" {
		t.Errorf("Expected ForLLM 'Message sent to custom-channel:custom-chat-id', got '%s'", result.ForLLM)
	}
}

func TestMessageTool_Execute_IgnoresLegacySendCallback(t *testing.T) {
	tool := NewMessageTool()
	called := false
	tool.SetSendCallback(func(
		ctx context.Context,
		channel, chatID, content, replyToMessageID string,
		mediaParts []bus.MediaPart,
	) error {
		called = true
		return nil
	})

	ctx := WithToolContext(context.Background(), "test-channel", "test-chat-id")
	args := map[string]any{
		"content": "Test message",
	}

	result := tool.Execute(ctx, args)
	if result.IsError {
		t.Fatalf("expected declarative send success, got %s", result.ForLLM)
	}
	if called {
		t.Fatal("legacy send callback should not be called")
	}
}

func TestMessageTool_Execute_MissingContent(t *testing.T) {
	tool := NewMessageTool()

	ctx := WithToolContext(context.Background(), "test-channel", "test-chat-id")
	args := map[string]any{} // content missing

	result := tool.Execute(ctx, args)

	// Verify error result for missing content/media
	if !result.IsError {
		t.Error("Expected IsError=true for missing content/media")
	}
	if result.ForLLM != "content or media is required" {
		t.Errorf("Expected ForLLM 'content or media is required', got '%s'", result.ForLLM)
	}
}

func TestMessageTool_Execute_NoTargetChannel(t *testing.T) {
	tool := NewMessageTool()
	// No WithToolContext — channel/chatID are empty

	tool.SetSendCallback(func(
		ctx context.Context,
		channel, chatID, content, replyToMessageID string,
		mediaParts []bus.MediaPart,
	) error {
		return nil
	})

	ctx := context.Background()
	args := map[string]any{
		"content": "Test message",
	}

	result := tool.Execute(ctx, args)

	// Verify error when no target channel specified
	if !result.IsError {
		t.Error("Expected IsError=true when no target channel")
	}
	if result.ForLLM != "No target channel/chat specified" {
		t.Errorf("Expected ForLLM 'No target channel/chat specified', got '%s'", result.ForLLM)
	}
}

func TestMessageTool_Execute_DeclarativeWithoutCallback(t *testing.T) {
	tool := NewMessageTool()
	// No SetSendCallback called

	ctx := WithToolContext(context.Background(), "test-channel", "test-chat-id")
	args := map[string]any{
		"content": "Test message",
	}

	result := tool.Execute(ctx, args)

	if result.IsError {
		t.Fatalf("expected declarative delivery without callback, got %s", result.ForLLM)
	}
	if result.Outbound == nil || result.Outbound.Text != "Test message" {
		t.Fatalf("unexpected outbound: %+v", result.Outbound)
	}
}

func TestMessageTool_Name(t *testing.T) {
	tool := NewMessageTool()
	if tool.Name() != "message" {
		t.Errorf("Expected name 'message', got '%s'", tool.Name())
	}
}

func TestMessageTool_Description(t *testing.T) {
	tool := NewMessageTool()
	desc := tool.Description()
	if desc == "" {
		t.Error("Description should not be empty")
	}
}

func TestMessageTool_Parameters(t *testing.T) {
	tool := NewMessageTool()
	params := tool.Parameters()

	// Verify parameters structure
	typ, ok := params["type"].(string)
	if !ok || typ != "object" {
		t.Error("Expected type 'object'")
	}

	props, ok := params["properties"].(map[string]any)
	if !ok {
		t.Fatal("Expected properties to be a map")
	}

	// Check required properties
	required, ok := params["required"].([]string)
	if !ok || len(required) != 1 || required[0] != "content" {
		t.Fatal("Expected content-only required schema when local media is disabled")
	}

	// Check content property
	contentProp, ok := props["content"].(map[string]any)
	if !ok {
		t.Error("Expected 'content' property")
	}
	if contentProp["type"] != "string" {
		t.Error("Expected content type to be 'string'")
	}

	if _, hasMediaProp := props["media"]; hasMediaProp {
		t.Fatal("did not expect 'media' property when local media is disabled")
	}

	// Check channel property (optional)
	channelProp, ok := props["channel"].(map[string]any)
	if !ok {
		t.Error("Expected 'channel' property")
	}
	if channelProp["type"] != "string" {
		t.Error("Expected channel type to be 'string'")
	}

	// Check chat_id property (optional)
	chatIDProp, ok := props["chat_id"].(map[string]any)
	if !ok {
		t.Error("Expected 'chat_id' property")
	}
	if chatIDProp["type"] != "string" {
		t.Error("Expected chat_id type to be 'string'")
	}

	// Check reply_to_message_id property (optional)
	replyToProp, ok := props["reply_to_message_id"].(map[string]any)
	if !ok {
		t.Error("Expected 'reply_to_message_id' property")
	}
	if replyToProp["type"] != "string" {
		t.Error("Expected reply_to_message_id type to be 'string'")
	}
}

func TestMessageTool_Parameters_WithLocalMediaEnabled(t *testing.T) {
	tool := NewMessageTool()
	tool.ConfigureLocalMedia(t.TempDir(), true, 1024*1024, nil)
	params := tool.Parameters()

	props, ok := params["properties"].(map[string]any)
	if !ok {
		t.Fatal("Expected properties to be a map")
	}
	mediaProp, ok := props["media"].(map[string]any)
	if !ok {
		t.Fatal("Expected 'media' property")
	}
	if mediaProp["type"] != "array" {
		t.Error("Expected media type to be 'array'")
	}
	anyOf, ok := params["anyOf"].([]map[string]any)
	if !ok || len(anyOf) != 2 {
		t.Fatal("Expected anyOf content/media requirement")
	}
	if _, ok := params["required"]; ok {
		t.Fatal("did not expect top-level required content when media is enabled")
	}
}

func TestMessageTool_Execute_WithMediaDisabled(t *testing.T) {
	tool := NewMessageTool()
	tool.SetSendCallback(func(
		ctx context.Context,
		channel, chatID, content, replyToMessageID string,
		mediaParts []bus.MediaPart,
	) error {
		t.Fatal("send callback should not run when message media is disabled")
		return nil
	})

	ctx := WithToolContext(context.Background(), "telegram", "-1001")
	result := tool.Execute(ctx, map[string]any{
		"media": []any{
			map[string]any{"path": "photo.jpg"},
		},
	})
	if !result.IsError {
		t.Fatal("expected error when message media is disabled")
	}
	if result.ForLLM != "message media attachments are disabled; enable tools.message.media_enabled to send local media through message" {
		t.Fatalf("unexpected error: %q", result.ForLLM)
	}
}

func TestMessageTool_Execute_WithReplyToMessageID(t *testing.T) {
	tool := NewMessageTool()

	ctx := WithToolContext(context.Background(), "test-channel", "test-chat-id")
	args := map[string]any{
		"content":             "Reply test",
		"reply_to_message_id": "msg-123",
	}

	result := tool.Execute(ctx, args)
	if result.IsError {
		t.Fatalf("expected success, got error: %s", result.ForLLM)
	}
	if result.Outbound == nil || result.Outbound.ReplyToMessageID != "msg-123" {
		t.Fatalf("unexpected reply_to_message_id in outbound: %+v", result.Outbound)
	}
}

func TestMessageTool_Execute_TracksSentTargetForTurnSuppression(t *testing.T) {
	tool := NewMessageTool()

	ctx := WithToolContext(context.Background(), "test-channel", "test-chat-id")
	ctx = WithToolSessionContext(ctx, "main", "sk_v1_tool", &session.SessionScope{
		Version:    session.ScopeVersionV1,
		AgentID:    "main",
		Channel:    "telegram",
		Dimensions: []string{"chat"},
		Values: map[string]string{
			"chat": "direct:test-chat-id",
		},
	})

	result := tool.Execute(ctx, map[string]any{"content": "Hello, world!"})
	if result.IsError {
		t.Fatalf("expected success, got error: %s", result.ForLLM)
	}
	if !tool.HasSentTo("sk_v1_tool", "test-channel", "test-chat-id") {
		t.Fatal("expected sent target tracking for final-response suppression")
	}
}

func TestMessageTool_Execute_WithMedia(t *testing.T) {
	tool := NewMessageTool()
	store := media.NewFileMediaStore()
	dir := t.TempDir()
	imgPath := filepath.Join(dir, "photo.jpg")
	if err := os.WriteFile(imgPath, []byte("fake image bytes"), 0o644); err != nil {
		t.Fatalf("write image: %v", err)
	}
	tool.ConfigureLocalMedia(dir, true, 1024*1024, []*regexp.Regexp{})
	tool.SetMediaStore(store)

	ctx := WithToolContext(context.Background(), "telegram", "-1001")
	result := tool.Execute(ctx, map[string]any{
		"content": "Caption text",
		"media": []any{
			map[string]any{
				"path": imgPath,
			},
		},
	})
	if result.IsError {
		t.Fatalf("expected success, got error: %s", result.ForLLM)
	}
	if result.Outbound == nil {
		t.Fatal("expected declarative outbound delivery")
	}
	if result.Outbound.Text != "Caption text" {
		t.Fatalf("content = %q, want Caption text", result.Outbound.Text)
	}
	if len(result.Outbound.Media) != 1 {
		t.Fatalf("expected 1 media part, got %d", len(result.Outbound.Media))
	}
	if result.Outbound.Media[0].Caption != "Caption text" {
		t.Fatalf("first part caption = %q, want Caption text", result.Outbound.Media[0].Caption)
	}
	if result.Outbound.Media[0].Ref == "" {
		t.Fatal("expected media ref to be populated")
	}
	if result.Outbound.Media[0].Type == "" {
		t.Fatal("expected media type to be inferred")
	}
}
