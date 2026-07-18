package agent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/media"
	"github.com/sipeed/picoclaw/pkg/testharness/llmscenario"
	"github.com/sipeed/picoclaw/pkg/tools"
)

func TestMockLLMScenario_ProcessDirectExecutesToolAndReturnsFinalAnswer(t *testing.T) {
	const toolName = "scenario_extract_recipe"

	provider := llmscenario.NewScriptedProvider(
		"scenario-model",
		llmscenario.ProviderStep{
			Name: "request recipe extraction tool",
			Assert: func(call llmscenario.ProviderCall) error {
				if err := llmscenario.RequireToolDefinition(toolName)(call); err != nil {
					return err
				}
				if len(call.Messages) == 0 ||
					!strings.Contains(call.Messages[len(call.Messages)-1].Content, "Instagram caption") {
					return fmt.Errorf("first model call did not receive user prompt: %#v", call.Messages)
				}
				return nil
			},
			Response: llmscenario.ToolCallResponse(
				"I will extract the recipe.",
				llmscenario.ToolCall("call_extract_1", toolName, map[string]any{
					"url": "https://example.test/reel",
				}),
			),
		},
		llmscenario.ProviderStep{
			Name: "final answer after tool result",
			Assert: func(call llmscenario.ProviderCall) error {
				if err := llmscenario.RequireLastMessage("tool", "raspberries + chocolate")(call); err != nil {
					return err
				}
				return nil
			},
			Response: llmscenario.TextResponse("Final recipe: raspberries with dark chocolate."),
		},
	)

	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()

	stub := llmscenario.NewStubTool(
		toolName,
		tools.NewToolResult("recipe extracted: raspberries + chocolate"),
	)
	agent.Tools.Register(stub)

	response, err := al.ProcessDirectWithChannel(
		context.Background(),
		"Extract recipe from this Instagram caption",
		"scenario-session",
		"telegram",
		"chat-123",
	)
	if err != nil {
		t.Fatalf("ProcessDirectWithChannel failed: %v", err)
	}
	if response != "Final recipe: raspberries with dark chocolate." {
		t.Fatalf("response = %q", response)
	}
	if err := provider.AssertExhausted(); err != nil {
		t.Fatal(err)
	}

	toolCalls := stub.Calls()
	if len(toolCalls) != 1 {
		t.Fatalf("tool call count = %d, want 1", len(toolCalls))
	}
	if toolCalls[0].Args["url"] != "https://example.test/reel" {
		t.Fatalf("tool args = %#v", toolCalls[0].Args)
	}
	if toolCalls[0].Channel != "telegram" || toolCalls[0].ChatID != "chat-123" {
		t.Fatalf("tool context = channel %q chat %q, want telegram/chat-123", toolCalls[0].Channel, toolCalls[0].ChatID)
	}
}

func TestMockLLMScenario_StatelessDirectTurnsKeepToolsButNotPriorTurns(t *testing.T) {
	const toolName = "scenario_stateless_lookup"
	const firstPrompt = "review the first pull request"

	provider := llmscenario.NewScriptedProvider(
		"scenario-model",
		llmscenario.ProviderStep{
			Name: "first turn requests tool",
			Response: llmscenario.ToolCallResponse(
				"I will inspect it.",
				llmscenario.ToolCall("call_stateless_1", toolName, map[string]any{}),
			),
		},
		llmscenario.ProviderStep{
			Name:     "first turn receives tool result",
			Assert:   llmscenario.RequireLastMessage("tool", "current pull request details"),
			Response: llmscenario.TextResponse("first review complete"),
		},
		llmscenario.ProviderStep{
			Name: "second turn excludes first turn",
			Assert: func(call llmscenario.ProviderCall) error {
				for _, msg := range call.Messages {
					if strings.Contains(msg.Content, firstPrompt) ||
						strings.Contains(msg.Content, "current pull request details") ||
						strings.Contains(msg.Content, "first review complete") {
						return fmt.Errorf("second stateless turn inherited prior content: %#v", call.Messages)
					}
				}
				return nil
			},
			Response: llmscenario.TextResponse("second review complete"),
		},
	)

	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()
	agent.Tools.Register(llmscenario.NewStubTool(
		toolName,
		tools.NewToolResult("current pull request details"),
	))

	opts := DirectTurnOptions{Stateless: true}
	response, err := al.ProcessDirectWithOptions(
		context.Background(), firstPrompt, "shared-session", "cli", "direct", opts,
	)
	if err != nil {
		t.Fatalf("first stateless turn failed: %v", err)
	}
	if response != "first review complete" {
		t.Fatalf("first response = %q", response)
	}

	response, err = al.ProcessDirectWithOptions(
		context.Background(), "review the second pull request", "shared-session", "cli", "direct", opts,
	)
	if err != nil {
		t.Fatalf("second stateless turn failed: %v", err)
	}
	if response != "second review complete" {
		t.Fatalf("second response = %q", response)
	}
	for _, sessionKey := range agent.Sessions.ListSessions() {
		if history := agent.Sessions.GetHistory(sessionKey); len(history) != 0 {
			t.Fatalf("stateless turns persisted %d messages in %q", len(history), sessionKey)
		}
	}
	if err := provider.AssertExhausted(); err != nil {
		t.Fatal(err)
	}
}

func TestMockLLMScenario_QueuedMediaFallbackContinuesToFinalAnswer(t *testing.T) {
	const toolName = "scenario_media_tool"

	provider := llmscenario.NewScriptedProvider(
		"scenario-model",
		llmscenario.ProviderStep{
			Name:   "request media tool",
			Assert: llmscenario.RequireToolDefinition(toolName),
			Response: llmscenario.ToolCallResponse(
				"I will prepare the image.",
				llmscenario.ToolCall("call_media_1", toolName, map[string]any{}),
			),
		},
		llmscenario.ProviderStep{
			Name: "final answer after queued media tool result",
			Assert: func(call llmscenario.ProviderCall) error {
				for _, msg := range call.Messages {
					if msg.Role == "tool" && strings.Contains(msg.Content, "queued media payload") {
						return nil
					}
				}
				return fmt.Errorf(
					"provider call did not include tool result with queued media payload: %#v",
					call.Messages,
				)
			},
			Response: llmscenario.TextResponse("Final answer after queued media."),
		},
	)

	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()

	store := media.NewFileMediaStore()
	al.SetMediaStore(store)

	imagePath := filepath.Join(t.TempDir(), "queued.png")
	if err := os.WriteFile(imagePath, []byte("fake queued image"), 0o644); err != nil {
		t.Fatalf("WriteFile(imagePath) error = %v", err)
	}
	ref, err := store.Store(imagePath, media.MediaMeta{
		Filename:    filepath.Base(imagePath),
		ContentType: "image/png",
		Source:      "test:scenario_media_tool",
	}, "test:scenario_media")
	if err != nil {
		t.Fatalf("store.Store() error = %v", err)
	}

	stub := llmscenario.NewStubTool(
		toolName,
		tools.MediaResult("queued media payload", []string{ref}).WithResponseHandled(),
	)
	agent.Tools.Register(stub)

	response, err := al.ProcessDirectWithChannel(
		context.Background(),
		"Prepare image and then finish the answer",
		"scenario-queued-media",
		"telegram",
		"chat-queued",
	)
	if err != nil {
		t.Fatalf("ProcessDirectWithChannel failed: %v", err)
	}
	if response != "Final answer after queued media." {
		t.Fatalf("response = %q", response)
	}
	if err := provider.AssertExhausted(); err != nil {
		t.Fatal(err)
	}

	msgBus, ok := al.bus.(*bus.MessageBus)
	if !ok {
		t.Fatalf("agent bus = %T, want *bus.MessageBus", al.bus)
	}
	select {
	case outbound := <-msgBus.OutboundMediaChan():
		if outbound.Channel != "telegram" || outbound.ChatID != "chat-queued" {
			t.Fatalf("unexpected outbound media target: %+v", outbound)
		}
		if len(outbound.Parts) != 1 {
			t.Fatalf("outbound media parts = %d, want 1", len(outbound.Parts))
		}
	default:
		t.Fatal("expected queued outbound media message")
	}

	if calls := provider.Calls(); len(calls) != 2 {
		t.Fatalf("provider call count = %d, want 2", len(calls))
	}
}

func TestMockLLMScenario_DirectMediaDeliverySkipsFollowUpLLM(t *testing.T) {
	const toolName = "scenario_media_tool"

	provider := llmscenario.NewScriptedProvider(
		"scenario-model",
		llmscenario.ProviderStep{
			Name:   "request media tool",
			Assert: llmscenario.RequireToolDefinition(toolName),
			Response: llmscenario.ToolCallResponse(
				"I will send the image now.",
				llmscenario.ToolCall("call_media_1", toolName, map[string]any{}),
			),
		},
	)

	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()

	store := media.NewFileMediaStore()
	al.SetMediaStore(store)
	telegramChannel := &fakeMediaChannel{fakeChannel: fakeChannel{id: "rid-telegram"}}
	al.SetChannelManager(newStartedTestChannelManager(t, al.bus.(*bus.MessageBus), store, "telegram", telegramChannel))

	imagePath := filepath.Join(t.TempDir(), "direct.png")
	if err := os.WriteFile(imagePath, []byte("fake direct image"), 0o644); err != nil {
		t.Fatalf("WriteFile(imagePath) error = %v", err)
	}
	ref, err := store.Store(imagePath, media.MediaMeta{
		Filename:    filepath.Base(imagePath),
		ContentType: "image/png",
		Source:      "test:scenario_media_tool",
	}, "test:scenario_media")
	if err != nil {
		t.Fatalf("store.Store() error = %v", err)
	}

	stub := llmscenario.NewStubTool(
		toolName,
		tools.MediaResult("direct media payload", []string{ref}).WithResponseHandled(),
	)
	agent.Tools.Register(stub)

	response, err := al.ProcessDirectWithChannel(
		context.Background(),
		"Send the image without any follow-up",
		"scenario-direct-media",
		"telegram",
		"chat-direct",
	)
	if err != nil {
		t.Fatalf("ProcessDirectWithChannel failed: %v", err)
	}
	if response != "" {
		t.Fatalf("response = %q, want empty", response)
	}
	if err := provider.AssertExhausted(); err != nil {
		t.Fatal(err)
	}
	if len(provider.Calls()) != 1 {
		t.Fatalf("provider call count = %d, want 1", len(provider.Calls()))
	}
	if len(telegramChannel.sentMedia) != 1 {
		t.Fatalf("sent media count = %d, want 1", len(telegramChannel.sentMedia))
	}
	if telegramChannel.sentMedia[0].Channel != "telegram" || telegramChannel.sentMedia[0].ChatID != "chat-direct" {
		t.Fatalf("unexpected sent media target: %+v", telegramChannel.sentMedia[0])
	}

	msgBus := al.bus.(*bus.MessageBus)
	select {
	case extra := <-msgBus.OutboundMediaChan():
		t.Fatalf("expected direct media delivery to bypass outbound media queue, got %+v", extra)
	default:
	}
}
