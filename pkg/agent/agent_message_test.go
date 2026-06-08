package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
)

func TestWrapScheduledPayloadAddsDeliveryGuard(t *testing.T) {
	payload := "Напоминание: позвонить в PG&E."

	got := wrapScheduledPayload(payload)

	if !strings.Contains(got, "scheduled job that is firing now") {
		t.Fatalf("wrapped payload missing scheduled-job guard:\n%s", got)
	}
	if !strings.Contains(got, "Do not create, update, remove") {
		t.Fatalf("wrapped payload missing anti-reschedule instruction:\n%s", got)
	}
	if !strings.Contains(got, payload) {
		t.Fatalf("wrapped payload missing original content:\n%s", got)
	}
}

func TestProcessScheduledWithChannelWrapsPayloadBeforeLLM(t *testing.T) {
	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         t.TempDir(),
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	provider := &recordingProvider{}
	al := NewAgentLoop(cfg, bus.NewMessageBus(), provider)

	payload := "Напоминание: позвонить в PG&E."
	response, err := al.ProcessScheduledWithChannel(
		context.Background(),
		payload,
		"agent:cron-test",
		"telegram",
		"chat-1",
	)
	if err != nil {
		t.Fatalf("ProcessScheduledWithChannel() error = %v", err)
	}
	if response != "Mock response" {
		t.Fatalf("ProcessScheduledWithChannel() response = %q, want %q", response, "Mock response")
	}
	if len(provider.lastMessages) == 0 {
		t.Fatal("provider did not receive any messages")
	}

	lastMessage := provider.lastMessages[len(provider.lastMessages)-1]
	if lastMessage.Role != "user" {
		t.Fatalf("last provider message role = %q, want user", lastMessage.Role)
	}
	if !strings.Contains(lastMessage.Content, "scheduled job that is firing now") {
		t.Fatalf("last provider message missing scheduled-job guard:\n%s", lastMessage.Content)
	}
	if !strings.Contains(lastMessage.Content, payload) {
		t.Fatalf("last provider message missing original payload:\n%s", lastMessage.Content)
	}
}
