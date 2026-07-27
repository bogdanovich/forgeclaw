//go:build !mipsle && !netbsd && !(freebsd && arm)

package agent

import (
	"context"
	"fmt"
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/logger"
	"github.com/bogdanovich/mintclaw/pkg/providers"
)

func BenchmarkContextManagerAssemble(b *testing.B) {
	logger.DisableConsole()

	for _, messageCount := range []int{100, 500, 1000} {
		b.Run(fmt.Sprintf("messages=%d", messageCount), func(b *testing.B) {
			history := benchHistory(messageCount)
			b.Run("none/assemble", func(b *testing.B) {
				al := newBenchContextAgentLoop(b, "none", history)
				benchAssembleOnly(b, al.contextManager)
			})
			b.Run("seahorse/assemble", func(b *testing.B) {
				al := newBenchContextAgentLoop(b, "seahorse", history)
				benchAssembleOnly(b, al.contextManager)
			})
			b.Run("none/assemble_build_messages", func(b *testing.B) {
				al := newBenchContextAgentLoop(b, "none", history)
				agent := al.registry.GetDefaultAgent()
				benchAssembleAndBuildMessages(b, al.contextManager, agent)
			})
			b.Run("seahorse/assemble_build_messages", func(b *testing.B) {
				al := newBenchContextAgentLoop(b, "seahorse", history)
				agent := al.registry.GetDefaultAgent()
				benchAssembleAndBuildMessages(b, al.contextManager, agent)
			})
		})
	}
}

func benchAssembleOnly(b *testing.B, cm ContextManager) {
	b.Helper()
	ctx := context.Background()
	req := &AssembleRequest{
		SessionKey: "bench-session",
		Budget:     131072,
		MaxTokens:  4096,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := cm.Assemble(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
		if resp == nil {
			b.Fatal("nil assemble response")
		}
	}
}

func benchAssembleAndBuildMessages(b *testing.B, cm ContextManager, agent *AgentInstance) {
	b.Helper()
	ctx := context.Background()
	req := &AssembleRequest{
		SessionKey: "bench-session",
		Budget:     131072,
		MaxTokens:  4096,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := cm.Assemble(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
		messages := agent.ContextBuilder.BuildMessagesFromPrompt(PromptBuildRequest{
			History:        resp.History,
			Summary:        resp.Summary,
			CurrentMessage: "current user message",
			Channel:        "bench",
			ChatID:         "bench-chat",
		})
		if len(messages) == 0 {
			b.Fatal("empty built messages")
		}
	}
}

func newBenchContextAgentLoop(b *testing.B, contextManager string, history []providers.Message) *AgentLoop {
	b.Helper()
	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         b.TempDir(),
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
				ContextWindow:     131072,
				ContextManager:    contextManager,
			},
		},
	}
	al := newCMTestAgentLoop(cfg)
	agent := al.registry.GetDefaultAgent()
	agent.Sessions.SetHistory("bench-session", history)
	for _, msg := range history {
		if err := al.contextManager.Ingest(context.Background(), &IngestRequest{
			SessionKey: "bench-session",
			Message:    msg,
		}); err != nil {
			b.Fatalf("ingest: %v", err)
		}
	}
	return al
}

func benchHistory(messageCount int) []providers.Message {
	history := make([]providers.Message, 0, messageCount)
	for i := 0; i < messageCount; i++ {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		history = append(history, providers.Message{
			Role: role,
			Content: fmt.Sprintf(
				"Message %04d. This is representative conversation content with enough words to exercise context assembly, ordering, and token accounting.",
				i,
			),
		})
	}
	return history
}
