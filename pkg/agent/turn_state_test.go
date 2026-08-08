package agent

import (
	"errors"
	"strings"
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/providers"
)

func TestNewLLMIterationStateDoesNotRetainPriorCallState(t *testing.T) {
	first := newLLMIterationState(1)
	first.response = &providers.LLMResponse{Content: "stale response"}
	first.normalizedToolCalls = []providers.ToolCall{{ID: "stale-tool"}}
	first.allResponsesHandled = true
	first.streamingPublisher = &streamingChunkPublisher{}
	first.streamingFallback = true
	first.suppressReasoning = true
	first.callMessages = []providers.Message{{Role: "user", Content: "stale request"}}
	first.providerToolDefs = []providers.ToolDefinition{{}}
	first.llmModel = "stale-model"
	first.llmOpts = map[string]any{"stale": true}
	first.gracefulTerminal = true
	first.useNativeSearch = true
	first.assistantToolCallsPersisted = true
	first.assistantToolCallsWriteErr = errors.New("stale journal error")

	second := newLLMIterationState(2)

	if second.iteration != 2 {
		t.Fatalf("iteration = %d, want 2", second.iteration)
	}
	if second.response != nil || len(second.normalizedToolCalls) != 0 || second.allResponsesHandled {
		t.Fatalf("response state leaked into next iteration: %+v", second)
	}
	if second.streamingPublisher != nil || second.streamingFallback || second.suppressReasoning {
		t.Fatalf("streaming state leaked into next iteration: %+v", second)
	}
	if len(second.callMessages) != 0 || len(second.providerToolDefs) != 0 ||
		second.llmModel != "" || second.llmOpts != nil {
		t.Fatalf("request state leaked into next iteration: %+v", second)
	}
	if second.gracefulTerminal || second.useNativeSearch ||
		second.assistantToolCallsPersisted || second.assistantToolCallsWriteErr != nil {
		t.Fatalf("terminal or journal state leaked into next iteration: %+v", second)
	}
}

func TestMatchingTurnMessageTail_IgnoresInternalRuntimeFields(t *testing.T) {
	history := []providers.Message{
		{Role: "user", Content: "question"},
		{
			Role: "assistant",
			ToolCalls: []providers.ToolCall{
				{
					ID:   "call_1",
					Type: "function",
					Function: &providers.FunctionCall{
						Name:      "read_file",
						Arguments: `{"path":"/tmp/test"}`,
					},
				},
			},
		},
	}

	persisted := []providers.Message{
		userPromptMessage("question", nil),
		{
			Role: "assistant",
			ToolCalls: []providers.ToolCall{
				{
					ID:               "call_1",
					Type:             "function",
					Name:             "read_file",
					Arguments:        map[string]any{"path": "/tmp/test"},
					ThoughtSignature: "internal-signature",
					Function: &providers.FunctionCall{
						Name:             "read_file",
						Arguments:        `{"path":"/tmp/test"}`,
						ThoughtSignature: "internal-signature",
					},
				},
			},
		},
	}

	if got := matchingTurnMessageTail(history, persisted); got != 2 {
		t.Fatalf("matchingTurnMessageTail() = %d, want 2", got)
	}
}

func TestSplitHistoryForActiveTurn_ProtectsPersistedTail(t *testing.T) {
	history := []providers.Message{
		{Role: "user", Content: "old question"},
		{Role: "assistant", Content: "old answer"},
		{Role: "user", Content: "current question"},
		{Role: "tool", Content: "tool output", ToolCallID: "call_1"},
	}

	persisted := []providers.Message{
		userPromptMessage("current question", nil),
		{Role: "tool", Content: "tool output", ToolCallID: "call_1"},
	}

	stable, protected := splitHistoryForActiveTurn(history, persisted)
	if len(stable) != 2 {
		t.Fatalf("stable history len = %d, want 2", len(stable))
	}
	if len(protected) != 2 {
		t.Fatalf("protected tail len = %d, want 2", len(protected))
	}
	if protected[0].Content != "current question" {
		t.Fatalf("protected[0].Content = %q, want current question", protected[0].Content)
	}
}

func TestTrimHistoryToFitContextWindow_WithProtectedTurnTailKeepsActiveTurn(t *testing.T) {
	current := strings.Repeat("current turn ", 80)
	history := []providers.Message{
		{Role: "user", Content: strings.Repeat("old turn ", 60)},
		{Role: "assistant", Content: strings.Repeat("old reply ", 60)},
		{Role: "user", Content: current},
	}

	stable, protected := splitHistoryForActiveTurn(history, []providers.Message{
		userPromptMessage(current, nil),
	})
	trimmedStable, messages, fit := trimHistoryToFitContextWindow(
		stable,
		func(trimmedHistory []providers.Message) []providers.Message {
			return append(append([]providers.Message(nil), trimmedHistory...), protected...)
		},
		120,
		nil,
		0,
	)

	if fit {
		t.Fatal("expected protected active turn alone to remain over budget")
	}
	if len(trimmedStable) != 0 {
		t.Fatalf("trimmed stable history len = %d, want 0", len(trimmedStable))
	}
	if len(messages) != 1 {
		t.Fatalf("messages len = %d, want 1 protected active-turn message", len(messages))
	}
	if messages[0].Content != current {
		t.Fatalf("messages[0].Content = %q, want protected current turn", messages[0].Content)
	}
}
