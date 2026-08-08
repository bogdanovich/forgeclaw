package agent

import (
	"context"
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/providers"
	toolshared "github.com/bogdanovich/mintclaw/pkg/tools/shared"
)

func TestNewFinalizationContextCapturesTerminalSnapshot(t *testing.T) {
	ts := &turnState{
		opts: processOptions{
			SendResponse:                false,
			AllowInterimMintClawPublish: true,
			EnableSummary:               true,
		},
		followUps: []bus.InboundMessage{{Content: "follow up"}},
	}
	ts.RecordLLMUsage(&providers.UsageInfo{
		PromptTokens:     125,
		CompletionTokens: 25,
		TotalTokens:      150,
	})
	publisher := &streamingChunkPublisher{}
	exec := &turnExecution{
		model: turnExecutionModel{
			llmModelName:     "fallback-model",
			defaultModelName: "primary-model",
		},
		completionMedia:        []toolshared.CompletionMedia{{Ref: "media://result"}},
		sawAdditionalUserInput: true,
	}
	llm := &LLMIterationState{
		response:           &providers.LLMResponse{ReasoningContent: "reasoning"},
		streamingPublisher: publisher,
		streamingFallback:  true,
		llmModel:           "provider/fallback-model",
	}

	finalization := newFinalizationContext(ts, exec, llm, TurnEndStatusCompleted, "final answer")

	if finalization.disposition != finalResponsePending {
		t.Fatalf("disposition = %v, want pending", finalization.disposition)
	}
	if finalization.historyMessage == nil {
		t.Fatal("history message is nil")
	}
	if finalization.historyMessage.Content != "final answer" ||
		finalization.historyMessage.ModelName != "fallback-model" ||
		finalization.historyMessage.ReasoningContent != "reasoning" {
		t.Fatalf("history message = %+v", *finalization.historyMessage)
	}
	if finalization.usage != (finalizationUsage{inputTokens: 125, outputTokens: 25, totalTokens: 150}) {
		t.Fatalf("usage = %+v", finalization.usage)
	}
	if finalization.stream.publisher != publisher || !finalization.stream.fallback ||
		finalization.stream.modelName != "provider/fallback-model" {
		t.Fatalf("stream = %+v", finalization.stream)
	}
	if !finalization.delivery.allowInterimMintClawPublish ||
		!finalization.delivery.preferNewOutboundReply ||
		!finalization.delivery.compactAfterDelivery {
		t.Fatalf("delivery = %+v", finalization.delivery)
	}

	exec.completionMedia[0].Ref = "media://mutated"
	ts.followUps[0].Content = "mutated"
	if finalization.completionMedia[0].Ref != "media://result" {
		t.Fatalf("completion media changed after capture: %+v", finalization.completionMedia)
	}
	if finalization.followUps[0].Content != "follow up" {
		t.Fatalf("follow ups changed after capture: %+v", finalization.followUps)
	}
}

func TestFinalizationContextAlreadyHandledSkipsHistoryAndCompaction(t *testing.T) {
	ts := &turnState{
		opts: processOptions{EnableSummary: true},
	}
	exec := &turnExecution{
		model: turnExecutionModel{
			llmModelName:     "active-model",
			defaultModelName: "default-model",
		},
	}
	llm := &LLMIterationState{allResponsesHandled: true}
	finalization := newFinalizationContext(ts, exec, llm, TurnEndStatusCompleted, "")
	if finalization.disposition != finalResponseAlreadyHandled {
		t.Fatalf("disposition = %v, want already handled", finalization.disposition)
	}
	if finalization.historyMessage != nil {
		t.Fatalf("history message = %+v, want nil", finalization.historyMessage)
	}

	result, err := (&Pipeline{}).Finalize(context.Background(), ts, finalization)
	if err != nil {
		t.Fatalf("Finalize() error = %v", err)
	}
	if result.compactAfterDelivery {
		t.Fatal("already-handled response requested compaction")
	}
	if result.modelName != "active-model" || result.defaultModelName != "default-model" {
		t.Fatalf("result models = (%q, %q)", result.modelName, result.defaultModelName)
	}
}
