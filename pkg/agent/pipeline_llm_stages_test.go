// MintClaw - Ultra-lightweight personal AI agent

package agent

import (
	"context"
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/providers"
)

func TestLLMCallStagesKeepPreparationInvocationAndNormalizationSeparate(t *testing.T) {
	provider := &sequenceProvider{responses: []*providers.LLMResponse{{
		Content:      "stage response",
		FinishReason: "stop",
		Usage: &providers.UsageInfo{
			PromptTokens:     11,
			CompletionTokens: 7,
			TotalTokens:      18,
		},
	}}}
	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()

	pipeline := NewPipeline(al)
	ts := newTurnState(agent, makeTestProcessOpts("llm-stage-session"), turnEventScope{
		turnID:  "llm-stage-turn",
		context: newTurnContext(nil, nil, nil),
	})
	exec, err := pipeline.SetupTurn(t.Context(), ts)
	if err != nil {
		t.Fatalf("SetupTurn() error = %v", err)
	}
	llm := newLLMIterationState(1)

	prepared, err := pipeline.prepareLLMRequest(t.Context(), ts, exec, llm)
	if err != nil {
		t.Fatalf("prepareLLMRequest() error = %v", err)
	}
	if prepared.disposition == llmStageComplete {
		t.Fatalf("prepareLLMRequest() completed turn: %+v", prepared.outcome)
	}
	if provider.callCount != 0 || llm.response != nil {
		t.Fatalf(
			"preparation invoked provider or populated response: calls=%d response=%+v",
			provider.callCount,
			llm.response,
		)
	}
	if len(llm.callMessages) == 0 || llm.llmModel == "" || llm.llmOpts == nil {
		t.Fatalf("preparation did not populate request state: %+v", llm)
	}

	invoked, err := pipeline.invokeLLMWithRetry(t.Context(), t.Context(), ts, exec, llm)
	if err != nil {
		t.Fatalf("invokeLLMWithRetry() error = %v", err)
	}
	if invoked.disposition == llmStageComplete {
		t.Fatalf("invokeLLMWithRetry() completed turn: %+v", invoked.outcome)
	}
	if provider.callCount != 1 || llm.response == nil || llm.response.Content != "stage response" {
		t.Fatalf("invocation result = calls=%d response=%+v", provider.callCount, llm.response)
	}
	if calls, _, _, _ := ts.llmUsageTotals(); calls != 0 {
		t.Fatalf("invocation recorded usage before normalization: calls=%d", calls)
	}

	outcome, err := pipeline.normalizeAndDispatchLLMResponse(context.Background(), ts, exec, llm)
	if err != nil {
		t.Fatalf("normalizeAndDispatchLLMResponse() error = %v", err)
	}
	if outcome.Control != ControlBreak || outcome.FinalContent != "stage response" {
		t.Fatalf("normalizeAndDispatchLLMResponse() outcome = %+v", outcome)
	}
	if calls, prompt, completion, total := ts.llmUsageTotals(); calls != 1 || prompt != 11 || completion != 7 ||
		total != 18 {
		t.Fatalf(
			"usage totals = calls=%d prompt=%d completion=%d total=%d",
			calls,
			prompt,
			completion,
			total,
		)
	}
}
