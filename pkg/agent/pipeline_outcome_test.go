package agent

import (
	"context"
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/providers"
)

type abortingPipelineHook struct {
	action HookAction
}

func (h abortingPipelineHook) BeforeLLM(
	_ context.Context,
	req *LLMHookRequest,
) (*LLMHookRequest, HookDecision) {
	return req, HookDecision{Action: h.action}
}

func (h abortingPipelineHook) AfterLLM(
	_ context.Context,
	resp *LLMHookResponse,
) (*LLMHookResponse, HookDecision) {
	return resp, HookDecision{}
}

func (h abortingPipelineHook) BeforeTool(
	_ context.Context,
	req *ToolCallHookRequest,
) (*ToolCallHookRequest, HookDecision) {
	return req, HookDecision{Action: h.action}
}

func (h abortingPipelineHook) AfterTool(
	_ context.Context,
	resp *ToolResultHookResponse,
) (*ToolResultHookResponse, HookDecision) {
	return resp, HookDecision{}
}

func (abortingPipelineHook) ApproveTool(context.Context, *ToolApprovalRequest) ApprovalDecision {
	return ApprovalDecision{Approved: true}
}

func TestPipelinePhaseOutcomesCarryAbortCause(t *testing.T) {
	tests := []struct {
		name   string
		action HookAction
		want   TurnAbortCause
	}{
		{name: "hook abort", action: HookActionAbortTurn, want: TurnAbortHook},
		{name: "hard abort", action: HookActionHardAbort, want: TurnAbortHard},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			al, agent, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
			defer cleanup()

			pipeline := NewPipeline(al)
			pipeline.Interaction.Hooks = abortingPipelineHook{action: test.action}
			ts := newTurnState(agent, makeTestProcessOpts("outcome-abort"), turnEventScope{
				turnID:  "turn-outcome-abort",
				context: newTurnContext(nil, nil, nil),
			})
			exec, err := pipeline.SetupTurn(t.Context(), ts)
			if err != nil {
				t.Fatalf("SetupTurn() error = %v", err)
			}
			if exec.model.cleanup != nil {
				defer exec.model.cleanup()
			}

			llm := newLLMIterationState(1)
			llmOutcome, err := pipeline.CallLLM(t.Context(), t.Context(), ts, exec, llm)
			if err != nil {
				t.Fatalf("CallLLM() error = %v", err)
			}
			if llmOutcome.Control != ControlBreak || llmOutcome.AbortCause != test.want {
				t.Fatalf("LLM outcome = %#v, want break with abort cause %v", llmOutcome, test.want)
			}

			toolTS := newTurnState(agent, makeTestProcessOpts("outcome-tool-abort"), turnEventScope{
				turnID:  "turn-outcome-tool-abort",
				context: newTurnContext(nil, nil, nil),
			})
			toolExec := newTurnExecution(agent, toolTS.opts, nil, "", nil)
			toolLLM := &LLMIterationState{
				iteration:           1,
				normalizedToolCalls: []providers.ToolCall{{ID: "call-1", Name: "unused"}},
			}
			toolOutcome := pipeline.ExecuteTools(t.Context(), t.Context(), toolTS, toolExec, toolLLM)
			if toolOutcome.Control != ToolControlBreak || toolOutcome.AbortCause != test.want {
				t.Fatalf("tool outcome = %#v, want break with abort cause %v", toolOutcome, test.want)
			}
		})
	}
}

func TestLLMCallOutcomeTerminalCandidate(t *testing.T) {
	tests := []struct {
		name    string
		outcome LLMCallOutcome
		want    string
	}{
		{
			name:    "continue retains prior answer",
			outcome: LLMCallOutcome{Control: ControlContinue},
			want:    "retained answer",
		},
		{
			name:    "tool loop retains prior answer",
			outcome: LLMCallOutcome{Control: ControlToolLoop},
			want:    "retained answer",
		},
		{
			name: "terminal answer replaces prior answer",
			outcome: LLMCallOutcome{
				Control:      ControlBreak,
				FinalContent: "replacement answer",
			},
			want: "replacement answer",
		},
		{
			name:    "empty terminal answer clears prior answer",
			outcome: LLMCallOutcome{Control: ControlBreak},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.outcome.terminalCandidate("retained answer"); got != test.want {
				t.Fatalf("terminal candidate = %q, want %q", got, test.want)
			}
		})
	}
}
