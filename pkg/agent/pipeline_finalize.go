// MintClaw - Ultra-lightweight personal AI agent

package agent

import (
	"context"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/providers"
	"github.com/bogdanovich/mintclaw/pkg/tools"
)

// Finalize handles turn finalization, either:
// - Early return when allResponsesHandled=true (ExecuteTools already finalized)
// - Normal finalization for allResponsesHandled=false (sets finalContent, saves session, compact)
func (p *Pipeline) Finalize(
	ctx context.Context,
	turnCtx context.Context,
	ts *turnState,
	exec *turnExecution,
	turnStatus TurnEndStatus,
	finalContent string,
) (turnResult, error) {
	_, usageInputTokens, usageOutputTokens, usageTotalTokens := ts.llmUsageTotals()

	// When allResponsesHandled=true, ExecuteTools already finalized
	// (added handledToolResponseSummary, saved session, set phase to Completed).
	// But still check for hard abort - if requested, abort the turn.
	if exec.allResponsesHandled {
		if ts.hardAbortRequested() {
			return p.abortTurn(ts)
		}
		ts.setPhase(TurnPhaseCompleted)
		return turnResult{
			finalContent:           finalContent,
			modelName:              exec.model.llmModelName,
			defaultModelName:       exec.model.defaultModelName,
			usageInputTokens:       usageInputTokens,
			usageOutputTokens:      usageOutputTokens,
			usageTotalTokens:       usageTotalTokens,
			completionMedia:        append([]tools.CompletionMedia(nil), exec.completionMedia...),
			status:                 turnStatus,
			followUps:              append([]bus.InboundMessage(nil), ts.followUps...),
			preferNewOutboundReply: exec.sawAdditionalUserInput,
		}, nil
	}

	ts.setPhase(TurnPhaseFinalizing)
	ts.setFinalContent(finalContent)
	if !ts.opts.NoHistory {
		finalMsg := providers.Message{
			Role:             "assistant",
			Content:          finalContent,
			ModelName:        exec.model.llmModelName,
			ReasoningContent: responseReasoningContent(exec.response),
		}
		if writeErr := persistFullSessionMessage(turnCtx, ts.agent.Sessions, ts.sessionKey, finalMsg); writeErr != nil {
			cancelConfiguredStreamingLLM(turnCtx, exec)
			return turnResult{status: TurnEndStatusError}, writeErr
		}
		ts.recordPersistedMessage(finalMsg)
		p.ingestMessage(turnCtx, ts, finalMsg, nil)
	}

	contextUsage := computeContextUsage(ts.agent, ts.sessionKey)
	streamErr := finalizeConfiguredStreamingLLM(turnCtx, ts, exec, finalContent, contextUsage)
	// If streaming never became visible, keep the legacy MintClaw interim publish path
	// so the final answer is still delivered outside normal SendResponse.
	if ((streamErr != nil && !isConfiguredStreamingVisibleError(streamErr)) || exec.streamingFallback) &&
		!ts.opts.SendResponse && ts.opts.AllowInterimMintClawPublish &&
		finalContent != "" {
		msg := outboundMessageForTurnWithOptions(ts, finalContent, outboundTurnMessageOptions{
			modelName: exec.model.llmModelName,
		})
		msg.ContextUsage = contextUsage
		markFinalOutbound(&msg)
		_ = p.Runtime.Bus.PublishOutbound(turnCtx, msg)
	}
	if streamErr != nil && isConfiguredStreamingVisibleError(streamErr) {
		ts.setPhase(TurnPhaseCompleted)
		return turnResult{
			finalContent:           finalContent,
			modelName:              exec.model.llmModelName,
			defaultModelName:       exec.model.defaultModelName,
			usageInputTokens:       usageInputTokens,
			usageOutputTokens:      usageOutputTokens,
			usageTotalTokens:       usageTotalTokens,
			completionMedia:        append([]tools.CompletionMedia(nil), exec.completionMedia...),
			status:                 TurnEndStatusError,
			followUps:              append([]bus.InboundMessage(nil), ts.followUps...),
			preferNewOutboundReply: exec.sawAdditionalUserInput,
			compactAfterDelivery:   ts.opts.EnableSummary,
		}, streamErr
	}
	ts.setPhase(TurnPhaseCompleted)
	return turnResult{
		finalContent:           finalContent,
		modelName:              exec.model.llmModelName,
		defaultModelName:       exec.model.defaultModelName,
		usageInputTokens:       usageInputTokens,
		usageOutputTokens:      usageOutputTokens,
		usageTotalTokens:       usageTotalTokens,
		completionMedia:        append([]tools.CompletionMedia(nil), exec.completionMedia...),
		status:                 turnStatus,
		followUps:              append([]bus.InboundMessage(nil), ts.followUps...),
		preferNewOutboundReply: exec.sawAdditionalUserInput,
		compactAfterDelivery:   ts.opts.EnableSummary,
	}, nil
}
