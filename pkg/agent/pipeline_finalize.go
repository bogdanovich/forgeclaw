// MintClaw - Ultra-lightweight personal AI agent

package agent

import (
	"context"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/providers"
	toolshared "github.com/bogdanovich/mintclaw/pkg/tools/shared"
)

type finalResponseDisposition uint8

const (
	finalResponsePending finalResponseDisposition = iota
	finalResponseAlreadyHandled
)

type finalizationUsage struct {
	inputTokens  int
	outputTokens int
	totalTokens  int
}

type finalizationStream struct {
	publisher *streamingChunkPublisher
	fallback  bool
	modelName string
}

type finalizationDelivery struct {
	sendResponse                bool
	allowInterimMintClawPublish bool
	preferNewOutboundReply      bool
	compactAfterDelivery        bool
}

// FinalizationContext is the terminal snapshot consumed by Finalize. It keeps
// iteration-owned state out of the finalization phase.
type FinalizationContext struct {
	content          string
	status           TurnEndStatus
	disposition      finalResponseDisposition
	modelName        string
	defaultModelName string
	usage            finalizationUsage
	completionMedia  []toolshared.CompletionMedia
	followUps        []bus.InboundMessage
	historyMessage   *providers.Message
	stream           finalizationStream
	delivery         finalizationDelivery
}

func newFinalizationContext(
	ts *turnState,
	exec *turnExecution,
	llm *LLMIterationState,
	status TurnEndStatus,
	content string,
) FinalizationContext {
	_, inputTokens, outputTokens, totalTokens := ts.llmUsageTotals()
	disposition := finalResponsePending
	if llm.allResponsesHandled {
		disposition = finalResponseAlreadyHandled
	}

	var historyMessage *providers.Message
	if disposition == finalResponsePending && !ts.opts.NoHistory {
		message := providers.Message{
			Role:             "assistant",
			Content:          content,
			ModelName:        exec.model.llmModelName,
			ReasoningContent: responseReasoningContent(llm.response),
		}
		historyMessage = &message
	}

	return FinalizationContext{
		content:          content,
		status:           status,
		disposition:      disposition,
		modelName:        exec.model.llmModelName,
		defaultModelName: exec.model.defaultModelName,
		usage: finalizationUsage{
			inputTokens:  inputTokens,
			outputTokens: outputTokens,
			totalTokens:  totalTokens,
		},
		completionMedia: append([]toolshared.CompletionMedia(nil), exec.completionMedia...),
		followUps:       append([]bus.InboundMessage(nil), ts.followUps...),
		historyMessage:  historyMessage,
		stream: finalizationStream{
			publisher: llm.streamingPublisher,
			fallback:  llm.streamingFallback,
			modelName: llm.llmModel,
		},
		delivery: finalizationDelivery{
			sendResponse:                ts.opts.SendResponse,
			allowInterimMintClawPublish: ts.opts.AllowInterimMintClawPublish,
			preferNewOutboundReply:      exec.sawAdditionalUserInput,
			compactAfterDelivery:        ts.opts.EnableSummary,
		},
	}
}

func (p *Pipeline) finalizeTurn(
	turnCtx context.Context,
	ts *turnState,
	exec *turnExecution,
	llm *LLMIterationState,
	status TurnEndStatus,
	content string,
) (turnResult, error) {
	finalization := newFinalizationContext(ts, exec, llm, status, content)
	return p.Finalize(turnCtx, ts, finalization)
}

// Finalize commits and delivers a terminal turn snapshot.
func (p *Pipeline) Finalize(
	turnCtx context.Context,
	ts *turnState,
	finalization FinalizationContext,
) (turnResult, error) {
	// When the response was already handled, ExecuteTools already finalized
	// (added handledToolResponseSummary, saved session, set phase to Completed).
	// But still check for hard abort - if requested, abort the turn.
	if finalization.disposition == finalResponseAlreadyHandled {
		if ts.hardAbortRequested() {
			return p.abortTurn(ts)
		}
		ts.setPhase(TurnPhaseCompleted)
		return finalization.result(false), nil
	}

	ts.setPhase(TurnPhaseFinalizing)
	ts.setFinalContent(finalization.content)
	if finalization.historyMessage != nil {
		finalMsg := *finalization.historyMessage
		if writeErr := persistFullSessionMessage(turnCtx, ts.agent.Sessions, ts.sessionKey, finalMsg); writeErr != nil {
			finalization.stream.cancel(turnCtx)
			return turnResult{status: TurnEndStatusError}, writeErr
		}
		ts.recordPersistedMessage(finalMsg)
		p.ingestMessage(turnCtx, ts, finalMsg, nil)
	}

	contextUsage := computeContextUsage(ts.agent, ts.sessionKey)
	streamErr := finalization.stream.finalize(turnCtx, ts, finalization.content, contextUsage)
	// If streaming never became visible, keep the legacy MintClaw interim publish path
	// so the final answer is still delivered outside normal SendResponse.
	if ((streamErr != nil && !isConfiguredStreamingVisibleError(streamErr)) || finalization.stream.fallback) &&
		!finalization.delivery.sendResponse && finalization.delivery.allowInterimMintClawPublish &&
		finalization.content != "" {
		msg := outboundMessageForTurnWithOptions(ts, finalization.content, outboundTurnMessageOptions{
			modelName: finalization.modelName,
		})
		msg.ContextUsage = contextUsage
		markFinalOutbound(&msg)
		_ = p.Runtime.Bus.PublishOutbound(turnCtx, msg)
	}
	if streamErr != nil && isConfiguredStreamingVisibleError(streamErr) {
		ts.setPhase(TurnPhaseCompleted)
		result := finalization.result(true)
		result.status = TurnEndStatusError
		return result, streamErr
	}
	ts.setPhase(TurnPhaseCompleted)
	return finalization.result(true), nil
}

func (f *FinalizationContext) result(includeCompaction bool) turnResult {
	result := turnResult{
		finalContent:           f.content,
		modelName:              f.modelName,
		defaultModelName:       f.defaultModelName,
		usageInputTokens:       f.usage.inputTokens,
		usageOutputTokens:      f.usage.outputTokens,
		usageTotalTokens:       f.usage.totalTokens,
		completionMedia:        append([]toolshared.CompletionMedia(nil), f.completionMedia...),
		status:                 f.status,
		followUps:              append([]bus.InboundMessage(nil), f.followUps...),
		preferNewOutboundReply: f.delivery.preferNewOutboundReply,
	}
	if includeCompaction {
		result.compactAfterDelivery = f.delivery.compactAfterDelivery
	}
	return result
}
