package agent

import (
	"context"
	"time"

	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/session"
	"github.com/sipeed/picoclaw/pkg/tools"
)

func persistFullSessionMessage(
	store session.SessionStore,
	sessionKey string,
	msg providers.Message,
) error {
	if writer, ok := store.(session.ErrorAwareSessionWriter); ok {
		return writer.AddFullMessageWithError(sessionKey, msg)
	}
	store.AddFullMessage(sessionKey, msg)
	return nil
}

func persistSessionMessage(
	store session.SessionStore,
	sessionKey, role, content string,
) error {
	if writer, ok := store.(session.ErrorAwareSessionWriter); ok {
		return writer.AddMessageWithError(sessionKey, role, content)
	}
	store.AddMessage(sessionKey, role, content)
	return nil
}

func (p *Pipeline) ingestMessage(
	ctx context.Context,
	ts *turnState,
	msg providers.Message,
	canonicalWriteErr error,
) {
	if p == nil || ts == nil || p.Context.Runtime == nil {
		return
	}
	if canonicalWriteErr != nil {
		logger.WarnCF("agent", "Canonical session write failed before context ingest", map[string]any{
			"session_key": ts.sessionKey,
			"error":       canonicalWriteErr.Error(),
		})
	}
	if err := p.Context.Runtime.Ingest(ctx, &IngestRequest{
		Agent:             ts.agent,
		SessionKey:        ts.sessionKey,
		Message:           msg,
		CanonicalWriteErr: canonicalWriteErr,
	}); err != nil {
		logger.WarnCF("agent", "Context manager ingest failed", map[string]any{
			"session_key": ts.sessionKey,
			"error":       err.Error(),
		})
	}
}

func (p *Pipeline) scheduleBackgroundCompaction(
	agent *AgentInstance,
	sessionKey string,
	reason ContextCompressReason,
	budget int,
	messageKind string,
) {
	if p == nil || p.Context.BackgroundCompaction == nil {
		return
	}
	p.Context.BackgroundCompaction.scheduleBackgroundCompaction(
		agent,
		sessionKey,
		reason,
		budget,
		messageKind,
	)
}

func (p *Pipeline) dequeueSteeringMessagesForTurn(ts *turnState) []providers.Message {
	if p == nil || p.Context.Steering == nil || ts == nil {
		return nil
	}
	return p.Context.Steering.dequeueSteeringMessagesForTurn(
		ts.runtimeSessionScope(),
		ts.opts.Dispatch.SenderID(),
	)
}

func (p *Pipeline) updateAutoFallbackSelection(
	routeSessionKey string,
	selectedCandidates []providers.FallbackCandidate,
	result *providers.FallbackResult,
	usedLight bool,
) {
	if p == nil || p.Context.ModelExecution == nil {
		return
	}
	p.Context.ModelExecution.updateAutoFallbackSelection(
		routeSessionKey,
		selectedCandidates,
		result,
		usedLight,
	)
}

func (p *Pipeline) abortTurn(ts *turnState) (turnResult, error) {
	if p == nil || p.Runtime.TurnControl == nil {
		return turnResult{status: TurnEndStatusAborted}, nil
	}
	return p.Runtime.TurnControl.abortTurn(ts)
}

func (p *Pipeline) targetReasoningChannelID(channelName string) string {
	if p == nil || p.Interaction.Reasoning == nil {
		return ""
	}
	return p.Interaction.Reasoning.targetReasoningChannelID(channelName)
}

func (p *Pipeline) publishPicoReasoning(
	ctx context.Context,
	reasoningContent, chatID, sessionKey, modelName string,
) {
	if p == nil || p.Interaction.Reasoning == nil {
		return
	}
	p.Interaction.Reasoning.publishPicoReasoning(
		ctx,
		reasoningContent,
		chatID,
		sessionKey,
		modelName,
	)
}

func (p *Pipeline) publishPicoToolCallInterim(
	ctx context.Context,
	ts *turnState,
	modelName string,
	reasoningContent string,
	content string,
	toolCalls []providers.ToolCall,
) {
	if p == nil || p.Interaction.Reasoning == nil {
		return
	}
	p.Interaction.Reasoning.publishPicoToolCallInterim(
		ctx,
		ts,
		modelName,
		reasoningContent,
		content,
		toolCalls,
	)
}

func (p *Pipeline) shouldPublishToolFeedback(ts *turnState) bool {
	if p == nil || p.Interaction.ToolFeedback == nil {
		return false
	}
	return p.Interaction.ToolFeedback.shouldPublishToolFeedback(ts)
}

func (p *Pipeline) handleReasoning(
	ctx context.Context,
	reasoningContent, channelName, channelID string,
) {
	if p == nil || p.Interaction.Reasoning == nil {
		return
	}
	p.Interaction.Reasoning.handleReasoning(ctx, reasoningContent, channelName, channelID)
}

func (p *Pipeline) publishToolFeedbackForCall(
	ctx context.Context,
	ts *turnState,
	response *providers.LLMResponse,
	toolCall providers.ToolCall,
	toolName string,
	toolArgs map[string]any,
	messages []providers.Message,
) {
	if p == nil || p.Interaction.ToolFeedback == nil {
		return
	}
	p.Interaction.ToolFeedback.publishToolFeedbackForCall(
		ctx,
		ts,
		response,
		toolCall,
		toolName,
		toolArgs,
		messages,
	)
}

func (p *Pipeline) applySyncToolResultDelivery(
	ctx context.Context,
	ts *turnState,
	result *tools.ToolResult,
	toolName string,
) ([]providers.Attachment, *tools.ToolResult) {
	if p == nil || p.Interaction.SyncToolDelivery == nil {
		return nil, result
	}
	if result != nil && result.ImmediateDelivery {
		// Immediate delivery may trigger a self-restart. Clear the tracked
		// feedback before the tool can tear down this gateway process.
		dismissCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		p.dismissToolFeedbackForTurn(dismissCtx, ts)
	}
	return p.Interaction.SyncToolDelivery.applySyncToolResultDelivery(ctx, ts, result, toolName)
}

func (p *Pipeline) deliverAsyncToolCompletion(req AsyncDeliveryRequest) {
	if p == nil || p.Interaction.ToolDelivery == nil {
		return
	}
	p.Interaction.ToolDelivery.deliverAsyncToolCompletion(req)
}

func (p *Pipeline) dismissToolFeedbackForTurn(ctx context.Context, ts *turnState) {
	if p == nil || p.Interaction.ToolFeedback == nil {
		return
	}
	p.Interaction.ToolFeedback.dismissToolFeedbackForTurn(ctx, ts)
}
