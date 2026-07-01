// PicoClaw - Ultra-lightweight personal AI agent

package agent

import (
	"context"

	"github.com/sipeed/picoclaw/pkg/providers"
)

func (p *Pipeline) targetReasoningChannelID(channelName string) string {
	if p == nil || p.Reasoning == nil {
		return ""
	}
	return p.Reasoning.targetReasoningChannelID(channelName)
}

func (p *Pipeline) publishPicoReasoning(
	ctx context.Context,
	reasoningContent, chatID, sessionKey, modelName string,
) {
	if p == nil || p.Reasoning == nil {
		return
	}
	p.Reasoning.publishPicoReasoning(ctx, reasoningContent, chatID, sessionKey, modelName)
}

func (p *Pipeline) publishPicoToolCallInterim(
	ctx context.Context,
	ts *turnState,
	modelName string,
	reasoningContent string,
	content string,
	toolCalls []providers.ToolCall,
) {
	if p == nil || p.Reasoning == nil {
		return
	}
	p.Reasoning.publishPicoToolCallInterim(ctx, ts, modelName, reasoningContent, content, toolCalls)
}

func (p *Pipeline) handleReasoning(
	ctx context.Context,
	reasoningContent, channelName, channelID string,
) {
	if p == nil || p.Reasoning == nil {
		return
	}
	p.Reasoning.handleReasoning(ctx, reasoningContent, channelName, channelID)
}
