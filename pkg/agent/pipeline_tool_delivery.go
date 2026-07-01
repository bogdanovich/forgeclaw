// PicoClaw - Ultra-lightweight personal AI agent

package agent

import (
	"context"

	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/tools"
)

func (p *Pipeline) publishToolFeedbackForCall(
	ctx context.Context,
	ts *turnState,
	response *providers.LLMResponse,
	toolCall providers.ToolCall,
	toolName string,
	toolArgs map[string]any,
	messages []providers.Message,
) {
	if p == nil || p.ToolDelivery == nil {
		return
	}
	p.ToolDelivery.publishToolFeedbackForCall(ctx, ts, response, toolCall, toolName, toolArgs, messages)
}

func (p *Pipeline) applySyncToolResultDelivery(
	ctx context.Context,
	ts *turnState,
	result *tools.ToolResult,
	toolName string,
) ([]providers.Attachment, *tools.ToolResult) {
	if p == nil || p.ToolDelivery == nil {
		return nil, result
	}
	return p.ToolDelivery.applySyncToolResultDelivery(ctx, ts, result, toolName)
}

func (p *Pipeline) deliverAsyncToolCompletion(req AsyncDeliveryRequest) {
	if p == nil || p.ToolDelivery == nil {
		return
	}
	p.ToolDelivery.deliverAsyncToolCompletion(req)
}

func (p *Pipeline) dismissToolFeedbackForTurn(ctx context.Context, ts *turnState) {
	if p == nil || p.ToolDelivery == nil {
		return
	}
	p.ToolDelivery.dismissToolFeedbackForTurn(ctx, ts)
}
