package agent

import (
	"context"
	"fmt"

	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/tools"
)

type syncToolResultDelivery struct {
	deliverToUser func(
		ctx context.Context,
		ts *turnState,
		result *tools.ToolResult,
		toolName string,
	) ([]providers.Attachment, toolResultDeliveryOutcome, error)
}

func (al *AgentLoop) syncToolResultDelivery() *syncToolResultDelivery {
	if al == nil {
		return nil
	}
	return &syncToolResultDelivery{deliverToUser: al.deliverToolResultToUser}
}

func (d *syncToolResultDelivery) applySyncToolResultDelivery(
	ctx context.Context,
	ts *turnState,
	result *tools.ToolResult,
	toolName string,
) ([]providers.Attachment, *tools.ToolResult) {
	if result == nil {
		return nil, tools.ErrorResult("nil tool result")
	}

	if ts.opts.SuppressToolUserDelivery {
		result.ResponseHandled = false
		result.ImmediateDelivery = false
	}

	if !ts.opts.SuppressToolUserDelivery && result.ImmediateDelivery {
		if d == nil || d.deliverToUser == nil {
			return nil, tools.ErrorResult("tool result delivery is not initialized")
		}
		if _, _, err := d.deliverToUser(ctx, ts, result, toolName); err != nil {
			return nil, tools.ErrorResult(fmt.Sprintf("failed to deliver attachment: %v", err)).
				WithError(err)
		}
	}

	if !ts.opts.SuppressToolUserDelivery && result.ResponseHandled {
		if d == nil || d.deliverToUser == nil {
			return nil, tools.ErrorResult("tool result delivery is not initialized")
		}
		attachments, outcome, err := d.deliverToUser(ctx, ts, result, toolName)
		if err != nil {
			return nil, tools.ErrorResult(fmt.Sprintf("failed to deliver attachment: %v", err)).
				WithError(err)
		}
		if outcome != toolResultDeliveryDirect && len(toolResultMediaRefs(result)) > 0 {
			result.ResponseHandled = false
		}
		if outcome == toolResultDeliveryDirect {
			return attachments, result
		}
	}

	return nil, result
}
