package agent

import (
	"context"
	"fmt"

	"github.com/bogdanovich/mintclaw/pkg/providers"
	toolshared "github.com/bogdanovich/mintclaw/pkg/tools/shared"
)

type syncToolResultDelivery struct {
	deliverToUser func(
		ctx context.Context,
		ts *turnState,
		result *toolshared.ToolResult,
		toolName string,
	) ([]providers.Attachment, toolResultDeliveryOutcome, error)
}

func (al *AgentLoop) syncToolResultDelivery() *syncToolResultDelivery {
	if al == nil {
		return nil
	}
	return &syncToolResultDelivery{deliverToUser: al.deliverToolResultToUser}
}

func normalizeToolResultForSyncDelivery(ts *turnState, result *toolshared.ToolResult) *toolshared.ToolResult {
	if result == nil {
		return toolshared.ErrorResult("nil tool result")
	}
	if ts != nil && ts.opts.SuppressToolUserDelivery {
		result.ResponseHandled = false
		result.ImmediateDelivery = false
	}
	return result
}

func (d *syncToolResultDelivery) applySyncToolResultDelivery(
	ctx context.Context,
	ts *turnState,
	result *toolshared.ToolResult,
	toolName string,
) ([]providers.Attachment, *toolshared.ToolResult) {
	result = normalizeToolResultForSyncDelivery(ts, result)

	if !ts.opts.SuppressToolUserDelivery && result.ImmediateDelivery {
		if d == nil || d.deliverToUser == nil {
			return nil, toolshared.ErrorResult("tool result delivery is not initialized")
		}
		if _, _, err := d.deliverToUser(ctx, ts, result, toolName); err != nil {
			return nil, wrapToolDeliveryError(result, fmt.Sprintf("failed to deliver attachment: %v", err), err)
		}
	}

	if !ts.opts.SuppressToolUserDelivery && result.ResponseHandled {
		if d == nil || d.deliverToUser == nil {
			return nil, toolshared.ErrorResult("tool result delivery is not initialized")
		}
		attachments, outcome, err := d.deliverToUser(ctx, ts, result, toolName)
		if err != nil {
			return nil, wrapToolDeliveryError(result, fmt.Sprintf("failed to deliver attachment: %v", err), err)
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

func commitToolResultOutbound(ctx context.Context, result *toolshared.ToolResult) error {
	if result == nil || result.CommitOutbound == nil {
		return nil
	}
	commit := result.CommitOutbound
	result.CommitOutbound = nil
	return commit(ctx)
}

func wrapToolDeliveryError(
	original *toolshared.ToolResult,
	message string,
	err error,
) *toolshared.ToolResult {
	wrapped := toolshared.ErrorResult(message).WithError(err)
	if original == nil || len(original.WriteAudit) == 0 {
		return wrapped
	}
	wrapped.WriteAudit = append(wrapped.WriteAudit, original.WriteAudit...)
	return wrapped
}
