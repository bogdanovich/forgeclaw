package agent

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/agent/interfaces"
	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
	taskregistry "github.com/sipeed/picoclaw/pkg/tasks"
	"github.com/sipeed/picoclaw/pkg/tools"
)

// AsyncDeliveryDecision is the routing plan for a completed async tool result.
//
// This is intentionally decision-only for now. The current runtime still
// performs delivery in pipeline_execute.go, but all routing policy should flow
// through this type so media, duplicate, timeout, and restart handling can move
// behind the same coordinator boundary later.
type AsyncDeliveryDecision struct {
	TaskID        string
	DeliveryMode  tools.AsyncDeliveryMode
	PublishToUser bool
	QueueParent   bool
	ParentHandled bool
	ContentLen    int
	ForUserLen    int
	MediaCount    int
	IsError       bool
}

type AsyncDeliveryRequest struct {
	TurnState    *turnState
	ToolName     string
	CompletionID string
	Result       *tools.ToolResult
	Decision     AsyncDeliveryDecision
}

type asyncToolCompletionDelivery struct {
	bus                             interfaces.MessageBus
	cfg                             *config.Config
	events                          runtimeEventEmitter
	deliverToUser                   func(context.Context, *turnState, *tools.ToolResult, string) ([]providers.Attachment, toolResultDeliveryOutcome, error)
	processCompletion               func(context.Context, AsyncCompletionInput) (string, error)
	asyncTaskDeliveryAlreadyHandled func(workspace, taskID, completionID string) bool
	recordAsyncTaskDeliveryDecision func(workspace string, decision AsyncDeliveryDecision, completionID, sourceTool string)
	updateAsyncTaskDeliveryStatus   func(workspace, taskID string, status taskregistry.DeliveryStatus, completionID, errorSummary string)
}

func (al *AgentLoop) asyncToolCompletionDelivery() *asyncToolCompletionDelivery {
	if al == nil {
		return nil
	}
	return &asyncToolCompletionDelivery{
		bus:                             al.bus,
		cfg:                             al.GetConfig(),
		events:                          al.runtimeEventEmitter(),
		deliverToUser:                   al.deliverToolResultToUser,
		processCompletion:               al.processAsyncCompletion,
		asyncTaskDeliveryAlreadyHandled: al.asyncTaskDeliveryAlreadyHandled,
		recordAsyncTaskDeliveryDecision: al.recordAsyncTaskDeliveryDecision,
		updateAsyncTaskDeliveryStatus:   al.updateAsyncTaskDeliveryStatus,
	}
}

func (al *AgentLoop) deliverAsyncToolCompletion(req AsyncDeliveryRequest) {
	al.asyncToolCompletionDelivery().deliverAsyncToolCompletion(req)
}

func (d *asyncToolCompletionDelivery) deliverAsyncToolCompletion(req AsyncDeliveryRequest) {
	ts := req.TurnState
	result := req.Result
	asyncToolName := strings.TrimSpace(req.ToolName)
	if ts == nil || result == nil {
		return
	}
	if asyncToolName == "" {
		asyncToolName = "async_tool"
	}
	delivery := req.Decision
	if delivery.DeliveryMode == "" {
		delivery = decideAsyncToolResultDelivery(result)
	}
	completionID := strings.TrimSpace(req.CompletionID)
	if d.isAsyncTaskDeliveryAlreadyHandled(ts.workspace, delivery.TaskID, completionID) {
		logger.InfoCF("agent", "Skipping duplicate async delivery",
			map[string]any{
				"tool":          asyncToolName,
				"completion_id": completionID,
				"task_id":       delivery.TaskID,
			})
		return
	}
	d.recordDeliveryDecision(ts.workspace, delivery, completionID, asyncToolName)
	if result.IsError {
		content := strings.TrimSpace(result.ForUser)
		if content == "" {
			content = strings.TrimSpace(result.ContentForLLM())
		}
		delivered := false
		deliveryErr := ""
		if content != "" && !result.Silent {
			outCtx, outCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer outCancel()
			if err := d.publishOutbound(outCtx, outboundMessageForTurn(ts, content)); err != nil {
				deliveryErr = err.Error()
			} else {
				delivered = true
			}
		}
		switch {
		case delivered:
			d.updateDeliveryStatus(
				ts.workspace,
				delivery.TaskID,
				taskregistry.DeliveryDelivered,
				completionID,
				"",
			)
		case deliveryErr != "":
			d.updateDeliveryStatus(
				ts.workspace,
				delivery.TaskID,
				taskregistry.DeliveryFailed,
				completionID,
				deliveryErr,
			)
		default:
			d.updateDeliveryStatus(
				ts.workspace,
				delivery.TaskID,
				taskregistry.DeliveryNotApplicable,
				completionID,
				"",
			)
		}
		return
	}
	if delivery.PublishToUser {
		outCtx, outCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer outCancel()
		userDelivered := false
		userDeliveryErr := ""
		if _, outcome, err := d.deliverToUserResult(outCtx, ts, result, asyncToolName); err != nil {
			userDeliveryErr = err.Error()
			logger.WarnCF("agent", "Failed to deliver async tool result to user",
				map[string]any{
					"tool":    asyncToolName,
					"channel": ts.channel,
					"chat_id": ts.chatID,
					"error":   err.Error(),
				})
		} else if outcome == toolResultDeliveryQueued {
			userDelivered = true
		} else if outcome == toolResultDeliveryNone && strings.TrimSpace(result.ForUser) != "" && !result.Silent {
			if err := d.publishOutbound(outCtx, outboundMessageForTurn(ts, result.ForUser)); err != nil {
				userDeliveryErr = err.Error()
			} else {
				userDelivered = true
			}
		} else if outcome == toolResultDeliveryDirect {
			userDelivered = true
		}
		if !delivery.QueueParent {
			if userDelivered {
				d.updateDeliveryStatus(
					ts.workspace,
					delivery.TaskID,
					taskregistry.DeliveryDelivered,
					completionID,
					"",
				)
			} else if userDeliveryErr != "" {
				d.updateDeliveryStatus(
					ts.workspace,
					delivery.TaskID,
					taskregistry.DeliveryFailed,
					completionID,
					userDeliveryErr,
				)
			} else {
				d.updateDeliveryStatus(
					ts.workspace,
					delivery.TaskID,
					taskregistry.DeliveryNotApplicable,
					completionID,
					"",
				)
			}
			return
		}
	}

	if !delivery.QueueParent {
		d.updateDeliveryStatus(
			ts.workspace,
			delivery.TaskID,
			taskregistry.DeliveryNotApplicable,
			completionID,
			"",
		)
		return
	}

	content := result.ContentForLLM()
	if d.cfg != nil {
		content = d.cfg.FilterSensitiveData(content)
	}

	logger.InfoCF("agent", "Async tool completed, publishing result",
		map[string]any{
			"tool":        asyncToolName,
			"content_len": len(content),
			"channel":     ts.channel,
		})
	d.emitEvent(
		runtimeevents.KindAgentFollowUpQueued,
		ts.scope.meta(0, "delivery_coordinator", "turn.follow_up.queued"),
		FollowUpQueuedPayload{
			SourceTool: asyncToolName,
			ContentLen: len(content),
		},
	)
	origin := bus.InboundContext{
		Channel:  ts.channel,
		ChatID:   ts.chatID,
		ChatType: "direct",
		SenderID: fmt.Sprintf("async:%s", asyncToolName),
		TopicID:  originTopicID(ts.opts.Dispatch.InboundContext),
	}
	if ts.opts.Dispatch.InboundContext != nil {
		origin = *cloneInboundContext(ts.opts.Dispatch.InboundContext)
		if strings.TrimSpace(origin.Channel) == "" {
			origin.Channel = ts.channel
		}
		if strings.TrimSpace(origin.ChatID) == "" {
			origin.ChatID = ts.chatID
		}
		if strings.TrimSpace(origin.ChatType) == "" {
			origin.ChatType = "direct"
		}
		origin.SenderID = fmt.Sprintf("async:%s", asyncToolName)
	}
	completionCtx, completionCancel := context.WithTimeout(context.Background(), asyncCompletionSynthesisTimeout)
	defer completionCancel()
	if _, err := d.processAsyncCompletion(completionCtx, AsyncCompletionInput{
		SourceTool:   asyncToolName,
		CompletionID: completionID,
		Content:      asyncCompletionPrompt(asyncToolName, content),
		Origin:       origin,
		SenderID:     fmt.Sprintf("async:%s", asyncToolName),
	}); err != nil {
		d.updateDeliveryStatus(
			ts.workspace,
			delivery.TaskID,
			taskregistry.DeliveryFailed,
			completionID,
			err.Error(),
		)
		logger.WarnCF("agent", "Failed to process async completion",
			map[string]any{
				"tool":          asyncToolName,
				"completion_id": completionID,
				"channel":       ts.channel,
				"chat_id":       ts.chatID,
				"error":         err.Error(),
			})
	} else if delivery.DeliveryMode == tools.AsyncDeliveryParentOnly {
		d.updateDeliveryStatus(
			ts.workspace,
			delivery.TaskID,
			taskregistry.DeliverySessionQueued,
			completionID,
			"",
		)
	} else {
		d.updateDeliveryStatus(
			ts.workspace,
			delivery.TaskID,
			taskregistry.DeliveryDelivered,
			completionID,
			"",
		)
	}
}

func (d *asyncToolCompletionDelivery) publishOutbound(ctx context.Context, msg bus.OutboundMessage) error {
	if d == nil || d.bus == nil {
		return fmt.Errorf("message bus not initialized")
	}
	return d.bus.PublishOutbound(ctx, msg)
}

func (d *asyncToolCompletionDelivery) deliverToUserResult(
	ctx context.Context,
	ts *turnState,
	result *tools.ToolResult,
	toolName string,
) ([]providers.Attachment, toolResultDeliveryOutcome, error) {
	if d == nil || d.deliverToUser == nil {
		return nil, toolResultDeliveryNone, fmt.Errorf("tool result delivery is not initialized")
	}
	return d.deliverToUser(ctx, ts, result, toolName)
}

func (d *asyncToolCompletionDelivery) processAsyncCompletion(
	ctx context.Context,
	input AsyncCompletionInput,
) (string, error) {
	if d == nil || d.processCompletion == nil {
		return "", fmt.Errorf("async completion processor is not initialized")
	}
	return d.processCompletion(ctx, input)
}

func (d *asyncToolCompletionDelivery) isAsyncTaskDeliveryAlreadyHandled(
	workspace,
	taskID,
	completionID string,
) bool {
	if d == nil || d.asyncTaskDeliveryAlreadyHandled == nil {
		return false
	}
	return d.asyncTaskDeliveryAlreadyHandled(workspace, taskID, completionID)
}

func (d *asyncToolCompletionDelivery) recordDeliveryDecision(
	workspace string,
	decision AsyncDeliveryDecision,
	completionID,
	sourceTool string,
) {
	if d == nil || d.recordAsyncTaskDeliveryDecision == nil {
		return
	}
	d.recordAsyncTaskDeliveryDecision(workspace, decision, completionID, sourceTool)
}

func (d *asyncToolCompletionDelivery) updateDeliveryStatus(
	workspace,
	taskID string,
	status taskregistry.DeliveryStatus,
	completionID,
	errorSummary string,
) {
	if d == nil || d.updateAsyncTaskDeliveryStatus == nil {
		return
	}
	d.updateAsyncTaskDeliveryStatus(workspace, taskID, status, completionID, errorSummary)
}

func (d *asyncToolCompletionDelivery) emitEvent(kind runtimeevents.Kind, meta HookMeta, payload any) {
	if d == nil || d.events == nil {
		return
	}
	d.events.emitEvent(kind, meta, payload)
}

func decideAsyncToolResultDelivery(result *tools.ToolResult) AsyncDeliveryDecision {
	decision := AsyncDeliveryDecision{
		DeliveryMode: effectiveAsyncToolResultDelivery(result),
	}
	if result == nil {
		return decision
	}

	content := result.ContentForLLM()
	decision.TaskID = result.AsyncTaskID
	decision.ContentLen = len(content)
	decision.ForUserLen = len(result.ForUser)
	decision.MediaCount = len(result.Media)
	if result.Completion != nil {
		decision.MediaCount += len(result.Completion.Media)
	}
	decision.IsError = result.IsError

	if decision.DeliveryMode != tools.AsyncDeliveryParentOnly {
		decision.PublishToUser = !result.Silent && (result.ForUser != "" || decision.MediaCount > 0)
	}
	if decision.DeliveryMode != tools.AsyncDeliveryUserOnly {
		decision.QueueParent = content != ""
	}
	decision.ParentHandled = !decision.QueueParent && !result.IsError &&
		decision.DeliveryMode == tools.AsyncDeliveryUserOnly
	return decision
}

func effectiveAsyncToolResultDelivery(result *tools.ToolResult) tools.AsyncDeliveryMode {
	if result == nil || result.AsyncDelivery == "" {
		return tools.AsyncDeliveryUserAndParent
	}
	return result.AsyncDelivery
}

func asyncDeliveryModeFromToolArgs(toolName string, args map[string]any) (tools.AsyncDeliveryMode, error) {
	if toolName != "spawn" && toolName != "delegate" {
		return tools.AsyncDeliveryUserAndParent, nil
	}
	raw, ok := args["delivery_mode"]
	if !ok || raw == nil {
		if toolName == "spawn" {
			return tools.AsyncDeliveryUserOnly, nil
		}
		return tools.AsyncDeliveryParentOnly, nil
	}
	value, ok := raw.(string)
	if !ok {
		return "", nil
	}
	switch mode := tools.AsyncDeliveryMode(strings.TrimSpace(value)); mode {
	case tools.AsyncDeliveryUserOnly, tools.AsyncDeliveryParentOnly, tools.AsyncDeliveryUserAndParent:
		return mode, nil
	default:
		return "", nil
	}
}
