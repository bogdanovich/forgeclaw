package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/commands"
	"github.com/sipeed/picoclaw/pkg/interactions"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/session"
	taskregistry "github.com/sipeed/picoclaw/pkg/tasks"
	"github.com/sipeed/picoclaw/pkg/tools"
)

const answerCommand = "/answer"

type interactionInboundOwnership int

const (
	interactionInboundCallerOwned interactionInboundOwnership = iota
	interactionInboundClaimed
	interactionInboundDeferred
)

func (al *AgentLoop) cancelInteractionForControlMessage(
	ctx context.Context,
	msg bus.InboundMessage,
	target *inboundDispatchTarget,
) error {
	name, ok := commands.CommandName(msg.Content)
	if !ok || (name != "new" && name != "reset" && name != "clear" && name != "stop") ||
		al == nil || target == nil || target.Agent == nil {
		return nil
	}
	registry := al.interactionRegistryForWorkspace(target.Agent.Workspace)
	record, found := activeInteractionForSession(registry, target.SessionKey)
	if !found || !interactionRouteAuthorizes(record.Route, target, msg.Context) {
		return nil
	}
	if name == "stop" {
		claim, _, claimed := al.claimRuntimeRouteSession(
			target,
			fmt.Sprintf("pending-interaction-cancel-%s-%d", record.ShortID, al.turnSeq.Add(1)),
		)
		if !claimed {
			return fmt.Errorf("interaction session is busy while canceling")
		}
		defer claim.releaseIfOwned()
		if record.Status != interactions.StatusCanceling {
			var err error
			record, err = registry.BeginCancellation(
				record.ID,
				record.Revision,
				"session_control_stop",
			)
			if err != nil {
				return fmt.Errorf("begin stop cancellation: %w", err)
			}
		}
		if err := al.ensureInteractionCancellationToolResult(
			ctx,
			al.interactionContinuationAgent(record, target.Agent),
			record,
			record.FailureCode,
		); err != nil {
			return fmt.Errorf("persist stop cancellation result: %w", err)
		}
		if _, err := registry.CompleteCancellation(record.ID, record.Revision); err != nil {
			return fmt.Errorf("complete stop cancellation: %w", err)
		}
		return nil
	}
	if _, err := registry.Cancel(record.ID, record.Revision, "session_control_"+name); err != nil {
		logger.WarnCF("agent", "Failed to cancel interaction for session control", map[string]any{
			"interaction_id": record.ID, "command": name, "error": err.Error(),
		})
		return err
	}
	return nil
}

func (al *AgentLoop) shouldHandleInteractionInbound(
	msg bus.InboundMessage,
	target *inboundDispatchTarget,
) bool {
	if al == nil || target == nil || target.Agent == nil {
		return false
	}
	registry := al.interactionRegistryForWorkspace(target.Agent.Workspace)
	if registry == nil {
		return false
	}
	if registry.LastLoadError() != nil {
		return true
	}
	record, ok := activeInteractionForSession(registry, target.SessionKey)
	if !ok || !interactionRouteAuthorizes(record.Route, target, msg.Context) {
		return false
	}
	if commands.HasCommandPrefix(msg.Content) &&
		!strings.HasPrefix(strings.ToLower(strings.TrimSpace(msg.Content)), answerCommand) {
		return false
	}
	switch record.Status {
	case interactions.StatusWaiting, interactions.StatusClaimed, interactions.StatusResuming:
		return true
	default:
		return false
	}
}

func (al *AgentLoop) hasNonterminalInteraction(workspace, sessionKey string) bool {
	registry := al.interactionRegistryForWorkspace(workspace)
	if registry == nil {
		return false
	}
	if registry.LastLoadError() != nil {
		return true
	}
	for _, record := range registry.ListNonterminal() {
		if record.Route.SessionKey == sessionKey {
			return true
		}
	}
	return false
}

func (c *inboundTurnCoordinator) handleInteractionInbound(
	ctx context.Context,
	msg bus.InboundMessage,
	target *inboundDispatchTarget,
) {
	claim, _, claimed := c.claimSession(target)
	if !claimed {
		c.deferInteractionInbound(ctx, msg, target)
		return
	}
	go c.runInteractionWorker(ctx, msg, target, claim)
}

func (c *inboundTurnCoordinator) runInteractionWorker(
	ctx context.Context,
	msg bus.InboundMessage,
	target *inboundDispatchTarget,
	claim *runtimeSessionClaim,
) {
	admittedCtx, releaseCapacity, err := c.acquireTurnCapacity(ctx, target.Agent.ID)
	if err != nil {
		claim.releaseIfOwned()
		c.al.releaseInboundMessage(context.Background(), msg, err)
		return
	}
	defer releaseCapacity()
	ctx = admittedCtx
	defer claim.releaseIfOwned()
	defer c.recoverWorkerPanic(claim.scope.sessionKey, msg)
	if c.al.channelManager != nil {
		defer c.al.channelManager.InvokeTypingStop(msg.Channel, msg.ChatID)
	}

	ownership, processErr := c.al.processInteractionInbound(ctx, msg, target)
	if processErr != nil {
		logger.WarnCF("agent", "Failed to process human interaction answer", map[string]any{
			"session_key": target.SessionKey,
			"error":       processErr.Error(),
		})
		if ownership == interactionInboundCallerOwned {
			c.al.releaseInboundMessage(context.Background(), msg, processErr)
		}
		return
	}
	if ownership == interactionInboundDeferred {
		return
	}
	if ownership == interactionInboundCallerOwned {
		c.al.ackInboundMessage(ctx, msg)
	}
	if c.al.hasNonterminalInteraction(target.Agent.Workspace, target.SessionKey) {
		return
	}
	if err := c.al.drainDeferredInteractionIngress(ctx, target.Agent.Workspace, interactions.Route{
		SessionKey: target.SessionKey,
		Channel:    msg.Channel,
		ChatID:     msg.ChatID,
	}, msg.Context); err != nil {
		logger.WarnCF("agent", "Failed to continue messages deferred by human interaction", map[string]any{
			"session_key": target.SessionKey,
			"error":       err.Error(),
		})
	}
}

func (al *AgentLoop) drainDeferredInteractionIngress(
	ctx context.Context,
	workspace string,
	route interactions.Route,
	inbound bus.InboundContext,
) error {
	if al.hasNonterminalInteraction(workspace, route.SessionKey) {
		return nil
	}
	continued, err := al.drainQueuedSteeringContinuations(ctx, &continuationTarget{
		AgentID:    route.AgentID,
		SessionKey: route.SessionKey,
		Channel:    route.Channel,
		ChatID:     route.ChatID,
		Workspace:  workspace,
	})
	if err != nil {
		return err
	}
	if strings.TrimSpace(continued) != "" {
		al.publishResponseWithContextIfNeeded(
			ctx,
			route.Channel,
			route.ChatID,
			route.SessionKey,
			continued,
			&inbound,
			finalResponseAlwaysPublish,
		)
	}
	return nil
}

func (al *AgentLoop) processInteractionInbound(
	ctx context.Context,
	msg bus.InboundMessage,
	target *inboundDispatchTarget,
) (interactionInboundOwnership, error) {
	registry := al.interactionRegistryForWorkspace(target.Agent.Workspace)
	if registry.LastLoadError() != nil {
		return interactionInboundCallerOwned, al.publishInteractionNotice(
			ctx,
			msg,
			target.SessionKey,
			"Pending input state is unavailable; this session cannot continue until it is recovered.",
		)
	}
	record, ok := activeInteractionForSession(registry, target.SessionKey)
	if !ok {
		return interactionInboundCallerOwned, fmt.Errorf(
			"active interaction disappeared for session %q",
			target.SessionKey,
		)
	}
	if record.Status == interactions.StatusClaimed || record.Status == interactions.StatusResuming {
		if interactionInboundReplaysAnswer(record, msg.Context) {
			al.ackInboundMessage(ctx, msg)
			return interactionInboundClaimed, al.resumeClaimedInteraction(
				ctx,
				registry,
				target.Agent.Workspace,
				al.interactionContinuationAgent(record, target.Agent),
				&target.Allocation.Scope,
				msg.Context,
				record,
			)
		}
		if err := newInboundTurnCoordinator(al).enqueueDeferredInteractionInbound(
			ctx,
			msg,
			target,
		); err != nil {
			return interactionInboundCallerOwned, err
		}
		return interactionInboundDeferred, nil
	}
	if record.Status != interactions.StatusWaiting {
		return interactionInboundCallerOwned, fmt.Errorf(
			"interaction %q is not accepting input from status %q",
			record.ID,
			record.Status,
		)
	}
	if !interactionRouteAuthorizes(record.Route, target, msg.Context) {
		return interactionInboundCallerOwned, al.publishInteractionNotice(
			ctx,
			msg,
			target.SessionKey,
			"This session is waiting for an answer from the authorized user.",
		)
	}
	answer, err := parseInteractionAnswer(record, msg.Content, msg.Context.MessageID)
	if err != nil {
		return interactionInboundCallerOwned, al.publishInteractionNotice(
			ctx,
			msg,
			target.SessionKey,
			"I could not accept that answer: "+err.Error(),
		)
	}
	outcome := interactions.OutcomeAnswered
	if record.Kind == interactions.KindApproval {
		if answer.Text == "allow_once" {
			outcome = interactions.OutcomeAllowed
		} else {
			outcome = interactions.OutcomeDenied
		}
	}
	claimed, err := registry.ClaimAnswer(
		record.ID,
		record.Revision,
		answer,
		outcome,
	)
	if err != nil {
		if errors.Is(err, interactions.ErrAnswerTooLate) || errors.Is(err, interactions.ErrDuplicateAnswer) {
			return interactionInboundCallerOwned, al.publishInteractionNotice(
				ctx,
				msg,
				target.SessionKey,
				"An answer is already being processed for this session.",
			)
		}
		return interactionInboundCallerOwned, err
	}
	al.ackInboundMessage(ctx, msg)
	return interactionInboundClaimed, al.resumeClaimedInteraction(
		ctx,
		registry,
		target.Agent.Workspace,
		al.interactionContinuationAgent(claimed, target.Agent),
		&target.Allocation.Scope,
		msg.Context,
		claimed,
	)
}

func interactionInboundReplaysAnswer(record interactions.Record, inbound bus.InboundContext) bool {
	return record.Answer != nil && record.Answer.MessageID != "" &&
		record.Answer.MessageID == strings.TrimSpace(inbound.MessageID)
}

func activeInteractionForSession(
	registry *interactions.Registry,
	sessionKey string,
) (interactions.Record, bool) {
	if registry == nil {
		return interactions.Record{}, false
	}
	for _, record := range registry.ListNonterminal() {
		if record.Route.SessionKey == sessionKey {
			return record, true
		}
	}
	return interactions.Record{}, false
}

func interactionRouteAuthorizes(
	route interactions.Route,
	target *inboundDispatchTarget,
	inbound bus.InboundContext,
) bool {
	if target == nil || route.SessionKey != target.SessionKey ||
		route.Channel != inbound.Channel || route.ChatID != inbound.ChatID ||
		route.SenderID != inbound.SenderID {
		return false
	}
	if route.RouteSessionKey != "" && route.RouteSessionKey != target.Allocation.RouteScopeKey {
		return false
	}
	checks := [][2]string{
		{route.AccountID, inbound.Account},
		{route.ChatType, inbound.ChatType},
		{route.TopicID, inbound.TopicID},
		{route.SpaceID, inbound.SpaceID},
		{route.SpaceType, inbound.SpaceType},
	}
	for _, check := range checks {
		if check[0] != "" && check[0] != check[1] {
			return false
		}
	}
	return true
}

func parseInteractionAnswer(
	record interactions.Record,
	content string,
	messageID string,
) (interactions.Answer, error) {
	content = strings.TrimSpace(content)
	if strings.HasPrefix(strings.ToLower(content), answerCommand) {
		remainder := strings.TrimSpace(content[len(answerCommand):])
		shortID, answerText, ok := strings.Cut(remainder, " ")
		if !ok || !strings.EqualFold(strings.TrimSpace(shortID), record.ShortID) {
			return interactions.Answer{}, fmt.Errorf("use `/answer %s <answer>`", record.ShortID)
		}
		content = strings.TrimSpace(answerText)
	}
	if content == "" {
		return interactions.Answer{}, fmt.Errorf("answer cannot be empty")
	}
	answer := interactions.Answer{
		Text: content, MessageID: strings.TrimSpace(messageID), ReceivedAt: time.Now().UnixMilli(),
	}
	if record.Kind == interactions.KindApproval {
		normalized := strings.NewReplacer("-", "_", " ", "_").Replace(strings.ToLower(content))
		switch normalized {
		case "allow", "allow_once":
			answer.Text = "allow_once"
		case "deny":
			answer.Text = "deny"
		default:
			return interactions.Answer{}, fmt.Errorf("reply `allow_once` or `deny`")
		}
		return answer, nil
	}
	if len(record.Questions) == 1 {
		answer.Values = map[string]string{record.Questions[0].ID: content}
		return answer, nil
	}
	values := make(map[string]string, len(record.Questions))
	known := make(map[string]struct{}, len(record.Questions))
	for _, question := range record.Questions {
		known[question.ID] = struct{}{}
	}
	for _, line := range strings.Split(content, "\n") {
		key, value, ok := strings.Cut(line, ":")
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if !ok || key == "" || value == "" {
			return interactions.Answer{}, fmt.Errorf("use one `question_id: answer` line per question")
		}
		if _, ok := known[key]; !ok {
			return interactions.Answer{}, fmt.Errorf("unknown question id %q", key)
		}
		if _, duplicate := values[key]; duplicate {
			return interactions.Answer{}, fmt.Errorf("duplicate answer for %q", key)
		}
		values[key] = value
	}
	for _, question := range record.Questions {
		if values[question.ID] == "" {
			return interactions.Answer{}, fmt.Errorf("missing answer for %q", question.ID)
		}
	}
	answer.Values = values
	return answer, nil
}

func (al *AgentLoop) publishInteractionNotice(
	ctx context.Context,
	msg bus.InboundMessage,
	sessionKey string,
	content string,
) error {
	if al == nil || al.bus == nil {
		return fmt.Errorf("message bus unavailable")
	}
	return al.bus.PublishOutbound(ctx, bus.OutboundMessage{
		Channel:    msg.Channel,
		ChatID:     msg.ChatID,
		Context:    outboundContextFromInbound(&msg.Context, msg.Channel, msg.ChatID, msg.MessageID),
		SessionKey: sessionKey,
		Content:    content,
	})
}

type interactionToolResultPayload struct {
	InteractionID string               `json:"interaction_id"`
	Outcome       interactions.Outcome `json:"outcome"`
	Answers       map[string]string    `json:"answers,omitempty"`
	Text          string               `json:"text,omitempty"`
}

func (al *AgentLoop) resumeClaimedInteraction(
	ctx context.Context,
	registry *interactions.Registry,
	interactionWorkspace string,
	agent *AgentInstance,
	scope *session.SessionScope,
	inbound bus.InboundContext,
	record interactions.Record,
) error {
	if registry == nil || agent == nil {
		return fmt.Errorf("interaction continuation runtime is unavailable")
	}
	continuationSessionKey := interactionContinuationSessionKey(record)
	approvalAllowed := record.Kind == interactions.KindApproval &&
		record.Outcome == interactions.OutcomeAllowed
	if !approvalAllowed {
		if err := al.ensureInteractionToolResult(ctx, agent, record); err != nil {
			_, _ = registry.RecordResumeFailure(record.ID, record.Revision, err.Error())
			return err
		}
	}
	resuming := record
	if record.Status == interactions.StatusClaimed {
		var err error
		resuming, err = registry.MarkResuming(record.ID, record.Revision)
		if err != nil {
			return err
		}
	} else if record.Status != interactions.StatusResuming {
		return fmt.Errorf("cannot resume interaction from status %q", record.Status)
	}
	if approvalAllowed {
		current, ok := registry.Get(resuming.ID)
		if !ok {
			return interactions.ErrNotFound
		}
		resuming = current
		if _, resultIndex := interactionToolPairIndexes(
			agent.Sessions.GetHistory(continuationSessionKey),
			resuming.Origin.ToolCallID,
		); resultIndex < 0 {
			if resuming.ApprovalConsumedAt != 0 {
				if err := al.persistInteractionToolResult(
					ctx,
					agent,
					resuming,
					interactionToolResultPayload{
						InteractionID: resuming.ID,
						Outcome:       interactions.OutcomeDeliveryUnknown,
						Text: "The one-time approval was consumed before restart, but tool execution " +
							"could not be confirmed. The tool was not retried.",
					},
				); err != nil {
					return err
				}
			} else {
				control, err := al.executeApprovedInteractionTool(
					ctx, registry, interactionWorkspace, agent, scope, resuming,
				)
				if err != nil {
					_, _ = registry.RecordResumeFailure(resuming.ID, resuming.Revision, err.Error())
					return err
				}
				if control == ToolControlSuspend {
					return nil
				}
			}
		}
		current, ok = registry.Get(resuming.ID)
		if !ok {
			return interactions.ErrNotFound
		}
		if current.Status == interactions.StatusResolved {
			return nil
		}
		resuming = current
	}
	if finalContent, ok := interactionFinalAfterToolResult(
		agent.Sessions.GetHistory(continuationSessionKey),
		record.Origin.ToolCallID,
	); ok {
		return al.deliverInteractionFinal(
			ctx, registry, interactionWorkspace, resuming, inbound, finalContent,
		)
	}

	routeSessionKey := record.Route.RouteSessionKey
	if routeSessionKey == "" {
		routeSessionKey = record.Route.SessionKey
	}
	modelBinding := al.bindEffectiveModel(routeSessionKey, agent)
	defer modelBinding.Cleanup()
	turnStatus := TurnEndStatusCompleted
	finalContent, runErr := al.runAgentLoop(ctx, agent, processOptions{
		ModelBinding:          modelBinding,
		TaskID:                record.Origin.TaskID,
		InteractionWorkspace:  interactionWorkspace,
		InteractionSessionKey: record.Route.SessionKey,
		InteractionRouteKey:   routeSessionKey,
		TurnStatus:            &turnStatus,
		Dispatch: DispatchRequest{
			RouteSessionKey: routeSessionKey,
			BaseSessionKey:  continuationSessionKey,
			SessionKey:      continuationSessionKey,
			InboundContext:  cloneInboundContext(&inbound),
			SessionScope:    session.CloneScope(scope),
		},
		DefaultResponse:         defaultResponse,
		EnableSummary:           true,
		SendResponse:            false,
		ExpectFinalDelivery:     true,
		AllowInterimPicoPublish: true,
		SkipInitialSteeringPoll: true,
	})
	if runErr != nil {
		_, _ = registry.RecordResumeFailure(resuming.ID, resuming.Revision, runErr.Error())
		return runErr
	}
	if turnStatus == TurnEndStatusSuspended {
		return nil
	}
	return al.deliverInteractionFinal(
		ctx, registry, interactionWorkspace, resuming, inbound, finalContent,
	)
}

func (al *AgentLoop) executeApprovedInteractionTool(
	ctx context.Context,
	registry *interactions.Registry,
	interactionWorkspace string,
	agent *AgentInstance,
	scope *session.SessionScope,
	record interactions.Record,
) (ToolControl, error) {
	history := agent.Sessions.GetHistory(interactionContinuationSessionKey(record))
	toolCall, ok := interactionOriginToolCall(history, record.Origin.ToolCallID)
	if !ok {
		return ToolControlBreak, fmt.Errorf(
			"originating approval tool call %q is missing",
			record.Origin.ToolCallID,
		)
	}
	originalInbound := cloneInboundContext(record.Origin.ExecutionContext)
	if originalInbound == nil {
		if err := al.persistInteractionToolResult(
			ctx,
			agent,
			record,
			interactionToolResultPayload{
				InteractionID: record.ID,
				Outcome:       interactions.OutcomeDenied,
				Text: "The protected tool was not executed because its original execution " +
					"context is unavailable after restart.",
			},
		); err != nil {
			return ToolControlBreak, err
		}
		return ToolControlBreak, nil
	}
	toolCall = providers.NormalizeToolCall(toolCall)
	routeSessionKey := record.Route.RouteSessionKey
	if routeSessionKey == "" {
		routeSessionKey = record.Route.SessionKey
	}
	opts := processOptions{
		TaskID:                record.Origin.TaskID,
		InteractionWorkspace:  interactionWorkspace,
		InteractionSessionKey: record.Route.SessionKey,
		InteractionRouteKey:   routeSessionKey,
		ApprovalGrant: &ToolApprovalGrant{
			InteractionID: record.ID,
			Revision:      record.Revision,
		},
		Dispatch: DispatchRequest{
			RouteSessionKey: routeSessionKey,
			BaseSessionKey:  interactionContinuationSessionKey(record),
			SessionKey:      interactionContinuationSessionKey(record),
			InboundContext:  originalInbound,
			SessionScope:    session.CloneScope(scope),
		},
		DefaultResponse: defaultResponse,
		EnableSummary:   true,
		SendResponse:    false,
	}
	var err error
	opts, err = resolveTurnProfileOptions(al.GetConfig(), opts)
	if err != nil {
		return ToolControlBreak, fmt.Errorf("resolve approved tool profile: %w", err)
	}
	turnScope := al.newTurnEventScope(
		agent.ID,
		opts.Dispatch.SessionKey,
		newTurnContext(opts.Dispatch.InboundContext, nil, opts.Dispatch.SessionScope),
	)
	ts := newTurnState(agent, opts, turnScope)
	pipeline := NewPipeline(al)
	exec, err := pipeline.SetupTurn(ctx, ts)
	if err != nil {
		return ToolControlBreak, err
	}
	if exec.model.cleanup != nil {
		defer exec.model.cleanup()
	}
	exec.response = &providers.LLMResponse{ToolCalls: []providers.ToolCall{toolCall}}
	exec.normalizedToolCalls = []providers.ToolCall{toolCall}
	exec.allResponsesHandled = true
	exec.assistantToolCallsPersisted = true
	control := pipeline.ExecuteTools(ctx, ctx, ts, exec, 1)
	if control == ToolControlSuspend {
		return control, nil
	}
	if _, resultIndex := interactionToolPairIndexes(
		agent.Sessions.GetHistory(interactionContinuationSessionKey(record)),
		record.Origin.ToolCallID,
	); resultIndex < 0 {
		return control, fmt.Errorf("approved tool execution did not persist a matching result")
	}
	_, ok = registry.Get(record.ID)
	if !ok {
		return control, interactions.ErrNotFound
	}
	return control, nil
}

func (al *AgentLoop) interactionContinuationAgent(
	record interactions.Record,
	fallback *AgentInstance,
) *AgentInstance {
	if al != nil && strings.TrimSpace(record.Route.AgentID) != "" {
		if registry := al.GetRegistry(); registry != nil {
			if agent, ok := registry.GetAgent(record.Route.AgentID); ok && agent != nil {
				return agent
			}
		}
	}
	return fallback
}

func interactionContinuationSessionKey(record interactions.Record) string {
	if key := strings.TrimSpace(record.Origin.ContinuationSessionKey); key != "" {
		return key
	}
	return record.Route.SessionKey
}

func (al *AgentLoop) deliverInteractionFinal(
	ctx context.Context,
	registry *interactions.Registry,
	interactionWorkspace string,
	record interactions.Record,
	inbound bus.InboundContext,
	content string,
) error {
	if strings.TrimSpace(record.Origin.TaskID) != "" {
		return al.deliverTaskInteractionFinal(
			ctx, registry, interactionWorkspace, record, inbound, content,
		)
	}
	if record.FinalDelivered || strings.TrimSpace(content) == "" {
		updated, err := registry.Resolve(record.ID, record.Revision)
		if err == nil {
			al.completeInteractionTask(
				interactionWorkspace, updated, content, taskregistry.DeliveryNotApplicable,
			)
		}
		return err
	}
	if al.channelManager == nil {
		_, _ = registry.RecordFinalDeliveryAttempt(
			record.ID, record.Revision, false, "channel manager unavailable",
		)
		return fmt.Errorf("channel manager unavailable")
	}
	started, stateErr := registry.BeginFinalDelivery(record.ID, record.Revision)
	if stateErr != nil {
		return fmt.Errorf("begin final interaction delivery: %w", stateErr)
	}
	if inbound.Raw == nil {
		inbound.Raw = make(map[string]string)
	}
	inbound.Raw[interactionIDMetadata] = record.ID
	inbound.Raw[interactionShortIDMeta] = record.ShortID
	inbound.Raw["delivery_key"] = interactionDeliveryKey(record.ID, "final")
	deliveryErr := al.sendInteractionMessage(ctx, bus.OutboundMessage{
		Channel: record.Route.Channel, ChatID: record.Route.ChatID,
		Context: inbound, AgentID: record.Route.AgentID,
		SessionKey: record.Route.SessionKey, Content: content,
	})
	updated, stateErr := registry.CompleteFinalDelivery(
		started.ID,
		started.Revision,
		deliveryErr == nil,
		deliveryErr != nil && !channels.DeliveryDefinitelyNotSent(deliveryErr),
		errString(deliveryErr),
	)
	if stateErr != nil {
		return fmt.Errorf("record final interaction delivery: %w", stateErr)
	}
	if deliveryErr != nil {
		return deliveryErr
	}
	resolved, err := registry.Resolve(updated.ID, updated.Revision)
	if err == nil {
		al.completeInteractionTask(
			interactionWorkspace, resolved, content, taskregistry.DeliveryDelivered,
		)
	}
	return err
}

func (al *AgentLoop) deliverTaskInteractionFinal(
	ctx context.Context,
	registry *interactions.Registry,
	workspace string,
	record interactions.Record,
	inbound bus.InboundContext,
	content string,
) error {
	taskRegistry := al.taskRegistryForWorkspace(workspace)
	taskID := strings.TrimSpace(record.Origin.TaskID)
	if taskRegistry == nil || taskID == "" {
		return fmt.Errorf("owning task registry is unavailable")
	}
	task, ok := taskRegistry.Get(taskID)
	if !ok {
		return fmt.Errorf("owning task %q is unavailable", taskID)
	}
	if err := taskRegistry.CompleteInteractionTask(
		taskID, record.ID, content, taskregistry.DeliveryPending,
	); err != nil {
		return err
	}
	started, stateErr := registry.BeginFinalDelivery(record.ID, record.Revision)
	if stateErr != nil {
		return fmt.Errorf("begin task interaction delivery: %w", stateErr)
	}
	mode := tools.AsyncDeliveryMode(strings.TrimSpace(task.DeliveryMode))
	switch mode {
	case tools.AsyncDeliveryParentOnly, tools.AsyncDeliveryUserOnly, tools.AsyncDeliveryUserAndParent:
	default:
		mode = tools.AsyncDeliveryUserOnly
	}
	result := (&tools.ToolResult{ForLLM: content, ForUser: content}).
		WithAsyncTaskID(taskID).
		WithAsyncDelivery(mode)
	if strings.TrimSpace(content) != "" {
		result.WithCompletion(&tools.CompletionResult{Text: content})
	}
	agent := al.interactionContinuationAgent(record, nil)
	turnState := &turnState{
		agent: agent, agentID: record.Route.AgentID,
		workspace: workspace, channel: record.Route.Channel, chatID: record.Route.ChatID,
		sessionKey: record.Route.SessionKey,
		opts: processOptions{Dispatch: DispatchRequest{
			RouteSessionKey: record.Route.RouteSessionKey,
			SessionKey:      record.Route.SessionKey,
			InboundContext:  cloneInboundContext(&inbound),
		}},
		scope: al.newTurnEventScope(
			record.Route.AgentID,
			record.Route.SessionKey,
			newTurnContext(&inbound, nil, nil),
		),
	}
	completionID := "interaction:" + record.ID
	al.deliverAsyncToolCompletion(AsyncDeliveryRequest{
		TurnState:    turnState,
		ToolName:     task.TaskKind,
		CompletionID: completionID,
		Result:       result,
		Decision:     decideAsyncToolResultDelivery(result),
	})
	task, _ = taskRegistry.Get(taskID)
	success := task.DeliveryStatus == taskregistry.DeliveryDelivered ||
		task.DeliveryStatus == taskregistry.DeliverySessionQueued ||
		task.DeliveryStatus == taskregistry.DeliveryNotApplicable
	detail := task.DeliveryError
	ambiguous := !success
	if task.DeliveryStatus == taskregistry.DeliveryParentMissing &&
		mode == tools.AsyncDeliveryParentOnly {
		ambiguous = false
	}
	updated, stateErr := registry.CompleteFinalDelivery(
		started.ID, started.Revision, success, ambiguous, detail,
	)
	if stateErr != nil {
		return fmt.Errorf("record task interaction delivery: %w", stateErr)
	}
	if !success {
		if detail == "" {
			detail = "task completion delivery did not reach a final state"
		}
		return fmt.Errorf("deliver resumed task completion: %s", detail)
	}
	_, err := registry.Resolve(updated.ID, updated.Revision)
	return err
}

func (al *AgentLoop) completeInteractionTask(
	workspace string,
	record interactions.Record,
	content string,
	delivery taskregistry.DeliveryStatus,
) {
	taskID := strings.TrimSpace(record.Origin.TaskID)
	if al == nil || taskID == "" {
		return
	}
	registry := al.taskRegistryForWorkspace(workspace)
	if registry == nil {
		return
	}
	if err := registry.CompleteInteractionTask(
		taskID, record.ID, content, delivery,
	); err != nil {
		logger.WarnCF("agent", "Failed to complete resumed interaction task", map[string]any{
			"workspace": workspace, "task_id": taskID,
			"interaction_id": record.ID, "error": err.Error(),
		})
	}
}

func interactionFinalAfterToolResult(
	history []providers.Message,
	toolCallID string,
) (string, bool) {
	_, resultIndex := interactionToolPairIndexes(history, toolCallID)
	if resultIndex < 0 {
		return "", false
	}
	for _, message := range history[resultIndex+1:] {
		if message.Role == "assistant" && len(message.ToolCalls) == 0 &&
			strings.TrimSpace(message.Content) != "" {
			return message.Content, true
		}
	}
	return "", false
}

func (al *AgentLoop) ensureInteractionToolResult(
	ctx context.Context,
	agent *AgentInstance,
	record interactions.Record,
) error {
	history := agent.Sessions.GetHistory(interactionContinuationSessionKey(record))
	originIndex, resultIndex := interactionToolPairIndexes(history, record.Origin.ToolCallID)
	if originIndex < 0 {
		return fmt.Errorf("originating tool call %q is missing from session history", record.Origin.ToolCallID)
	}
	if resultIndex >= 0 {
		return nil
	}
	if record.Answer == nil {
		return fmt.Errorf("interaction %q has no claimed answer", record.ID)
	}
	return al.persistInteractionToolResult(ctx, agent, record, interactionToolResultPayload{
		InteractionID: record.ID,
		Outcome:       record.Outcome,
		Text:          record.Answer.Text,
		Answers:       record.Answer.Values,
	})
}

func (al *AgentLoop) ensureInteractionCancellationToolResult(
	ctx context.Context,
	agent *AgentInstance,
	record interactions.Record,
	code string,
) error {
	history := agent.Sessions.GetHistory(interactionContinuationSessionKey(record))
	originIndex, resultIndex := interactionToolPairIndexes(history, record.Origin.ToolCallID)
	if originIndex < 0 {
		return fmt.Errorf("originating tool call %q is missing from session history", record.Origin.ToolCallID)
	}
	if resultIndex >= 0 {
		return nil
	}
	return al.persistInteractionToolResult(ctx, agent, record, interactionToolResultPayload{
		InteractionID: record.ID,
		Outcome:       interactions.OutcomeCanceled,
		Text:          code,
	})
}

func (al *AgentLoop) persistInteractionToolResult(
	ctx context.Context,
	agent *AgentInstance,
	record interactions.Record,
	payload interactionToolResultPayload,
) error {
	content, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	message := providers.Message{
		Role: "tool", Content: string(content), ToolCallID: record.Origin.ToolCallID,
		ToolResultStatus: providers.ToolResultStatusSuccess,
	}
	continuationSessionKey := interactionContinuationSessionKey(record)
	writeErr := persistFullSessionMessage(agent.Sessions, continuationSessionKey, message)
	if writeErr != nil {
		return writeErr
	}
	if al.contextManager != nil {
		if err := al.contextManager.Ingest(ctx, &IngestRequest{
			SessionKey: continuationSessionKey,
			Message:    message,
		}); err != nil {
			logger.WarnCF("agent", "Context ingest failed for interaction answer", map[string]any{
				"interaction_id": record.ID,
				"error":          err.Error(),
			})
		}
	}
	return nil
}

func interactionToolPairIndexes(
	history []providers.Message,
	toolCallID string,
) (originIndex int, resultIndex int) {
	originIndex = -1
	resultIndex = -1
	for index, message := range history {
		if message.Role != "assistant" {
			continue
		}
		for _, call := range message.ToolCalls {
			if call.ID == toolCallID {
				originIndex = index
				resultIndex = -1
				break
			}
		}
	}
	if originIndex < 0 {
		return originIndex, resultIndex
	}
	for index := originIndex + 1; index < len(history); index++ {
		message := history[index]
		if message.Role == "tool" && message.ToolCallID == toolCallID {
			return originIndex, index
		}
	}
	return originIndex, resultIndex
}

func interactionOriginToolCall(
	history []providers.Message,
	toolCallID string,
) (providers.ToolCall, bool) {
	for index := len(history) - 1; index >= 0; index-- {
		message := history[index]
		if message.Role != "assistant" {
			continue
		}
		for _, call := range message.ToolCalls {
			if call.ID == toolCallID {
				return call, true
			}
		}
	}
	return providers.ToolCall{}, false
}
