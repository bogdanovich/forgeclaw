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
)

const answerCommand = "/answer"

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
			target.Agent,
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
	claim, activeTarget, claimed := c.claimSession(target)
	if !claimed {
		c.handleInteractionBusy(ctx, msg, activeTarget.SessionKey)
		return
	}
	go c.runInteractionWorker(ctx, msg, target, claim)
}

func (c *inboundTurnCoordinator) handleInteractionBusy(
	ctx context.Context,
	msg bus.InboundMessage,
	sessionKey string,
) {
	if err := c.al.publishInteractionNotice(
		ctx,
		msg,
		sessionKey,
		"An answer is already being processed for this session.",
	); err != nil {
		c.al.releaseInboundMessage(context.Background(), msg, err)
		return
	}
	c.al.ackInboundMessage(ctx, msg)
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
	defer c.recoverWorkerPanic(claim.sessionKey, msg)
	if c.al.channelManager != nil {
		defer c.al.channelManager.InvokeTypingStop(msg.Channel, msg.ChatID)
	}

	if processErr := c.al.processInteractionInbound(ctx, msg, target); processErr != nil {
		logger.WarnCF("agent", "Failed to process human interaction answer", map[string]any{
			"session_key": target.SessionKey,
			"error":       processErr.Error(),
		})
		c.al.releaseInboundMessage(context.Background(), msg, processErr)
		return
	}
	c.al.ackInboundMessage(ctx, msg)
	if err := c.al.drainDeferredInteractionIngress(ctx, interactions.Route{
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
	route interactions.Route,
	inbound bus.InboundContext,
) error {
	continued, err := al.drainQueuedSteeringContinuations(ctx, &continuationTarget{
		SessionKey: route.SessionKey,
		Channel:    route.Channel,
		ChatID:     route.ChatID,
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
) error {
	registry := al.interactionRegistryForWorkspace(target.Agent.Workspace)
	if registry.LastLoadError() != nil {
		return al.publishInteractionNotice(
			ctx,
			msg,
			target.SessionKey,
			"Pending input state is unavailable; this session cannot continue until it is recovered.",
		)
	}
	record, ok := activeInteractionForSession(registry, target.SessionKey)
	if !ok {
		return fmt.Errorf("active interaction disappeared for session %q", target.SessionKey)
	}
	if record.Status == interactions.StatusClaimed || record.Status == interactions.StatusResuming {
		if interactionInboundReplaysAnswer(record, msg.Context) {
			return al.resumeClaimedInteraction(
				ctx,
				target.Agent,
				&target.Allocation.Scope,
				msg.Context,
				record,
			)
		}
		return al.publishInteractionNotice(
			ctx,
			msg,
			target.SessionKey,
			"An answer is already being processed for this session.",
		)
	}
	if record.Status != interactions.StatusWaiting {
		return fmt.Errorf("interaction %q is not accepting input from status %q", record.ID, record.Status)
	}
	if !interactionRouteAuthorizes(record.Route, target, msg.Context) {
		return al.publishInteractionNotice(
			ctx,
			msg,
			target.SessionKey,
			"This session is waiting for an answer from the authorized user.",
		)
	}
	answer, err := parseInteractionAnswer(record, msg.Content, msg.Context.MessageID)
	if err != nil {
		return al.publishInteractionNotice(ctx, msg, target.SessionKey, "I could not accept that answer: "+err.Error())
	}
	claimed, err := registry.ClaimAnswer(
		record.ID,
		record.Revision,
		answer,
		interactions.OutcomeAnswered,
	)
	if err != nil {
		if errors.Is(err, interactions.ErrAnswerTooLate) || errors.Is(err, interactions.ErrDuplicateAnswer) {
			return al.publishInteractionNotice(
				ctx,
				msg,
				target.SessionKey,
				"An answer is already being processed for this session.",
			)
		}
		return err
	}
	return al.resumeClaimedInteraction(ctx, target.Agent, &target.Allocation.Scope, msg.Context, claimed)
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
	agent *AgentInstance,
	scope *session.SessionScope,
	inbound bus.InboundContext,
	record interactions.Record,
) error {
	registry := al.interactionRegistryForWorkspace(agent.Workspace)
	if err := al.ensureInteractionToolResult(ctx, agent, record); err != nil {
		_, _ = registry.RecordResumeFailure(record.ID, record.Revision, err.Error())
		return err
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
	if finalContent, ok := interactionFinalAfterToolResult(
		agent.Sessions.GetHistory(record.Route.SessionKey),
		record.Origin.ToolCallID,
	); ok {
		return al.deliverInteractionFinal(ctx, registry, resuming, inbound, finalContent)
	}

	routeSessionKey := record.Route.RouteSessionKey
	if routeSessionKey == "" {
		routeSessionKey = record.Route.SessionKey
	}
	modelBinding := al.bindEffectiveModel(routeSessionKey, agent)
	defer modelBinding.Cleanup()
	finalContent, runErr := al.runAgentLoop(ctx, agent, processOptions{
		ModelBinding: modelBinding,
		Dispatch: DispatchRequest{
			RouteSessionKey: routeSessionKey,
			BaseSessionKey:  record.Route.SessionKey,
			SessionKey:      record.Route.SessionKey,
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
	return al.deliverInteractionFinal(ctx, registry, resuming, inbound, finalContent)
}

func (al *AgentLoop) deliverInteractionFinal(
	ctx context.Context,
	registry *interactions.Registry,
	record interactions.Record,
	inbound bus.InboundContext,
	content string,
) error {
	if record.FinalDelivered || strings.TrimSpace(content) == "" {
		_, err := registry.Resolve(record.ID, record.Revision)
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
	_, err := registry.Resolve(updated.ID, updated.Revision)
	return err
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
	history := agent.Sessions.GetHistory(record.Route.SessionKey)
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
	history := agent.Sessions.GetHistory(record.Route.SessionKey)
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
	writeErr := persistFullSessionMessage(agent.Sessions, record.Route.SessionKey, message)
	if writeErr != nil {
		return writeErr
	}
	if al.contextManager != nil {
		if err := al.contextManager.Ingest(ctx, &IngestRequest{
			SessionKey: record.Route.SessionKey,
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
