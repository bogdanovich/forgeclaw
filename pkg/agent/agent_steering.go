// PicoClaw - Ultra-lightweight personal AI agent

package agent

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/sipeed/picoclaw/pkg/bus"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/logger"
)

func (al *AgentLoop) processMessageSync(ctx context.Context, msg bus.InboundMessage) bool {
	if al.channelManager != nil {
		defer al.channelManager.InvokeTypingStop(msg.Channel, msg.ChatID)
	}

	_, routedAgent, _ := al.resolveMessageRoute(msg)
	workspace, agentID := "", ""
	if routedAgent != nil {
		workspace, agentID = routedAgent.Workspace, routedAgent.ID
	}
	response, err := al.processMessage(ctx, msg)
	if err != nil {
		if !al.maybePublishErrorWithPolicy(
			ctx,
			workspace,
			agentID,
			msg.Channel,
			msg.ChatID,
			msg.SessionKey,
			err,
			finalResponseAlwaysPublish,
		) {
			return false
		}
		response = ""
	}
	al.publishResponseWithContextIfNeeded(
		ctx,
		workspace,
		agentID,
		msg.Channel,
		msg.ChatID,
		msg.SessionKey,
		response,
		&msg.Context,
		finalResponseAlwaysPublish,
	)
	return true
}

func (al *AgentLoop) ackInboundMessage(ctx context.Context, msg bus.InboundMessage) {
	if msg.SpoolID == "" || al.bus == nil {
		return
	}
	if err := al.bus.AckInbound(ctx, msg); err != nil {
		logger.WarnCF("agent", "Failed to ack inbound spool entry",
			map[string]any{
				"spool_id":    msg.SpoolID,
				"channel":     msg.Channel,
				"chat_id":     msg.ChatID,
				"session_key": msg.SessionKey,
				"error":       err.Error(),
			})
	}
}

func (al *AgentLoop) releaseInboundMessage(
	ctx context.Context,
	msg bus.InboundMessage,
	cause error,
) {
	if msg.SpoolID == "" || al.bus == nil {
		return
	}
	if err := al.bus.ReleaseInbound(ctx, msg, cause); err != nil {
		logger.WarnCF("agent", "Failed to release inbound spool entry",
			map[string]any{
				"spool_id":    msg.SpoolID,
				"channel":     msg.Channel,
				"chat_id":     msg.ChatID,
				"session_key": msg.SessionKey,
				"error":       err.Error(),
			})
	}
}

func (al *AgentLoop) runInboundTurnWithSteering(
	ctx context.Context,
	turn inboundMessageTurn,
) bool {
	traceScopes := make([]runtimeevents.TraceScope, 0, 2)
	observeTurn := func(scope runtimeevents.TraceScope) {
		traceScopes = appendUniqueTraceScope(traceScopes, scope)
	}
	turn.Options.ObserveFinalDeliveryTurn = observeTurn
	target := &continuationTarget{
		SessionKey:               turn.SessionKey,
		Channel:                  turn.Message.Channel,
		ChatID:                   turn.Message.ChatID,
		ObserveFinalDeliveryTurn: observeTurn,
	}
	turn.Options.ObserveFinalResponse = target.observeFinalResponse
	if turn.Agent != nil {
		target.AgentID = turn.Agent.ID
		target.Workspace = turn.Agent.Workspace
	}
	return al.runTurnAndDrainSteering(ctx, turn.Message, func() (string, error) {
		return al.processInboundMessageTurn(ctx, turn)
	}, target, &traceScopes)
}

func (al *AgentLoop) runTurnAndDrainSteering(
	ctx context.Context,
	initialMsg bus.InboundMessage,
	process func() (string, error),
	target *continuationTarget,
	traceScopes *[]runtimeevents.TraceScope,
) bool {
	response, err := process()
	if err != nil {
		if !al.maybePublishErrorWithScopes(
			ctx,
			target.Workspace,
			target.AgentID,
			initialMsg.Channel,
			initialMsg.ChatID,
			initialMsg.SessionKey,
			err,
			finalResponseAlwaysPublish,
			*traceScopes,
		) {
			return false // context canceled
		}
		response = ""
	}
	responses := appendSteeringResponse(nil, response)
	initialMetadata := target.responseMetadata
	target.responseMetadata = bus.OutboundMetadata{}

	continued, continueErr := al.drainQueuedSteeringContinuations(ctx, target)
	if continueErr != nil {
		target.responseMetadata = initialMetadata
		if ctx.Err() != nil {
			return false
		}
		logger.WarnCF("agent", "Failed to continue queued steering",
			map[string]any{
				"channel": target.Channel,
				"chat_id": target.ChatID,
				"error":   continueErr.Error(),
			})
	} else {
		continuedResponses := appendSteeringResponse(nil, continued)
		if len(continuedResponses) > 0 {
			responses = continuedResponses
		} else {
			target.responseMetadata = initialMetadata
		}
	}

	// Publish final response
	finalResponse := joinSteeringResponses(responses)
	if finalResponse != "" {
		al.publishResponseWithMetadataAndScopes(
			ctx,
			target.Workspace,
			target.AgentID,
			target.Channel,
			target.ChatID,
			target.SessionKey,
			finalResponse,
			&bus.InboundContext{
				Channel: initialMsg.Context.Channel,
				ChatID:  initialMsg.Context.ChatID,
				TopicID: initialMsg.Context.TopicID,
				Raw: func() map[string]string {
					raw := make(map[string]string, len(initialMsg.Context.Raw)+1)
					for k, v := range initialMsg.Context.Raw {
						raw[k] = v
					}
					raw[metadataKeyMessageKind] = messageKindFinalReply
					return raw
				}(),
			},
			finalResponseAlwaysPublish,
			target.responseMetadata,
			*traceScopes,
		)
	}
	return true
}

func (t *continuationTarget) observeFinalResponse(metadata bus.OutboundMetadata) {
	if t == nil {
		return
	}
	if metadata.ModelName != "" {
		t.responseMetadata.ModelName = metadata.ModelName
	}
	if metadata.DefaultModelName != "" {
		t.responseMetadata.DefaultModelName = metadata.DefaultModelName
	}
	t.responseMetadata.UsageInputTokens += metadata.UsageInputTokens
	t.responseMetadata.UsageOutputTokens += metadata.UsageOutputTokens
	t.responseMetadata.UsageTotalTokens += metadata.UsageTotalTokens
}

func (t *continuationTarget) retainResponseMetadata(
	snapshot bus.OutboundMetadata,
	response string,
) bool {
	if t == nil || strings.TrimSpace(response) != "" {
		return true
	}
	t.responseMetadata = snapshot
	return false
}

func (t *continuationTarget) appendContinuationResponse(
	responses []string,
	snapshot bus.OutboundMetadata,
	response string,
) ([]string, bool) {
	if !t.retainResponseMetadata(snapshot, response) {
		return responses, false
	}
	retained := appendSteeringResponse(responses, response)
	if len(retained) == len(responses) {
		t.responseMetadata = snapshot
	}
	return retained, true
}

func (al *AgentLoop) drainQueuedSteeringContinuations(
	ctx context.Context,
	target *continuationTarget,
) (string, error) {
	if target == nil {
		return "", nil
	}

	scope := newRuntimeSessionScope(target.Workspace, target.SessionKey)
	if !scope.complete() {
		return "", fmt.Errorf("continuation workspace and session are required")
	}
	responses := make([]string, 0, 2)
	for al.pendingSteeringCountForScope(scope) > 0 {
		if err := ctx.Err(); err != nil {
			return joinSteeringResponses(responses), err
		}
		if target.Workspace != "" &&
			al.hasNonterminalInteraction(target.Workspace, target.SessionKey) {
			return joinSteeringResponses(responses), nil
		}

		logger.InfoCF("agent", "Continuing queued steering after turn end",
			map[string]any{
				"channel":     target.Channel,
				"chat_id":     target.ChatID,
				"session_key": target.SessionKey,
				"queue_depth": al.pendingSteeringCountForScope(scope),
			})

		metadataBefore := target.responseMetadata
		continued, continueErr := al.continueRuntimeSession(ctx, target)
		if continueErr != nil {
			target.responseMetadata = metadataBefore
			return joinSteeringResponses(responses), continueErr
		}
		var keepDraining bool
		responses, keepDraining = target.appendContinuationResponse(
			responses,
			metadataBefore,
			continued,
		)
		if !keepDraining {
			break
		}
	}

	return joinSteeringResponses(responses), nil
}

func appendUniqueTraceScope(
	values []runtimeevents.TraceScope,
	value runtimeevents.TraceScope,
) []runtimeevents.TraceScope {
	value = runtimeevents.NewTraceScope(value.Workspace, value.TurnID)
	if !value.Complete() {
		return values
	}
	if slices.Contains(values, value) {
		return values
	}
	return append(values, value)
}

func appendSteeringResponse(responses []string, response string) []string {
	response = strings.TrimSpace(response)
	if response == "" {
		return responses
	}
	if n := len(responses); n > 0 && responses[n-1] == response {
		return responses
	}
	return append(responses, response)
}

func joinSteeringResponses(responses []string) string {
	if len(responses) == 0 {
		return ""
	}
	return strings.Join(responses, "\n\n")
}

func (al *AgentLoop) resolveSteeringTarget(msg bus.InboundMessage) (*inboundDispatchTarget, bool) {
	if msg.Channel == "system" {
		return nil, false
	}

	route, agent, err := al.resolveMessageRoute(msg)
	if err != nil || agent == nil {
		return nil, false
	}
	allocation := al.allocateRouteSession(route, msg)
	routeClaimKey := runtimeRouteClaimKey(allocation.RouteScopeKey, msg.SessionKey)
	routeScope := newRuntimeRouteScope(agent.Workspace, routeClaimKey)
	if activeTarget, ok := al.activeRouteSessions.Load(routeScope); ok {
		target, targetOK := activeTarget.(*inboundDispatchTarget)
		if targetOK {
			al.touchActiveSessionLifecycle(target)
		}
		return target, targetOK
	}
	allocation, err = al.applySessionLifecycle(allocation, route.SessionPolicy.Lifecycle)
	if err != nil {
		return nil, false
	}
	return &inboundDispatchTarget{
		Route:      route,
		Agent:      agent,
		Allocation: allocation,
		SessionKey: al.resolveEffectiveSessionKey(
			allocation.RouteScopeKey,
			allocation.SessionKey,
			msg.SessionKey,
		),
		RouteClaimKey: routeClaimKey,
	}, true
}
