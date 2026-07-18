// PicoClaw - Ultra-lightweight personal AI agent

package agent

import (
	"context"
	"strings"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/logger"
)

func (al *AgentLoop) processMessageSync(ctx context.Context, msg bus.InboundMessage) bool {
	if al.channelManager != nil {
		defer al.channelManager.InvokeTypingStop(msg.Channel, msg.ChatID)
	}

	response, err := al.processMessage(ctx, msg)
	if err != nil {
		if !al.maybePublishErrorWithPolicy(
			ctx,
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
	target := &continuationTarget{
		SessionKey: turn.SessionKey,
		Channel:    turn.Message.Channel,
		ChatID:     turn.Message.ChatID,
	}
	return al.runTurnAndDrainSteering(ctx, turn.Message, func() (string, error) {
		return al.processInboundMessageTurn(ctx, turn)
	}, target)
}

func (al *AgentLoop) runTurnAndDrainSteering(
	ctx context.Context,
	initialMsg bus.InboundMessage,
	process func() (string, error),
	target *continuationTarget,
) bool {
	response, err := process()
	if err != nil {
		if !al.maybePublishErrorWithPolicy(
			ctx,
			initialMsg.Channel,
			initialMsg.ChatID,
			initialMsg.SessionKey,
			err,
			finalResponseAlwaysPublish,
		) {
			return false // context canceled
		}
		response = ""
	}
	responses := appendSteeringResponse(nil, response)

	continued, continueErr := al.drainQueuedSteeringContinuations(ctx, target)
	if continueErr != nil {
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
		}
	}

	// Publish final response
	finalResponse := joinSteeringResponses(responses)
	if finalResponse != "" {
		al.publishResponseWithContextIfNeeded(
			ctx,
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
		)
	}
	return true
}

func (al *AgentLoop) drainQueuedSteeringContinuations(
	ctx context.Context,
	target *continuationTarget,
) (string, error) {
	if target == nil {
		return "", nil
	}

	responses := make([]string, 0, 2)
	for al.pendingSteeringCountForScope(target.SessionKey) > 0 {
		if err := ctx.Err(); err != nil {
			return joinSteeringResponses(responses), err
		}

		logger.InfoCF("agent", "Continuing queued steering after turn end",
			map[string]any{
				"channel":     target.Channel,
				"chat_id":     target.ChatID,
				"session_key": target.SessionKey,
				"queue_depth": al.pendingSteeringCountForScope(target.SessionKey),
			})

		continued, continueErr := al.Continue(ctx, target.SessionKey, target.Channel, target.ChatID)
		if continueErr != nil {
			return joinSteeringResponses(responses), continueErr
		}
		if continued == "" {
			break
		}
		responses = appendSteeringResponse(responses, continued)
	}

	return joinSteeringResponses(responses), nil
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
	if activeTarget, ok := al.activeRouteSessions.Load(allocation.RouteScopeKey); ok {
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
	}, true
}
