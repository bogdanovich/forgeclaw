// PicoClaw - Ultra-lightweight personal AI agent

package agent

import (
	"context"
	"fmt"
	"strings"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
)

type inboundTurnCoordinator struct {
	al *AgentLoop
}

func newInboundTurnCoordinator(al *AgentLoop) *inboundTurnCoordinator {
	return &inboundTurnCoordinator{al: al}
}

func (c *inboundTurnCoordinator) handleInbound(ctx context.Context, msg bus.InboundMessage) {
	al := c.al

	target, ok := al.resolveSteeringTarget(msg)
	if !ok {
		// Non-routable message (e.g. system) stays synchronous so it preserves
		// the historical ordering guarantee and does not enter session steering.
		if al.processMessageSync(ctx, msg) {
			al.ackInboundMessage(ctx, msg)
		} else {
			al.releaseInboundMessage(context.Background(), msg, ctx.Err())
		}
		return
	}
	if err := al.cancelInteractionForControlMessage(ctx, msg, target); err != nil {
		if noticeErr := al.publishInteractionNotice(
			ctx,
			msg,
			target.SessionKey,
			"The pending interaction could not be canceled; please retry.",
		); noticeErr != nil {
			al.releaseInboundMessage(context.Background(), msg, err)
		} else {
			al.ackInboundMessage(ctx, msg)
		}
		return
	}
	if al.shouldHandleInteractionInbound(msg, target) {
		c.handleInteractionInbound(ctx, msg, target)
		return
	}
	if al.hasNonterminalInteraction(target.Agent.Workspace, target.SessionKey) {
		c.deferInteractionInbound(ctx, msg, target)
		return
	}

	claim, activeTarget, claimed := c.claimSession(target)
	if !claimed {
		c.handleBusySession(ctx, msg, activeTarget)
		return
	}

	c.startWorker(ctx, msg, target, claim)
}

func (c *inboundTurnCoordinator) deferInteractionInbound(
	ctx context.Context,
	msg bus.InboundMessage,
	target *inboundDispatchTarget,
) {
	if err := c.enqueueDeferredInteractionInbound(ctx, msg, target); err != nil {
		c.al.releaseInboundMessage(context.Background(), msg, err)
	}
}

func (c *inboundTurnCoordinator) enqueueDeferredInteractionInbound(
	ctx context.Context,
	msg bus.InboundMessage,
	target *inboundDispatchTarget,
) error {
	msg = c.al.prepareInboundMessageForAgent(ctx, msg)
	return c.al.enqueueSteeringMessageWithSender(
		target.runtimeSessionScope(),
		target.Agent.ID,
		msg.SenderID,
		providers.Message{
			Role:           "user",
			Content:        msg.Content,
			Media:          append([]string(nil), msg.Media...),
			InboundSpoolID: msg.SpoolID,
		},
	)
}

func (c *inboundTurnCoordinator) claimSession(
	target *inboundDispatchTarget,
) (*runtimeSessionClaim, *inboundDispatchTarget, bool) {
	al := c.al
	return al.claimRuntimeRouteSession(
		target,
		makePendingTurnID(target.SessionKey, al.turnSeq.Add(1)),
	)
}

func (c *inboundTurnCoordinator) handleBusySession(
	ctx context.Context,
	msg bus.InboundMessage,
	target *inboundDispatchTarget,
) {
	al := c.al
	if target == nil || target.Agent == nil {
		al.releaseInboundMessage(ctx, msg, fmt.Errorf("active session target is unavailable"))
		return
	}
	scope := target.runtimeSessionScope()
	if al.tryHandleStopCommand(ctx, msg, scope, target.Agent.ID) {
		al.ackInboundMessage(ctx, msg)
		return
	}

	msg = al.prepareInboundMessageForAgent(ctx, msg)
	if err := al.enqueueSteeringMessageWithSender(scope, target.Agent.ID, msg.SenderID, providers.Message{
		Role:           "user",
		Content:        msg.Content,
		Media:          append([]string(nil), msg.Media...),
		InboundSpoolID: msg.SpoolID,
	}); err != nil {
		logger.WarnCF("agent", "Failed to enqueue steering message",
			map[string]any{
				"error":       err.Error(),
				"channel":     msg.Channel,
				"chat_id":     msg.ChatID,
				"session_key": scope.sessionKey,
			})
		al.releaseInboundMessage(ctx, msg, err)
	}
}

func (c *inboundTurnCoordinator) startWorker(
	ctx context.Context,
	msg bus.InboundMessage,
	target *inboundDispatchTarget,
	claim *runtimeSessionClaim,
) {
	go c.runWorker(ctx, msg, target, claim)
}

func (c *inboundTurnCoordinator) runWorker(
	ctx context.Context,
	msg bus.InboundMessage,
	target *inboundDispatchTarget,
	claim *runtimeSessionClaim,
) {
	al := c.al
	admittedCtx, releaseCapacity, err := c.acquireTurnCapacity(ctx, target.Agent.ID)
	if err != nil {
		claim.releaseIfOwned()
		al.releaseInboundMessage(context.Background(), msg, err)
		return
	}
	defer releaseCapacity()
	ctx = admittedCtx

	defer claim.releaseIfOwned()
	defer c.recoverWorkerPanic(claim.scope.sessionKey, msg)

	if al.channelManager != nil {
		defer al.channelManager.InvokeTypingStop(msg.Channel, msg.ChatID)
	}

	if al.takePendingStop(claim.scope) {
		c.handlePendingStop(ctx, msg, claim, target)
		return
	}

	turn := al.buildInboundMessageTurnForTarget(ctx, msg, target)
	if al.runInboundTurnWithSteering(ctx, turn) {
		al.ackInboundMessage(ctx, msg)
	} else {
		al.releaseInboundMessage(context.Background(), msg, ctx.Err())
	}
}

func (c *inboundTurnCoordinator) acquireTurnCapacity(
	ctx context.Context,
	agentID string,
) (context.Context, func(), error) {
	for {
		admittedCtx, releaseAdmission, err := c.al.acquireAgentTurn(ctx, agentID)
		if err != nil {
			return ctx, nil, err
		}
		select {
		case c.al.workerSem <- struct{}{}:
			return admittedCtx, func() {
				<-c.al.workerSem
				releaseAdmission()
			}, nil
		default:
			releaseAdmission()
		}

		// Wait for worker progress without retaining the agent admission. The
		// worker token is released immediately and both resources are retried.
		select {
		case c.al.workerSem <- struct{}{}:
			<-c.al.workerSem
		case <-ctx.Done():
			return ctx, nil, ctx.Err()
		}
	}
}

func (c *inboundTurnCoordinator) handlePendingStop(
	ctx context.Context,
	msg bus.InboundMessage,
	claim *runtimeSessionClaim,
	dispatchTarget *inboundDispatchTarget,
) {
	al := c.al
	claim.releaseIfOwned()
	al.ackInboundMessage(ctx, msg)

	target := &continuationTarget{
		SessionKey: claim.scope.sessionKey,
		Channel:    msg.Channel,
		ChatID:     msg.ChatID,
		Workspace:  claim.scope.workspace,
	}
	if dispatchTarget != nil && dispatchTarget.Agent != nil {
		target.AgentID = dispatchTarget.Agent.ID
	}
	continued, continueErr := al.drainQueuedSteeringContinuations(ctx, target)
	if continueErr != nil {
		al.maybePublishErrorWithPolicy(
			ctx,
			msg.Channel,
			msg.ChatID,
			claim.scope.sessionKey,
			continueErr,
			finalResponseAlwaysPublish,
		)
		return
	}
	if continued != "" {
		al.publishResponseWithContextIfNeeded(
			ctx,
			target.Channel,
			target.ChatID,
			target.SessionKey,
			continued,
			&msg.Context,
			finalResponseAlwaysPublish,
		)
	}
}

func (c *inboundTurnCoordinator) recoverWorkerPanic(sessionKey string, msg bus.InboundMessage) {
	if r := recover(); r != nil {
		logger.RecoverPanicNoExit(r)
		logger.ErrorCF("agent", "Worker goroutine panicked",
			map[string]any{
				"session_key": sessionKey,
				"channel":     msg.Channel,
				"chat_id":     msg.ChatID,
				"panic":       fmt.Sprintf("%v", r),
			})
	}
}

func isPendingTurnState(ts *turnState) bool {
	return ts != nil && strings.HasPrefix(ts.turnID, pendingTurnPrefix)
}
