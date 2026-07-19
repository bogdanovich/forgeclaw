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
	if al.hasNonterminalInteraction(target.Agent.Workspace, target.SessionKey) {
		c.handleInteractionInbound(ctx, msg, target)
		return
	}

	claim, activeTarget, claimed := c.claimSession(target)
	if !claimed {
		c.handleBusySession(ctx, msg, activeTarget.SessionKey, activeTarget.Agent.ID)
		return
	}

	c.startWorker(ctx, msg, target, claim)
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
	sessionKey string,
	agentID string,
) {
	al := c.al
	if al.tryHandleStopCommand(ctx, msg, sessionKey) {
		al.ackInboundMessage(ctx, msg)
		return
	}

	msg = al.prepareInboundMessageForAgent(ctx, msg)
	if err := al.enqueueSteeringMessageWithSender(sessionKey, agentID, msg.SenderID, providers.Message{
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
				"session_key": sessionKey,
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
	if !c.acquireWorker(ctx, msg, claim) {
		return
	}

	defer claim.releaseIfOwned()
	defer c.recoverWorkerPanic(claim.sessionKey, msg)
	defer func() { <-al.workerSem }()

	if al.channelManager != nil {
		defer al.channelManager.InvokeTypingStop(msg.Channel, msg.ChatID)
	}

	if al.takePendingStop(claim.sessionKey) {
		c.handlePendingStop(ctx, msg, claim)
		return
	}

	turn := al.buildInboundMessageTurnForTarget(ctx, msg, target)
	if al.runInboundTurnWithSteering(ctx, turn) {
		al.ackInboundMessage(ctx, msg)
	} else {
		al.releaseInboundMessage(context.Background(), msg, ctx.Err())
	}
}

func (c *inboundTurnCoordinator) acquireWorker(
	ctx context.Context,
	msg bus.InboundMessage,
	claim *runtimeSessionClaim,
) bool {
	select {
	case c.al.workerSem <- struct{}{}:
		return true
	case <-ctx.Done():
		claim.releaseIfOwned()
		c.al.releaseInboundMessage(context.Background(), msg, ctx.Err())
		return false
	}
}

func (c *inboundTurnCoordinator) handlePendingStop(
	ctx context.Context,
	msg bus.InboundMessage,
	claim *runtimeSessionClaim,
) {
	al := c.al
	claim.releaseIfOwned()
	al.ackInboundMessage(ctx, msg)

	target := &continuationTarget{
		SessionKey: claim.sessionKey,
		Channel:    msg.Channel,
		ChatID:     msg.ChatID,
	}
	continued, continueErr := al.drainQueuedSteeringContinuations(ctx, target)
	if continueErr != nil {
		al.maybePublishErrorWithPolicy(
			ctx,
			msg.Channel,
			msg.ChatID,
			claim.sessionKey,
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
