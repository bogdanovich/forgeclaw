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

type inboundSessionClaim struct {
	coordinator *inboundTurnCoordinator
	sessionKey  string
	placeholder *turnState
}

func newInboundTurnCoordinator(al *AgentLoop) *inboundTurnCoordinator {
	return &inboundTurnCoordinator{al: al}
}

func (c *inboundTurnCoordinator) handleInbound(ctx context.Context, msg bus.InboundMessage) {
	al := c.al

	sessionKey, agentID, ok := al.resolveSteeringTarget(msg)
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

	claim, claimed := c.claimSession(sessionKey)
	if !claimed {
		c.handleBusySession(ctx, msg, sessionKey, agentID)
		return
	}

	c.startWorker(ctx, msg, claim)
}

func (c *inboundTurnCoordinator) claimSession(sessionKey string) (*inboundSessionClaim, bool) {
	al := c.al
	placeholder := &turnState{
		turnID: makePendingTurnID(sessionKey, al.turnSeq.Add(1)),
		phase:  TurnPhaseSetup,
	}
	if _, loaded := al.activeTurnStates.LoadOrStore(sessionKey, placeholder); loaded {
		return nil, false
	}
	return &inboundSessionClaim{
		coordinator: c,
		sessionKey:  sessionKey,
		placeholder: placeholder,
	}, true
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
	claim *inboundSessionClaim,
) {
	go c.runWorker(ctx, msg, claim)
}

func (c *inboundTurnCoordinator) runWorker(
	ctx context.Context,
	msg bus.InboundMessage,
	claim *inboundSessionClaim,
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

	if al.runTurnWithSteering(ctx, msg) {
		al.ackInboundMessage(ctx, msg)
	} else {
		al.releaseInboundMessage(context.Background(), msg, ctx.Err())
	}
}

func (c *inboundTurnCoordinator) acquireWorker(
	ctx context.Context,
	msg bus.InboundMessage,
	claim *inboundSessionClaim,
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
	claim *inboundSessionClaim,
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

func (claim *inboundSessionClaim) releaseIfOwned() {
	if claim == nil || claim.placeholder == nil || claim.coordinator == nil {
		return
	}
	if actual, ok := claim.coordinator.al.activeTurnStates.Load(claim.sessionKey); ok && actual == claim.placeholder {
		claim.coordinator.al.activeTurnStates.Delete(claim.sessionKey)
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
