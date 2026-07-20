package agent

import (
	"context"
	"fmt"
	"strings"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/commands"
)

func (al *AgentLoop) tryHandleStopCommand(
	ctx context.Context,
	msg bus.InboundMessage,
	scope runtimeSessionScope,
	agentID string,
) bool {
	cmdName, ok := commands.CommandName(msg.Content)
	if !ok || cmdName != "stop" {
		return false
	}

	result, err := al.stopActiveTurnForScope(scope)

	// This function is only called when loaded=true (another turn already
	// claimed this session). If stopActiveTurnForSession found a pending
	// placeholder but didn't stop it, that placeholder belongs to the other
	// message's worker which hasn't started yet — arm a pending stop so the
	// worker will bail when it checks before running.
	if err == nil && !result.Stopped {
		if ts := al.getActiveTurnState(scope); ts != nil {
			snap := ts.snapshot()
			if strings.HasPrefix(snap.TurnID, pendingTurnPrefix) {
				al.markPendingStop(scope)
				result.Stopped = true
			}
		}
	}

	reply := commands.FormatStopReply(result)
	if err != nil {
		reply = "Failed to stop task: " + err.Error()
	}

	if al.channelManager != nil {
		al.channelManager.InvokeTypingStop(msg.Channel, msg.ChatID)
	}
	al.resetMessageToolRound(scope, agentID)
	al.PublishResponseIfNeeded(ctx, msg.Channel, msg.ChatID, scope.sessionKey, reply)
	return true
}

func (al *AgentLoop) stopActiveTurnForScope(scope runtimeSessionScope) (commands.StopResult, error) {
	if !scope.complete() {
		return commands.StopResult{}, fmt.Errorf("workspace and session key are required")
	}

	result := commands.StopResult{}
	cleared := al.clearSteeringMessagesForScope(scope)
	al.clearPendingSkills(scope)

	ts := al.getActiveTurnState(scope)
	if ts == nil {
		result.Stopped = cleared > 0
		return result, nil
	}

	snap := ts.snapshot()
	result.TaskName = snap.UserMessage

	if strings.HasPrefix(snap.TurnID, pendingTurnPrefix) {
		// A pending placeholder means this session is either idle (our own
		// placeholder from the /stop command) or another message is queued but
		// hasn't started yet. In both cases, we don't arm a pending stop here;
		// the caller (tryHandleStopCommand) handles the "another message queued"
		// case explicitly, since it knows loaded=true.
		return result, nil
	}

	if err := al.hardAbortScope(scope); err != nil {
		if al.getActiveTurnState(scope) == nil {
			result.Stopped = cleared > 0
			return result, nil
		}
		return commands.StopResult{}, err
	}

	result.Stopped = true
	return result, nil
}

func (al *AgentLoop) markPendingStop(scope runtimeSessionScope) {
	if !scope.complete() {
		return
	}
	al.pendingStops.Store(scope, struct{}{})
}

func (al *AgentLoop) takePendingStop(scope runtimeSessionScope) bool {
	if !scope.complete() {
		return false
	}
	_, ok := al.pendingStops.LoadAndDelete(scope)
	return ok
}

func (al *AgentLoop) resetMessageToolRound(scope runtimeSessionScope, agentID string) {
	if !scope.complete() {
		return
	}
	if agent := al.agentForRuntimeScope(scope, agentID); agent != nil {
		if tool, ok := agent.Tools.Get("message"); ok {
			if resetter, ok := tool.(interface{ ResetSentInRound(sessionKey string) }); ok {
				resetter.ResetSentInRound(scope.sessionKey)
			}
		}
	}
}
