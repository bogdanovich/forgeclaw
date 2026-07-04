package agent

import (
	"context"

	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/providers"
)

type turnRuntimeHost interface {
	emitEvent(kind runtimeevents.Kind, meta HookMeta, payload any)
	ackAcceptedSteeringMessages(ctx context.Context, msgs []providers.Message)
	releaseSteeringMessages(ctx context.Context, msgs []providers.Message, cause error)
	abortTurn(ts *turnState) (turnResult, error)
	dequeueSteeringMessagesForTurnWithFallback(scope, senderID string) []providers.Message
	dequeueSteeringMessagesForTurn(scope, senderID string) []providers.Message
	filterSensitiveData(content string) string
	renderFinalTurnReply(ctx context.Context, ts *turnState, exec *turnExecution, fallback string) string
	tryRenderFinalTurnReply(ctx context.Context, ts *turnState, exec *turnExecution, fallback string) (string, bool)
}

func (al *AgentLoop) filterSensitiveData(content string) string {
	if al == nil || al.cfg == nil {
		return content
	}
	return al.cfg.FilterSensitiveData(content)
}

func (al *AgentLoop) renderFinalTurnReply(
	ctx context.Context,
	ts *turnState,
	exec *turnExecution,
	fallback string,
) string {
	return renderFinalTurnReply(ctx, al, ts, exec, fallback)
}

func (al *AgentLoop) tryRenderFinalTurnReply(
	ctx context.Context,
	ts *turnState,
	exec *turnExecution,
	fallback string,
) (string, bool) {
	return tryRenderFinalTurnReply(ctx, al, ts, exec, fallback)
}
