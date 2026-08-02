package agent

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/outbox"
)

type outboundSource struct {
	id      string
	ordinal atomic.Int64
}

type outboundSourceKey struct{}

type durableMessageAdmission struct {
	message     bus.OutboundMessage
	coordinator *outbox.Coordinator
	durable     bool
	dispatch    bool
}

func withOutboundSource(ctx context.Context, sourceID string) context.Context {
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return ctx
	}
	return context.WithValue(ctx, outboundSourceKey{}, &outboundSource{id: sourceID})
}

func hasOutboundSource(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	source, ok := ctx.Value(outboundSourceKey{}).(*outboundSource)
	return ok && source != nil && strings.TrimSpace(source.id) != ""
}

func takeOutboundIdentity(
	ctx context.Context,
	kind outbox.Kind,
	channel, chatID, sessionKey string,
) (outbox.Identity, bool) {
	if ctx == nil {
		return outbox.Identity{}, false
	}
	source, ok := ctx.Value(outboundSourceKey{}).(*outboundSource)
	if !ok || source == nil || strings.TrimSpace(source.id) == "" {
		return outbox.Identity{}, false
	}
	return outbox.Identity{
		SourceID:   source.id,
		Ordinal:    int(source.ordinal.Add(1) - 1),
		Kind:       kind,
		Channel:    channel,
		ChatID:     chatID,
		SessionKey: sessionKey,
	}, true
}

func (al *AgentLoop) SetOutboundOutbox(coordinator *outbox.Coordinator) {
	if al == nil {
		return
	}
	al.mu.Lock()
	al.outboundOutbox = coordinator
	al.mu.Unlock()
}

func (al *AgentLoop) admitDurableMessage(
	ctx context.Context,
	workspace string,
	msg bus.OutboundMessage,
) (durableMessageAdmission, error) {
	result := durableMessageAdmission{message: msg}
	if al == nil {
		return result, nil
	}
	al.mu.RLock()
	coordinator := al.outboundOutbox
	al.mu.RUnlock()
	if coordinator == nil {
		return result, nil
	}
	identity, ok := takeOutboundIdentity(
		ctx,
		outbox.KindMessage,
		msg.Context.Channel,
		msg.Context.ChatID,
		msg.SessionKey,
	)
	if !ok {
		return result, nil
	}
	result.coordinator = coordinator
	result.durable = true
	admission, err := coordinator.AdmitMessage(workspace, identity, msg)
	if err != nil {
		return result, err
	}
	intent := admission.Intent
	if intent.Message == nil {
		return result, errors.New("durable outbound intent has no message payload")
	}
	result.message = *intent.Message
	result.dispatch = admission.Dispatch
	return result, nil
}
