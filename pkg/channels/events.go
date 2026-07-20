package channels

import (
	"github.com/sipeed/picoclaw/pkg/bus"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
)

func channelTypeForEvent(m *Manager, channelName string) string {
	if m == nil || m.config == nil {
		return channelName
	}
	if bc := m.config.Channels.Get(channelName); bc != nil && bc.Type != "" {
		return bc.Type
	}
	return channelName
}

func (m *Manager) publishChannelEvent(
	kind runtimeevents.Kind,
	channelName string,
	scope runtimeevents.Scope,
	severity runtimeevents.Severity,
	payload any,
) {
	if m == nil || m.runtimeEvents == nil {
		return
	}
	if scope.Channel == "" {
		scope.Channel = channelName
	}
	m.runtimeEvents.PublishNonBlocking(runtimeevents.Event{
		Kind:     kind,
		Source:   runtimeevents.Source{Component: "channel", Name: channelName},
		Scope:    scope,
		Severity: severity,
		Payload:  payload,
		Attrs:    channelEventAttrs(payload),
	})
}

func channelEventAttrs(payload any) map[string]any {
	switch payload := payload.(type) {
	case ChannelLifecyclePayload:
		attrs := map[string]any{}
		setAttrString(attrs, "type", payload.Type)
		setAttrString(attrs, "error", payload.Error)
		return attrs
	case ChannelOutboundPayload:
		attrs := map[string]any{}
		if len(payload.TraceScopes) > 0 {
			attrs["trace_scopes_count"] = len(payload.TraceScopes)
		}
		if payload.Media {
			attrs["media"] = payload.Media
		}
		if payload.ContentLen > 0 {
			attrs["content_len"] = payload.ContentLen
		}
		if len(payload.MessageIDs) > 0 {
			attrs["message_ids_count"] = len(payload.MessageIDs)
		}
		setAttrString(attrs, "reply_to_message_id", payload.ReplyToMessageID)
		setAttrString(attrs, "error", payload.Error)
		if payload.Retries > 0 {
			attrs["retries"] = payload.Retries
		}
		return attrs
	default:
		return nil
	}
}

func setAttrString(attrs map[string]any, key, value string) {
	if value != "" {
		attrs[key] = value
	}
}

func (m *Manager) publishOutboundSent(
	channelName string,
	msg bus.OutboundMessage,
	messageIDs []string,
) {
	m.publishChannelEvent(
		runtimeevents.KindChannelMessageOutboundSent,
		channelName,
		scopeFromOutboundMessage(msg),
		runtimeevents.SeverityInfo,
		ChannelOutboundPayload{
			TraceScopes:      bus.NormalizeTraceScopes(msg.TraceScopes),
			TraceSettlement:  msg.TraceSettlement,
			ContentLen:       len([]rune(msg.Content)),
			MessageIDs:       append([]string(nil), messageIDs...),
			ReplyToMessageID: msg.ReplyToMessageID,
		},
	)
}

func (m *Manager) publishOutboundQueued(
	channelName string,
	msg bus.OutboundMessage,
) {
	m.publishChannelEvent(
		runtimeevents.KindChannelMessageOutboundQueued,
		channelName,
		scopeFromOutboundMessage(msg),
		runtimeevents.SeverityInfo,
		ChannelOutboundPayload{
			TraceScopes:      bus.NormalizeTraceScopes(msg.TraceScopes),
			ContentLen:       len([]rune(msg.Content)),
			ReplyToMessageID: msg.ReplyToMessageID,
		},
	)
}

func (m *Manager) publishOutboundFailed(
	channelName string,
	msg bus.OutboundMessage,
	err error,
	media bool,
) {
	payload := ChannelOutboundPayload{
		TraceScopes:      bus.NormalizeTraceScopes(msg.TraceScopes),
		TraceSettlement:  msg.TraceSettlement,
		Media:            media,
		ContentLen:       len([]rune(msg.Content)),
		ReplyToMessageID: msg.ReplyToMessageID,
		Retries:          maxRetries,
	}
	if err != nil {
		payload.Error = err.Error()
	}
	m.publishChannelEvent(
		runtimeevents.KindChannelMessageOutboundFailed,
		channelName,
		scopeFromOutboundMessage(msg),
		runtimeevents.SeverityError,
		payload,
	)
}

func (m *Manager) publishOutboundMediaSent(
	channelName string,
	msg bus.OutboundMediaMessage,
	messageIDs []string,
) {
	m.publishChannelEvent(
		runtimeevents.KindChannelMessageOutboundSent,
		channelName,
		scopeFromOutboundMediaMessage(msg),
		runtimeevents.SeverityInfo,
		ChannelOutboundPayload{
			TraceScopes: bus.NormalizeTraceScopes(msg.TraceScopes), TraceSettlement: msg.TraceSettlement,
			Media:      true,
			MessageIDs: append([]string(nil), messageIDs...),
		},
	)
}

func (m *Manager) publishOutboundMediaQueued(
	channelName string,
	msg bus.OutboundMediaMessage,
) {
	m.publishChannelEvent(
		runtimeevents.KindChannelMessageOutboundQueued,
		channelName,
		scopeFromOutboundMediaMessage(msg),
		runtimeevents.SeverityInfo,
		ChannelOutboundPayload{
			TraceScopes: bus.NormalizeTraceScopes(msg.TraceScopes), Media: true,
		},
	)
}

func (m *Manager) publishOutboundMediaFailed(
	channelName string,
	msg bus.OutboundMediaMessage,
	err error,
) {
	payload := ChannelOutboundPayload{
		TraceScopes:     bus.NormalizeTraceScopes(msg.TraceScopes),
		TraceSettlement: msg.TraceSettlement,
		Media:           true,
		Retries:         maxRetries,
	}
	if err != nil {
		payload.Error = err.Error()
	}
	m.publishChannelEvent(
		runtimeevents.KindChannelMessageOutboundFailed,
		channelName,
		scopeFromOutboundMediaMessage(msg),
		runtimeevents.SeverityError,
		payload,
	)
}

func scopeFromOutboundContext(ctx bus.InboundContext) runtimeevents.Scope {
	return runtimeevents.Scope{
		Channel:   ctx.Channel,
		Account:   ctx.Account,
		ChatID:    ctx.ChatID,
		TopicID:   ctx.TopicID,
		SpaceID:   ctx.SpaceID,
		SpaceType: ctx.SpaceType,
		ChatType:  ctx.ChatType,
		SenderID:  ctx.SenderID,
		MessageID: ctx.MessageID,
	}
}

func scopeFromOutboundMessage(msg bus.OutboundMessage) runtimeevents.Scope {
	scope := scopeFromOutboundContext(msg.Context)
	scope.AgentID = msg.AgentID
	scope.SessionKey = msg.SessionKey
	traceScopes := bus.NormalizeTraceScopes(msg.TraceScopes)
	if len(traceScopes) > 0 {
		scope.TraceScope = traceScopes[0]
	}
	return scope
}

func scopeFromOutboundMediaMessage(msg bus.OutboundMediaMessage) runtimeevents.Scope {
	scope := scopeFromOutboundContext(msg.Context)
	scope.AgentID = msg.AgentID
	scope.SessionKey = msg.SessionKey
	traceScopes := bus.NormalizeTraceScopes(msg.TraceScopes)
	if len(traceScopes) > 0 {
		scope.TraceScope = traceScopes[0]
	}
	return scope
}
