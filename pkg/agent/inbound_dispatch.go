package agent

import (
	"context"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/session"
)

type inboundMessageTurn struct {
	Message      bus.InboundMessage
	Agent        *AgentInstance
	Options      processOptions
	ScopeKey     string
	SessionKey   string
	ModelBinding effectiveModelBinding
}

func (t inboundMessageTurn) Cleanup() {
	t.ModelBinding.Cleanup()
}

func (t inboundMessageTurn) resetMessageToolRound() {
	if t.Agent == nil {
		return
	}
	tool, ok := t.Agent.Tools.Get("message")
	if !ok {
		return
	}
	resetter, ok := tool.(interface{ ResetSentInRound(sessionKey string) })
	if !ok {
		return
	}
	resetter.ResetSentInRound(t.SessionKey)
}

func (al *AgentLoop) buildInboundMessageTurn(
	ctx context.Context,
	msg bus.InboundMessage,
) (inboundMessageTurn, error) {
	msg = al.prepareInboundMessageForAgent(ctx, msg)
	if msg.Channel == "system" {
		return inboundMessageTurn{Message: msg}, nil
	}

	route, agent, routeErr := al.resolveMessageRoute(msg)
	if routeErr != nil {
		return inboundMessageTurn{}, routeErr
	}

	allocation := al.allocateRouteSession(route, msg)
	scopeKey := al.resolveEffectiveSessionKey(allocation.SessionKey, msg.SessionKey)
	sessionKey := scopeKey
	modelBinding := al.bindEffectiveModel(allocation.SessionKey, agent)

	opts := processOptions{
		Dispatch: DispatchRequest{
			RouteSessionKey: allocation.SessionKey,
			SessionKey:      sessionKey,
			SessionAliases: buildSessionAliases(
				sessionKey,
				sessionAliasCandidates(
					allocation.SessionKey,
					sessionKey,
					allocation.SessionAliases,
					msg.SessionKey,
				)...),
			InboundContext: cloneInboundContext(&msg.Context),
			RouteResult:    cloneResolvedRoute(&route),
			SessionScope:   session.CloneScope(&allocation.Scope),
			UserMessage:    msg.Content,
			Media:          append([]string(nil), msg.Media...),
		},
		ModelBinding:            modelBinding,
		SenderID:                msg.SenderID,
		SenderDisplayName:       msg.Sender.DisplayName,
		DefaultResponse:         defaultResponse,
		EnableSummary:           true,
		SendResponse:            false,
		AllowInterimPicoPublish: true,
	}

	return inboundMessageTurn{
		Message:      msg,
		Agent:        agent,
		Options:      opts,
		ScopeKey:     scopeKey,
		SessionKey:   sessionKey,
		ModelBinding: modelBinding,
	}, nil
}
