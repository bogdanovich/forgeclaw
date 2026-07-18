package agent

import (
	"context"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/routing"
	"github.com/sipeed/picoclaw/pkg/session"
)

type inboundDispatchTarget struct {
	Route      routing.ResolvedRoute
	Agent      *AgentInstance
	Allocation session.Allocation
	SessionKey string
}

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
	if msg.Channel == "system" {
		msg = al.prepareInboundMessageForAgent(ctx, msg)
		return inboundMessageTurn{Message: msg}, nil
	}

	target, err := al.resolveInboundDispatchTarget(msg)
	if err != nil {
		return inboundMessageTurn{}, err
	}
	return al.buildInboundMessageTurnForTarget(ctx, msg, target), nil
}

func (al *AgentLoop) resolveInboundDispatchTarget(msg bus.InboundMessage) (*inboundDispatchTarget, error) {
	route, agent, routeErr := al.resolveMessageRoute(msg)
	if routeErr != nil {
		return nil, routeErr
	}

	allocation := al.allocateRouteSession(route, msg)
	allocation, routeErr = al.applySessionLifecycle(allocation, route.SessionPolicy.Lifecycle)
	if routeErr != nil {
		return nil, routeErr
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
	}, nil
}

func (al *AgentLoop) buildInboundMessageTurnForTarget(
	ctx context.Context,
	msg bus.InboundMessage,
	target *inboundDispatchTarget,
) inboundMessageTurn {
	msg = al.prepareInboundMessageForAgent(ctx, msg)
	allocation := target.Allocation
	sessionKey := target.SessionKey
	modelBinding := al.bindEffectiveModel(allocation.RouteScopeKey, target.Agent)

	opts := processOptions{
		Dispatch: DispatchRequest{
			RouteSessionKey: allocation.RouteScopeKey,
			BaseSessionKey:  allocation.SessionKey,
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
			RouteResult:    cloneResolvedRoute(&target.Route),
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
		ExpectFinalDelivery:     true,
		AllowInterimPicoPublish: true,
	}

	return inboundMessageTurn{
		Message:      msg,
		Agent:        target.Agent,
		Options:      opts,
		ScopeKey:     sessionKey,
		SessionKey:   sessionKey,
		ModelBinding: modelBinding,
	}
}
