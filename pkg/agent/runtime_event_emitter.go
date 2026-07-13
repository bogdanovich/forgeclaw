package agent

import runtimeevents "github.com/sipeed/picoclaw/pkg/events"

type agentRuntimeEventEmitter struct {
	events runtimeevents.Bus
}

func (al *AgentLoop) runtimeEventEmitter() *agentRuntimeEventEmitter {
	if al == nil {
		return nil
	}
	return &agentRuntimeEventEmitter{
		events: al.runtimeEvents,
	}
}

func (e *agentRuntimeEventEmitter) emitEvent(kind runtimeevents.Kind, meta HookMeta, payload any) {
	clonedMeta := cloneHookMeta(meta)
	eventCtx := cloneTurnContext(clonedMeta.turnContext)
	evt := runtimeevents.Event{
		Kind:        kind,
		Source:      runtimeevents.Source{Component: "agent", Name: clonedMeta.AgentID},
		Scope:       runtimeScopeFromHookMeta(clonedMeta, eventCtx),
		Correlation: runtimeCorrelationFromHookMeta(clonedMeta),
		Severity:    runtimeSeverityForAgentEvent(kind, payload),
		Payload:     payload,
		Attrs:       runtimeAttrsFromHookMeta(clonedMeta),
	}

	e.publishRuntimeEvent(evt)
}

func (e *agentRuntimeEventEmitter) publishRuntimeEvent(evt runtimeevents.Event) {
	if e == nil || e.events == nil {
		return
	}

	e.events.PublishNonBlocking(evt)
}
