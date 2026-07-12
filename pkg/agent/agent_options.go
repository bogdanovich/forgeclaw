package agent

import runtimeevents "github.com/sipeed/picoclaw/pkg/events"

// AgentLoopOption configures an AgentLoop at construction time.
type AgentLoopOption func(*AgentLoop)

// WithRuntimeEvents injects the runtime event bus used for new observation APIs.
//
// The injected bus is treated as externally owned and will not be closed by
// AgentLoop.Close. Passing nil leaves the default owned runtime bus enabled.
func WithRuntimeEvents(bus runtimeevents.Bus) AgentLoopOption {
	return func(al *AgentLoop) {
		if bus == nil {
			return
		}
		al.runtimeEvents = bus
		al.ownsRuntimeEvents = false
	}
}

// WithIsolatedToolBootstrap prevents shared production tools and their state
// managers from being constructed. Callers must provide an explicit tool
// allowlist and register every permitted tool after construction.
func WithIsolatedToolBootstrap() AgentLoopOption {
	return func(al *AgentLoop) {
		al.isolatedToolBootstrap = true
	}
}
