package agent

import (
	"github.com/sipeed/picoclaw/pkg/config"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
)

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

// WithIsolatedSkillBootstrap restricts every agent's skill loader to its own
// workspace. It prevents tests and evaluations from observing global or
// built-in skills installed in the host process environment.
func WithIsolatedSkillBootstrap() AgentLoopOption {
	return func(al *AgentLoop) {
		if al == nil || al.registry == nil || al.cfg == nil {
			return
		}
		al.isolatedSkillBootstrap = true
		al.isolateSkillRegistry(al.registry, al.cfg)
	}
}

func (al *AgentLoop) isolateSkillRegistry(registry *AgentRegistry, cfg *config.Config) {
	if registry == nil || cfg == nil {
		return
	}
	for _, agentID := range registry.ListAgentIDs() {
		instance, ok := registry.GetAgent(agentID)
		if !ok || instance == nil {
			continue
		}
		instance.ContextBuilder = newContextBuilder(instance.Workspace, "", "").
			WithSplitOnMarker(cfg.Agents.Defaults.SplitOnMarker).
			WithPromptMemoryConfig(cfg.Agents.Defaults.PromptMemory).
			WithAgentDiscovery(instance.ID, registry.ListSpawnableAgents)
	}
}
