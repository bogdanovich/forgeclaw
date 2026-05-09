package agent

import (
	"path"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/tools"
)

func agentAllowsTool(agent *AgentInstance, toolName string) bool {
	if agent == nil {
		return true
	}
	if !toolAllowedByPolicy(agent.ToolPolicy, toolName) {
		return false
	}
	return toolAllowedByConfig(agent.ID, agent.ToolFilter, toolName)
}

func toolAllowedByConfig(_ string, cfg *config.AgentToolsConfig, toolName string) bool {
	if cfg == nil {
		return true
	}

	return toolAllowedByPolicy(cfg, toolName)
}

func toolAllowedByPolicy(cfg *config.AgentToolsConfig, toolName string) bool {
	if cfg == nil {
		return true
	}

	allowed := true
	if cfg.Allow != nil {
		allowed = matchesAnyGlob(toolName, cfg.Allow)
	}
	if !allowed {
		return false
	}
	if len(cfg.Deny) > 0 && matchesAnyGlob(toolName, cfg.Deny) {
		return false
	}
	return true
}

func registerToolWithPolicies(
	registry *tools.ToolRegistry,
	tool tools.Tool,
	policies ...*config.AgentToolsConfig,
) bool {
	if registry == nil || tool == nil {
		return false
	}
	for _, policy := range policies {
		if !toolAllowedByPolicy(policy, tool.Name()) {
			return false
		}
	}
	registry.Register(tool)
	return true
}

func matchesAnyGlob(name string, patterns []string) bool {
	for _, pattern := range patterns {
		if pattern == "" {
			continue
		}
		if ok, err := path.Match(pattern, name); err == nil && ok {
			return true
		}
	}
	return false
}

func registerToolIfAllowed(agent *AgentInstance, tool tools.Tool) bool {
	if tool == nil {
		return false
	}
	if !agentAllowsTool(agent, tool.Name()) {
		logger.DebugCF("agent", "Skipped tool by agent filter", map[string]any{
			"agent_id": agent.ID,
			"tool":     tool.Name(),
		})
		return false
	}
	agent.Tools.Register(tool)
	return true
}

func registerHiddenToolIfAllowed(agent *AgentInstance, tool tools.Tool) bool {
	if tool == nil {
		return false
	}
	if !agentAllowsTool(agent, tool.Name()) {
		logger.DebugCF("agent", "Skipped hidden tool by agent filter", map[string]any{
			"agent_id": agent.ID,
			"tool":     tool.Name(),
		})
		return false
	}
	agent.Tools.RegisterHidden(tool)
	return true
}
