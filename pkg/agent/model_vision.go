package agent

import (
	"strings"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/providers"
)

const (
	visionRouteSameModel     = "same_model"
	visionRouteModelOverride = "model_override"
)

func resolveVisionOverrideModel(mc *config.ModelConfig) (string, []string, bool) {
	if mc == nil || mc.Capabilities == nil || mc.Capabilities.Vision == nil {
		return "", nil, false
	}
	primary := strings.TrimSpace(mc.Capabilities.Vision.Model)
	if primary == "" {
		return "", nil, false
	}
	fallbacks := append([]string(nil), mc.Capabilities.Vision.Fallbacks...)
	return primary, fallbacks, true
}

func (al *AgentLoop) maybeBuildVisionExecutionState(
	baseAgent *AgentInstance,
	execution effectiveExecutionState,
	messages []providers.Message,
) (effectiveExecutionState, func(), string, bool, error) {
	if baseAgent == nil || !hasMediaRefs(messages) {
		return execution, nil, visionRouteSameModel, false, nil
	}

	activeModelConfig := resolveActiveModelConfig(
		al.GetConfig(),
		baseAgent.Workspace,
		execution.Candidates,
		execution.Model,
		al.GetConfig().Agents.Defaults.Provider,
	)
	primary, fallbacks, ok := resolveVisionOverrideModel(activeModelConfig)
	if !ok {
		return execution, nil, visionRouteSameModel, false, nil
	}

	visionExecution, cleanup, err := al.buildExecutionStateForModel(baseAgent, primary, fallbacks)
	if err != nil {
		return effectiveExecutionState{}, nil, "", false, err
	}
	return visionExecution, cleanup, visionRouteModelOverride, true, nil
}
