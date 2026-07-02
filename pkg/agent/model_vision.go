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

func (m *modelExecutionManager) maybeBuildVisionExecutionState(
	baseAgent *AgentInstance,
	execution effectiveExecutionState,
	messages []providers.Message,
) (effectiveExecutionState, func(), string, bool, error) {
	if baseAgent == nil || !hasMediaRefs(messages) {
		return execution, nil, visionRouteSameModel, false, nil
	}

	cfg := m.config()
	activeModelConfig := resolveActiveModelConfig(
		cfg,
		baseAgent.Workspace,
		execution.Candidates,
		execution.Model,
		cfg.Agents.Defaults.Provider,
	)
	primary, fallbacks, ok := resolveVisionOverrideModel(activeModelConfig)
	if !ok {
		return execution, nil, visionRouteSameModel, false, nil
	}

	visionExecution, cleanup, err := m.buildExecutionStateForModel(baseAgent, primary, fallbacks)
	if err != nil {
		return effectiveExecutionState{}, nil, "", false, err
	}
	return visionExecution, cleanup, visionRouteModelOverride, true, nil
}

func (al *AgentLoop) maybeBuildVisionExecutionState(
	baseAgent *AgentInstance,
	execution effectiveExecutionState,
	messages []providers.Message,
) (effectiveExecutionState, func(), string, bool, error) {
	manager := al.modelExecutionManager()
	if manager == nil {
		return execution, nil, visionRouteSameModel, false, nil
	}
	return manager.maybeBuildVisionExecutionState(baseAgent, execution, messages)
}

func (m *modelExecutionManager) maybeApplyVisionExecutionState(
	baseAgent *AgentInstance,
	exec *turnExecution,
) (bool, error) {
	if baseAgent == nil || exec == nil {
		return false, nil
	}
	if exec.model.visionRoute != visionRouteSameModel || !hasMediaRefs(exec.messages) {
		return false, nil
	}

	cfg := m.config()
	activeModelConfig := exec.model.activeModelConfig
	if activeModelConfig == nil {
		activeModelConfig = resolveActiveModelConfig(
			cfg,
			baseAgent.Workspace,
			exec.model.activeCandidates,
			exec.model.activeModel,
			cfg.Agents.Defaults.Provider,
		)
	}
	primary, fallbacks, ok := resolveVisionOverrideModel(activeModelConfig)
	if !ok {
		return false, nil
	}

	visionExecution, cleanup, err := m.buildExecutionStateForModel(baseAgent, primary, fallbacks)
	if err != nil {
		return false, err
	}

	if exec.model.cleanup != nil {
		exec.model.cleanup()
	}

	exec.model.selectedCandidates = append([]providers.FallbackCandidate(nil), visionExecution.Candidates...)
	exec.model.activeCandidates = append([]providers.FallbackCandidate(nil), visionExecution.Candidates...)
	exec.model.activeModel = resolvedCandidateModel(visionExecution.Candidates, visionExecution.Model)
	exec.model.activeModelConfig = resolveActiveModelConfig(
		cfg,
		baseAgent.Workspace,
		visionExecution.Candidates,
		visionExecution.Model,
		cfg.Agents.Defaults.Provider,
	)
	exec.model.llmModelName = resolvedCandidateModelName(
		visionExecution.Candidates,
		strings.TrimSpace(visionExecution.Model),
	)
	exec.model.activeProvider = visionExecution.Provider
	exec.model.candidateProviders = visionExecution.CandidateProviders
	exec.model.cleanup = cleanup
	exec.model.usedLight = false
	exec.model.autoFallback = false
	exec.model.visionRoute = visionRouteModelOverride

	return true, nil
}

func (al *AgentLoop) maybeApplyVisionExecutionState(
	baseAgent *AgentInstance,
	exec *turnExecution,
) (bool, error) {
	manager := al.modelExecutionManager()
	if manager == nil {
		return false, nil
	}
	return manager.maybeApplyVisionExecutionState(baseAgent, exec)
}
