package agent

import (
	"context"
	"fmt"
	"strings"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
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

func (p *Pipeline) callFallbackCandidateWithCapabilities(
	ctx context.Context,
	ts *turnState,
	exec *turnExecution,
	candidate providers.FallbackCandidate,
	messages []providers.Message,
	toolDefs []providers.ToolDefinition,
) (*providers.LLMResponse, error) {
	return p.callCandidateWithCapabilities(
		ctx,
		ts,
		exec,
		candidate,
		exec.model.candidateProviders,
		exec.model.activeProvider,
		messages,
		toolDefs,
		nil,
	)
}

func (p *Pipeline) callCandidateWithCapabilities(
	ctx context.Context,
	ts *turnState,
	exec *turnExecution,
	candidate providers.FallbackCandidate,
	candidateProviders map[string]providers.LLMProvider,
	activeProvider providers.LLMProvider,
	messages []providers.Message,
	toolDefs []providers.ToolDefinition,
	routePath map[string]struct{},
) (*providers.LLMResponse, error) {
	candidateKey := candidate.StableKey()
	if _, found := routePath[candidateKey]; found {
		return nil, fmt.Errorf("vision capability route cycle at %q", candidateKey)
	}
	nextPath := cloneVisionRoutePath(routePath)
	nextPath[candidateKey] = struct{}{}

	candidateConfig := p.activeModelConfig(
		ts.agent.Workspace,
		[]providers.FallbackCandidate{candidate},
		candidate.Model,
	)
	visionModel, visionFallbacks, useVision := resolveVisionOverrideModel(candidateConfig)
	if !hasMediaRefs(messages) || !useVision {
		return p.callResolvedFallbackCandidate(
			ctx,
			ts,
			exec,
			candidate,
			candidateProviders,
			activeProvider,
			messages,
			toolDefs,
		)
	}
	if p.Context.ModelExecution == nil {
		return nil, fmt.Errorf("vision override %q cannot be resolved", visionModel)
	}

	visionExecution, cleanup, err := p.Context.ModelExecution.buildExecutionStateForModel(
		ts.agent,
		visionModel,
		visionFallbacks,
	)
	if err != nil {
		return nil, err
	}
	if cleanup != nil {
		defer cleanup()
	}

	logger.InfoCF("agent", "Routed fallback candidate through vision override", map[string]any{
		"agent_id":     ts.agent.ID,
		"source_model": candidate.Model,
		"vision_model": visionExecution.Model,
	})

	callVisionCandidate := func(
		callCtx context.Context,
		visionCandidate providers.FallbackCandidate,
	) (*providers.LLMResponse, error) {
		return p.callCandidateWithCapabilities(
			callCtx,
			ts,
			exec,
			visionCandidate,
			visionExecution.CandidateProviders,
			visionExecution.Provider,
			messages,
			toolDefs,
			nextPath,
		)
	}
	if len(visionExecution.Candidates) > 1 && p.Interaction.Fallback != nil {
		result, fallbackErr := p.Interaction.Fallback.ExecuteCandidate(
			ctx,
			visionExecution.Candidates,
			callVisionCandidate,
		)
		if fallbackErr != nil {
			return nil, fallbackErr
		}
		return result.Response, nil
	}
	if len(visionExecution.Candidates) == 0 {
		return nil, fmt.Errorf("vision override %q resolved no candidates", visionModel)
	}
	return callVisionCandidate(ctx, visionExecution.Candidates[0])
}

func cloneVisionRoutePath(routePath map[string]struct{}) map[string]struct{} {
	result := make(map[string]struct{}, len(routePath)+1)
	for key := range routePath {
		result[key] = struct{}{}
	}
	return result
}

func (p *Pipeline) callResolvedFallbackCandidate(
	ctx context.Context,
	ts *turnState,
	exec *turnExecution,
	candidate providers.FallbackCandidate,
	candidateProviders map[string]providers.LLMProvider,
	activeProvider providers.LLMProvider,
	messages []providers.Message,
	toolDefs []providers.ToolDefinition,
) (*providers.LLMResponse, error) {
	candidateProvider, err := providerForFallbackCandidate(
		candidateProviders,
		activeProvider,
		candidate.Provider,
		candidate.Model,
	)
	if err != nil {
		return nil, err
	}
	callOpts := shallowCloneLLMOptions(exec.llmOpts)
	delete(callOpts, "thinking_level")
	candidateConfig := p.activeModelConfig(
		ts.agent.Workspace,
		[]providers.FallbackCandidate{candidate},
		candidate.Model,
	)
	candidateThinking := thinkingSettingsFromModelConfig(candidateConfig)
	applyThinkingOption(
		callOpts,
		candidateProvider,
		candidateThinking,
		true,
		ts.agent.ID,
	)
	exec.suppressReasoning = shouldSuppressReasoningFor(candidateThinking)
	return candidateProvider.Chat(
		ctx,
		messages,
		toolDefs,
		candidate.Model,
		callOpts,
	)
}
