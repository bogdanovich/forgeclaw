package agent

import (
	"fmt"
	"strings"

	"github.com/sipeed/picoclaw/pkg/commands"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/routing"
	"github.com/sipeed/picoclaw/pkg/state"
)

type effectiveModelBinding struct {
	RouteSessionKey string
	WorkspaceAgent  *AgentInstance
	Execution       effectiveExecutionState
	Override        state.SessionModelOverride
	cleanup         func()
}

type effectiveExecutionState struct {
	AgentID                 string
	Model                   string
	Provider                providers.LLMProvider
	Candidates              []providers.FallbackCandidate
	CandidateProviders      map[string]providers.LLMProvider
	Router                  *routing.Router
	LightCandidates         []providers.FallbackCandidate
	LightProvider           providers.LLMProvider
	ThinkingLevel           ThinkingLevel
	ThinkingLevelConfigured bool
}

type modelSelectionInspection struct {
	WorkspaceAgent *AgentInstance
	Execution      effectiveExecutionState
	Override       state.SessionModelOverride
}

func (b effectiveModelBinding) Cleanup() {
	if b.cleanup != nil {
		b.cleanup()
	}
}

func effectiveExecutionStateForAgent(agent *AgentInstance) effectiveExecutionState {
	if agent == nil {
		return effectiveExecutionState{}
	}
	return effectiveExecutionState{
		AgentID:                 agent.ID,
		Model:                   agent.Model,
		Provider:                agent.Provider,
		Candidates:              append([]providers.FallbackCandidate(nil), agent.Candidates...),
		CandidateProviders:      cloneCandidateProviderMap(agent.CandidateProviders),
		Router:                  agent.Router,
		LightCandidates:         append([]providers.FallbackCandidate(nil), agent.LightCandidates...),
		LightProvider:           agent.LightProvider,
		ThinkingLevel:           agent.ThinkingLevel,
		ThinkingLevelConfigured: agent.ThinkingLevelConfigured,
	}
}

func (b effectiveModelBinding) ExecutionState() effectiveExecutionState {
	if b.Execution.Model != "" || b.Execution.Provider != nil || len(b.Execution.Candidates) > 0 {
		return b.Execution
	}
	return effectiveExecutionStateForAgent(b.WorkspaceAgent)
}

func selectionInfoForInspection(
	cfg *config.Config,
	inspection modelSelectionInspection,
) commands.ModelSelectionInfo {
	return buildSessionModelSelectionInfo(cfg, inspection)
}

func buildSessionModelSelectionInfo(
	cfg *config.Config,
	inspection modelSelectionInspection,
) commands.ModelSelectionInfo {
	workspaceAgent := inspection.WorkspaceAgent
	execution := inspection.Execution
	override := normalizeSessionModelOverride(inspection.Override)
	if workspaceAgent == nil {
		return commands.ModelSelectionInfo{}
	}
	if override.Model != "" && strings.TrimSpace(workspaceAgent.Model) != override.Model {
		execution.Model = override.Model
		execution.Candidates = resolveModelCandidates(
			cfg,
			cfg.Agents.Defaults.Provider,
			override.Model,
			workspaceAgent.Fallbacks,
		)
	}
	return buildSessionModelSelectionInfoValues(workspaceAgent, execution, override)
}

func normalizeSessionModelOverride(
	override state.SessionModelOverride,
) state.SessionModelOverride {
	override.Model = strings.TrimSpace(override.Model)
	return override
}

func buildSessionModelSelectionInfoValues(
	workspaceAgent *AgentInstance,
	execution effectiveExecutionState,
	override state.SessionModelOverride,
) commands.ModelSelectionInfo {
	info := commands.ModelSelectionInfo{}
	if workspaceAgent != nil {
		info.WorkspaceName = workspaceAgent.Model
		info.WorkspaceProvider = resolvedCandidateProvider(workspaceAgent.Candidates, "")
	}
	info.EffectiveName = resolvedCandidateModelName(
		execution.Candidates,
		strings.TrimSpace(execution.Model),
	)
	info.EffectiveProvider = resolvedCandidateProvider(execution.Candidates, "")
	override = normalizeSessionModelOverride(override)
	if override.Model != "" {
		info.SessionOverride = override.Model
		info.HasSessionOverride = true
	}
	return info
}

func canonicalModelOverrideValue(cfg *config.Config, raw string) (string, error) {
	modelCfg, err := resolvedSwitchableModelConfig(cfg, strings.TrimSpace(raw))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(modelCfg.ModelName), nil
}

func cloneCandidateProviderMap(
	in map[string]providers.LLMProvider,
) map[string]providers.LLMProvider {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]providers.LLMProvider, len(in))
	for key, provider := range in {
		out[key] = provider
	}
	return out
}

func (al *AgentLoop) buildExecutionStateForModel(
	baseAgent *AgentInstance,
	modelName string,
	fallbacks []string,
) (effectiveExecutionState, func(), error) {
	if baseAgent == nil {
		return effectiveExecutionState{}, nil, fmt.Errorf("agent not initialized")
	}
	cfg := al.GetConfig()
	modelCfg, err := resolvedModelConfig(cfg, modelName, baseAgent.Workspace)
	if err != nil {
		return effectiveExecutionState{}, nil, err
	}

	overrideProvider, _, err := providers.CreateProviderFromConfig(modelCfg)
	if err != nil {
		return effectiveExecutionState{}, nil, fmt.Errorf("failed to initialize model %q: %w", modelName, err)
	}

	overrideCandidates := resolveModelCandidates(
		cfg,
		cfg.Agents.Defaults.Provider,
		modelName,
		fallbacks,
	)
	if len(overrideCandidates) == 0 {
		if stateful, ok := overrideProvider.(providers.StatefulProvider); ok {
			stateful.Close()
		}
		return effectiveExecutionState{}, nil, fmt.Errorf(
			"model %q did not resolve to any provider candidates",
			modelName,
		)
	}

	candidateProviders := cloneCandidateProviderMap(baseAgent.CandidateProviders)
	existingKeys := make(map[string]struct{}, len(candidateProviders))
	for key := range candidateProviders {
		existingKeys[key] = struct{}{}
	}
	if candidateProviders == nil {
		candidateProviders = make(map[string]providers.LLMProvider)
	}
	populateCandidateProvidersFromNames(
		cfg,
		baseAgent.Workspace,
		append([]string{modelName}, fallbacks...),
		candidateProviders,
	)
	if len(overrideCandidates) > 0 {
		candidateProviders[overrideCandidates[0].StableKey()] = overrideProvider
	}

	cleanup := func() {
		overrideClosed := false
		for key, provider := range candidateProviders {
			if _, exists := existingKeys[key]; exists || provider == nil {
				continue
			}
			if provider == overrideProvider {
				overrideClosed = true
			}
			if stateful, ok := provider.(providers.StatefulProvider); ok {
				stateful.Close()
			}
		}
		if !overrideClosed {
			if stateful, ok := overrideProvider.(providers.StatefulProvider); ok {
				stateful.Close()
			}
		}
	}

	return effectiveExecutionState{
		AgentID:                 baseAgent.ID,
		Model:                   modelName,
		Provider:                overrideProvider,
		Candidates:              overrideCandidates,
		CandidateProviders:      candidateProviders,
		ThinkingLevel:           parseThinkingLevel(modelCfg.ThinkingLevel),
		ThinkingLevelConfigured: isConfiguredThinkingLevel(modelCfg.ThinkingLevel),
	}, cleanup, nil
}

func (al *AgentLoop) buildSessionOverrideExecution(
	baseAgent *AgentInstance,
	modelName string,
) (effectiveExecutionState, func(), error) {
	return al.buildExecutionStateForModel(baseAgent, modelName, baseAgent.Fallbacks)
}

func (al *AgentLoop) bindEffectiveModel(
	routeSessionKey string,
	baseAgent *AgentInstance,
) effectiveModelBinding {
	binding := effectiveModelBinding{
		RouteSessionKey: strings.TrimSpace(routeSessionKey),
		WorkspaceAgent:  baseAgent,
	}
	if binding.RouteSessionKey == "" || baseAgent == nil {
		return binding
	}

	override, ok := al.getSessionModelOverride(binding.RouteSessionKey)
	if !ok {
		return binding
	}
	override = normalizeSessionModelOverride(override)
	if override.Model == "" {
		return binding
	}

	binding.Override = override
	if override.Model == baseAgent.Model {
		return binding
	}

	execution, cleanup, err := al.buildSessionOverrideExecution(baseAgent, override.Model)
	if err != nil {
		logger.WarnCF("agent", "Clearing invalid session model override",
			map[string]any{
				"agent_id":       baseAgent.ID,
				"session_key":    binding.RouteSessionKey,
				"override":       override.Model,
				"override_error": err.Error(),
			})
		_ = al.clearSessionModelOverride(binding.RouteSessionKey)
		binding.Override = state.SessionModelOverride{}
		return binding
	}

	binding.Execution = execution
	binding.cleanup = cleanup
	return binding
}

func (al *AgentLoop) buildModelSelectionInspection(
	binding effectiveModelBinding,
) modelSelectionInspection {
	inspection := modelSelectionInspection{
		WorkspaceAgent: binding.WorkspaceAgent,
		Execution:      binding.ExecutionState(),
		Override:       normalizeSessionModelOverride(binding.Override),
	}
	workspaceAgent := inspection.WorkspaceAgent
	if workspaceAgent == nil {
		return inspection
	}

	workspaceSelection := effectiveExecutionStateForAgent(workspaceAgent)
	if binding.RouteSessionKey != "" {
		override, _ := al.getSessionModelOverride(binding.RouteSessionKey)
		inspection.Override = normalizeSessionModelOverride(override)
	}
	if inspection.Override.Model == "" {
		executionDecision := al.previewStickyAutoFallback(modelSelectionDecision{
			selectedCandidates: append([]providers.FallbackCandidate(nil), workspaceSelection.Candidates...),
			activeCandidates:   append([]providers.FallbackCandidate(nil), workspaceSelection.Candidates...),
			model:              resolvedCandidateModel(workspaceSelection.Candidates, workspaceSelection.Model),
		}, binding.RouteSessionKey)
		workspaceSelection.Candidates = executionDecision.activeCandidates
		workspaceSelection.Model = executionDecision.model
		inspection.Execution = workspaceSelection
	}
	return inspection
}
