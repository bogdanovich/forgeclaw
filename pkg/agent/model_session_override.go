package agent

import (
	"fmt"
	"strings"

	"github.com/sipeed/picoclaw/pkg/commands"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/state"
)

type effectiveModelBinding struct {
	RouteSessionKey string
	WorkspaceAgent  *AgentInstance
	EffectiveAgent  *AgentInstance
	Override        state.SessionModelOverride
	cleanup         func()
}

func (b effectiveModelBinding) Cleanup() {
	if b.cleanup != nil {
		b.cleanup()
	}
}

func (b effectiveModelBinding) SelectionInfo() commands.ModelSelectionInfo {
	return buildSessionModelSelectionInfo(b.WorkspaceAgent, b.EffectiveAgent, b.Override)
}

func selectionInfoForBinding(
	cfg *config.Config,
	binding effectiveModelBinding,
) commands.ModelSelectionInfo {
	workspaceAgent := binding.WorkspaceAgent
	effectiveAgent := binding.EffectiveAgent
	override := normalizeSessionModelOverride(binding.Override)
	if workspaceAgent == nil {
		workspaceAgent = effectiveAgent
	}
	if workspaceAgent == nil {
		return commands.ModelSelectionInfo{}
	}
	if override.Model != "" && strings.TrimSpace(workspaceAgent.Model) != override.Model {
		overrideView := *workspaceAgent
		overrideView.Model = override.Model
		overrideView.Candidates = resolveModelCandidates(
			cfg,
			cfg.Agents.Defaults.Provider,
			override.Model,
			workspaceAgent.Fallbacks,
		)
		effectiveAgent = &overrideView
	}
	if effectiveAgent == nil {
		effectiveAgent = workspaceAgent
	}
	return buildSessionModelSelectionInfo(workspaceAgent, effectiveAgent, override)
}

func normalizeSessionModelOverride(
	override state.SessionModelOverride,
) state.SessionModelOverride {
	override.Model = strings.TrimSpace(override.Model)
	return override
}

func buildSessionModelSelectionInfo(
	workspaceAgent *AgentInstance,
	effectiveAgent *AgentInstance,
	override state.SessionModelOverride,
) commands.ModelSelectionInfo {
	info := commands.ModelSelectionInfo{}
	if workspaceAgent != nil {
		info.WorkspaceName = workspaceAgent.Model
		info.WorkspaceProvider = resolvedCandidateProvider(workspaceAgent.Candidates, "")
	}
	if effectiveAgent != nil {
		info.EffectiveName = effectiveAgent.Model
		info.EffectiveProvider = resolvedCandidateProvider(effectiveAgent.Candidates, "")
	}
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

func (al *AgentLoop) buildSessionOverrideAgent(
	baseAgent *AgentInstance,
	modelName string,
) (*AgentInstance, func(), error) {
	if baseAgent == nil {
		return nil, nil, fmt.Errorf("agent not initialized")
	}
	cfg := al.GetConfig()
	modelCfg, err := resolvedModelConfig(cfg, modelName, baseAgent.Workspace)
	if err != nil {
		return nil, nil, err
	}

	overrideProvider, _, err := providers.CreateProviderFromConfig(modelCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize model %q: %w", modelName, err)
	}

	overrideCandidates := resolveModelCandidates(
		cfg,
		cfg.Agents.Defaults.Provider,
		modelName,
		baseAgent.Fallbacks,
	)
	if len(overrideCandidates) == 0 {
		if stateful, ok := overrideProvider.(providers.StatefulProvider); ok {
			stateful.Close()
		}
		return nil, nil, fmt.Errorf(
			"model %q did not resolve to any provider candidates",
			modelName,
		)
	}

	overrideAgent := *baseAgent
	overrideAgent.Model = modelName
	overrideAgent.Provider = overrideProvider
	overrideAgent.Candidates = overrideCandidates
	overrideAgent.ThinkingLevel = parseThinkingLevel(modelCfg.ThinkingLevel)
	overrideAgent.ThinkingLevelConfigured = isConfiguredThinkingLevel(modelCfg.ThinkingLevel)
	overrideAgent.Router = nil
	overrideAgent.LightCandidates = nil
	overrideAgent.LightProvider = nil
	overrideAgent.CandidateProviders = cloneCandidateProviderMap(baseAgent.CandidateProviders)
	existingKeys := make(map[string]struct{}, len(overrideAgent.CandidateProviders))
	for key := range overrideAgent.CandidateProviders {
		existingKeys[key] = struct{}{}
	}
	if overrideAgent.CandidateProviders == nil {
		overrideAgent.CandidateProviders = make(map[string]providers.LLMProvider)
	}
	populateCandidateProvidersFromNames(
		cfg,
		baseAgent.Workspace,
		append([]string{modelName}, baseAgent.Fallbacks...),
		overrideAgent.CandidateProviders,
	)
	if len(overrideCandidates) > 0 {
		overrideAgent.CandidateProviders[overrideCandidates[0].StableKey()] = overrideProvider
	}

	cleanup := func() {
		overrideClosed := false
		for key, provider := range overrideAgent.CandidateProviders {
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

	return &overrideAgent, cleanup, nil
}

func (al *AgentLoop) bindEffectiveModel(
	routeSessionKey string,
	baseAgent *AgentInstance,
) effectiveModelBinding {
	binding := effectiveModelBinding{
		RouteSessionKey: strings.TrimSpace(routeSessionKey),
		WorkspaceAgent:  baseAgent,
		EffectiveAgent:  baseAgent,
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

	overrideAgent, cleanup, err := al.buildSessionOverrideAgent(baseAgent, override.Model)
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

	binding.EffectiveAgent = overrideAgent
	binding.cleanup = cleanup
	return binding
}
