package agent

import (
	"fmt"
	"strings"

	"github.com/sipeed/picoclaw/pkg/commands"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/state"
)

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

	cleanup := func() {
		for key, provider := range overrideAgent.CandidateProviders {
			if _, exists := existingKeys[key]; exists || provider == nil {
				continue
			}
			if stateful, ok := provider.(providers.StatefulProvider); ok {
				stateful.Close()
			}
		}
		if stateful, ok := overrideProvider.(providers.StatefulProvider); ok {
			stateful.Close()
		}
	}

	return &overrideAgent, cleanup, nil
}
