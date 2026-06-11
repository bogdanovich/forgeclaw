package agent

import (
	"fmt"
	"strings"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/providers"
)

func ensureProtocolModel(model string) string {
	model = strings.TrimSpace(model)
	if model == "" {
		return ""
	}
	if strings.Contains(model, "/") {
		return model
	}
	return "openai/" + model
}

func modelConfigIdentityKey(mc *config.ModelConfig) string {
	if mc == nil {
		return ""
	}
	if name := strings.TrimSpace(mc.ModelName); name != "" {
		return "model_name:" + name
	}
	return ""
}

func effectiveDefaultProvider(defaultProvider string) string {
	defaultProvider = strings.TrimSpace(defaultProvider)
	if defaultProvider == "" {
		return "openai"
	}
	return providers.NormalizeProvider(defaultProvider)
}

func modelProviderAndIDForResolution(defaultProvider string, mc *config.ModelConfig) (provider string, modelID string) {
	if mc == nil {
		return "", ""
	}
	return providers.ExtractProtocol(mc)
}

func cloneModelConfigForResolution(
	defaultProvider string,
	mc *config.ModelConfig,
	workspace string,
) *config.ModelConfig {
	if mc == nil {
		return nil
	}
	clone := *mc
	if clone.Workspace == "" {
		clone.Workspace = workspace
	}
	return &clone
}

func candidateFromModelConfig(
	defaultProvider string,
	mc *config.ModelConfig,
) (providers.FallbackCandidate, bool) {
	if mc == nil {
		return providers.FallbackCandidate{}, false
	}

	protocol, modelID := modelProviderAndIDForResolution(defaultProvider, mc)
	if strings.TrimSpace(modelID) == "" {
		return providers.FallbackCandidate{}, false
	}

	return providers.FallbackCandidate{
		Provider:    protocol,
		Model:       modelID,
		DisplayName: strings.TrimSpace(mc.ModelName),
		RPM:         mc.RPM,
		IdentityKey: modelConfigIdentityKey(mc),
	}, true
}

func lookupModelConfigByRef(cfg *config.Config, raw string, defaultProvider ...string) *config.ModelConfig {
	raw = strings.TrimSpace(raw)
	if raw == "" || cfg == nil {
		return nil
	}

	if mc, err := cfg.GetModelConfig(raw); err == nil && mc != nil && strings.TrimSpace(mc.Model) != "" {
		return mc
	}

	rawRef := providers.ParseModelRef(raw, "")
	rawKey := ""
	if rawRef != nil && strings.TrimSpace(rawRef.Provider) != "" && strings.TrimSpace(rawRef.Model) != "" {
		rawKey = providers.ModelKey(rawRef.Provider, rawRef.Model)
	}

	fallbackProvider := ""
	if len(defaultProvider) > 0 {
		fallbackProvider = effectiveDefaultProvider(defaultProvider[0])
	}
	for i := range cfg.ModelList {
		mc := cfg.ModelList[i]
		if mc == nil {
			continue
		}
		fullModel := strings.TrimSpace(mc.Model)
		if fullModel == "" {
			continue
		}
		protocol, modelID := modelProviderAndIDForResolution(fallbackProvider, mc)
		if fullModel == raw {
			return mc
		}
		if modelID == raw {
			if fallbackProvider == "" || providers.NormalizeProvider(protocol) == fallbackProvider {
				return mc
			}
		}
		if rawKey != "" && providers.ModelKey(protocol, modelID) == rawKey {
			return mc
		}
	}

	return nil
}

func resolveModelCandidate(
	cfg *config.Config,
	defaultProvider string,
	raw string,
) (providers.FallbackCandidate, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return providers.FallbackCandidate{}, false
	}
	defaultProvider = effectiveDefaultProvider(defaultProvider)

	if mc := lookupModelConfigByRef(cfg, raw, defaultProvider); mc != nil {
		return candidateFromModelConfig(defaultProvider, mc)
	}

	ref := providers.ParseModelRef(raw, defaultProvider)
	if ref == nil {
		return providers.FallbackCandidate{}, false
	}

	return providers.FallbackCandidate{
		Provider:    ref.Provider,
		Model:       ref.Model,
		DisplayName: raw,
	}, true
}

func resolveModelCandidates(
	cfg *config.Config,
	defaultProvider string,
	primary string,
	fallbacks []string,
) []providers.FallbackCandidate {
	seen := make(map[string]bool)
	candidates := make([]providers.FallbackCandidate, 0, 1+len(fallbacks))

	addCandidate := func(raw string) {
		candidate, ok := resolveModelCandidate(cfg, defaultProvider, raw)
		if !ok {
			return
		}

		key := candidate.StableKey()
		if seen[key] {
			return
		}
		seen[key] = true
		candidates = append(candidates, candidate)
	}

	addCandidate(primary)
	for _, fallback := range fallbacks {
		addCandidate(fallback)
	}

	return candidates
}

func resolvedCandidateModel(candidates []providers.FallbackCandidate, fallback string) string {
	if len(candidates) > 0 && strings.TrimSpace(candidates[0].Model) != "" {
		return candidates[0].Model
	}
	return fallback
}

func resolvedCandidateProvider(candidates []providers.FallbackCandidate, fallback string) string {
	if len(candidates) > 0 && strings.TrimSpace(candidates[0].Provider) != "" {
		return candidates[0].Provider
	}
	return fallback
}

func resolvedCandidateModelName(candidates []providers.FallbackCandidate, fallback string) string {
	if len(candidates) > 0 {
		if name := modelAliasFromCandidateIdentityKey(candidates[0].IdentityKey); strings.TrimSpace(name) != "" {
			return name
		}
		if displayName := strings.TrimSpace(candidates[0].DisplayName); displayName != "" {
			return displayName
		}
	}
	return strings.TrimSpace(fallback)
}

func resolvedModelConfig(cfg *config.Config, modelName, workspace string) (*config.ModelConfig, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is nil")
	}
	modelCfg, err := resolvedSwitchableModelConfig(cfg, strings.TrimSpace(modelName))
	if err != nil {
		return nil, err
	}

	clone := *modelCfg
	if clone.Workspace == "" {
		clone.Workspace = workspace
	}

	return &clone, nil
}

func resolvedSwitchableModelConfig(cfg *config.Config, modelName string) (*config.ModelConfig, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is nil")
	}
	modelName = strings.TrimSpace(modelName)
	if modelName == "" {
		return nil, fmt.Errorf("model name is required")
	}
	var matches []*config.ModelConfig
	for _, modelCfg := range cfg.ModelList {
		if modelCfg == nil || modelCfg.IsVirtual() || !modelCfg.Enabled {
			continue
		}
		if modelCfg.ModelName == modelName {
			matches = append(matches, modelCfg)
		}
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("model %q not found in enabled model_list", modelName)
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	filtered := *cfg
	filtered.ModelList = matches
	return filtered.GetModelConfig(modelName)
}

func resolveActiveModelConfig(
	cfg *config.Config,
	workspace string,
	candidates []providers.FallbackCandidate,
	activeModel string,
	defaultProvider string,
) *config.ModelConfig {
	if cfg == nil {
		return nil
	}
	defaultProvider = effectiveDefaultProvider(defaultProvider)

	if len(candidates) > 0 {
		candidate := candidates[0]
		identityKey := strings.TrimSpace(candidate.IdentityKey)
		if identityKey != "" {
			for _, mc := range cfg.ModelList {
				if mc == nil || modelConfigIdentityKey(mc) != identityKey {
					continue
				}
				protocol, modelID := modelProviderAndIDForResolution(defaultProvider, mc)
				if providers.ModelKey(protocol, modelID) == providers.ModelKey(candidate.Provider, candidate.Model) {
					return cloneModelConfigForResolution(defaultProvider, mc, workspace)
				}
			}
		}
		for _, mc := range cfg.ModelList {
			if mc == nil {
				continue
			}
			protocol, modelID := modelProviderAndIDForResolution(defaultProvider, mc)
			if providers.ModelKey(protocol, modelID) == providers.ModelKey(candidate.Provider, candidate.Model) {
				return cloneModelConfigForResolution(defaultProvider, mc, workspace)
			}
		}
		return nil
	}

	if mc := lookupModelConfigByRef(cfg, activeModel, defaultProvider); mc != nil {
		return cloneModelConfigForResolution(defaultProvider, mc, workspace)
	}

	return nil
}
