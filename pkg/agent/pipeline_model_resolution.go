package agent

import (
	"strings"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/providers"
)

type configPipelineModelResolution struct {
	cfg *config.Config
}

func newConfigPipelineModelResolution(cfg *config.Config) pipelineModelResolution {
	return configPipelineModelResolution{cfg: cfg}
}

func (r configPipelineModelResolution) defaultProvider() string {
	provider := "openai"
	if r.cfg != nil {
		if configured := strings.TrimSpace(r.cfg.Agents.Defaults.Provider); configured != "" {
			provider = configured
		}
	}
	return effectiveDefaultProvider(provider)
}

func (r configPipelineModelResolution) modelCandidates(
	primary string,
	fallbacks []string,
) []providers.FallbackCandidate {
	return resolveModelCandidates(r.cfg, r.defaultProvider(), primary, fallbacks)
}

func (r configPipelineModelResolution) activeModelConfig(
	workspace string,
	candidates []providers.FallbackCandidate,
	activeModel string,
) *config.ModelConfig {
	return resolveActiveModelConfig(
		r.cfg,
		workspace,
		candidates,
		activeModel,
		r.defaultProvider(),
	)
}

func (p *Pipeline) modelCandidates(
	primary string,
	fallbacks []string,
) []providers.FallbackCandidate {
	if p == nil {
		return nil
	}
	if p.ModelResolution == nil {
		return newConfigPipelineModelResolution(p.Cfg).modelCandidates(primary, fallbacks)
	}
	return p.ModelResolution.modelCandidates(primary, fallbacks)
}

func (p *Pipeline) activeModelConfig(
	workspace string,
	candidates []providers.FallbackCandidate,
	activeModel string,
) *config.ModelConfig {
	if p == nil {
		return nil
	}
	if p.ModelResolution == nil {
		return newConfigPipelineModelResolution(p.Cfg).activeModelConfig(workspace, candidates, activeModel)
	}
	return p.ModelResolution.activeModelConfig(workspace, candidates, activeModel)
}
