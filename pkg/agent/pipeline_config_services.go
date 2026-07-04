package agent

import (
	"strings"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
)

type configChannelStreamingProvider struct {
	cfg *config.Config
}

func newConfigChannelStreamingProvider(cfg *config.Config) channelStreamingConfigProvider {
	return configChannelStreamingProvider{cfg: cfg}
}

func (p configChannelStreamingProvider) channelStreamingConfig(channelName string) (config.StreamingConfig, bool) {
	if p.cfg == nil || p.cfg.Channels == nil {
		return config.StreamingConfig{}, false
	}
	ch := p.cfg.Channels[channelName]
	if ch == nil {
		return config.StreamingConfig{}, false
	}
	decoded, err := ch.GetDecoded()
	if err != nil {
		logger.WarnCF("agent", "channel streaming config decode failed", map[string]any{
			"channel": channelName,
			"error":   err.Error(),
		})
		return config.StreamingConfig{}, false
	}
	return streamingConfigFromDecodedSettings(decoded)
}

type configNativeSearchPolicy struct {
	cfg *config.Config
}

func newConfigNativeSearchPolicy(cfg *config.Config) nativeSearchPolicy {
	return configNativeSearchPolicy{cfg: cfg}
}

func (p configNativeSearchPolicy) useNativeSearch(
	profile config.EffectiveTurnProfile,
	provider providers.LLMProvider,
) bool {
	if p.cfg == nil {
		return false
	}
	if !p.cfg.Tools.IsToolEnabled("web") || !p.cfg.Tools.Web.PreferNative {
		return false
	}
	if !turnProfileToolAllowed(profile, "web_search") {
		return false
	}
	nativeProvider, ok := provider.(providers.NativeSearchCapable)
	return ok && nativeProvider.SupportsNativeSearch()
}

func (p *Pipeline) nativeSearchEnabled(
	profile config.EffectiveTurnProfile,
	provider providers.LLMProvider,
) bool {
	if p == nil {
		return false
	}
	if p.Config.NativeSearch == nil {
		return newConfigNativeSearchPolicy(p.Cfg).useNativeSearch(profile, provider)
	}
	return p.Config.NativeSearch.useNativeSearch(profile, provider)
}

type configLLMRetryPolicy struct {
	cfg *config.Config
}

func newConfigLLMRetryPolicy(cfg *config.Config) llmRetryPolicy {
	return configLLMRetryPolicy{cfg: cfg}
}

func (p configLLMRetryPolicy) llmRetrySettings() (int, int) {
	maxRetries := 2
	backoffSecs := 2
	if p.cfg != nil {
		if configuredRetries := p.cfg.Agents.Defaults.MaxLLMRetries; configuredRetries > 0 {
			maxRetries = configuredRetries
		}
		if configuredBackoff := p.cfg.Agents.Defaults.LLMRetryBackoffSecs; configuredBackoff > 0 {
			backoffSecs = configuredBackoff
		}
	}
	return maxRetries, backoffSecs
}

func (p *Pipeline) llmRetrySettings() (int, int) {
	if p == nil {
		return 2, 2
	}
	if p.Config.LLMRetry == nil {
		return newConfigLLMRetryPolicy(p.Cfg).llmRetrySettings()
	}
	return p.Config.LLMRetry.llmRetrySettings()
}

type configMediaLimitsProvider struct {
	cfg *config.Config
}

func newConfigMediaLimitsProvider(cfg *config.Config) mediaLimitsProvider {
	return configMediaLimitsProvider{cfg: cfg}
}

func (p configMediaLimitsProvider) maxMediaSize() int {
	if p.cfg == nil {
		return config.DefaultMaxMediaSize
	}
	return p.cfg.Agents.Defaults.GetMaxMediaSize()
}

func (p *Pipeline) maxMediaSize() int {
	if p == nil {
		return config.DefaultMaxMediaSize
	}
	if p.Config.MediaLimits == nil {
		return newConfigMediaLimitsProvider(p.Cfg).maxMediaSize()
	}
	return p.Config.MediaLimits.maxMediaSize()
}

type configFinalTurnRenderPolicy struct {
	cfg *config.Config
}

func newConfigFinalTurnRenderPolicy(cfg *config.Config) finalTurnRenderPolicy {
	return configFinalTurnRenderPolicy{cfg: cfg}
}

func (p configFinalTurnRenderPolicy) shouldFinalizeAfterToolLoop(exec *turnExecution) bool {
	return shouldFinalizeAfterToolLoopWithRenderConfig(p.cfg, exec)
}

func (p *Pipeline) shouldFinalizeAfterToolLoop(exec *turnExecution) bool {
	if p == nil {
		return false
	}
	if p.Config.FinalTurnRender == nil {
		return newConfigFinalTurnRenderPolicy(p.Cfg).shouldFinalizeAfterToolLoop(exec)
	}
	return p.Config.FinalTurnRender.shouldFinalizeAfterToolLoop(exec)
}

type configToolContentFilter struct {
	cfg *config.Config
}

func newConfigToolContentFilter(cfg *config.Config) toolContentFilter {
	return configToolContentFilter{cfg: cfg}
}

func (f configToolContentFilter) filterToolContentForLLM(content string) string {
	if f.cfg == nil || !f.cfg.Tools.IsFilterSensitiveDataEnabled() {
		return content
	}
	return f.cfg.FilterSensitiveData(content)
}

func (p *Pipeline) filterToolContentForLLM(content string) string {
	if p == nil {
		return content
	}
	if p.Config.ToolContentFilter == nil {
		return newConfigToolContentFilter(p.Cfg).filterToolContentForLLM(content)
	}
	return p.Config.ToolContentFilter.filterToolContentForLLM(content)
}

func (p *Pipeline) filterPendingResultForLLM(content string) string {
	return p.filterToolContentForLLM(content)
}

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
	if p.Config.ModelResolution == nil {
		return newConfigPipelineModelResolution(p.Cfg).modelCandidates(primary, fallbacks)
	}
	return p.Config.ModelResolution.modelCandidates(primary, fallbacks)
}

func (p *Pipeline) activeModelConfig(
	workspace string,
	candidates []providers.FallbackCandidate,
	activeModel string,
) *config.ModelConfig {
	if p == nil {
		return nil
	}
	if p.Config.ModelResolution == nil {
		return newConfigPipelineModelResolution(
			p.Cfg,
		).activeModelConfig(workspace, candidates, activeModel)
	}
	return p.Config.ModelResolution.activeModelConfig(workspace, candidates, activeModel)
}

type configPipelinePromptBuilder struct {
	cfg *config.Config
}

func newConfigPipelinePromptBuilder(cfg *config.Config) pipelinePromptBuilder {
	return configPipelinePromptBuilder{cfg: cfg}
}

func (b configPipelinePromptBuilder) buildTurnMessages(
	ts *turnState,
	history []providers.Message,
	summary string,
	currentMessage string,
	media []string,
	activeSkills []string,
) []providers.Message {
	if ts == nil || ts.agent == nil || ts.agent.ContextBuilder == nil {
		return nil
	}
	req := promptBuildRequestForTurn(ts, history, summary, currentMessage, media, b.cfg)
	req.ActiveSkills = append([]string(nil), activeSkills...)
	return ts.agent.ContextBuilder.BuildMessagesFromPrompt(req)
}

func (p *Pipeline) buildTurnMessages(
	ts *turnState,
	history []providers.Message,
	summary string,
	currentMessage string,
	media []string,
	activeSkills []string,
) []providers.Message {
	if p == nil {
		return nil
	}
	if p.Config.PromptBuilder == nil {
		return newConfigPipelinePromptBuilder(p.Cfg).
			buildTurnMessages(ts, history, summary, currentMessage, media, activeSkills)
	}
	return p.Config.PromptBuilder.buildTurnMessages(
		ts,
		history,
		summary,
		currentMessage,
		media,
		activeSkills,
	)
}
