package agent

import "github.com/sipeed/picoclaw/pkg/config"

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
	if p.LLMRetry == nil {
		return newConfigLLMRetryPolicy(p.Cfg).llmRetrySettings()
	}
	return p.LLMRetry.llmRetrySettings()
}
