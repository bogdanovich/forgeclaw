package agent

import "github.com/sipeed/picoclaw/pkg/config"

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
	if p.ToolContentFilter == nil {
		return newConfigToolContentFilter(p.Cfg).filterToolContentForLLM(content)
	}
	return p.ToolContentFilter.filterToolContentForLLM(content)
}

func (p *Pipeline) filterPendingResultForLLM(content string) string {
	if p == nil || p.Cfg == nil {
		return content
	}
	return p.Cfg.FilterSensitiveData(content)
}
