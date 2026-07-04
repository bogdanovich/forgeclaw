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
	if p == nil || p.ToolContentFilter == nil {
		return content
	}
	return p.ToolContentFilter.filterToolContentForLLM(content)
}
