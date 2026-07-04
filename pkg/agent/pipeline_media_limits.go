package agent

import (
	"github.com/sipeed/picoclaw/pkg/config"
)

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
	if p.MediaLimits == nil {
		return newConfigMediaLimitsProvider(p.Cfg).maxMediaSize()
	}
	return p.MediaLimits.maxMediaSize()
}
