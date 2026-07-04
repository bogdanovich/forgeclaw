package agent

import (
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/providers"
)

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
