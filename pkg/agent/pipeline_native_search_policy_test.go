package agent

import (
	"testing"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/providers"
)

type testNativeSearchPolicy struct {
	called bool
	result bool
}

func (p *testNativeSearchPolicy) useNativeSearch(
	config.EffectiveTurnProfile,
	providers.LLMProvider,
) bool {
	p.called = true
	return p.result
}

func TestPipelineNativeSearchEnabled_UsesInjectedPolicy(t *testing.T) {
	policy := &testNativeSearchPolicy{result: true}
	pipeline := &Pipeline{Config: PipelineConfigServices{NativeSearch: policy}}

	if !pipeline.nativeSearchEnabled(config.EffectiveTurnProfile{}, &plainProvider{}) {
		t.Fatal("nativeSearchEnabled() = false, want injected policy result")
	}
	if !policy.called {
		t.Fatal("injected native search policy was not called")
	}
}

func TestPipelineNativeSearchEnabled_FallsBackToConfig(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Tools.Web.Enabled = true
	cfg.Tools.Web.PreferNative = true
	pipeline := &Pipeline{Cfg: cfg}

	if !pipeline.nativeSearchEnabled(
		config.EffectiveTurnProfile{},
		&nativeSearchProvider{supported: true},
	) {
		t.Fatal("nativeSearchEnabled() = false, want true")
	}
}
