package agent

import (
	"testing"

	"github.com/sipeed/picoclaw/pkg/config"
)

type testMediaLimitsProvider struct {
	called bool
	size   int
}

func (p *testMediaLimitsProvider) maxMediaSize() int {
	p.called = true
	return p.size
}

func TestPipelineMaxMediaSize_UsesInjectedProvider(t *testing.T) {
	limits := &testMediaLimitsProvider{size: 1234}
	pipeline := &Pipeline{MediaLimits: limits}

	if got := pipeline.maxMediaSize(); got != 1234 {
		t.Fatalf("maxMediaSize() = %d, want 1234", got)
	}
	if !limits.called {
		t.Fatal("injected media limits provider was not called")
	}
}

func TestPipelineMaxMediaSize_FallsBackToConfig(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.MaxMediaSize = 5678
	pipeline := &Pipeline{Cfg: cfg}

	if got := pipeline.maxMediaSize(); got != 5678 {
		t.Fatalf("maxMediaSize() = %d, want 5678", got)
	}
}

func TestPipelineMaxMediaSize_DefaultsWhenConfigMissing(t *testing.T) {
	pipeline := &Pipeline{}

	if got := pipeline.maxMediaSize(); got != config.DefaultMaxMediaSize {
		t.Fatalf("maxMediaSize() = %d, want %d", got, config.DefaultMaxMediaSize)
	}
}
