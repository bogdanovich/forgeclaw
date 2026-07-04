package agent

import (
	"testing"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/providers"
)

type testPipelineModelResolution struct {
	candidatesCalled bool
	activeCfgCalled  bool
	candidates       []providers.FallbackCandidate
	activeCfg        *config.ModelConfig
}

func (r *testPipelineModelResolution) modelCandidates(
	string,
	[]string,
) []providers.FallbackCandidate {
	r.candidatesCalled = true
	return append([]providers.FallbackCandidate(nil), r.candidates...)
}

func (r *testPipelineModelResolution) activeModelConfig(
	string,
	[]providers.FallbackCandidate,
	string,
) *config.ModelConfig {
	r.activeCfgCalled = true
	return r.activeCfg
}

func TestPipelineModelCandidates_UsesInjectedResolver(t *testing.T) {
	resolver := &testPipelineModelResolution{
		candidates: []providers.FallbackCandidate{{
			Provider: "openrouter",
			Model:    "kimi",
		}},
	}
	pipeline := &Pipeline{Config: pipelineConfigServices{ModelResolution: resolver}}

	got := pipeline.modelCandidates("ignored", nil)
	if len(got) != 1 || got[0].Provider != "openrouter" || got[0].Model != "kimi" {
		t.Fatalf("modelCandidates() = %#v, want injected candidate", got)
	}
	if !resolver.candidatesCalled {
		t.Fatal("injected model resolver was not called for candidates")
	}
}

func TestPipelineActiveModelConfig_UsesInjectedResolver(t *testing.T) {
	want := &config.ModelConfig{ModelName: "injected"}
	resolver := &testPipelineModelResolution{activeCfg: want}
	pipeline := &Pipeline{Config: pipelineConfigServices{ModelResolution: resolver}}

	if got := pipeline.activeModelConfig("/workspace", nil, "ignored"); got != want {
		t.Fatalf("activeModelConfig() = %#v, want injected config", got)
	}
	if !resolver.activeCfgCalled {
		t.Fatal("injected model resolver was not called for active model config")
	}
}

func TestPipelineModelResolution_FallsBackToConfig(t *testing.T) {
	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{Provider: "openrouter"},
		},
		ModelList: []*config.ModelConfig{{
			ModelName: "kimi",
			Provider:  "openrouter",
			Model:     "moonshotai/kimi-k2",
		}},
	}
	pipeline := &Pipeline{Cfg: cfg}

	candidates := pipeline.modelCandidates("kimi", nil)
	if len(candidates) != 1 {
		t.Fatalf("modelCandidates() len = %d, want 1", len(candidates))
	}
	if candidates[0].Provider != "openrouter" || candidates[0].Model != "moonshotai/kimi-k2" {
		t.Fatalf("modelCandidates()[0] = %#v, want openrouter/moonshotai/kimi-k2", candidates[0])
	}

	active := pipeline.activeModelConfig("/workspace", candidates, "kimi")
	if active == nil {
		t.Fatal("activeModelConfig() = nil, want model config")
	}
	if active.ModelName != "kimi" || active.Workspace != "/workspace" {
		t.Fatalf("activeModelConfig() = %#v, want kimi config with workspace", active)
	}
}
