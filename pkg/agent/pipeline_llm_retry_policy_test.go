package agent

import (
	"testing"

	"github.com/sipeed/picoclaw/pkg/config"
)

type testLLMRetryPolicy struct {
	called      bool
	maxRetries  int
	backoffSecs int
}

func (p *testLLMRetryPolicy) llmRetrySettings() (int, int) {
	p.called = true
	return p.maxRetries, p.backoffSecs
}

func TestPipelineLLMRetrySettings_UsesInjectedPolicy(t *testing.T) {
	policy := &testLLMRetryPolicy{maxRetries: 5, backoffSecs: 7}
	pipeline := &Pipeline{Config: PipelineConfigServices{LLMRetry: policy}}

	maxRetries, backoffSecs := pipeline.llmRetrySettings()
	if maxRetries != 5 || backoffSecs != 7 {
		t.Fatalf("llmRetrySettings() = (%d, %d), want (5, 7)", maxRetries, backoffSecs)
	}
	if !policy.called {
		t.Fatal("injected LLM retry policy was not called")
	}
}

func TestPipelineLLMRetrySettings_FallsBackToConfig(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.MaxLLMRetries = 4
	cfg.Agents.Defaults.LLMRetryBackoffSecs = 6
	pipeline := &Pipeline{Cfg: cfg}

	maxRetries, backoffSecs := pipeline.llmRetrySettings()
	if maxRetries != 4 || backoffSecs != 6 {
		t.Fatalf("llmRetrySettings() = (%d, %d), want (4, 6)", maxRetries, backoffSecs)
	}
}

func TestPipelineLLMRetrySettings_DefaultsInvalidConfig(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.MaxLLMRetries = 0
	cfg.Agents.Defaults.LLMRetryBackoffSecs = 0
	pipeline := &Pipeline{Cfg: cfg}

	maxRetries, backoffSecs := pipeline.llmRetrySettings()
	if maxRetries != 2 || backoffSecs != 2 {
		t.Fatalf("llmRetrySettings() = (%d, %d), want (2, 2)", maxRetries, backoffSecs)
	}
}
