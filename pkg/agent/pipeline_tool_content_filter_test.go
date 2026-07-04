package agent

import (
	"testing"

	"github.com/sipeed/picoclaw/pkg/config"
)

func TestPipelineFilterToolContentForLLM_FallsBackToConfig(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Tools.FilterSensitiveData = true
	cfg.Tools.FilterMinLength = 8
	cfg.ModelList = config.SecureModelList{
		&config.ModelConfig{
			ModelName: "test",
			APIKeys:   config.SimpleSecureStrings("sk-long-key-12345"),
		},
	}
	pipeline := &Pipeline{Cfg: cfg}

	got := pipeline.filterToolContentForLLM("token sk-long-key-12345 should be hidden")
	if got != "token [FILTERED] should be hidden" {
		t.Fatal("expected config fallback to redact sensitive tool content")
	}
}

func TestPipelineFilterPendingResultForLLM_UsesConfigPath(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Tools.FilterSensitiveData = true
	cfg.Tools.FilterMinLength = 8
	cfg.ModelList = config.SecureModelList{
		&config.ModelConfig{
			ModelName: "test",
			APIKeys:   config.SimpleSecureStrings("sk-long-key-12345"),
		},
	}
	pipeline := &Pipeline{Cfg: cfg}

	got := pipeline.filterPendingResultForLLM("pending sk-long-key-12345 result")
	if got != "pending [FILTERED] result" {
		t.Fatal("expected pending result filter to use config redaction path")
	}
}

type testToolContentFilter struct{}

func (testToolContentFilter) filterToolContentForLLM(string) string {
	return "filtered by dependency"
}

func TestPipelineFilterPendingResultForLLM_UsesInjectedFilter(t *testing.T) {
	pipeline := &Pipeline{
		Config: PipelineConfigServices{ToolContentFilter: testToolContentFilter{}},
	}

	got := pipeline.filterPendingResultForLLM("pending content")
	if got != "filtered by dependency" {
		t.Fatalf("got %q, want injected filter result", got)
	}
}
