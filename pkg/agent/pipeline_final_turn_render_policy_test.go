package agent

import (
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/config"
)

type testFinalTurnRenderPolicy struct {
	called bool
	result bool
}

func (p *testFinalTurnRenderPolicy) shouldFinalizeAfterToolLoop(*turnExecution, *LLMIterationState) bool {
	p.called = true
	return p.result
}

func TestPipelineShouldFinalizeAfterToolLoop_UsesInjectedPolicy(t *testing.T) {
	policy := &testFinalTurnRenderPolicy{result: true}
	pipeline := &Pipeline{Config: PipelineConfigServices{FinalTurnRender: policy}}

	if !pipeline.shouldFinalizeAfterToolLoop(&turnExecution{}, newLLMIterationState(1)) {
		t.Fatal("shouldFinalizeAfterToolLoop() = false, want injected policy result")
	}
	if !policy.called {
		t.Fatal("injected final-turn render policy was not called")
	}
}

func TestPipelineShouldFinalizeAfterToolLoop_FallsBackToConfig(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.FinalTurnRenderMode = "llm"
	pipeline := &Pipeline{Cfg: cfg}
	exec := &turnExecution{
		sawSteering: true,
	}

	if !pipeline.shouldFinalizeAfterToolLoop(exec, newLLMIterationState(1)) {
		t.Fatal("shouldFinalizeAfterToolLoop() = false, want true")
	}
}

func TestPipelineShouldFinalizeAfterToolLoop_DefaultsWhenConfigMissing(t *testing.T) {
	pipeline := &Pipeline{}
	exec := &turnExecution{
		sawSteering: true,
	}

	if pipeline.shouldFinalizeAfterToolLoop(exec, newLLMIterationState(1)) {
		t.Fatal("shouldFinalizeAfterToolLoop() = true, want false without config")
	}
}
