package agent

import "github.com/sipeed/picoclaw/pkg/config"

type configFinalTurnRenderPolicy struct {
	cfg *config.Config
}

func newConfigFinalTurnRenderPolicy(cfg *config.Config) finalTurnRenderPolicy {
	return configFinalTurnRenderPolicy{cfg: cfg}
}

func (p configFinalTurnRenderPolicy) shouldFinalizeAfterToolLoop(exec *turnExecution) bool {
	return shouldFinalizeAfterToolLoopWithRenderConfig(p.cfg, exec)
}

func (p *Pipeline) shouldFinalizeAfterToolLoop(exec *turnExecution) bool {
	if p == nil {
		return false
	}
	if p.FinalTurnRender == nil {
		return newConfigFinalTurnRenderPolicy(p.Cfg).shouldFinalizeAfterToolLoop(exec)
	}
	return p.FinalTurnRender.shouldFinalizeAfterToolLoop(exec)
}
