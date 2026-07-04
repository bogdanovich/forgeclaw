package agent

import (
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/providers"
)

type configPipelinePromptBuilder struct {
	cfg *config.Config
}

func newConfigPipelinePromptBuilder(cfg *config.Config) pipelinePromptBuilder {
	return configPipelinePromptBuilder{cfg: cfg}
}

func (b configPipelinePromptBuilder) buildTurnMessages(
	ts *turnState,
	history []providers.Message,
	summary string,
	currentMessage string,
	media []string,
	activeSkills []string,
) []providers.Message {
	if ts == nil || ts.agent == nil || ts.agent.ContextBuilder == nil {
		return nil
	}
	req := promptBuildRequestForTurn(ts, history, summary, currentMessage, media, b.cfg)
	req.ActiveSkills = append([]string(nil), activeSkills...)
	return ts.agent.ContextBuilder.BuildMessagesFromPrompt(req)
}

func (p *Pipeline) buildTurnMessages(
	ts *turnState,
	history []providers.Message,
	summary string,
	currentMessage string,
	media []string,
	activeSkills []string,
) []providers.Message {
	if p == nil {
		return nil
	}
	if p.Config.PromptBuilder == nil {
		return newConfigPipelinePromptBuilder(p.Cfg).
			buildTurnMessages(ts, history, summary, currentMessage, media, activeSkills)
	}
	return p.Config.PromptBuilder.buildTurnMessages(
		ts,
		history,
		summary,
		currentMessage,
		media,
		activeSkills,
	)
}
