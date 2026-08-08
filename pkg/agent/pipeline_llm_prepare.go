// MintClaw - Ultra-lightweight personal AI agent

package agent

import (
	"context"
	"strings"

	runtimeevents "github.com/bogdanovich/mintclaw/pkg/events"
	"github.com/bogdanovich/mintclaw/pkg/logger"
	"github.com/bogdanovich/mintclaw/pkg/providers"
)

func (p *Pipeline) prepareLLMRequest(
	turnCtx context.Context,
	ts *turnState,
	exec *turnExecution,
	llm *LLMIterationState,
) (llmStageResult, error) {
	iteration := llm.iteration
	if iteration > 1 {
		exec.messages = resolveMediaRefs(exec.messages, p.Context.MediaResolver, p.maxMediaSize())
		usedVisionOverride, err := p.Context.ModelExecution.maybeApplyVisionExecutionState(ts.agent, exec)
		if err != nil {
			return llmStageResult{}, err
		}
		if usedVisionOverride {
			logger.InfoCF(
				"agent",
				"Switched turn to vision override model after media resolution",
				map[string]any{
					"agent_id":         ts.agent.ID,
					"iteration":        iteration,
					"vision_model":     exec.model.activeModel,
					"vision_route":     exec.model.visionRoute,
					"messages_count":   len(exec.messages),
					"active_candidate": len(exec.model.activeCandidates),
				},
			)
		}
	}

	llm.gracefulTerminal, _ = ts.gracefulInterruptRequested()
	llm.providerToolDefs = filterToolsByTurnProfile(ts.agent.Tools.ToProviderDefs(), ts.profile)
	llm.useNativeSearch = p.nativeSearchEnabled(ts.profile, exec.model.activeProvider)
	if llm.useNativeSearch {
		filtered := make([]providers.ToolDefinition, 0, len(llm.providerToolDefs))
		for _, toolDefinition := range llm.providerToolDefs {
			if toolDefinition.Function.Name != "web_search" {
				filtered = append(filtered, toolDefinition)
			}
		}
		llm.providerToolDefs = filtered
	}

	llm.callMessages = exec.messages
	if llm.gracefulTerminal {
		llm.callMessages = append(
			append([]providers.Message(nil), exec.messages...),
			ts.interruptHintMessage(),
		)
		llm.providerToolDefs = nil
		ts.markGracefulTerminalUsed()
	}

	llm.llmOpts = map[string]any{
		"max_tokens":       ts.agent.MaxTokens,
		"temperature":      ts.agent.Temperature,
		"prompt_cache_key": ts.agent.ID,
	}
	if llm.useNativeSearch {
		llm.llmOpts["native_search"] = true
	}
	execution := ts.model.ExecutionState()
	applyTurnThinkingOptions(exec, llm, execution, exec.model.activeProvider, true)
	llm.llmModel = exec.model.activeModel

	if p.Interaction.Hooks != nil {
		request, decision := p.Interaction.Hooks.BeforeLLM(turnCtx, &LLMHookRequest{
			Meta:             ts.eventMeta("runTurn", "turn.llm.request"),
			Context:          cloneTurnContext(ts.turnCtx),
			Model:            llm.llmModel,
			Messages:         llm.callMessages,
			Tools:            llm.providerToolDefs,
			Options:          llm.llmOpts,
			GracefulTerminal: llm.gracefulTerminal,
		})
		switch decision.normalizedAction() {
		case HookActionContinue, HookActionModify:
			if request != nil {
				previousModel := llm.llmModel
				llm.llmModel = request.Model
				llm.callMessages = request.Messages
				llm.providerToolDefs = filterToolsByTurnProfile(request.Tools, ts.profile)
				llm.llmOpts = request.Options
				nativeSearchAllowed := llm.useNativeSearch &&
					turnProfileToolAllowed(ts.profile, "web_search")
				if !nativeSearchAllowed {
					delete(llm.llmOpts, "native_search")
				}
				if strings.TrimSpace(llm.llmModel) != "" && llm.llmModel != previousModel {
					p.applyBeforeLLMModelRewrite(ts, exec, llm)
					applyTurnThinkingOptions(exec, llm, execution, exec.model.activeProvider, true)
				}
			}
		case HookActionAbortTurn:
			cancelConfiguredStreamingLLM(turnCtx, llm)
			return completeLLMStage(LLMCallOutcome{Control: ControlBreak, AbortCause: TurnAbortHook}), nil
		case HookActionHardAbort:
			cancelConfiguredStreamingLLM(turnCtx, llm)
			_ = ts.requestHardAbort()
			return completeLLMStage(LLMCallOutcome{Control: ControlBreak, AbortCause: TurnAbortHard}), nil
		}
	}

	p.emitEvent(
		runtimeevents.KindAgentLLMRequest,
		ts.eventMeta("runTurn", "turn.llm.request"),
		LLMRequestPayload{
			Provider:           primaryCandidateProvider(exec.model.activeCandidates),
			Model:              llm.llmModel,
			PromptHash:         safeJSONHash(traceCaptureSettingsFromConfig(p.Cfg), llm.callMessages),
			MessagesCount:      len(llm.callMessages),
			ToolsCount:         len(llm.providerToolDefs),
			MaxTokens:          ts.agent.MaxTokens,
			Temperature:        ts.agent.Temperature,
			DiagnosticMessages: diagnosticMessagesPreview(p.Cfg, llm.callMessages),
		},
	)

	logger.DebugCF("agent", "LLM request", map[string]any{
		"agent_id":          ts.agent.ID,
		"iteration":         iteration,
		"model":             llm.llmModel,
		"messages_count":    len(llm.callMessages),
		"tools_count":       len(llm.providerToolDefs),
		"max_tokens":        ts.agent.MaxTokens,
		"temperature":       ts.agent.Temperature,
		"system_prompt_len": len(llm.callMessages[0].Content),
	})
	logger.DebugCF("agent", "Full LLM request", map[string]any{
		"iteration":     iteration,
		"messages_json": formatMessagesForLog(llm.callMessages),
		"tools_json":    formatToolsForLog(llm.providerToolDefs),
	})

	return llmStageResult{}, nil
}
