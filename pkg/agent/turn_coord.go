// PicoClaw - Ultra-lightweight personal AI agent

package agent

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/config"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
)

func (al *AgentLoop) runTurn(
	ctx context.Context,
	ts *turnState,
	pipeline *Pipeline,
) (result turnResult, err error) {
	ctx, releaseAdmission, err := al.acquireAgentTurn(ctx, ts.agentID)
	if err != nil {
		return turnResult{}, err
	}
	defer releaseAdmission()

	host := turnRuntimeHost(al)
	turnCtx, turnCancel := context.WithCancel(ctx)
	defer turnCancel()
	ts.setTurnCancel(turnCancel)

	// Inject turnState and AgentLoop into context so tools (e.g. spawn) can retrieve them.
	turnCtx = withTurnState(turnCtx, ts)
	turnCtx = WithAgentLoop(turnCtx, al)

	al.registerActiveTurn(ts)
	defer al.clearActiveTurn(ts)
	defer ts.Finish(false)

	if al.takePendingStop(ts.runtimeSessionScope()) {
		_ = ts.requestHardAbort()
	}

	turnStatus := TurnEndStatusCompleted
	defer func() {
		attemptedSkills := ts.attemptedSkillsSnapshot()
		skillContextSnapshots := ts.skillContextSnapshotsSnapshot()
		llmCalls, promptTokens, completionTokens, totalTokens := ts.llmUsageTotals()
		finalSuccessfulPath := []string(nil)
		if turnStatus == TurnEndStatusCompleted {
			if latest := ts.latestSkillContextSnapshot(); len(latest) > 0 {
				finalSuccessfulPath = latest
			} else {
				finalSuccessfulPath = append([]string(nil), attemptedSkills...)
			}
		}
		if al.traceCapture != nil && al.traceCapture.enabled() {
			host.emitEvent(
				runtimeevents.KindAgentContextSnapshot,
				ts.eventMeta("runTurn", "turn.context.snapshot"),
				buildContextSnapshotPayload(al.GetConfig(), ts),
			)
		}
		host.emitEvent(
			runtimeevents.KindAgentTurnEnd,
			ts.eventMeta("runTurn", "turn.end"),
			TurnEndPayload{
				Status:    turnStatus,
				Workspace: ts.workspace,
				DeliveryExpected: turnStatus != TurnEndStatusSuspended &&
					(ts.opts.SendResponse || ts.opts.ExpectFinalDelivery),
				Iterations:            ts.currentIteration(),
				Duration:              time.Since(ts.startedAt),
				LLMCalls:              llmCalls,
				PromptTokens:          promptTokens,
				CompletionTokens:      completionTokens,
				TotalTokens:           totalTokens,
				FinalContentLen:       ts.finalContentLen(),
				UserMessage:           ts.userMessage,
				FinalContent:          ts.finalContentSnapshot(),
				ActiveSkills:          append([]string(nil), ts.activeSkills...),
				AttemptedSkills:       attemptedSkills,
				FinalSuccessfulPath:   finalSuccessfulPath,
				SkillContextSnapshots: skillContextSnapshots,
				ToolKinds:             ts.toolKindsSnapshot(),
				ToolExecutions:        ts.toolExecutionsSnapshot(),
				InteractionID:         result.suspendedInteractionID,
			},
		)
	}()
	defer func() {
		acceptedSteering := ts.acceptedSteeringSnapshot()
		if len(acceptedSteering) == 0 {
			return
		}
		if turnStatus == TurnEndStatusCompleted && err == nil {
			host.ackAcceptedSteeringMessages(ctx, acceptedSteering)
			return
		}
		host.releaseSteeringMessages(context.Background(), acceptedSteering, err)
	}()

	if ts.hardAbortRequested() {
		turnStatus = TurnEndStatusAborted
		return host.abortTurn(ts)
	}

	host.emitEvent(
		runtimeevents.KindAgentTurnStart,
		ts.eventMeta("runTurn", "turn.start"),
		TurnStartPayload{
			UserMessage: ts.userMessage,
			MediaCount:  len(ts.media),
			Workspace:   ts.workspace,
		},
	)

	result, turnStatus, err = pipeline.runTurnLoop(ctx, turnCtx, ts, host)
	return result, err
}

func (al *AgentLoop) abortTurn(ts *turnState) (turnResult, error) {
	return al.turnAbortController().abortTurn(ts)
}

func (al *AgentLoop) resolveContextManager() ContextManager {
	name := al.cfg.Agents.Defaults.ContextManager
	if name == "" || name == "legacy" {
		return &legacyContextManager{al: al}
	}
	factory, ok := lookupContextManager(name)
	if !ok {
		logger.WarnCF("agent", "Unknown context manager, falling back to legacy", map[string]any{
			"name": name,
		})
		return &legacyContextManager{al: al}
	}
	cm, err := factory(al.cfg.Agents.Defaults.ContextManagerConfig, al)
	if err != nil {
		logger.WarnCF(
			"agent",
			"Failed to create context manager, falling back to legacy",
			map[string]any{
				"name":  name,
				"error": err.Error(),
			},
		)
		return &legacyContextManager{al: al}
	}
	return cm
}

func (al *AgentLoop) askSideQuestion(
	ctx context.Context,
	agent *AgentInstance,
	opts *processOptions,
	question string,
) (string, error) {
	if agent == nil {
		return "", fmt.Errorf("askSideQuestion: no agent available for /btw")
	}

	question = strings.TrimSpace(question)
	if question == "" {
		return "", fmt.Errorf("askSideQuestion: %w", fmt.Errorf("Usage: /btw <question>"))
	}

	if opts != nil {
		normalizeProcessOptionsInPlace(opts)
		resolved, err := resolveTurnProfileOptions(al.GetConfig(), *opts)
		if err != nil {
			return "", err
		}
		*opts = resolved
	}

	var media []string
	var channel, chatID, senderID, senderDisplayName string
	if opts != nil {
		media = opts.Media
		channel = opts.Channel
		chatID = opts.ChatID
		senderID = opts.SenderID
		senderDisplayName = opts.SenderDisplayName
	}

	// Build messages with context but WITHOUT adding to session history
	var history []providers.Message
	var summary string
	if opts != nil && !opts.NoHistory {
		sideQuestionOpts := *opts
		sideQuestionOpts.UserMessage = question
		reserveTokens := estimateNonHistoryPromptReserveForProcessOptions(
			al.GetConfig(),
			agent,
			sideQuestionOpts,
			"",
		)
		resp, err := al.contextManager.Assemble(ctx, &AssembleRequest{
			Agent:         agent,
			SessionKey:    opts.SessionKey,
			Budget:        agent.ContextWindow,
			MaxTokens:     agent.MaxTokens,
			ReserveTokens: reserveTokens,
		})
		if err != nil {
			return "", fmt.Errorf("assemble side-question context: %w", err)
		}
		if resp != nil {
			history = resp.History
			summary = resp.Summary
		}
	}

	var promptReq PromptBuildRequest
	if opts == nil {
		promptReq = PromptBuildRequest{
			History:           history,
			Summary:           summary,
			CurrentMessage:    question,
			Media:             append([]string(nil), media...),
			Channel:           channel,
			ChatID:            chatID,
			SenderID:          senderID,
			SenderDisplayName: senderDisplayName,
		}
	} else {
		promptReq = promptBuildRequestForProcessOptions(
			al.GetConfig(),
			agent,
			*opts,
			history,
			summary,
			question,
			media,
		)
	}
	promptReq.SuppressToolUseRule = true
	promptReq.ToolUseFallback = false
	messages := agent.ContextBuilder.BuildMessagesFromPrompt(promptReq)

	maxMediaSize := al.GetConfig().Agents.Defaults.GetMaxMediaSize()
	currentTurnStart := len(messages)
	if strings.TrimSpace(question) != "" || len(media) > 0 {
		currentTurnStart = len(messages) - 1
	}
	messages = resolveMediaRefs(messages, al.mediaStore, maxMediaSize, currentTurnStart)

	execution := effectiveExecutionStateForAgent(agent)
	routeSessionKey := ""
	if opts != nil {
		execution = opts.ModelBinding.ExecutionState()
		routeSessionKey = opts.ModelBinding.RouteSessionKey
	}
	selection := al.selectCandidates(
		execution,
		question,
		messages,
		routeSessionKey,
	)
	activeCandidates, activeModel, usedLight := selection.activeCandidates, selection.model, selection.usedLight
	selectedModelName := resolvedCandidateModelName(activeCandidates, activeModel)
	if selectedModelName == "" {
		selectedModelName = sideQuestionModelName(agent, usedLight)
	}
	visionExecution, visionCleanup, _, usedVisionOverride, err := al.maybeBuildVisionExecutionState(
		agent,
		effectiveExecutionState{
			AgentID:            agent.ID,
			Model:              activeModel,
			Candidates:         append([]providers.FallbackCandidate(nil), activeCandidates...),
			CandidateProviders: cloneCandidateProviderMap(execution.CandidateProviders),
		},
		messages,
	)
	if err != nil {
		return "", err
	}
	defer func() {
		if visionCleanup != nil {
			visionCleanup()
		}
	}()
	if usedVisionOverride {
		activeCandidates = visionExecution.Candidates
		activeModel = resolvedCandidateModel(visionExecution.Candidates, visionExecution.Model)
		selectedModelName = resolvedCandidateModelName(
			visionExecution.Candidates,
			selectedModelName,
		)
	}

	llmOpts := map[string]any{
		"max_tokens":       agent.MaxTokens,
		"temperature":      agent.Temperature,
		"prompt_cache_key": agent.ID + ":btw",
	}

	hookModelChanged := false
	sideSuppressReasoning := false
	callProvider := func(
		ctx context.Context,
		candidate providers.FallbackCandidate,
		model string,
		forceModel bool,
		callMessages []providers.Message,
	) (*providers.LLMResponse, error) {
		baseModelName := selectedModelName
		if forceModel && strings.TrimSpace(model) != "" {
			baseModelName = model
		}
		provider, providerModel, modelCfg, cleanup, err := al.isolatedSideQuestionProvider(
			agent,
			baseModelName,
			candidate,
		)
		if err != nil {
			return nil, err
		}
		defer cleanup()
		if !forceModel || strings.TrimSpace(model) == "" {
			model = providerModel
		}
		callOpts := llmOpts
		settings := thinkingSettingsFromModelConfig(modelCfg)
		sideSuppressReasoning = shouldSuppressReasoningFor(settings)
		if _, exists := callOpts["thinking_level"]; !exists {
			if settings.configured {
				callOpts = shallowCloneLLMOptions(llmOpts)
				applyThinkingOption(callOpts, provider, settings, false, agent.ID)
			}
		}
		return provider.Chat(ctx, callMessages, nil, model, callOpts)
	}

	turnCtx := newTurnContext(nil, nil, nil)
	if opts != nil {
		turnCtx = newTurnContext(
			opts.Dispatch.InboundContext,
			opts.Dispatch.RouteResult,
			opts.Dispatch.SessionScope,
		)
	}
	llmModel := activeModel
	if al.hooks != nil {
		llmReq, decision := al.hooks.BeforeLLM(ctx, &LLMHookRequest{
			Meta: HookMeta{
				Source:      "askSideQuestion",
				TracePath:   "turn.llm.request",
				turnContext: cloneTurnContext(turnCtx),
			},
			Context:          cloneTurnContext(turnCtx),
			Model:            llmModel,
			Messages:         messages,
			Tools:            nil,
			Options:          llmOpts,
			GracefulTerminal: false,
		})
		switch decision.normalizedAction() {
		case HookActionContinue, HookActionModify:
			if llmReq != nil {
				if strings.TrimSpace(llmReq.Model) != "" && llmReq.Model != llmModel {
					hookModelChanged = true
				}
				llmModel = llmReq.Model
				messages = llmReq.Messages
				llmOpts = llmReq.Options
				delete(llmOpts, "native_search")
			}
		case HookActionAbortTurn:
			reason := decision.Reason
			if reason == "" {
				reason = "hook requested turn abort"
			}
			return "", fmt.Errorf("hook aborted turn during before_llm: %s", reason)
		case HookActionHardAbort:
			reason := decision.Reason
			if reason == "" {
				reason = "hook requested turn abort"
			}
			return "", fmt.Errorf("hook aborted turn during before_llm: %s", reason)
		}
	}
	if hookModelChanged {
		// Hook-selected models must not continue through the pre-hook fallback
		// candidate list, otherwise fallback execution would call the original
		// candidate model and silently ignore the hook decision.
		activeCandidates = nil
	}

	callSideLLM := func(callMessages []providers.Message) (*providers.LLMResponse, error) {
		if len(activeCandidates) > 1 && al.fallback != nil {
			fbResult, err := al.fallback.ExecuteCandidate(
				ctx,
				activeCandidates,
				func(ctx context.Context, candidate providers.FallbackCandidate) (*providers.LLMResponse, error) {
					return callProvider(ctx, candidate, candidate.Model, false, callMessages)
				},
			)
			if err != nil {
				return nil, err
			}
			return fbResult.Response, nil
		}

		var candidate providers.FallbackCandidate
		if len(activeCandidates) > 0 {
			candidate = activeCandidates[0]
		}
		return callProvider(ctx, candidate, llmModel, hookModelChanged, callMessages)
	}

	// Retry without media if vision is unsupported
	// Note: Vision retry is only applied to the initial call. If fallback chain
	// is used, vision errors from fallback providers will not trigger retry.
	var resp *providers.LLMResponse
	var callErr error
	resp, callErr = callSideLLM(messages)
	if callErr != nil && hasMediaRefs(messages) && isVisionUnsupportedError(callErr) {
		al.emitEvent(
			runtimeevents.KindAgentLLMRetry,
			HookMeta{
				Source:      "askSideQuestion",
				TracePath:   "turn.llm.retry",
				turnContext: cloneTurnContext(turnCtx),
			},
			LLMRetryPayload{
				Attempt:    1,
				MaxRetries: 1,
				Reason:     "vision_unsupported",
				Error:      callErr.Error(),
				Backoff:    0,
			},
		)
		messagesWithoutMedia := stripMessageMedia(messages)
		resp, callErr = callSideLLM(messagesWithoutMedia)
	}
	if callErr != nil {
		return "", callErr
	}
	if resp == nil {
		return "", nil
	}

	// Apply after_llm hooks
	if al.hooks != nil {
		llmResp, decision := al.hooks.AfterLLM(ctx, &LLMHookResponse{
			Meta: HookMeta{
				Source:      "askSideQuestion",
				TracePath:   "turn.llm.response",
				turnContext: cloneTurnContext(turnCtx),
			},
			Context:  cloneTurnContext(turnCtx),
			Model:    llmModel,
			Response: resp,
		})
		switch decision.normalizedAction() {
		case HookActionContinue, HookActionModify:
			if llmResp != nil && llmResp.Response != nil {
				resp = llmResp.Response
			}
		case HookActionAbortTurn, HookActionHardAbort:
			reason := decision.Reason
			if reason == "" {
				reason = "hook requested turn abort"
			}
			return "", fmt.Errorf("hook aborted turn during after_llm: %s", reason)
		}
	}
	if sideSuppressReasoning {
		resp.Reasoning = ""
		resp.ReasoningContent = ""
		resp.ReasoningDetails = nil
	}

	return sideQuestionResponseContent(resp), nil
}

func (al *AgentLoop) isolatedSideQuestionProvider(
	agent *AgentInstance,
	baseModelName string,
	candidate providers.FallbackCandidate,
) (providers.LLMProvider, string, *config.ModelConfig, func(), error) {
	if agent == nil {
		return nil, "", nil, func() {}, fmt.Errorf(
			"isolatedSideQuestionProvider: no agent available for /btw",
		)
	}

	modelCfg, err := al.sideQuestionModelConfig(agent, baseModelName, candidate)
	if err != nil {
		return nil, "", nil, func() {}, fmt.Errorf("isolatedSideQuestionProvider: %w", err)
	}

	factory := al.providerFactory
	if factory == nil {
		factory = providers.CreateProviderFromConfig
	}
	provider, modelID, err := factory(modelCfg)
	if err != nil {
		return nil, "", nil, func() {}, fmt.Errorf("isolatedSideQuestionProvider: %w", err)
	}

	cleanup := func() {
		closeProviderIfStateful(provider)
	}
	return provider, modelID, modelCfg, cleanup, nil
}

func (al *AgentLoop) sideQuestionModelConfig(
	agent *AgentInstance,
	baseModelName string,
	candidate providers.FallbackCandidate,
) (*config.ModelConfig, error) {
	if agent == nil {
		return nil, fmt.Errorf("sideQuestionModelConfig: no agent available for /btw")
	}

	if name := modelAliasFromCandidateIdentityKey(candidate.IdentityKey); name != "" {
		modelCfg, err := resolvedModelConfig(al.GetConfig(), name, agent.Workspace)
		if err == nil {
			return modelCfg, nil
		}
		// Fallback: create a minimal config if lookup fails
	}

	// Older identity keys used provider/model; keep resolving those by model.
	if name := modelNameFromIdentityKey(candidate.IdentityKey); name != "" {
		modelCfg, err := resolvedModelConfig(al.GetConfig(), name, agent.Workspace)
		if err == nil {
			return modelCfg, nil
		}
		// Fallback: create a minimal config if lookup fails
	}

	if candidate.Provider != "" && candidate.Model != "" {
		candidateRef := providers.NormalizeProvider(candidate.Provider) + "/" + candidate.Model
		if modelCfg, err := resolvedModelConfig(al.GetConfig(), candidateRef, agent.Workspace); err == nil {
			return modelCfg, nil
		}
		return &config.ModelConfig{
			ModelName: candidateRef,
			Model:     candidateRef,
			Workspace: agent.Workspace,
		}, nil
	}

	// Otherwise, clean up the base model name and use it
	baseModelName = strings.TrimSpace(baseModelName)
	modelCfg, err := resolvedModelConfig(al.GetConfig(), baseModelName, agent.Workspace)
	if err != nil {
		// Fallback: create a minimal config for test scenarios
		model := strings.TrimSpace(baseModelName)
		if candidate.Model != "" {
			model = candidate.Model
		}
		if candidate.Provider != "" && candidate.Model != "" {
			model = providers.NormalizeProvider(candidate.Provider) + "/" + candidate.Model
		} else {
			model = ensureProtocolModel(model)
		}
		return &config.ModelConfig{
			ModelName: baseModelName,
			Model:     model,
			Workspace: agent.Workspace,
		}, nil
	}

	// If candidate specifies a different provider/model, override
	clone := *modelCfg
	return &clone, nil
}
