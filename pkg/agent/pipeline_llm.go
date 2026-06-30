// PicoClaw - Ultra-lightweight personal AI agent

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/constants"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
)

const contextOverflowCompactTimeout = 45 * time.Second

// CallLLM performs an LLM call with fallback support, hook invocation, and retry logic.
// It handles PreLLM setup, the actual LLM invocation with retry, and AfterLLM processing.
// Returns Control indicating what the coordinator should do next.
func (p *Pipeline) CallLLM(
	ctx context.Context,
	turnCtx context.Context,
	ts *turnState,
	exec *turnExecution,
	iteration int,
) (Control, error) {
	al := p.al
	maxMediaSize := p.Cfg.Agents.Defaults.GetMaxMediaSize()

	// PreLLM: resolve media refs (except on iteration 1 where user media is already resolved)
	if iteration > 1 {
		exec.messages = resolveMediaRefs(exec.messages, p.MediaStore, maxMediaSize)
		usedVisionOverride, err := p.al.maybeApplyVisionExecutionState(ts.agent, exec)
		if err != nil {
			return ControlBreak, err
		}
		if usedVisionOverride {
			logger.InfoCF("agent", "Switched turn to vision override model after media resolution", map[string]any{
				"agent_id":         ts.agent.ID,
				"iteration":        iteration,
				"vision_model":     exec.model.activeModel,
				"vision_route":     exec.model.visionRoute,
				"messages_count":   len(exec.messages),
				"active_candidate": len(exec.model.activeCandidates),
			})
		}
	}

	// PreLLM: graceful terminal handling
	exec.gracefulTerminal, _ = ts.gracefulInterruptRequested()
	exec.providerToolDefs = ts.agent.Tools.ToProviderDefs()
	exec.providerToolDefs = filterToolsByTurnProfile(exec.providerToolDefs, ts.profile)

	// Native web search support
	webSearchEnabled := p.Cfg.Tools.IsToolEnabled("web") &&
		turnProfileToolAllowed(ts.profile, "web_search")
	exec.useNativeSearch = webSearchEnabled && p.Cfg.Tools.Web.PreferNative &&
		func() bool {
			if ns, ok := exec.model.activeProvider.(providers.NativeSearchCapable); ok {
				return ns.SupportsNativeSearch()
			}
			return false
		}()
	if exec.useNativeSearch {
		filtered := make([]providers.ToolDefinition, 0, len(exec.providerToolDefs))
		for _, td := range exec.providerToolDefs {
			if td.Function.Name != "web_search" {
				filtered = append(filtered, td)
			}
		}
		exec.providerToolDefs = filtered
	}

	exec.callMessages = exec.messages
	if exec.gracefulTerminal {
		exec.callMessages = append(
			append([]providers.Message(nil), exec.messages...),
			ts.interruptHintMessage(),
		)
		exec.providerToolDefs = nil
		ts.markGracefulTerminalUsed()
	}

	exec.llmOpts = map[string]any{
		"max_tokens":       ts.agent.MaxTokens,
		"temperature":      ts.agent.Temperature,
		"prompt_cache_key": ts.agent.ID,
	}
	if exec.useNativeSearch {
		exec.llmOpts["native_search"] = true
	}
	execution := ts.model.ExecutionState()
	applyTurnThinkingOptions(exec, execution, exec.model.activeProvider, true)

	exec.llmModel = exec.model.activeModel

	// BeforeLLM hook
	if p.Hooks != nil {
		llmReq, decision := p.Hooks.BeforeLLM(turnCtx, &LLMHookRequest{
			Meta:             ts.eventMeta("runTurn", "turn.llm.request"),
			Context:          cloneTurnContext(ts.turnCtx),
			Model:            exec.llmModel,
			Messages:         exec.callMessages,
			Tools:            exec.providerToolDefs,
			Options:          exec.llmOpts,
			GracefulTerminal: exec.gracefulTerminal,
		})
		switch decision.normalizedAction() {
		case HookActionContinue, HookActionModify:
			if llmReq != nil {
				prevModel := exec.llmModel
				exec.llmModel = llmReq.Model
				exec.callMessages = llmReq.Messages
				exec.providerToolDefs = filterToolsByTurnProfile(llmReq.Tools, ts.profile)
				exec.llmOpts = llmReq.Options
				nativeSearchAllowed := exec.useNativeSearch &&
					turnProfileToolAllowed(ts.profile, "web_search")
				if !nativeSearchAllowed {
					delete(exec.llmOpts, "native_search")
				}
				if strings.TrimSpace(exec.llmModel) != "" && exec.llmModel != prevModel {
					p.applyBeforeLLMModelRewrite(ts, exec)
					applyTurnThinkingOptions(exec, execution, exec.model.activeProvider, true)
				}
			}
		case HookActionAbortTurn:
			cancelConfiguredStreamingLLM(turnCtx, exec)
			exec.abortedByHook = true
			return ControlBreak, nil
		case HookActionHardAbort:
			cancelConfiguredStreamingLLM(turnCtx, exec)
			_ = ts.requestHardAbort()
			exec.abortedByHardAbort = true
			return ControlBreak, nil
		}
	}

	p.emitEvent(
		runtimeevents.KindAgentLLMRequest,
		ts.eventMeta("runTurn", "turn.llm.request"),
		LLMRequestPayload{
			Model:         exec.llmModel,
			MessagesCount: len(exec.callMessages),
			ToolsCount:    len(exec.providerToolDefs),
			MaxTokens:     ts.agent.MaxTokens,
			Temperature:   ts.agent.Temperature,
		},
	)

	logger.DebugCF("agent", "LLM request",
		map[string]any{
			"agent_id":          ts.agent.ID,
			"iteration":         iteration,
			"model":             exec.llmModel,
			"messages_count":    len(exec.callMessages),
			"tools_count":       len(exec.providerToolDefs),
			"max_tokens":        ts.agent.MaxTokens,
			"temperature":       ts.agent.Temperature,
			"system_prompt_len": len(exec.callMessages[0].Content),
		})
	logger.DebugCF("agent", "Full LLM request",
		map[string]any{
			"iteration":     iteration,
			"messages_json": formatMessagesForLog(exec.callMessages),
			"tools_json":    formatToolsForLog(exec.providerToolDefs),
		})

	// LLM call closure with fallback support
	callLLM := func(messagesForCall []providers.Message, toolDefsForCall []providers.ToolDefinition) (*providers.LLMResponse, error) {
		providerCtx, providerCancel := context.WithCancel(turnCtx)
		ts.setProviderCancel(providerCancel)
		defer func() {
			providerCancel()
			ts.clearProviderCancel(providerCancel)
		}()

		al.activeRequestsInc()
		defer al.activeRequestsDec()

		if response, handled, streamErr := p.tryConfiguredStreamingLLM(
			providerCtx,
			ts,
			exec,
			messagesForCall,
			toolDefsForCall,
		); handled {
			return response, streamErr
		}

		if len(exec.model.activeCandidates) > 1 && p.Fallback != nil {
			fbResult, fbErr := p.Fallback.ExecuteCandidate(
				providerCtx,
				exec.model.activeCandidates,
				func(ctx context.Context, candidate providers.FallbackCandidate) (*providers.LLMResponse, error) {
					candidateProvider, err := providerForFallbackCandidate(
						exec.model.candidateProviders,
						exec.model.activeProvider,
						candidate.Provider,
						candidate.Model,
					)
					if err != nil {
						return nil, err
					}
					callOpts := shallowCloneLLMOptions(exec.llmOpts)
					delete(callOpts, "thinking_level")
					candidateCfg := resolveActiveModelConfig(
						p.Cfg,
						ts.agent.Workspace,
						[]providers.FallbackCandidate{candidate},
						candidate.Model,
						p.Cfg.Agents.Defaults.Provider,
					)
					candidateThinking := thinkingSettingsFromModelConfig(candidateCfg)
					applyThinkingOption(
						callOpts,
						candidateProvider,
						candidateThinking,
						true,
						ts.agent.ID,
					)
					exec.suppressReasoning = shouldSuppressReasoningFor(candidateThinking)
					return candidateProvider.Chat(
						ctx,
						messagesForCall,
						toolDefsForCall,
						candidate.Model,
						callOpts,
					)
				},
			)
			if fbErr != nil {
				return nil, fbErr
			}
			if fbResult.Provider != "" && len(fbResult.Attempts) > 0 {
				logger.InfoCF(
					"agent",
					fmt.Sprintf("Fallback: succeeded with %s/%s after %d attempts",
						fbResult.Provider, fbResult.Model, len(fbResult.Attempts)+1),
					map[string]any{"agent_id": ts.agent.ID, "iteration": iteration},
				)
			}
			for _, candidate := range exec.model.activeCandidates {
				if candidate.StableKey() != fbResult.IdentityKey {
					continue
				}
				exec.model.llmModelName = resolvedCandidateModelName(
					[]providers.FallbackCandidate{candidate},
					exec.model.llmModelName,
				)
				break
			}
			if exec.model.autoFallback {
				updateAutoFallbackSelection(
					p.al,
					ts.model.RouteSessionKey,
					exec.model.selectedCandidates,
					fbResult,
					exec.model.usedLight,
				)
			}
			return fbResult.Response, nil
		}
		resp, err := exec.model.activeProvider.Chat(
			providerCtx,
			messagesForCall,
			toolDefsForCall,
			exec.llmModel,
			exec.llmOpts,
		)
		if err == nil &&
			exec.model.autoFallback &&
			strings.TrimSpace(ts.model.RouteSessionKey) != "" &&
			len(exec.model.selectedCandidates) > 0 {
			updateAutoFallbackSelection(
				p.al,
				ts.model.RouteSessionKey,
				exec.model.selectedCandidates,
				&providers.FallbackResult{
					Response: resp,
					Provider: exec.model.activeCandidates[0].Provider,
					Model:    exec.model.activeCandidates[0].Model,
				},
				exec.model.usedLight,
			)
		}
		return resp, err
	}

	// Retry loop
	var err error
	maxRetries := p.Cfg.Agents.Defaults.MaxLLMRetries
	if maxRetries <= 0 {
		maxRetries = 2
	}
	backoffSecs := p.Cfg.Agents.Defaults.LLMRetryBackoffSecs
	if backoffSecs <= 0 {
		backoffSecs = 2
	}
	for retry := 0; retry <= maxRetries; retry++ {
		exec.response, err = callLLM(exec.callMessages, exec.providerToolDefs)
		if err == nil {
			break
		}
		if ts.hardAbortRequested() && errors.Is(err, context.Canceled) {
			_ = ts.requestHardAbort()
			exec.abortedByHardAbort = true
			return ControlBreak, nil
		}
		if isConfiguredStreamingVisibleError(err) {
			break
		}

		// Retry without media if vision is unsupported
		if hasMediaRefs(exec.callMessages) && isVisionUnsupportedError(err) && retry < maxRetries {
			p.emitEvent(
				runtimeevents.KindAgentLLMRetry,
				ts.eventMeta("runTurn", "turn.llm.retry"),
				LLMRetryPayload{
					Attempt:    retry + 1,
					MaxRetries: maxRetries,
					Reason:     "vision_unsupported",
					Error:      err.Error(),
					Backoff:    0,
				},
			)
			logger.WarnCF("agent", "Vision unsupported, retrying without media", map[string]any{
				"error": err.Error(),
				"retry": retry,
			})
			exec.callMessages = stripMessageMedia(exec.callMessages)
			if !ts.opts.NoHistory {
				exec.history = stripMessageMedia(exec.history)
				ts.agent.Sessions.SetHistory(ts.sessionKey, exec.history)
				for i := range ts.persistedMessages {
					ts.persistedMessages[i].Media = nil
				}
				ts.refreshRestorePointFromSession(ts.agent)
			}
			continue
		}

		errMsg := strings.ToLower(err.Error())
		retryReason, isTransientError := transientLLMRetryReason(err)
		isContextError := !isTransientError &&
			(strings.Contains(errMsg, "context_length_exceeded") ||
				strings.Contains(errMsg, "context window") ||
				strings.Contains(errMsg, "context_window") ||
				strings.Contains(errMsg, "maximum context length") ||
				strings.Contains(errMsg, "token limit") ||
				strings.Contains(errMsg, "too many tokens") ||
				strings.Contains(errMsg, "max_tokens") ||
				strings.Contains(errMsg, "invalidparameter") ||
				strings.Contains(errMsg, "prompt is too long") ||
				strings.Contains(errMsg, "request too large"))

		if isTransientError && retry < maxRetries {
			backoff := time.Duration(retry+1) * time.Duration(backoffSecs) * time.Second
			p.emitEvent(
				runtimeevents.KindAgentLLMRetry,
				ts.eventMeta("runTurn", "turn.llm.retry"),
				LLMRetryPayload{
					Attempt:    retry + 1,
					MaxRetries: maxRetries,
					Reason:     retryReason,
					Error:      err.Error(),
					Backoff:    backoff,
				},
			)
			logger.WarnCF("agent", "Transient LLM error, retrying after backoff", map[string]any{
				"error":   err.Error(),
				"reason":  retryReason,
				"retry":   retry,
				"backoff": backoff.String(),
			})
			if sleepErr := sleepWithContext(turnCtx, backoff); sleepErr != nil {
				if ts.hardAbortRequested() {
					_ = ts.requestHardAbort()
					return ControlBreak, nil
				}
				err = sleepErr
				break
			}
			continue
		}

		if isContextError && retry < maxRetries && !ts.opts.NoHistory {
			p.emitEvent(
				runtimeevents.KindAgentLLMRetry,
				ts.eventMeta("runTurn", "turn.llm.retry"),
				LLMRetryPayload{
					Attempt:    retry + 1,
					MaxRetries: maxRetries,
					Reason:     "context_limit",
					Error:      err.Error(),
				},
			)
			logger.WarnCF(
				"agent",
				"Context window error detected, attempting compression",
				map[string]any{
					"error": err.Error(),
					"retry": retry,
				},
			)

			if retry == 0 && !constants.IsInternalChannel(ts.channel) {
				p.Bus.PublishOutbound(ctx, outboundMessageForTurn(
					ts,
					"Context window exceeded. Compressing history and retrying...",
				))
			}

			contextualSkills := ts.activeSkills
			if ts.agent.ContextBuilder != nil {
				contextualSkills = ts.agent.ContextBuilder.ResolveActiveSkillsForContext(
					ts.activeSkills,
				)
			}
			reserveTokens := p.estimateNonHistoryPromptReserve(
				ts,
				contextualSkills,
				exec.providerToolDefs,
				p.Cfg.Agents.Defaults.GetMaxMediaSize(),
			)
			compactBudget := effectiveHistoryBudget(
				ts.agent.ContextWindow,
				ts.agent.MaxTokens,
				reserveTokens,
			)
			compactCtx, compactCancel := context.WithTimeout(ctx, contextOverflowCompactTimeout)
			if compactErr := p.ContextManager.Compact(compactCtx, &CompactRequest{
				SessionKey: ts.sessionKey,
				Reason:     ContextCompressReasonRetry,
				Budget:     compactBudget,
			}); compactErr != nil {
				logger.WarnCF("agent", "Context overflow compact failed", map[string]any{
					"session_key": ts.sessionKey,
					"timeout_ms":  contextOverflowCompactTimeout.Milliseconds(),
					"error":       compactErr.Error(),
				})
			}
			compactCancel()
			ts.refreshRestorePointFromSession(ts.agent)
			if asmResp, asmErr := p.ContextManager.Assemble(ctx, &AssembleRequest{
				SessionKey:    ts.sessionKey,
				Budget:        ts.agent.ContextWindow,
				MaxTokens:     ts.agent.MaxTokens,
				ReserveTokens: reserveTokens,
			}); asmErr == nil && asmResp != nil {
				exec.history = asmResp.History
				exec.summary = asmResp.Summary
			}
			ts.recordSkillContextSnapshot(skillContextTriggerContextRetryRebuild, contextualSkills)
			stableHistory, protectedTurnTail := splitHistoryForActiveTurn(
				exec.history,
				ts.persistedMessagesSnapshot(),
			)
			buildMessages := func(trimmedHistory []providers.Message) []providers.Message {
				fullHistory := append(
					append([]providers.Message(nil), trimmedHistory...),
					protectedTurnTail...)
				rebuildPromptReq := promptBuildRequestForTurn(
					ts,
					fullHistory,
					exec.summary,
					"",
					nil,
					p.Cfg,
				)
				rebuildPromptReq.ActiveSkills = append([]string(nil), contextualSkills...)
				return ts.agent.ContextBuilder.BuildMessagesFromPrompt(rebuildPromptReq)
			}
			originalHistoryCount := len(exec.history)
			var fit bool
			var trimmedStableHistory []providers.Message
			trimmedStableHistory, exec.callMessages, fit = trimHistoryToFitContextWindow(
				stableHistory,
				func(trimmedHistory []providers.Message) []providers.Message {
					rebuilt := buildMessages(trimmedHistory)
					if exec.gracefulTerminal {
						return append(
							append([]providers.Message(nil), rebuilt...),
							ts.interruptHintMessage(),
						)
					}
					return rebuilt
				},
				ts.agent.ContextWindow,
				exec.providerToolDefs,
				ts.agent.MaxTokens,
			)
			exec.history = append(trimmedStableHistory, protectedTurnTail...)
			exec.messages = buildMessages(trimmedStableHistory)
			if exec.gracefulTerminal {
				msgs := append([]providers.Message(nil), exec.messages...)
				exec.callMessages = append(msgs, ts.interruptHintMessage())
			}
			if dropped := originalHistoryCount - len(exec.history); dropped > 0 {
				logger.WarnCF(
					"agent",
					"Trimmed rebuilt history after context retry compaction",
					map[string]any{
						"session_key":     ts.sessionKey,
						"retry":           retry,
						"dropped_msgs":    dropped,
						"remaining_msgs":  len(exec.history),
						"context_window":  ts.agent.ContextWindow,
						"max_tokens":      ts.agent.MaxTokens,
						"still_overlimit": !fit,
					},
				)
			} else if !fit {
				logger.WarnCF("agent", "Context still exceeds budget after retry compaction rebuild", map[string]any{
					"session_key":         ts.sessionKey,
					"retry":               retry,
					"history_msgs":        len(exec.history),
					"protected_turn_msgs": len(protectedTurnTail),
					"context_window":      ts.agent.ContextWindow,
					"max_tokens":          ts.agent.MaxTokens,
				})
			}
			if !fit {
				err = fmt.Errorf(
					"context window still exceeded after retry compaction; refusing to drop active turn messages: %w",
					err,
				)
				break
			}
			continue
		}
		break
	}

	if err != nil {
		p.emitEvent(
			runtimeevents.KindAgentError,
			ts.eventMeta("runTurn", "turn.error"),
			ErrorPayload{
				Stage:   "llm",
				Message: err.Error(),
			},
		)
		logger.ErrorCF("agent", "LLM call failed",
			map[string]any{
				"agent_id":  ts.agent.ID,
				"iteration": iteration,
				"model":     exec.llmModel,
				"error":     err.Error(),
			})
		return ControlBreak, fmt.Errorf("LLM call failed after retries: %w", err)
	}

	// AfterLLM hook
	if p.Hooks != nil {
		llmResp, decision := p.Hooks.AfterLLM(turnCtx, &LLMHookResponse{
			Meta:     ts.eventMeta("runTurn", "turn.llm.response"),
			Context:  cloneTurnContext(ts.turnCtx),
			Model:    exec.llmModel,
			Response: exec.response,
		})
		switch decision.normalizedAction() {
		case HookActionContinue, HookActionModify:
			if llmResp != nil && llmResp.Response != nil {
				exec.response = llmResp.Response
			}
		case HookActionAbortTurn:
			cancelConfiguredStreamingLLM(turnCtx, exec)
			exec.abortedByHook = true
			return ControlBreak, nil
		case HookActionHardAbort:
			cancelConfiguredStreamingLLM(turnCtx, exec)
			_ = ts.requestHardAbort()
			exec.abortedByHardAbort = true
			return ControlBreak, nil
		}
	}

	// Save finishReason and provider usage on the active turn state.
	if ts != nil {
		ts.SetLastFinishReason(exec.response.FinishReason)
		if exec.response.Usage != nil {
			ts.SetLastUsage(exec.response.Usage)
		}
		ts.RecordLLMUsage(exec.response.Usage)
	}

	if exec.suppressReasoning {
		exec.response.Reasoning = ""
		exec.response.ReasoningContent = ""
		exec.response.ReasoningDetails = nil
	}
	reasoningContent := responseReasoningContent(exec.response)
	shouldPublishPicoToolCallInterim := ts.channel == "pico" && len(exec.response.ToolCalls) > 0
	if shouldPublishPicoToolCallInterim {
		// Pico tool-call turns publish their reasoning/content/tool summary as a
		// structured sequence after the tool-call payload is normalized below.
	} else if ts.channel == "pico" {
		if exec.streamingPublisher != nil && exec.streamingPublisher.ReasoningPublished() {
			if err := exec.streamingPublisher.FinalizeReasoning(turnCtx, reasoningContent); err != nil {
				logger.WarnCF("agent", "Failed to finalize streamed pico reasoning", map[string]any{
					"channel": ts.channel,
					"chat_id": ts.chatID,
					"error":   err.Error(),
				})
			}
		} else {
			// Publish pico thoughts before the turn context is canceled at return time.
			// The async variant can race with turn teardown and intermittently drop the
			// thought message in CI even though the LLM produced reasoning content.
			al.publishPicoReasoning(turnCtx, reasoningContent, ts.chatID, ts.sessionKey, exec.model.llmModelName)
		}
	} else {
		go al.handleReasoning(
			turnCtx,
			reasoningContent,
			ts.channel,
			al.targetReasoningChannelID(ts.channel),
		)
	}
	p.emitEvent(
		runtimeevents.KindAgentLLMResponse,
		ts.eventMeta("runTurn", "turn.llm.response"),
		LLMResponsePayload{
			ContentLen:       len(exec.response.Content),
			ToolCalls:        len(exec.response.ToolCalls),
			HasReasoning:     exec.response.Reasoning != "" || exec.response.ReasoningContent != "",
			HasProviderUsage: exec.response.Usage != nil,
			PromptTokens:     usagePromptTokens(exec.response.Usage),
			CompletionTokens: usageCompletionTokens(exec.response.Usage),
			TotalTokens:      usageTotalTokens(exec.response.Usage),
		},
	)

	llmResponseFields := map[string]any{
		"agent_id":       ts.agent.ID,
		"iteration":      iteration,
		"content_chars":  len(exec.response.Content),
		"tool_calls":     len(exec.response.ToolCalls),
		"reasoning":      exec.response.Reasoning,
		"target_channel": al.targetReasoningChannelID(ts.channel),
		"channel":        ts.channel,
	}
	if exec.response.Usage != nil {
		llmResponseFields["prompt_tokens"] = exec.response.Usage.PromptTokens
		llmResponseFields["completion_tokens"] = exec.response.Usage.CompletionTokens
		llmResponseFields["total_tokens"] = exec.response.Usage.TotalTokens
	}
	logger.DebugCF("agent", "LLM response", llmResponseFields)

	// No-tool-call path: steering check and direct response
	if len(exec.response.ToolCalls) == 0 || exec.gracefulTerminal {
		responseContent := exec.response.Content
		if responseContent == "" && exec.response.ReasoningContent != "" && ts.channel != "pico" {
			responseContent = exec.response.ReasoningContent
		}
		exec.actionLog = appendTurnActionRecord(
			exec.actionLog,
			"assistant_direct",
			"",
			responseContent,
			false,
		)
		if steerMsgs := al.dequeueSteeringMessagesForTurn(ts.sessionKey, ts.opts.Dispatch.SenderID()); len(
			steerMsgs,
		) > 0 {
			exec.markSteeringObserved()
			cancelConfiguredStreamingLLM(turnCtx, exec)
			logger.InfoCF("agent", "Steering arrived after direct LLM response; continuing turn",
				map[string]any{
					"agent_id":       ts.agent.ID,
					"iteration":      iteration,
					"steering_count": len(steerMsgs),
				})
			exec.pendingMessages = append(exec.pendingMessages, steerMsgs...)
			return ControlContinue, nil
		}

		exec.finalContent = responseContent
		logger.InfoCF("agent", "LLM response without tool calls (direct answer)",
			map[string]any{
				"agent_id":      ts.agent.ID,
				"iteration":     iteration,
				"content_chars": len(exec.finalContent),
			})
		return ControlBreak, nil
	}
	cancelConfiguredStreamingLLM(turnCtx, exec)

	// Tool-call path: normalize and prepare for tool execution
	exec.normalizedToolCalls = make([]providers.ToolCall, 0, len(exec.response.ToolCalls))
	for _, tc := range exec.response.ToolCalls {
		exec.normalizedToolCalls = append(exec.normalizedToolCalls, providers.NormalizeToolCall(tc))
	}

	toolNames := make([]string, 0, len(exec.normalizedToolCalls))
	for _, tc := range exec.normalizedToolCalls {
		toolNames = append(toolNames, tc.Name)
	}
	logger.InfoCF("agent", "LLM requested tool calls",
		map[string]any{
			"agent_id":  ts.agent.ID,
			"tools":     toolNames,
			"count":     len(exec.normalizedToolCalls),
			"iteration": iteration,
		})

	exec.allResponsesHandled = len(exec.normalizedToolCalls) > 0
	assistantMsg := providers.Message{
		Role:             "assistant",
		Content:          exec.response.Content,
		ModelName:        exec.model.llmModelName,
		ReasoningContent: reasoningContent,
	}
	for _, tc := range exec.normalizedToolCalls {
		argumentsJSON, _ := json.Marshal(tc.Arguments)
		toolFeedbackExplanation := toolFeedbackExplanationForToolCall(
			exec.response,
			tc,
			exec.messages,
		)
		extraContent := tc.ExtraContent
		if strings.TrimSpace(toolFeedbackExplanation) != "" {
			if extraContent == nil {
				extraContent = &providers.ExtraContent{}
			}
			extraContent.ToolFeedbackExplanation = toolFeedbackExplanation
		}
		thoughtSignature := ""
		if tc.Function != nil {
			thoughtSignature = tc.Function.ThoughtSignature
		}
		assistantMsg.ToolCalls = append(assistantMsg.ToolCalls, providers.ToolCall{
			ID:   tc.ID,
			Type: "function",
			Name: tc.Name,
			Function: &providers.FunctionCall{
				Name:             tc.Name,
				Arguments:        string(argumentsJSON),
				ThoughtSignature: thoughtSignature,
			},
			ExtraContent:     extraContent,
			ThoughtSignature: thoughtSignature,
		})
	}
	exec.messages = append(exec.messages, assistantMsg)
	if !ts.opts.NoHistory {
		ts.agent.Sessions.AddFullMessage(ts.sessionKey, assistantMsg)
		ts.recordPersistedMessage(assistantMsg)
		ts.ingestMessage(turnCtx, al, assistantMsg)
	}
	if shouldPublishPicoToolCallInterim {
		al.publishPicoToolCallInterim(
			turnCtx,
			ts,
			exec.model.llmModelName,
			reasoningContent,
			exec.response.Content,
			assistantMsg.ToolCalls,
		)
	}

	return ControlToolLoop, nil
}

func (p *Pipeline) applyBeforeLLMModelRewrite(ts *turnState, exec *turnExecution) {
	if p == nil || ts == nil || ts.agent == nil || exec == nil {
		return
	}
	rawModel := strings.TrimSpace(exec.llmModel)
	if rawModel == "" {
		return
	}

	execution, cleanup, err := p.al.buildExecutionStateForModel(ts.agent, rawModel, nil)
	if err != nil {
		logger.WarnCF(
			"agent",
			"BeforeLLM model rewrite could not rebuild dedicated execution state; falling back to active provider",
			map[string]any{
				"agent_id": ts.agent.ID,
				"model":    rawModel,
				"error":    err.Error(),
			},
		)
		defaultProvider := "openai"
		if p.Cfg != nil {
			if provider := strings.TrimSpace(p.Cfg.Agents.Defaults.Provider); provider != "" {
				defaultProvider = provider
			}
		}
		defaultProvider = effectiveDefaultProvider(defaultProvider)
		candidates := resolveModelCandidates(p.Cfg, defaultProvider, rawModel, nil)
		exec.model.selectedCandidates = append([]providers.FallbackCandidate(nil), candidates...)
		exec.model.activeCandidates = candidates
		exec.model.activeModel = resolvedCandidateModel(candidates, rawModel)
		exec.llmModel = exec.model.activeModel
		exec.model.activeModelConfig = resolveActiveModelConfig(
			p.Cfg,
			ts.agent.Workspace,
			candidates,
			rawModel,
			defaultProvider,
		)
		exec.model.llmModelName = resolvedCandidateModelName(candidates, rawModel)
		exec.model.autoFallback = false
		return
	}
	exec.model.selectedCandidates = append(
		[]providers.FallbackCandidate(nil),
		execution.Candidates...)
	if exec.model.cleanup != nil {
		exec.model.cleanup()
	}
	exec.model.activeCandidates = execution.Candidates
	exec.model.activeModel = resolvedCandidateModel(execution.Candidates, rawModel)
	exec.llmModel = exec.model.activeModel
	exec.model.activeModelConfig = resolveActiveModelConfig(
		p.Cfg,
		ts.agent.Workspace,
		execution.Candidates,
		rawModel,
		effectiveDefaultProvider(p.Cfg.Agents.Defaults.Provider),
	)
	exec.model.activeProvider = execution.Provider
	exec.model.candidateProviders = execution.CandidateProviders
	exec.model.cleanup = cleanup
	exec.model.llmModelName = resolvedCandidateModelName(execution.Candidates, rawModel)
	exec.model.usedLight = false
	exec.model.autoFallback = false
}

func providerForFallbackCandidate(
	candidateProviders map[string]providers.LLMProvider,
	activeProvider providers.LLMProvider,
	provider string,
	model string,
) (providers.LLMProvider, error) {
	if cp, ok := candidateProviders[providers.ModelKey(provider, model)]; ok && cp != nil {
		return cp, nil
	}
	if activeProvider == nil {
		return nil, fmt.Errorf("fallback model %q has no active provider", model)
	}
	return activeProvider, nil
}

func transientLLMRetryReason(err error) (string, bool) {
	if err == nil {
		return "", false
	}

	if failErr := providers.ClassifyError(err, "", ""); failErr != nil {
		switch failErr.Reason {
		case providers.FailoverTimeout:
			if failErr.Status >= 500 {
				return "server_error", true
			}
			return "timeout", true
		case providers.FailoverNetwork:
			return "network", true
		case providers.FailoverRateLimit, providers.FailoverOverloaded:
			return "rate_limit", true
		}
	}

	errMsg := strings.ToLower(err.Error())
	if errors.Is(err, context.DeadlineExceeded) ||
		strings.Contains(errMsg, "deadline exceeded") ||
		strings.Contains(errMsg, "client.timeout") ||
		strings.Contains(errMsg, "timed out") ||
		strings.Contains(errMsg, "timeout exceeded") {
		return "timeout", true
	}

	if strings.Contains(errMsg, "connection reset") ||
		strings.Contains(errMsg, "connection refused") ||
		strings.Contains(errMsg, "broken pipe") ||
		strings.Contains(errMsg, "no such host") ||
		strings.Contains(errMsg, "network is unreachable") ||
		strings.Contains(errMsg, "read tcp") ||
		strings.Contains(errMsg, "write tcp") ||
		strings.Contains(errMsg, "eof") {
		return "network", true
	}

	return "", false
}
