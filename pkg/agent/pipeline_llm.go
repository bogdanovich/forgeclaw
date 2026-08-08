// MintClaw - Ultra-lightweight personal AI agent

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/constants"
	runtimeevents "github.com/bogdanovich/mintclaw/pkg/events"
	"github.com/bogdanovich/mintclaw/pkg/logger"
	"github.com/bogdanovich/mintclaw/pkg/providers"
)

const contextOverflowCompactTimeout = 45 * time.Second

type llmStageDisposition uint8

const (
	llmStageContinue llmStageDisposition = iota
	llmStageComplete
)

type llmStageResult struct {
	disposition llmStageDisposition
	outcome     LLMCallOutcome
}

func completeLLMStage(outcome LLMCallOutcome) llmStageResult {
	return llmStageResult{disposition: llmStageComplete, outcome: outcome}
}

// CallLLM performs an LLM call with fallback support, hook invocation, and retry logic.
// It handles PreLLM setup, the actual LLM invocation with retry, and AfterLLM processing.
// Returns an explicit outcome indicating what the coordinator should do next.
func (p *Pipeline) CallLLM(
	ctx context.Context,
	turnCtx context.Context,
	ts *turnState,
	exec *turnExecution,
	llm *LLMIterationState,
) (LLMCallOutcome, error) {
	stage, err := p.prepareLLMRequest(turnCtx, ts, exec, llm)
	if err != nil || stage.disposition == llmStageComplete {
		return stage.outcome, err
	}
	stage, err = p.invokeLLMWithRetry(ctx, turnCtx, ts, exec, llm)
	if err != nil || stage.disposition == llmStageComplete {
		return stage.outcome, err
	}
	return p.normalizeAndDispatchLLMResponse(turnCtx, ts, exec, llm)
}

func (p *Pipeline) invokeLLMWithRetry(
	ctx context.Context,
	turnCtx context.Context,
	ts *turnState,
	exec *turnExecution,
	llm *LLMIterationState,
) (llmStageResult, error) {
	iteration := llm.iteration

	// LLM call closure with fallback support
	callLLM := func(messagesForCall []providers.Message, toolDefsForCall []providers.ToolDefinition) (*providers.LLMResponse, error) {
		providerCtx, providerCancel := context.WithCancel(turnCtx)
		ts.setProviderCancel(providerCancel)
		defer func() {
			providerCancel()
			ts.clearProviderCancel(providerCancel)
		}()

		defer p.trackActiveRequest()()

		if response, handled, streamErr := p.tryConfiguredStreamingLLM(
			providerCtx,
			ts,
			exec,
			llm,
			messagesForCall,
			toolDefsForCall,
		); handled {
			return response, streamErr
		}

		if len(exec.model.activeCandidates) > 1 && p.Interaction.Fallback != nil {
			fallbackAttempt := 0
			fbResult, fbErr := executeFallbackWithObserver(
				p.Interaction.Fallback,
				providerCtx,
				exec.model.activeCandidates,
				func(ctx context.Context, candidate providers.FallbackCandidate) (*providers.LLMResponse, error) {
					return p.callFallbackCandidateWithCapabilities(
						ctx,
						ts,
						exec,
						llm,
						candidate,
						messagesForCall,
						toolDefsForCall,
					)
				},
				func(attempt providers.FallbackAttempt) {
					fallbackAttempt++
					status := "failed"
					if attempt.Skipped {
						status = "skipped"
					} else if attempt.Succeeded {
						status = "succeeded"
					}
					reason := string(attempt.Reason)
					p.emitEvent(
						runtimeevents.KindAgentLLMFallbackAttempt,
						ts.eventMeta("runTurn", "turn.llm.fallback_attempt"),
						LLMFallbackAttemptPayload{
							Provider: attempt.Provider, Model: attempt.Model,
							IdentityKey: attempt.IdentityKey, Attempt: fallbackAttempt,
							Status: status, Reason: reason, ErrorCode: reason,
							Skipped: attempt.Skipped,
						},
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
				p.updateAutoFallbackSelection(ts.model.RouteSessionKey,
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
			llm.llmModel,
			llm.llmOpts,
		)
		if err == nil &&
			exec.model.autoFallback &&
			strings.TrimSpace(ts.model.RouteSessionKey) != "" &&
			len(exec.model.selectedCandidates) > 0 {
			p.updateAutoFallbackSelection(ts.model.RouteSessionKey,
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
	maxRetries, backoffSecs := p.llmRetrySettings()
	for retry := 0; retry <= maxRetries; retry++ {
		llm.response, err = callLLM(llm.callMessages, llm.providerToolDefs)
		if err == nil {
			break
		}
		if ts.hardAbortRequested() && errors.Is(err, context.Canceled) {
			_ = ts.requestHardAbort()
			return completeLLMStage(LLMCallOutcome{Control: ControlBreak, AbortCause: TurnAbortHard}), nil
		}
		if isConfiguredStreamingVisibleError(err) {
			break
		}

		// Retry without media if vision is unsupported
		if hasMediaRefs(llm.callMessages) &&
			isVisionUnsupportedError(err) &&
			retry < maxRetries &&
			!turnIntroducedMedia(ts) {
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
			llm.callMessages = stripMessageMedia(llm.callMessages)
			if !ts.opts.NoHistory {
				exec.history = stripMessageMedia(exec.history)
				ts.agent.Sessions.SetHistory(ts.sessionKey, exec.history)
				for i := range ts.persistedMessages {
					ts.persistedMessages[i].Media = nil
				}
				ts.refreshCanonicalRestorePointFromSession()
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
			if sleepErr := p.sleepBeforeLLMRetry(turnCtx, backoff); sleepErr != nil {
				if ts.hardAbortRequested() {
					_ = ts.requestHardAbort()
					return completeLLMStage(LLMCallOutcome{Control: ControlBreak, AbortCause: TurnAbortHard}), nil
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
				p.Runtime.Bus.PublishOutbound(ctx, outboundMessageForTurn(
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
				llm.providerToolDefs,
				p.maxMediaSize(),
			)
			compactBudget := effectiveHistoryBudget(
				ts.agent.ContextWindow,
				ts.agent.MaxTokens,
				reserveTokens,
			)
			compactCtx, compactCancel := context.WithTimeout(ctx, contextOverflowCompactTimeout)
			if compactErr := p.Context.Runtime.Compact(compactCtx, &CompactRequest{
				Agent:      ts.agent,
				SessionKey: ts.sessionKey,
				Workspace:  ts.workspace,
				TraceScope: ts.scope.traceScope(),
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
			ts.refreshCanonicalRestorePointFromSession()
			persistedTurn := ts.persistedMessagesSnapshot()
			protectedTurnTail := append([]providers.Message(nil), persistedTurn...)
			asmResp, asmErr := p.Context.Runtime.Assemble(ctx, &AssembleRequest{
				Agent:         ts.agent,
				SessionKey:    ts.sessionKey,
				Budget:        ts.agent.ContextWindow,
				MaxTokens:     ts.agent.MaxTokens,
				ReserveTokens: reserveTokens,
			})
			if asmErr != nil {
				err = fmt.Errorf("reassemble context after compaction: %w", asmErr)
				break
			}
			if asmResp != nil {
				exec.history = asmResp.History
				exec.summary = asmResp.Summary
			}
			ts.recordSkillContextSnapshot(skillContextTriggerContextRetryRebuild, contextualSkills)
			stableHistory, assembledTurnTail := splitHistoryForActiveTurn(
				exec.history,
				persistedTurn,
			)
			if len(protectedTurnTail) == 0 {
				protectedTurnTail = assembledTurnTail
			}
			exec.history = append(
				append([]providers.Message(nil), stableHistory...),
				protectedTurnTail...,
			)
			buildMessages := func(trimmedHistory []providers.Message) []providers.Message {
				fullHistory := append(
					append([]providers.Message(nil), trimmedHistory...),
					protectedTurnTail...)
				rebuilt := p.buildTurnMessages(
					ts,
					fullHistory,
					exec.summary,
					"",
					nil,
					contextualSkills,
				)
				activeTailCount := matchingTurnMessageTail(rebuilt, protectedTurnTail)
				return resolveMediaRefs(
					rebuilt,
					p.Context.MediaResolver,
					p.maxMediaSize(),
					len(rebuilt)-activeTailCount,
				)
			}
			originalHistoryCount := len(exec.history)
			trimmedStableHistory, callMessages, fit := trimHistoryToFitContextWindow(
				stableHistory,
				func(trimmedHistory []providers.Message) []providers.Message {
					rebuilt := buildMessages(trimmedHistory)
					if llm.gracefulTerminal {
						return append(
							append([]providers.Message(nil), rebuilt...),
							ts.interruptHintMessage(),
						)
					}
					return rebuilt
				},
				ts.agent.ContextWindow,
				llm.providerToolDefs,
				ts.agent.MaxTokens,
			)
			llm.callMessages = callMessages
			exec.history = append(trimmedStableHistory, protectedTurnTail...)
			exec.messages = buildMessages(trimmedStableHistory)
			if llm.gracefulTerminal {
				msgs := append([]providers.Message(nil), exec.messages...)
				llm.callMessages = append(msgs, ts.interruptHintMessage())
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
				"model":     llm.llmModel,
				"error":     err.Error(),
			})
		return llmStageResult{}, fmt.Errorf("LLM call failed after retries: %w", err)
	}
	return llmStageResult{}, nil
}

func (p *Pipeline) normalizeAndDispatchLLMResponse(
	turnCtx context.Context,
	ts *turnState,
	exec *turnExecution,
	llm *LLMIterationState,
) (LLMCallOutcome, error) {
	iteration := llm.iteration

	if p.Interaction.Hooks != nil {
		llmResp, decision := p.Interaction.Hooks.AfterLLM(turnCtx, &LLMHookResponse{
			Meta:     ts.eventMeta("runTurn", "turn.llm.response"),
			Context:  cloneTurnContext(ts.turnCtx),
			Model:    llm.llmModel,
			Response: llm.response,
		})
		switch decision.normalizedAction() {
		case HookActionContinue, HookActionModify:
			if llmResp != nil && llmResp.Response != nil {
				llm.response = llmResp.Response
			}
		case HookActionAbortTurn:
			cancelConfiguredStreamingLLM(turnCtx, llm)
			return LLMCallOutcome{Control: ControlBreak, AbortCause: TurnAbortHook}, nil
		case HookActionHardAbort:
			cancelConfiguredStreamingLLM(turnCtx, llm)
			_ = ts.requestHardAbort()
			return LLMCallOutcome{Control: ControlBreak, AbortCause: TurnAbortHard}, nil
		}
	}

	// Save finishReason and usage on the active turn state. Use ts directly
	// (the authoritative turn state for this call) rather than relying on a
	// context lookup: the raw ctx passed to CallLLM is not seeded with turn
	// state, and the streaming publisher reads usage from ts at finalize.
	if ts != nil {
		ts.SetLastFinishReason(llm.response.FinishReason)
		ts.SetLastUsage(llm.response.Usage)
		ts.RecordLLMUsage(llm.response.Usage)
	}

	if llm.suppressReasoning {
		llm.response.Reasoning = ""
		llm.response.ReasoningContent = ""
		llm.response.ReasoningDetails = nil
	}
	reasoningContent := responseReasoningContent(llm.response)
	shouldPublishMintClawToolCallInterim := ts.channel == "mintclaw" && len(llm.response.ToolCalls) > 0
	if shouldPublishMintClawToolCallInterim {
		// MintClaw tool-call turns publish their reasoning/content/tool summary as a
		// structured sequence after the tool-call payload is normalized below.
	} else if ts.channel == "mintclaw" {
		if llm.streamingPublisher != nil && llm.streamingPublisher.ReasoningPublished() {
			if err := llm.streamingPublisher.FinalizeReasoning(turnCtx, reasoningContent); err != nil {
				logger.WarnCF("agent", "Failed to finalize streamed mintclaw reasoning", map[string]any{
					"channel": ts.channel,
					"chat_id": ts.chatID,
					"error":   err.Error(),
				})
			}
		} else {
			// Publish mintclaw thoughts before the turn context is canceled at return time.
			// The async variant can race with turn teardown and intermittently drop the
			// thought message in CI even though the LLM produced reasoning content.
			p.publishMintClawReasoning(turnCtx, reasoningContent, ts.chatID, ts.sessionKey, exec.model.llmModelName)
		}
	} else {
		go p.handleReasoning(
			turnCtx,
			reasoningContent,
			ts.channel,
			p.targetReasoningChannelID(ts.channel),
		)
	}
	diagnosticResponseContent, diagnosticResponseReasoning, sensitiveDiagnosticResponse := diagnosticLLMResponseContent(
		llm.response,
		llm.callMessages,
	)
	p.emitEvent(
		runtimeevents.KindAgentLLMResponse,
		ts.eventMeta("runTurn", "turn.llm.response"),
		LLMResponsePayload{
			ResponseHash:     diagnosticSafeHash(p.Cfg, llm.response.Content),
			ContentLen:       len(llm.response.Content),
			ToolCalls:        len(llm.response.ToolCalls),
			HasReasoning:     llm.response.Reasoning != "" || llm.response.ReasoningContent != "",
			HasProviderUsage: llm.response.Usage != nil,
			PromptTokens:     usagePromptTokens(llm.response.Usage),
			CompletionTokens: usageCompletionTokens(llm.response.Usage),
			TotalTokens:      usageTotalTokens(llm.response.Usage),
			DiagnosticContent: diagnosticTextPreview(
				p.Cfg, diagnosticResponseContent, diagnosticModelResponseBytes,
			),
			DiagnosticReasoning: diagnosticTextPreview(
				p.Cfg,
				diagnosticResponseReasoning,
				diagnosticModelReasoningBytes,
			),
			DiagnosticToolCalls: diagnosticToolCallsPreviewWithSensitivity(
				p.Cfg,
				llm.response.ToolCalls,
				sensitiveDiagnosticResponse,
			),
		},
	)

	llmResponseFields := map[string]any{
		"agent_id":       ts.agent.ID,
		"iteration":      iteration,
		"content_chars":  len(llm.response.Content),
		"tool_calls":     len(llm.response.ToolCalls),
		"target_channel": p.targetReasoningChannelID(ts.channel),
		"channel":        ts.channel,
	}
	if sensitiveDiagnosticResponse {
		llmResponseFields["reasoning_redacted"] = true
	} else {
		llmResponseFields["reasoning"] = llm.response.Reasoning
	}
	if llm.response.Usage != nil {
		llmResponseFields["prompt_tokens"] = llm.response.Usage.PromptTokens
		llmResponseFields["completion_tokens"] = llm.response.Usage.CompletionTokens
		llmResponseFields["total_tokens"] = llm.response.Usage.TotalTokens
	}
	logger.DebugCF("agent", "LLM response", llmResponseFields)

	// No-tool-call path: steering check and direct response
	if len(llm.response.ToolCalls) == 0 || llm.gracefulTerminal {
		responseContent := llm.response.Content
		if responseContent == "" && llm.response.ReasoningContent != "" && ts.channel != "mintclaw" {
			responseContent = llm.response.ReasoningContent
		}
		exec.actionLog = appendTurnActionRecord(
			exec.actionLog,
			"assistant_direct",
			"",
			responseContent,
			false,
			false,
		)
		if steerMsgs := p.dequeueSteeringMessagesForTurn(ts); len(
			steerMsgs,
		) > 0 {
			exec.markSteeringObserved()
			cancelConfiguredStreamingLLM(turnCtx, llm)
			logger.InfoCF("agent", "Steering arrived after direct LLM response; continuing turn",
				map[string]any{
					"agent_id":       ts.agent.ID,
					"iteration":      iteration,
					"steering_count": len(steerMsgs),
				})
			exec.pendingMessages = append(exec.pendingMessages, steerMsgs...)
			return LLMCallOutcome{Control: ControlContinue}, nil
		}

		logger.InfoCF("agent", "LLM response without tool calls (direct answer)",
			map[string]any{
				"agent_id":      ts.agent.ID,
				"iteration":     iteration,
				"content_chars": len(responseContent),
			})
		return LLMCallOutcome{Control: ControlBreak, FinalContent: responseContent}, nil
	}
	cancelConfiguredStreamingLLM(turnCtx, llm)

	// Tool-call path: normalize and prepare for tool execution
	llm.normalizedToolCalls = make([]providers.ToolCall, 0, len(llm.response.ToolCalls))
	for _, tc := range llm.response.ToolCalls {
		llm.normalizedToolCalls = append(llm.normalizedToolCalls, providers.NormalizeToolCall(tc))
	}

	toolNames := make([]string, 0, len(llm.normalizedToolCalls))
	for _, tc := range llm.normalizedToolCalls {
		toolNames = append(toolNames, tc.Name)
	}
	logger.InfoCF("agent", "LLM requested tool calls",
		map[string]any{
			"agent_id":  ts.agent.ID,
			"tools":     toolNames,
			"count":     len(llm.normalizedToolCalls),
			"iteration": iteration,
		})

	llm.allResponsesHandled = len(llm.normalizedToolCalls) > 0
	assistantMsg := providers.Message{
		Role:             "assistant",
		Content:          llm.response.Content,
		ModelName:        exec.model.llmModelName,
		ReasoningContent: reasoningContent,
	}
	for _, tc := range llm.normalizedToolCalls {
		argumentsJSON, _ := json.Marshal(tc.Arguments)
		toolFeedbackExplanation := toolFeedbackExplanationForToolCall(
			llm.response,
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
	llm.assistantToolCallsPersisted = false
	llm.assistantToolCallsWriteErr = nil
	if !ts.opts.NoHistory {
		writeErr := persistFullSessionMessage(turnCtx, ts.agent.Sessions, ts.sessionKey, assistantMsg)
		llm.assistantToolCallsWriteErr = writeErr
		llm.assistantToolCallsPersisted = writeErr == nil
		if writeErr == nil {
			ts.recordPersistedMessage(assistantMsg)
		}
		p.ingestMessage(turnCtx, ts, assistantMsg, writeErr)
	}
	if shouldPublishMintClawToolCallInterim && (ts.opts.NoHistory || llm.assistantToolCallsPersisted) {
		interimContent := llm.response.Content
		if p.shouldPublishToolFeedback(ts) {
			interimContent = ""
		}
		p.publishMintClawToolCallInterim(
			turnCtx,
			ts,
			exec.model.llmModelName,
			reasoningContent,
			interimContent,
			assistantMsg.ToolCalls,
		)
	}

	return LLMCallOutcome{Control: ControlToolLoop}, nil
}

func turnIntroducedMedia(ts *turnState) bool {
	if ts == nil {
		return false
	}
	if len(ts.media) > 0 {
		return true
	}
	return hasMediaRefs(ts.persistedMessagesSnapshot())
}

func (p *Pipeline) applyBeforeLLMModelRewrite(
	ts *turnState,
	exec *turnExecution,
	llm *LLMIterationState,
) {
	if p == nil || ts == nil || ts.agent == nil || exec == nil || llm == nil {
		return
	}
	rawModel := strings.TrimSpace(llm.llmModel)
	if rawModel == "" {
		return
	}

	execution, cleanup, err := p.Context.ModelExecution.buildExecutionStateForModel(
		ts.agent,
		rawModel,
		nil,
	)
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
		candidates := p.modelCandidates(rawModel, nil)
		exec.model.selectedCandidates = append([]providers.FallbackCandidate(nil), candidates...)
		exec.model.activeCandidates = candidates
		exec.model.activeModel = resolvedCandidateModel(candidates, rawModel)
		llm.llmModel = exec.model.activeModel
		exec.model.activeModelConfig = p.activeModelConfig(
			ts.agent.Workspace,
			candidates,
			rawModel,
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
	llm.llmModel = exec.model.activeModel
	exec.model.activeModelConfig = p.activeModelConfig(
		ts.agent.Workspace,
		execution.Candidates,
		rawModel,
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
