package agent

import (
	"context"
	"fmt"
	"strings"

	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
)

func (p *Pipeline) runTurnLoop(
	ctx context.Context,
	turnCtx context.Context,
	ts *turnState,
	host turnRuntimeHost,
) (turnResult, TurnEndStatus, error) {
	turnStatus := TurnEndStatusCompleted

	exec, err := p.SetupTurn(turnCtx, ts)
	if err != nil {
		return turnResult{}, turnStatus, err
	}
	defer func() {
		if exec != nil && exec.model.cleanup != nil {
			exec.model.cleanup()
		}
	}()

	messages := exec.messages
	maxMediaSize := p.maxMediaSize()
	finalContent := exec.finalContent
	mediaResolver := p.Context.MediaResolver

	for {
		graceful, _ := ts.gracefulInterruptRequested()
		canRun := ts.currentIteration() < ts.agent.MaxIterations || len(exec.pendingMessages) > 0 || graceful
		if !canRun && !p.continueWithPendingSubTurnResults(ts, exec) {
			break
		}
		if ts.hardAbortRequested() {
			turnStatus = TurnEndStatusAborted
			result, abortErr := host.abortTurn(ts)
			return result, turnStatus, abortErr
		}

		iteration := ts.currentIteration() + 1
		ts.setIteration(iteration)
		ts.setPhase(TurnPhaseRunning)

		pendingMessages := append([]providers.Message(nil), exec.pendingMessages...)
		if len(pendingMessages) > 0 {
			exec.markSteeringObserved()
			exec.pendingMessages = nil
		}
		if iteration == 1 && !ts.opts.SkipInitialSteeringPoll {
			if steerMsgs := host.dequeueSteeringMessagesForTurnWithFallback(
				ts.sessionKey,
				ts.opts.Dispatch.SenderID(),
			); len(steerMsgs) > 0 {
				exec.markSteeringObserved()
				pendingMessages = append(pendingMessages, steerMsgs...)
			}
		}

		// Check if parent turn has ended.
		if ts.parentTurnState != nil && ts.IsParentEnded() {
			if !ts.critical {
				logger.InfoCF(
					"agent",
					"Parent turn ended, non-critical SubTurn exiting gracefully",
					map[string]any{
						"agent_id":  ts.agentID,
						"iteration": iteration,
						"turn_id":   ts.turnID,
					},
				)
				break
			}
			logger.InfoCF(
				"agent",
				"Parent turn ended, critical SubTurn continues running",
				map[string]any{
					"agent_id":  ts.agentID,
					"iteration": iteration,
					"turn_id":   ts.turnID,
				},
			)
		}

		// Poll for pending SubTurn results.
		if ts.pendingResults != nil {
			if result, ok := ts.dequeuePendingResult(); ok && result != nil && result.ForLLM != "" {
				content := host.filterSensitiveData(result.ForLLM)
				msg := subTurnResultPromptMessage(content)
				pendingMessages = append(pendingMessages, msg)
			}
		}

		// Inject pending steering messages
		if len(pendingMessages) > 0 {
			resolvedPending := resolveMediaRefs(pendingMessages, mediaResolver, maxMediaSize, 0)
			totalContentLen := 0
			for i, pm := range pendingMessages {
				providerMsg := providerPromptMessageForTurn(resolvedPending[i])
				messages = append(messages, providerMsg)
				totalContentLen += len(providerMsg.Content)
				if !ts.opts.NoHistory {
					writeErr := persistFullSessionMessage(ts.agent.Sessions, ts.sessionKey, pm)
					if writeErr == nil {
						ts.recordPersistedMessage(pm)
					}
					p.ingestMessage(turnCtx, ts, pm, writeErr)
				}
				if exec.shouldTrackTurnOwnedSteering(pm) {
					ts.recordAcceptedSteeringMessage(pm)
				}
				logger.InfoCF("agent", "Injected steering message into context",
					map[string]any{
						"agent_id":    ts.agent.ID,
						"iteration":   iteration,
						"content_len": len(providerMsg.Content),
						"media_count": len(pm.Media),
					})
			}
			host.emitEvent(
				runtimeevents.KindAgentSteeringInjected,
				ts.eventMeta("runTurn", "turn.steering.injected"),
				SteeringInjectedPayload{
					Count:           len(pendingMessages),
					TotalContentLen: totalContentLen,
				},
			)
			// Clear exec.pendingMessages after injection so InitialSteeringMessages
			// are not re-injected on subsequent iterations (Issue 2 fix).
			exec.pendingMessages = nil
		}
		// Always sync messages into exec.messages so CallLLM sees the updated state
		exec.messages = messages

		logger.DebugCF("agent", "LLM iteration",
			map[string]any{
				"agent_id":  ts.agent.ID,
				"iteration": iteration,
				"max":       ts.agent.MaxIterations,
			})

		// Execute LLM call via Pipeline
		ts.setPhase(TurnPhaseRunning)
		ctrl, callErr := p.CallLLM(ctx, turnCtx, ts, exec, iteration)
		if callErr != nil {
			turnStatus = TurnEndStatusError
			return turnResult{}, turnStatus, callErr
		}
		messages = exec.messages
		finalContent = exec.finalContent

		switch ctrl {
		case ControlContinue:
			continue
		case ControlBreak:
			// Hard abort: delegate to abortTurn (sets TurnEndStatusAborted)
			if exec.abortedByHardAbort {
				turnStatus = TurnEndStatusAborted
				result, abortErr := host.abortTurn(ts)
				return result, turnStatus, abortErr
			}
			// Hook abort (HookActionAbortTurn): sets TurnEndStatusError, returns error
			if exec.abortedByHook {
				turnStatus = TurnEndStatusError
				return turnResult{}, turnStatus, fmt.Errorf("hook requested turn abort")
			}
			// Ensure empty response falls back to DefaultResponse
			if finalContent == "" {
				finalContent = ts.opts.DefaultResponse
			}
			if p.continueWithPendingSubTurnResults(ts, exec) {
				messages = exec.messages
				continue
			}
			finalContent = host.renderFinalTurnReply(turnCtx, ts, exec, finalContent)
			result, finalizeErr := p.Finalize(
				ctx,
				turnCtx,
				ts,
				exec,
				turnStatus,
				finalContent,
			)
			if finalizeErr != nil {
				turnStatus = TurnEndStatusError
			}
			return result, turnStatus, finalizeErr
		case ControlToolLoop:
			// Execute tools via Pipeline
			toolCtrl := p.ExecuteTools(ctx, turnCtx, ts, exec, iteration)
			switch toolCtrl {
			case ToolControlContinue:
				// Re-read exec.messages since ExecuteTools may have updated it
				// (added tool results/skipped messages) before returning ControlContinue
				messages = exec.messages
				continue
			case ToolControlFinalize:
				renderedContent, rendered := host.tryRenderFinalTurnReply(
					turnCtx,
					ts,
					exec,
					finalContent,
				)
				finalContent = renderedContent
				if !rendered {
					messages = exec.messages
					continue
				}
				if steerMsgs := host.dequeueSteeringMessagesForTurn(ts.sessionKey, ts.opts.Dispatch.SenderID()); len(
					steerMsgs,
				) > 0 {
					exec.markSteeringObserved()
					logger.InfoCF(
						"agent",
						"Steering arrived during terminal render; continuing turn",
						map[string]any{
							"agent_id":       ts.agent.ID,
							"iteration":      iteration,
							"steering_count": len(steerMsgs),
						},
					)
					exec.pendingMessages = append(exec.pendingMessages, steerMsgs...)
					messages = exec.messages
					continue
				}
				if p.continueWithPendingSubTurnResults(ts, exec) {
					messages = exec.messages
					continue
				}
				result, finalizeErr := p.Finalize(ctx, turnCtx, ts, exec, turnStatus, finalContent)
				if finalizeErr != nil {
					turnStatus = TurnEndStatusError
				}
				return result, turnStatus, finalizeErr
			case ToolControlBreak:
				// Hard abort: delegate to abortTurn (sets TurnEndStatusAborted)
				if exec.abortedByHardAbort {
					turnStatus = TurnEndStatusAborted
					result, abortErr := host.abortTurn(ts)
					return result, turnStatus, abortErr
				}
				// Hook abort (HookActionAbortTurn): sets TurnEndStatusError, returns error
				if exec.abortedByHook {
					turnStatus = TurnEndStatusError
					return turnResult{}, turnStatus, fmt.Errorf("hook requested turn abort")
				}
				// ExecuteTools returned ControlBreak:
				// - allResponsesHandled=true: finalize without DefaultResponse (exec.finalContent empty)
				// - allResponsesHandled=false: finalize with exec.finalContent when present
				if strings.TrimSpace(exec.finalContent) != "" {
					finalContent = exec.finalContent
				}
				if exec.allResponsesHandled {
					finalContent = ""
				}
				if p.continueWithPendingSubTurnResults(ts, exec) {
					messages = exec.messages
					continue
				}
				finalContent = host.renderFinalTurnReply(turnCtx, ts, exec, finalContent)
				result, finalizeErr := p.Finalize(
					ctx,
					turnCtx,
					ts,
					exec,
					turnStatus,
					finalContent,
				)
				if finalizeErr != nil {
					turnStatus = TurnEndStatusError
				}
				return result, turnStatus, finalizeErr
			}
		}
	}

	if ts.hardAbortRequested() {
		turnStatus = TurnEndStatusAborted
		result, abortErr := host.abortTurn(ts)
		return result, turnStatus, abortErr
	}

	if finalContent == "" {
		if ts.currentIteration() >= ts.agent.MaxIterations && ts.agent.MaxIterations > 0 {
			finalContent = toolLimitResponse
		} else {
			finalContent = ts.opts.DefaultResponse
		}
	}
	finalContent = host.renderFinalTurnReply(turnCtx, ts, exec, finalContent)

	// Check hard abort before finalizing (may have been set during tool execution)
	if ts.hardAbortRequested() {
		turnStatus = TurnEndStatusAborted
		result, abortErr := host.abortTurn(ts)
		return result, turnStatus, abortErr
	}

	result, err := p.Finalize(ctx, turnCtx, ts, exec, turnStatus, finalContent)
	if err != nil {
		turnStatus = TurnEndStatusError
	}
	return result, turnStatus, err
}

func (p *Pipeline) continueWithPendingSubTurnResults(
	ts *turnState,
	exec *turnExecution,
) bool {
	for {
		results, sealed := ts.sealOrDrainPendingResults()
		if sealed {
			return false
		}
		appended := false
		for _, result := range results {
			if result == nil || result.ForLLM == "" {
				continue
			}
			content := p.filterPendingResultForLLM(result.ForLLM)
			msg := subTurnResultPromptMessage(content)
			exec.pendingMessages = append(exec.pendingMessages, msg)
			appended = true
		}
		if appended {
			return true
		}
	}
}
