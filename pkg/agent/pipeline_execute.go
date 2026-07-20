// PicoClaw - Ultra-lightweight personal AI agent

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/interactions"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/tools"
	"github.com/sipeed/picoclaw/pkg/tools/loopguard"
	"github.com/sipeed/picoclaw/pkg/utils"
)

const repeatedFatalToolErrorStreakLimit = 3

type mcpServerTool interface {
	MCPServerName() string
}

func toolErrorSummary(result *tools.ToolResult) string {
	if result == nil || !result.IsError {
		return ""
	}
	content := strings.TrimSpace(result.ContentForLLM())
	if content == "" && result.Err != nil {
		content = strings.TrimSpace(result.Err.Error())
	}
	return utils.Truncate(content, 200)
}

func isFatalMCPTransportErrorSummary(summary string) bool {
	summary = strings.ToLower(strings.TrimSpace(summary))
	if summary == "" || !strings.Contains(summary, "mcp tool execution failed") {
		return false
	}
	return strings.Contains(summary, "client is closing") ||
		strings.Contains(summary, "connection closed: calling \"tools/call\"") ||
		strings.Contains(summary, "invalid character") ||
		strings.Contains(summary, "broken pipe") ||
		strings.Contains(summary, "eof")
}

func repeatedFatalToolErrorReply(toolName string) string {
	toolName = strings.TrimSpace(toolName)
	if toolName == "" {
		return "I hit repeated backend tool transport errors and stopped instead of retrying indefinitely. Please try again."
	}
	return fmt.Sprintf(
		"I hit repeated backend tool transport errors while using `%s` and stopped instead of retrying indefinitely. Please try again.",
		toolName,
	)
}

func fatalMCPServerErrorReply(serverName, toolName string) string {
	serverName = strings.TrimSpace(serverName)
	toolName = strings.TrimSpace(toolName)
	if serverName != "" {
		return fmt.Sprintf(
			"I hit a backend MCP transport error while using the `%s` server and stopped instead of trying workarounds. Please restart or fix that MCP server, then try again.",
			serverName,
		)
	}
	if toolName != "" {
		return fmt.Sprintf(
			"I hit a backend MCP transport error while using `%s` and stopped instead of trying workarounds. Please restart or fix that MCP server, then try again.",
			toolName,
		)
	}
	return "I hit a backend MCP transport error and stopped instead of trying workarounds. Please restart or fix that MCP server, then try again."
}

func (al *AgentLoop) applySyncToolResultDelivery(
	ctx context.Context,
	ts *turnState,
	result *tools.ToolResult,
	toolName string,
) ([]providers.Attachment, *tools.ToolResult) {
	return al.syncToolResultDelivery().applySyncToolResultDelivery(ctx, ts, result, toolName)
}

func mcpServerNameForTool(ts *turnState, toolName string) string {
	if ts == nil || ts.agent == nil || ts.agent.Tools == nil {
		return ""
	}
	tool, ok := ts.agent.Tools.Get(toolName)
	if !ok || tool == nil {
		return ""
	}
	mcpTool, ok := tool.(mcpServerTool)
	if !ok {
		return ""
	}
	return strings.TrimSpace(mcpTool.MCPServerName())
}

func inferSkillNamesFromToolCall(ts *turnState, toolName string, toolArgs map[string]any) []string {
	if ts == nil || toolName != "read_file" {
		return nil
	}

	rawPath, ok := toolArgs["path"].(string)
	if !ok {
		return nil
	}
	path := strings.TrimSpace(rawPath)
	if path == "" {
		return nil
	}

	cleanPath := filepath.Clean(path)
	if !filepath.IsAbs(cleanPath) {
		cleanPath = filepath.Join(ts.workspace, cleanPath)
	}
	if filepath.Base(cleanPath) != "SKILL.md" {
		return nil
	}

	var roots []string
	if ts.agent != nil && ts.agent.ContextBuilder != nil {
		roots = ts.agent.ContextBuilder.skillRoots()
	}
	if len(roots) == 0 && strings.TrimSpace(ts.workspace) != "" {
		roots = []string{filepath.Join(ts.workspace, "skills")}
	}

	found := make(map[string]struct{})
	for _, root := range roots {
		root = strings.TrimSpace(root)
		if root == "" {
			continue
		}
		rel, err := filepath.Rel(filepath.Clean(root), cleanPath)
		if err != nil {
			continue
		}
		if rel == "." || rel == "" || strings.HasPrefix(rel, "..") {
			continue
		}
		parts := strings.Split(rel, string(filepath.Separator))
		if len(parts) != 2 || parts[1] != "SKILL.md" {
			continue
		}

		skillName := strings.TrimSpace(parts[0])
		if skillName == "" {
			continue
		}
		if ts.agent != nil && ts.agent.ContextBuilder != nil {
			if canonical, ok := ts.agent.ContextBuilder.ResolveSkillName(skillName); ok {
				skillName = canonical
			}
		}
		found[skillName] = struct{}{}
	}

	if len(found) == 0 {
		return nil
	}

	names := make([]string, 0, len(found))
	for skillName := range found {
		names = append(names, skillName)
	}
	sort.Strings(names)
	return names
}

func shouldPublishAsyncToolResultToUser(result *tools.ToolResult) bool {
	return decideAsyncToolResultDelivery(result).PublishToUser
}

func shouldQueueAsyncToolResultForParent(result *tools.ToolResult) bool {
	return decideAsyncToolResultDelivery(result).QueueParent
}

func recordCompletionMedia(exec *turnExecution, store mediaResolver, refs []string) {
	if exec == nil || len(refs) == 0 {
		return
	}
	seen := make(map[string]struct{}, len(exec.completionMedia)+len(refs))
	for _, item := range exec.completionMedia {
		seen[item.Ref] = struct{}{}
	}
	for _, ref := range refs {
		ref = strings.TrimSpace(ref)
		if ref == "" {
			continue
		}
		if _, ok := seen[ref]; ok {
			continue
		}
		exec.completionMedia = append(exec.completionMedia, buildCompletionMedia(store, ref))
		seen[ref] = struct{}{}
	}
}

func buildCompletionMedia(store mediaResolver, ref string) tools.CompletionMedia {
	item := tools.CompletionMedia{Ref: ref}
	if store == nil {
		return item
	}
	_, meta, err := store.ResolveWithMeta(ref)
	if err != nil {
		return item
	}
	item.Filename = meta.Filename
	item.ContentType = meta.ContentType
	item.Type = inferMediaType(meta.Filename, meta.ContentType)
	return item
}

type toolLoopRunner struct {
	p         *Pipeline
	turnCtx   context.Context
	ts        *turnState
	exec      *turnExecution
	iteration int
	toolCalls []providers.ToolCall

	messages           []providers.Message
	handledAttachments []providers.Attachment
}

const queuedSteeringDeferredToolResult = "Deferred without execution because a newer user message arrived. " +
	"Reconcile this operation after reading the newer message: reissue it if it is still requested, " +
	"update it if the user corrected it, and omit it only if the user canceled or replaced it."

// ExecuteTools executes the tool loop, handling BeforeTool/ApproveTool/AfterTool hooks,
// tool execution with async callbacks, media delivery, and steering injection.
// Returns ToolControl indicating what the coordinator should do next:
//   - ToolControlContinue: all tool results handled, pendingMessages or steering exists, continue turn
//   - ToolControlBreak: tool loop exited, proceed to coordinator's hardAbort/finalContent/finalize
func (p *Pipeline) ExecuteTools(
	ctx context.Context,
	turnCtx context.Context,
	ts *turnState,
	exec *turnExecution,
	iteration int,
) ToolControl {
	normalizedToolCalls := exec.normalizedToolCalls
	runner := &toolLoopRunner{
		p:         p,
		turnCtx:   turnCtx,
		ts:        ts,
		exec:      exec,
		iteration: iteration,
		toolCalls: normalizedToolCalls,
		messages:  exec.messages,
	}

	ts.setPhase(TurnPhaseTools)
	runner.captureSteering(false)

toolLoop:
	for i, tc := range normalizedToolCalls {
		if ts.hardAbortRequested() {
			exec.abortedByHardAbort = true
			return ToolControlBreak
		}

		toolName := tc.Name
		toolArgs := cloneStringAnyMap(tc.Arguments)
		if p.Interaction.Hooks == nil && runner.skipPendingToolForInterrupt(tc, toolName, toolArgs) {
			continue
		}
		denyByTurnProfile := func() bool {
			if turnProfileToolAllowed(ts.profile, toolName) {
				return false
			}
			exec.allResponsesHandled = false
			denyContent := fmt.Sprintf("Tool %q is not allowed by the active turn profile.", toolName)
			p.emitEvent(
				runtimeevents.KindAgentToolExecSkipped,
				ts.eventMeta("runTurn", "turn.tool.skipped"),
				ToolExecSkippedPayload{
					ToolCallID: tc.ID,
					Tool:       toolName,
					Reason:     denyContent,
				},
			)
			deniedMsg := providers.Message{
				Role:       "tool",
				Content:    denyContent,
				ToolCallID: tc.ID,
			}
			runner.appendToolMessage(deniedMsg, toolMessagePersistOnly)
			return true
		}

		if denyByTurnProfile() {
			continue
		}
		if p.Interaction.Hooks != nil {
			toolReq, decision := p.Interaction.Hooks.BeforeTool(turnCtx, &ToolCallHookRequest{
				Meta:      ts.eventMeta("runTurn", "turn.tool.before"),
				Context:   cloneTurnContext(ts.turnCtx),
				Tool:      toolName,
				Arguments: toolArgs,
			})
			switch decision.normalizedAction() {
			case HookActionContinue, HookActionModify:
				if toolReq != nil {
					toolName = toolReq.Tool
					toolArgs = toolReq.Arguments
				}
			case HookActionRespond:
				if toolReq != nil && toolReq.HookResult != nil {
					hookResult := toolReq.HookResult
					runner.recordCommittedHookResponseDecision(tc, toolName)

					argsJSON, _ := json.Marshal(toolArgs)
					argsPreview := utils.Truncate(string(argsJSON), 200)
					logger.InfoCF("agent", fmt.Sprintf("Tool call (hook respond): %s(%s)", toolName, argsPreview),
						map[string]any{
							"agent_id":  ts.agent.ID,
							"tool":      toolName,
							"iteration": iteration,
						})

					p.emitEvent(
						runtimeevents.KindAgentToolExecStart,
						ts.eventMeta("runTurn", "turn.tool.start"),
						ToolExecStartPayload{
							ToolCallID: tc.ID,
							Tool:       toolName,
							Arguments:  cloneEventArguments(toolArgs),
						},
					)

					p.publishToolFeedbackForCall(turnCtx, ts, exec.response, tc, toolName, toolArgs, runner.messages)

					toolDuration := time.Duration(0)

					verifiedWrite := hasVerifiedWriteAudit(hookResult.WriteAudit)
					exec.writeAudit = appendTurnWriteAudit(exec.writeAudit, toolName, hookResult.WriteAudit)
					recordFinalRenderToolCall(exec, tc.ID, toolName, verifiedWrite)
					attachments, deliveredResult := p.applySyncToolResultDelivery(ctx, ts, hookResult, toolName)
					hookResult = deliveredResult
					runner.handledAttachments = append(runner.handledAttachments, attachments...)

					shouldSendForUser := !hookResult.ResponseHandled &&
						!ts.opts.SuppressToolUserDelivery &&
						!hookResult.Silent &&
						hookResult.ForUser != "" &&
						ts.opts.SendResponse
					if shouldSendForUser {
						p.Runtime.Bus.PublishOutbound(ctx, outboundMessageForTurn(ts, hookResult.ForUser))
					}

					if !hookResult.ResponseHandled {
						exec.allResponsesHandled = false
					}

					contentForLLM := p.filterToolContentForLLM(hookResult.ContentForLLM())
					var toolResultMedia []string
					if len(hookResult.Media) > 0 && !hookResult.ResponseHandled && !hookResult.ImmediateDelivery {
						recordCompletionMedia(exec, p.Context.MediaResolver, hookResult.Media)
						hookResult.ArtifactTags = buildArtifactTags(p.Context.MediaResolver, hookResult.Media)
						contentForLLM = p.filterToolContentForLLM(hookResult.ContentForLLM())
						toolResultMedia = append(toolResultMedia, hookResult.Media...)
					}
					_, semantics := p.beforeToolLoopDecision(ts, exec, toolName, toolArgs)
					loopDecision := p.afterToolLoopDecision(
						ts, exec, toolName, toolArgs, hookResult, contentForLLM, semantics,
					)
					contentForLLM = appendToolLoopGuidance(contentForLLM, loopDecision)
					toolResultMsg := providers.Message{
						Role:             "tool",
						Content:          contentForLLM,
						ToolCallID:       tc.ID,
						ToolResultStatus: toolResultContextStatus(hookResult),
						Media:            toolResultMedia,
					}

					p.emitEvent(
						runtimeevents.KindAgentToolExecEnd,
						ts.eventMeta("runTurn", "turn.tool.end"),
						ToolExecEndPayload{
							ToolCallID: tc.ID,
							Tool:       toolName,
							Duration:   toolDuration,
							ForLLMLen:  len(contentForLLM),
							ForUserLen: len(hookResult.ForUser),
							IsError:    hookResult.IsError,
							Async:      hookResult.Async,
							ResultHash: evaluationSafeHash(p.Cfg, contentForLLM),
						},
					)
					ts.recordToolExecution(
						toolName,
						!hookResult.IsError,
						toolErrorSummary(hookResult),
						inferSkillNamesFromToolCall(ts, toolName, toolArgs),
					)

					runner.appendToolMessage(toolResultMsg, toolMessagePersistAndIngest)
					if loopDecision.Action == loopguard.ActionHalt {
						runner.appendSkippedToolMessages(
							i+1,
							"tool loop hard stop",
							"Skipped because tool-loop protection stopped the current batch.",
						)
						break toolLoop
					}

					runner.captureAfterToolSteering(true)

					continue
				}
				logger.WarnCF("agent", "Hook returned respond action but no HookResult provided",
					map[string]any{
						"agent_id": ts.agent.ID,
						"tool":     toolName,
						"action":   "respond",
					})
			case HookActionDenyTool:
				exec.allResponsesHandled = false
				denyContent := hookDeniedToolContent("Tool execution denied by hook", decision.Reason)
				p.emitEvent(
					runtimeevents.KindAgentToolExecSkipped,
					ts.eventMeta("runTurn", "turn.tool.skipped"),
					ToolExecSkippedPayload{
						ToolCallID: tc.ID,
						Tool:       toolName,
						Reason:     denyContent,
					},
				)
				deniedMsg := providers.Message{
					Role:       "tool",
					Content:    denyContent,
					ToolCallID: tc.ID,
				}
				runner.appendToolMessage(deniedMsg, toolMessagePersistOnly)
				continue
			case HookActionAbortTurn:
				exec.abortedByHook = true
				return ToolControlBreak
			case HookActionHardAbort:
				_ = ts.requestHardAbort()
				exec.abortedByHardAbort = true
				return ToolControlBreak
			}
		}
		if p.Interaction.Hooks != nil && runner.skipPendingToolForInterrupt(tc, toolName, toolArgs) {
			continue
		}

		if p.Interaction.Hooks != nil || ts.opts.ApprovalGrant != nil {
			approval := ApprovalDecision{Approved: true}
			if p.Interaction.Hooks != nil {
				approval = p.Interaction.Hooks.ApproveTool(turnCtx, &ToolApprovalRequest{
					Meta:      ts.eventMeta("runTurn", "turn.tool.approve"),
					Context:   cloneTurnContext(ts.turnCtx),
					Tool:      toolName,
					Arguments: toolArgs,
				})
			} else {
				approval = ApprovalDecision{Reason: "approval policy is no longer available"}
			}
			interactionWorkspace := strings.TrimSpace(ts.opts.InteractionWorkspace)
			if interactionWorkspace == "" {
				interactionWorkspace = ts.workspace
			}
			if grant := ts.opts.ApprovalGrant; grant != nil {
				var consumeErr error
				if !approval.Approved && !approval.RequireHuman {
					consumeErr = fmt.Errorf("current approval policy denied execution: %s", approval.Reason)
				} else if p.Interaction.Suspension == nil {
					consumeErr = fmt.Errorf("human interaction suspension is unavailable in this runtime")
				} else {
					argumentHash, hashErr := interactions.HashArguments(interactionWorkspace, toolArgs)
					if hashErr != nil {
						consumeErr = hashErr
					} else {
						consumeErr = p.Interaction.Suspension.ConsumeApproval(
							ctx,
							ToolApprovalConsumptionRequest{
								Workspace: interactionWorkspace, InteractionID: grant.InteractionID,
								Revision: grant.Revision,
								Origin: interactions.Origin{
									ToolCallID: tc.ID, ToolName: toolName, ArgumentHash: argumentHash,
								},
							},
						)
					}
				}
				if consumeErr != nil {
					approval = ApprovalDecision{Reason: "one-time human approval was rejected: " + consumeErr.Error()}
				} else {
					ts.opts.ApprovalGrant = nil
					approval = ApprovalDecision{Approved: true}
				}
			}
			if approval.RequireHuman {
				argumentHash, hashErr := interactions.HashArguments(interactionWorkspace, toolArgs)
				if hashErr == nil {
					approvalAction, displayErr := renderApprovalAction(
						toolName,
						approval.ActionSummary,
					)
					if displayErr != nil {
						hashErr = displayErr
					} else {
						control, suspended, fallback := runner.trySuspendToolCall(
							ctx,
							i,
							tc,
							toolName,
							0,
							&tools.ToolResult{Silent: true, Suspension: &interactions.SuspensionRequest{
								Kind: interactions.KindApproval, PromptSummary: approval.ActionSummary,
								Timeout: time.Duration(approval.TimeoutSeconds) * time.Second,
							}},
							argumentHash,
							approvalAction,
						)
						if suspended {
							return control
						}
						if fallback != nil {
							hashErr = errors.New(fallback.ContentForLLM())
						}
					}
				}
				exec.allResponsesHandled = false
				denyContent := hookDeniedToolContent(
					"Tool execution denied because human approval could not be requested",
					errString(hashErr),
				)
				p.emitEvent(
					runtimeevents.KindAgentToolExecSkipped,
					ts.eventMeta("runTurn", "turn.tool.skipped"),
					ToolExecSkippedPayload{ToolCallID: tc.ID, Tool: toolName, Reason: denyContent},
				)
				runner.appendToolMessage(providers.Message{
					Role: "tool", Content: denyContent, ToolCallID: tc.ID,
				}, toolMessagePersistOnly)
				continue
			}
			if !approval.Approved {
				exec.allResponsesHandled = false
				denyContent := hookDeniedToolContent("Tool execution denied by approval hook", approval.Reason)
				p.emitEvent(
					runtimeevents.KindAgentToolExecSkipped,
					ts.eventMeta("runTurn", "turn.tool.skipped"),
					ToolExecSkippedPayload{
						ToolCallID: tc.ID,
						Tool:       toolName,
						Reason:     denyContent,
					},
				)
				deniedMsg := providers.Message{
					Role:       "tool",
					Content:    denyContent,
					ToolCallID: tc.ID,
				}
				runner.appendToolMessage(deniedMsg, toolMessagePersistOnly)
				continue
			}
		}

		if denyByTurnProfile() {
			continue
		}

		loopDecision, toolSemantics := p.beforeToolLoopDecision(ts, exec, toolName, toolArgs)
		if !loopDecision.AllowsExecution() {
			p.emitToolLoopDecision(ts, loopDecision)
			blockedResult := blockedToolLoopResult(loopDecision)
			blockedContent := p.filterToolContentForLLM(blockedResult.ContentForLLM())
			p.emitEvent(
				runtimeevents.KindAgentToolExecSkipped,
				ts.eventMeta("runTurn", "turn.tool.skipped"),
				ToolExecSkippedPayload{ToolCallID: tc.ID, Tool: toolName, Reason: loopDecision.Code},
			)
			runner.appendToolMessage(providers.Message{
				Role: "tool", Content: blockedContent, ToolCallID: tc.ID,
			}, toolMessagePersistAndIngest)
			exec.allResponsesHandled = false
			runner.captureAfterToolSteering(false)
			continue
		}

		argsJSON, _ := json.Marshal(toolArgs)
		argsPreview := utils.Truncate(string(argsJSON), 200)
		logger.InfoCF("agent", fmt.Sprintf("Tool call: %s(%s)", toolName, argsPreview),
			map[string]any{
				"agent_id":  ts.agent.ID,
				"tool":      toolName,
				"iteration": iteration,
			})
		p.emitEvent(
			runtimeevents.KindAgentToolExecStart,
			ts.eventMeta("runTurn", "turn.tool.start"),
			ToolExecStartPayload{
				ToolCallID: tc.ID,
				Tool:       toolName,
				Arguments:  cloneEventArguments(toolArgs),
			},
		)

		p.publishToolFeedbackForCall(turnCtx, ts, exec.response, tc, toolName, toolArgs, runner.messages)

		toolCallID := tc.ID
		asyncToolName := toolName
		mcpServerName := mcpServerNameForTool(ts, toolName)
		var asyncAckDelivery AsyncDeliveryDecision
		if tool, ok := ts.agent.Tools.Get(toolName); ok {
			if _, isAsync := tool.(tools.AsyncExecutor); isAsync {
				if deliveryMode, err := asyncDeliveryModeFromToolArgs(toolName, toolArgs); err == nil {
					asyncAckDelivery = decideAsyncToolResultDelivery(
						tools.AsyncResult("").WithAsyncDelivery(deliveryMode),
					)
				}
			}
		}
		asyncCallback := func(_ context.Context, result *tools.ToolResult) {
			completionID := asyncCompletionID(ts.turnID, toolCallID, asyncToolName)
			delivery := decideAsyncToolResultDelivery(result)
			p.emitEvent(
				runtimeevents.KindAgentAsyncCompletion,
				ts.scope.meta(iteration, "runTurn", "turn.async.completion"),
				AsyncCompletionPayload{
					SourceTool:   asyncToolName,
					CompletionID: completionID,
					TaskID:       delivery.TaskID,
					DeliveryMode: string(delivery.DeliveryMode),
					ContentLen:   delivery.ContentLen,
					ForUserLen:   delivery.ForUserLen,
					MediaCount:   delivery.MediaCount,
					IsError:      delivery.IsError,
					WillUser:     delivery.PublishToUser,
					WillParent:   delivery.QueueParent,
				},
			)
			if result != nil && result.IsError {
				p.deliverAsyncToolCompletion(AsyncDeliveryRequest{
					TurnState:    ts,
					ToolName:     asyncToolName,
					CompletionID: completionID,
					Result:       result,
					Decision:     delivery,
				})
				return
			}
			p.deliverAsyncToolCompletion(AsyncDeliveryRequest{
				TurnState:    ts,
				ToolName:     asyncToolName,
				CompletionID: completionID,
				Result:       result,
				Decision:     delivery,
			})
		}

		toolStart := time.Now()
		execCtx := tools.WithToolInboundContext(
			turnCtx,
			ts.channel,
			ts.chatID,
			ts.opts.Dispatch.MessageID(),
			ts.opts.Dispatch.ReplyToMessageID(),
		)
		if ts.opts.Dispatch.InboundContext != nil {
			execCtx = tools.WithToolInboundMetadata(execCtx, *ts.opts.Dispatch.InboundContext)
		}
		execCtx = tools.WithToolTopicID(execCtx, originTopicID(ts.opts.Dispatch.InboundContext))
		execCtx = tools.WithToolSessionContext(
			execCtx,
			ts.agent.ID,
			ts.sessionKey,
			ts.opts.Dispatch.SessionScope,
		)
		execCtx = tools.WithToolRouteSessionKey(execCtx, ts.opts.Dispatch.RouteSessionKey)
		toolResult := ts.agent.Tools.ExecuteWithContext(
			execCtx,
			toolName,
			toolArgs,
			ts.channel,
			ts.chatID,
			asyncCallback,
		)
		if toolResult != nil && toolResult.Async && asyncAckDelivery.ParentHandled {
			toolResult.ResponseHandled = true
		}
		toolDuration := time.Since(toolStart)

		if ts.hardAbortRequested() {
			exec.abortedByHardAbort = true
			return ToolControlBreak
		}

		if p.Interaction.Hooks != nil {
			toolResp, decision := p.Interaction.Hooks.AfterTool(turnCtx, &ToolResultHookResponse{
				Meta:      ts.eventMeta("runTurn", "turn.tool.after"),
				Context:   cloneTurnContext(ts.turnCtx),
				Tool:      toolName,
				Arguments: toolArgs,
				Result:    toolResult,
				Duration:  toolDuration,
			})
			switch decision.normalizedAction() {
			case HookActionContinue, HookActionModify:
				if toolResp != nil {
					if toolResp.Tool != "" {
						toolName = toolResp.Tool
					}
					if toolResp.Result != nil {
						toolResult = toolResp.Result
					}
				}
			case HookActionAbortTurn:
				exec.abortedByHook = true
				return ToolControlBreak
			case HookActionHardAbort:
				_ = ts.requestHardAbort()
				exec.abortedByHardAbort = true
				return ToolControlBreak
			}
		}

		if toolResult == nil {
			toolResult = tools.ErrorResult("hook returned nil tool result")
		}
		if toolResult.Suspension != nil {
			control, suspended, fallback := runner.trySuspendToolCall(
				ctx,
				i,
				tc,
				toolName,
				toolDuration,
				toolResult,
				"",
				"",
			)
			if suspended {
				return control
			}
			toolResult = fallback
		}

		verifiedWrite := hasVerifiedWriteAudit(toolResult.WriteAudit)
		toolSummary := strings.TrimSpace(toolResult.ForUser)
		if toolSummary != "" {
			exec.actionLog = appendTurnActionRecord(
				exec.actionLog,
				"tool_result",
				toolName,
				toolSummary,
				toolResult.IsError,
				verifiedWrite,
			)
		}

		exec.writeAudit = appendTurnWriteAudit(exec.writeAudit, toolName, toolResult.WriteAudit)
		recordFinalRenderToolCall(exec, toolCallID, toolName, verifiedWrite)
		attachments, deliveredResult := p.applySyncToolResultDelivery(ctx, ts, toolResult, toolName)
		toolResult = deliveredResult
		runner.handledAttachments = append(runner.handledAttachments, attachments...)

		if len(toolResult.Media) > 0 && !toolResult.ResponseHandled && !toolResult.ImmediateDelivery {
			recordCompletionMedia(exec, p.Context.MediaResolver, toolResult.Media)
			toolResult.ArtifactTags = buildArtifactTags(p.Context.MediaResolver, toolResult.Media)
		}

		if !toolResult.ResponseHandled {
			exec.allResponsesHandled = false
		}

		shouldSendForUser := !toolResult.ResponseHandled &&
			!ts.opts.SuppressToolUserDelivery &&
			!toolResult.Silent &&
			toolResult.ForUser != "" &&
			ts.opts.SendResponse
		if shouldSendForUser {
			p.Runtime.Bus.PublishOutbound(ctx, outboundMessageForTurn(ts, toolResult.ForUser))
			logger.DebugCF("agent", "Sent tool result to user",
				map[string]any{
					"tool":        toolName,
					"content_len": len(toolResult.ForUser),
				})
		}
		contentForLLM := p.filterToolContentForLLM(toolResult.ContentForLLM())
		loopDecision = p.afterToolLoopDecision(
			ts, exec, toolName, toolArgs, toolResult, contentForLLM, toolSemantics,
		)
		contentForLLM = appendToolLoopGuidance(contentForLLM, loopDecision)

		toolResultMsg := providers.Message{
			Role:             "tool",
			Content:          contentForLLM,
			ToolCallID:       toolCallID,
			ToolResultStatus: toolResultContextStatus(toolResult),
		}
		if len(toolResult.Media) > 0 && !toolResult.ResponseHandled {
			toolResultMsg.Media = append(toolResultMsg.Media, toolResult.Media...)
		}
		p.emitEvent(
			runtimeevents.KindAgentToolExecEnd,
			ts.eventMeta("runTurn", "turn.tool.end"),
			ToolExecEndPayload{
				ToolCallID: toolCallID,
				Tool:       toolName,
				Duration:   toolDuration,
				ForLLMLen:  len(contentForLLM),
				ForUserLen: len(toolResult.ForUser),
				IsError:    toolResult.IsError,
				Async:      toolResult.Async,
				ResultHash: evaluationSafeHash(p.Cfg, contentForLLM),
			},
		)
		ts.recordToolExecution(
			toolName,
			!toolResult.IsError,
			toolErrorSummary(toolResult),
			inferSkillNamesFromToolCall(ts, toolName, toolArgs),
		)

		if toolResult.IsError {
			errSummary := toolErrorSummary(toolResult)
			if isFatalMCPTransportErrorSummary(errSummary) {
				if mcpServerName != "" {
					logger.WarnCF("agent", "Fatal MCP server transport error; aborting turn to avoid workaround loop",
						map[string]any{
							"agent_id":   ts.agent.ID,
							"iteration":  iteration,
							"tool":       toolName,
							"mcp_server": mcpServerName,
							"error":      errSummary,
							"session_id": ts.sessionKey,
						})
					exec.finalContent = fatalMCPServerErrorReply(mcpServerName, toolName)
					exec.allResponsesHandled = false
					runner.appendToolMessage(toolResultMsg, toolMessagePersistAndIngest)
					exec.messages = runner.messages
					return ToolControlBreak
				}
				streak := ts.recentToolExecutionErrorStreak(toolName, func(rec ToolExecutionRecord) bool {
					return isFatalMCPTransportErrorSummary(rec.ErrorSummary)
				})
				if streak >= repeatedFatalToolErrorStreakLimit {
					logger.WarnCF("agent", "Repeated fatal tool transport errors; aborting turn to avoid retry loop",
						map[string]any{
							"agent_id":   ts.agent.ID,
							"iteration":  iteration,
							"tool":       toolName,
							"error":      errSummary,
							"streak":     streak,
							"session_id": ts.sessionKey,
						})
					exec.finalContent = repeatedFatalToolErrorReply(toolName)
					exec.allResponsesHandled = false
					runner.appendToolMessage(toolResultMsg, toolMessagePersistAndIngest)
					exec.messages = runner.messages
					return ToolControlBreak
				}
			}
		}
		runner.appendToolMessage(toolResultMsg, toolMessagePersistAndIngest)
		if loopDecision.Action == loopguard.ActionHalt {
			runner.appendSkippedToolMessages(
				i+1,
				"tool loop hard stop",
				"Skipped because tool-loop protection stopped the current batch.",
			)
			break toolLoop
		}

		runner.captureAfterToolSteering(false)
	}

	exec.messages = runner.messages

	// Continue if pending steering exists (regardless of allResponsesHandled).
	// This covers the case where tools were partially executed and skipped due to steering,
	// but one tool had ResponseHandled=false (so allResponsesHandled=false).
	if len(exec.pendingMessages) > 0 {
		exec.markAdditionalUserInputObserved()
		logger.InfoCF("agent", "Pending steering after partial tool execution; continuing turn",
			map[string]any{
				"agent_id":            ts.agent.ID,
				"pending_count":       len(exec.pendingMessages),
				"allResponsesHandled": exec.allResponsesHandled,
			})
		exec.allResponsesHandled = false
		return ToolControlContinue
	}

	// Poll for newly arrived steering
	if steerMsgs := p.dequeueSteeringMessagesForTurn(ts); len(
		steerMsgs,
	) > 0 {
		exec.markSteeringObserved()
		logger.InfoCF("agent", "Steering arrived after tool delivery; continuing turn",
			map[string]any{
				"agent_id":       ts.agent.ID,
				"steering_count": len(steerMsgs),
			})
		exec.pendingMessages = append(exec.pendingMessages, steerMsgs...)
		exec.allResponsesHandled = false
		return ToolControlContinue
	}

	// No pending steering: finalize or break depending on allResponsesHandled
	if p.shouldFinalizeAfterToolLoop(exec) {
		logger.InfoCF(
			"agent",
			"Tool loop completed; rendering terminal reply from accumulated turn context",
			map[string]any{
				"agent_id":   ts.agent.ID,
				"iteration":  iteration,
				"tool_count": len(normalizedToolCalls),
			},
		)
		return ToolControlFinalize
	}

	if exec.allResponsesHandled {
		summaryMsg := providers.Message{
			Role:        "assistant",
			Content:     handledToolResponseSummary,
			Attachments: append([]providers.Attachment(nil), runner.handledAttachments...),
		}
		if !ts.opts.NoHistory {
			writeErr := persistFullSessionMessage(ts.agent.Sessions, ts.sessionKey, summaryMsg)
			if writeErr == nil {
				ts.recordPersistedMessage(summaryMsg)
			}
			p.ingestMessage(turnCtx, ts, summaryMsg, writeErr)
			if err := ts.agent.Sessions.Save(ts.sessionKey); err != nil {
				logger.WarnCF("agent", "Failed to save session after tool delivery",
					map[string]any{
						"agent_id": ts.agent.ID,
						"error":    err.Error(),
					})
			}
		}
		if !ts.opts.NoHistory && ts.opts.EnableSummary {
			p.Context.Runtime.Compact(turnCtx, &CompactRequest{
				Agent:      ts.agent,
				SessionKey: ts.sessionKey,
				Reason:     ContextCompressReasonSummarize,
				Budget:     ts.agent.ContextWindow,
			})
		}
		ts.setPhase(TurnPhaseCompleted)
		ts.setFinalContent("")
		p.dismissToolFeedbackForTurn(ctx, ts)
		logger.InfoCF("agent", "Tool output satisfied delivery; ending turn without follow-up LLM",
			map[string]any{
				"agent_id":   ts.agent.ID,
				"iteration":  iteration,
				"tool_count": len(normalizedToolCalls),
			})
		return ToolControlBreak
	}

	// allResponsesHandled=false and no pending steering: continue so coordinator
	// makes another LLM call. The tool result is in messages and the LLM will
	// return it as finalContent in the next iteration.
	ts.agent.Tools.TickTTL()
	logger.DebugCF("agent", "TTL tick after tool execution", map[string]any{
		"agent_id": ts.agent.ID, "iteration": iteration,
	})
	return ToolControlContinue
}

func toolResultContextStatus(result *tools.ToolResult) providers.ToolResultStatus {
	if result == nil || result.Async {
		return providers.ToolResultStatusUnresolved
	}
	if result.IsError {
		return providers.ToolResultStatusError
	}
	return providers.ToolResultStatusSuccess
}

type toolMessageIngestMode int

const (
	toolMessagePersistOnly toolMessageIngestMode = iota
	toolMessagePersistAndIngest
)

func (r *toolLoopRunner) appendToolMessage(msg providers.Message, ingest toolMessageIngestMode) {
	r.messages = append(r.messages, msg)
	if r.ts == nil || r.ts.opts.NoHistory {
		return
	}
	writeErr := persistFullSessionMessage(r.ts.agent.Sessions, r.ts.sessionKey, msg)
	if writeErr == nil {
		r.ts.recordPersistedMessage(msg)
	}
	if ingest == toolMessagePersistAndIngest && r.p != nil {
		r.p.ingestMessage(r.turnCtx, r.ts, msg, writeErr)
	} else if writeErr != nil {
		logger.WarnCF("agent", "Canonical tool message write failed", map[string]any{
			"session_key": r.ts.sessionKey,
			"error":       writeErr.Error(),
		})
	}
}

func (r *toolLoopRunner) captureAfterToolSteering(markAdditionalSteering bool) {
	r.captureSteering(markAdditionalSteering)
	r.appendPendingSubTurnResult()
}

func (r *toolLoopRunner) captureSteering(markAdditional bool) {
	steerMsgs := r.p.dequeueSteeringMessagesForTurn(r.ts)
	if len(steerMsgs) == 0 {
		return
	}
	if markAdditional {
		r.exec.markAdditionalUserInputObserved()
	} else {
		r.exec.markSteeringObserved()
	}
	r.exec.pendingMessages = append(r.exec.pendingMessages, steerMsgs...)
}

func (r *toolLoopRunner) pendingInterruptCause() string {
	cause := ""
	if len(r.exec.pendingMessages) > 0 {
		cause = "queued_user_steering"
	} else if gracefulPending, _ := r.ts.gracefulInterruptRequested(); gracefulPending {
		cause = "graceful_interrupt"
	}
	return cause
}

func (r *toolLoopRunner) skipPendingToolForInterrupt(
	tc providers.ToolCall,
	toolName string,
	toolArgs map[string]any,
) bool {
	cause := r.pendingInterruptCause()
	if cause == "" {
		return false
	}

	safety := tools.SteeringSafetyUnknown
	if r.ts != nil && r.ts.agent != nil && r.ts.agent.Tools != nil {
		safety = r.ts.agent.Tools.SteeringSafety(toolName, toolArgs)
	}
	decision := "skip"
	if cause == "queued_user_steering" &&
		(safety == tools.SteeringSafetyReadOnly || safety == tools.SteeringSafetyNonCancellable) {
		decision = "finish"
	}
	r.p.emitEvent(
		runtimeevents.KindAgentToolSteeringDecision,
		r.ts.eventMeta("runTurn", "turn.tool.steering_decision"),
		ToolSteeringDecisionPayload{
			ToolCallID: tc.ID, Tool: toolName, Classification: string(safety), Decision: decision, Cause: cause,
		},
	)
	if decision == "finish" {
		return false
	}

	reason := "queued user steering message"
	content := queuedSteeringDeferredToolResult
	if cause == "graceful_interrupt" {
		reason = "graceful interrupt requested"
		content = "Skipped due to graceful interrupt."
	}
	skippedTC := tc
	skippedTC.Name = toolName
	r.appendSkippedToolMessage(skippedTC, reason, content)
	return true
}

func (r *toolLoopRunner) recordCommittedHookResponseDecision(tc providers.ToolCall, toolName string) {
	cause := r.pendingInterruptCause()
	if cause == "" {
		return
	}
	r.p.emitEvent(
		runtimeevents.KindAgentToolSteeringDecision,
		r.ts.eventMeta("runTurn", "turn.tool.steering_decision"),
		ToolSteeringDecisionPayload{
			ToolCallID: tc.ID, Tool: toolName, Classification: string(tools.SteeringSafetyNonCancellable),
			Decision: "finish", Cause: cause,
		},
	)
}

func (r *toolLoopRunner) appendPendingSubTurnResult() {
	if r.ts.pendingResults == nil {
		return
	}
	if result, ok := r.ts.dequeuePendingResult(); ok && result != nil && result.ForLLM != "" {
		content := r.p.filterPendingResultForLLM(result.ForLLM)
		msg := subTurnResultPromptMessage(content)
		r.appendInjectedTurnMessage(msg)
	}
}

func (r *toolLoopRunner) appendInjectedTurnMessage(msg providers.Message) {
	r.messages = append(r.messages, msg)
	if r.ts == nil || r.ts.opts.NoHistory {
		return
	}
	writeErr := persistFullSessionMessage(r.ts.agent.Sessions, r.ts.sessionKey, msg)
	if writeErr == nil {
		r.ts.recordPersistedMessage(msg)
	}
	if r.p != nil {
		r.p.ingestMessage(r.turnCtx, r.ts, msg, writeErr)
	}
}

func (r *toolLoopRunner) appendSkippedToolMessages(start int, reason string, content string) {
	for i := start; i < len(r.toolCalls); i++ {
		r.appendSkippedToolMessage(r.toolCalls[i], reason, content)
	}
}

func (r *toolLoopRunner) trySuspendToolCall(
	ctx context.Context,
	callIndex int,
	toolCall providers.ToolCall,
	toolName string,
	duration time.Duration,
	result *tools.ToolResult,
	argumentHash string,
	approvalAction string,
) (ToolControl, bool, *tools.ToolResult) {
	if result == nil || result.Suspension == nil {
		return ToolControlContinue, false, result
	}

	// A newer user message wins if it arrived before durable suspension. Pair
	// every call in the current batch and let the next iteration reconcile it.
	r.captureSteering(false)
	if len(r.exec.pendingMessages) > 0 {
		r.appendToolMessage(providers.Message{
			Role:       "tool",
			Content:    queuedSteeringDeferredToolResult,
			ToolCallID: toolCall.ID,
		}, toolMessagePersistAndIngest)
		r.appendSkippedToolMessages(
			callIndex+1,
			"newer user message arrived before input suspension",
			queuedSteeringDeferredToolResult,
		)
		r.exec.messages = r.messages
		r.exec.allResponsesHandled = false
		return ToolControlContinue, true, nil
	}

	fallback := func(message string) (ToolControl, bool, *tools.ToolResult) {
		return ToolControlContinue, false, tools.ErrorResult(message)
	}
	if r.ts == nil || r.ts.opts.NoHistory {
		return fallback("request_user_input requires durable session history")
	}
	if !r.exec.assistantToolCallsPersisted {
		message := "cannot suspend because the originating assistant tool call was not persisted"
		if r.exec.assistantToolCallsWriteErr != nil {
			message += ": " + r.exec.assistantToolCallsWriteErr.Error()
		}
		return fallback(message)
	}
	if r.p == nil || r.p.Interaction.Suspension == nil {
		return fallback("human interaction suspension is unavailable in this runtime")
	}
	if err := interactions.ValidateSuspensionRequest(*result.Suspension); err != nil {
		return fallback(err.Error())
	}

	inbound := r.ts.opts.Dispatch.InboundContext
	interactionSessionKey := strings.TrimSpace(r.ts.opts.InteractionSessionKey)
	if interactionSessionKey == "" {
		interactionSessionKey = r.ts.sessionKey
	}
	interactionRouteKey := strings.TrimSpace(r.ts.opts.InteractionRouteKey)
	if interactionRouteKey == "" {
		interactionRouteKey = r.ts.opts.Dispatch.RouteSessionKey
	}
	interactionWorkspace := strings.TrimSpace(r.ts.opts.InteractionWorkspace)
	if interactionWorkspace == "" {
		interactionWorkspace = r.ts.workspace
	}
	route := interactions.Route{
		AgentID:         r.ts.agent.ID,
		SessionKey:      interactionSessionKey,
		RouteSessionKey: interactionRouteKey,
		Channel:         r.ts.channel,
		ChatID:          r.ts.chatID,
		SenderID:        r.ts.opts.Dispatch.SenderID(),
	}
	if inbound != nil {
		route.AccountID = inbound.Account
		route.ChatType = inbound.ChatType
		route.TopicID = inbound.TopicID
		route.SpaceID = inbound.SpaceID
		route.SpaceType = inbound.SpaceType
	}
	disposition, err := r.p.Interaction.Suspension.SuspendToolCall(ctx, ToolSuspensionRequest{
		Workspace:        interactionWorkspace,
		Prompt:           *result.Suspension,
		Route:            route,
		ApprovalAction:   strings.TrimSpace(approvalAction),
		ExecutionContext: cloneInboundContext(inbound),
		Origin: interactions.Origin{
			TurnID:                 r.ts.turnID,
			ToolCallID:             toolCall.ID,
			ToolName:               toolName,
			TaskID:                 r.ts.opts.TaskID,
			ContinuationSessionKey: r.ts.sessionKey,
			ArgumentHash:           strings.TrimSpace(argumentHash),
		},
	})
	if !disposition.Durable {
		if err == nil {
			err = fmt.Errorf("suspension manager returned without durable ownership")
		}
		return fallback("failed to suspend for human input: " + err.Error())
	}
	if err != nil {
		logger.WarnCF("agent", "Human interaction persisted with pending delivery recovery", map[string]any{
			"agent_id":       r.ts.agent.ID,
			"interaction_id": disposition.InteractionID,
			"error":          err.Error(),
		})
	}

	r.appendSkippedToolMessages(
		callIndex+1,
		"tool batch suspended for human input",
		"Deferred until the pending human input is resolved. Reissue this tool if it is still needed.",
	)
	r.exec.messages = r.messages
	r.exec.suspendedInteractionID = disposition.InteractionID
	r.p.emitEvent(
		runtimeevents.KindAgentToolExecEnd,
		r.ts.eventMeta("runTurn", "turn.tool.suspended"),
		ToolExecEndPayload{
			ToolCallID:    toolCall.ID,
			Tool:          toolName,
			Duration:      duration,
			Suspended:     true,
			InteractionID: disposition.InteractionID,
		},
	)
	return ToolControlSuspend, true, nil
}

func (r *toolLoopRunner) appendSkippedToolMessage(
	skippedTC providers.ToolCall,
	reason string,
	content string,
) {
	r.p.emitEvent(
		runtimeevents.KindAgentToolExecSkipped,
		r.ts.eventMeta("runTurn", "turn.tool.skipped"),
		ToolExecSkippedPayload{
			ToolCallID: skippedTC.ID,
			Tool:       skippedTC.Name,
			Reason:     reason,
		},
	)
	skippedMsg := providers.Message{
		Role:       "tool",
		Content:    content,
		ToolCallID: skippedTC.ID,
	}
	r.appendToolMessage(skippedMsg, toolMessagePersistOnly)
}
