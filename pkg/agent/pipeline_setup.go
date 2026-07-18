// PicoClaw - Ultra-lightweight personal AI agent

package agent

import (
	"context"
	"fmt"
	"strings"

	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
)

// SetupTurn extracts the one-time initialization phase, returning a
// turnExecution populated with history, messages, and candidate selection.
// It replaces lines 56-145 of the original runTurn.
func (p *Pipeline) SetupTurn(ctx context.Context, ts *turnState) (*turnExecution, error) {
	maxMediaSize := p.maxMediaSize()

	contextualSkills := ts.activeSkills
	if ts.agent.ContextBuilder != nil {
		contextualSkills = ts.agent.ContextBuilder.ResolveActiveSkillsForContext(ts.activeSkills)
	}
	toolDefs := filterToolsByTurnProfile(ts.agent.Tools.ToProviderDefs(), ts.profile)
	reserveTokens := p.estimateNonHistoryPromptReserve(ts, contextualSkills, toolDefs, maxMediaSize)

	var history []providers.Message
	var summary string
	var budgetReport *ContextBudgetReport
	if !ts.opts.NoHistory {
		resp, err := p.Context.Runtime.Assemble(ctx, &AssembleRequest{
			SessionKey:    ts.sessionKey,
			Budget:        ts.agent.ContextWindow,
			MaxTokens:     ts.agent.MaxTokens,
			ReserveTokens: reserveTokens,
		})
		if err != nil {
			return nil, fmt.Errorf("assemble context: %w", err)
		}
		if resp != nil {
			history = resp.History
			summary = resp.Summary
			budgetReport = resp.Budget
		}
	}
	ts.captureRestorePoint(history, summary)

	ts.recordSkillContextSnapshot(skillContextTriggerInitialBuild, contextualSkills)
	messages := p.buildTurnMessages(
		ts,
		history,
		summary,
		ts.userMessage,
		ts.media,
		contextualSkills,
	)

	messages = resolveMediaRefs(messages, p.Context.MediaResolver, maxMediaSize)

	if !ts.opts.NoHistory {
		if budgetReport != nil && len(budgetReport.PressureReasons) > 0 {
			p.emitAbsoluteBudgetPressure(ts, budgetReport, len(history))
			if budgetReport.NeedsCompaction {
				p.scheduleBackgroundCompaction(
					ts.agent,
					ts.sessionKey,
					ContextCompressReasonProactive,
					budgetReport.AvailableContext,
					"absolute_budget_pressure",
				)
			}
		}
		if isOverContextBudget(ts.agent.ContextWindow, messages, toolDefs, ts.agent.MaxTokens) {
			compactBudget := effectiveHistoryBudget(
				ts.agent.ContextWindow,
				ts.agent.MaxTokens,
				reserveTokens,
			)
			logger.WarnCF("agent", "Proactive context pressure: scheduling background compaction",
				map[string]any{
					"session_key":    ts.sessionKey,
					"context_window": ts.agent.ContextWindow,
					"max_tokens":     ts.agent.MaxTokens,
					"reserve_tokens": reserveTokens,
					"compact_budget": compactBudget,
				})
			p.scheduleBackgroundCompaction(
				ts.agent,
				ts.sessionKey,
				ContextCompressReasonProactive,
				compactBudget,
				"proactive_pressure",
			)
			originalHistoryCount := len(history)
			var fit bool
			history, messages, fit = trimHistoryToFitContextWindow(
				history,
				func(trimmedHistory []providers.Message) []providers.Message {
					rebuilt := p.buildTurnMessages(
						ts,
						trimmedHistory,
						summary,
						ts.userMessage,
						ts.media,
						contextualSkills,
					)
					return resolveMediaRefs(rebuilt, p.Context.MediaResolver, maxMediaSize)
				},
				ts.agent.ContextWindow,
				toolDefs,
				ts.agent.MaxTokens,
			)
			if dropped := originalHistoryCount - len(history); dropped > 0 {
				logger.WarnCF(
					"agent",
					"Trimmed rebuilt history after proactive compaction",
					map[string]any{
						"session_key":     ts.sessionKey,
						"dropped_msgs":    dropped,
						"remaining_msgs":  len(history),
						"context_window":  ts.agent.ContextWindow,
						"max_tokens":      ts.agent.MaxTokens,
						"still_overlimit": !fit,
					},
				)
			} else if !fit {
				logger.WarnCF("agent", "Context still exceeds budget "+
					"after proactive compaction rebuild", map[string]any{
					"session_key":    ts.sessionKey,
					"history_msgs":   len(history),
					"context_window": ts.agent.ContextWindow,
					"max_tokens":     ts.agent.MaxTokens,
				})
			}
			if !fit {
				return nil, fmt.Errorf(
					"context window still exceeded after proactive compaction; refusing oversized LLM request",
				)
			}
		}
	}

	if !ts.opts.NoHistory && (strings.TrimSpace(ts.userMessage) != "" || len(ts.media) > 0) {
		rootMsg := userPromptMessage(ts.userMessage, ts.media)
		var writeErr error
		if len(rootMsg.Media) > 0 {
			writeErr = persistFullSessionMessage(ts.agent.Sessions, ts.sessionKey, rootMsg)
		} else {
			writeErr = persistSessionMessage(ts.agent.Sessions, ts.sessionKey, rootMsg.Role, rootMsg.Content)
		}
		if writeErr == nil {
			ts.recordPersistedMessage(rootMsg)
		}
		p.ingestMessage(ctx, ts, rootMsg, writeErr)
	}

	execution := ts.model.ExecutionState()

	selection := p.Context.ModelExecution.selectCandidates(
		execution,
		ts.userMessage,
		messages,
		ts.model.RouteSessionKey,
	)
	activeProvider := execution.Provider
	if selection.usedLight && execution.LightProvider != nil {
		activeProvider = execution.LightProvider
	}
	activeModelName := strings.TrimSpace(execution.Model)
	if selection.usedLight {
		activeModelName = strings.TrimSpace(
			resolvedCandidateModelName(execution.LightCandidates, activeModelName),
		)
	}
	activeModelName = resolvedCandidateModelName(selection.activeCandidates, activeModelName)

	exec := newTurnExecution(
		ts.agent,
		ts.opts,
		history,
		summary,
		messages,
	)
	exec.model.selectedCandidates = selection.selectedCandidates
	exec.model.activeCandidates = selection.activeCandidates
	exec.model.activeModel = selection.model
	exec.model.activeModelConfig = p.activeModelConfig(
		ts.agent.Workspace,
		selection.activeCandidates,
		selection.model,
	)
	exec.model.llmModelName = activeModelName
	exec.model.activeProvider = activeProvider
	exec.model.candidateProviders = execution.CandidateProviders
	exec.model.cleanup = nil
	exec.model.usedLight = selection.usedLight
	exec.model.autoFallback = true
	exec.model.visionRoute = visionRouteSameModel

	routedExecution := execution
	routedExecution.Model = selection.model
	routedExecution.Provider = activeProvider
	routedExecution.Candidates = append(
		[]providers.FallbackCandidate(nil),
		selection.activeCandidates...)
	routedExecution.CandidateProviders = cloneCandidateProviderMap(execution.CandidateProviders)

	visionExecution, visionCleanup, visionRoute, usedVisionOverride, err := p.Context.ModelExecution.maybeBuildVisionExecutionState(
		ts.agent,
		routedExecution,
		messages,
	)
	if err != nil {
		return nil, err
	}
	if usedVisionOverride {
		exec.model.selectedCandidates = append(
			[]providers.FallbackCandidate(nil),
			visionExecution.Candidates...)
		exec.model.activeCandidates = append(
			[]providers.FallbackCandidate(nil),
			visionExecution.Candidates...)
		exec.model.activeModel = resolvedCandidateModel(
			visionExecution.Candidates,
			visionExecution.Model,
		)
		exec.model.activeModelConfig = p.activeModelConfig(
			ts.agent.Workspace,
			visionExecution.Candidates,
			visionExecution.Model,
		)
		exec.model.llmModelName = resolvedCandidateModelName(
			visionExecution.Candidates,
			strings.TrimSpace(visionExecution.Model),
		)
		exec.model.activeProvider = visionExecution.Provider
		exec.model.candidateProviders = visionExecution.CandidateProviders
		exec.model.cleanup = visionCleanup
		exec.model.autoFallback = false
		exec.model.visionRoute = visionRoute
	}

	return exec, nil
}

func effectiveHistoryBudget(contextWindow, maxTokens, reserveTokens int) int {
	budget := contextWindow - maxTokens - reserveTokens
	if budget > 0 {
		return budget
	}
	return 0
}

func (p *Pipeline) emitAbsoluteBudgetPressure(
	ts *turnState,
	report *ContextBudgetReport,
	remainingMessages int,
) {
	if ts == nil || report == nil {
		return
	}
	p.emitEvent(
		runtimeevents.KindAgentContextCompress,
		ts.eventMeta("SetupTurn", "turn.context.absolute_budget"),
		ContextCompressPayload{
			Reason:                   ContextCompressReasonProactive,
			RemainingMessages:        remainingMessages,
			ContextWindow:            report.ContextWindow,
			OutputReserve:            report.OutputReserve,
			NonHistoryReserve:        report.NonHistoryReserve,
			AvailableContext:         report.AvailableContext,
			HistoryBudget:            report.HistoryBudget,
			SummaryBudget:            report.SummaryBudget,
			SourceHistoryTokens:      report.SourceHistoryTokens,
			SourceSummaryTokens:      report.SourceSummaryTokens,
			SelectedHistoryTokens:    report.SelectedHistoryTokens,
			SelectedSummaryTokens:    report.SelectedSummaryTokens,
			RequestedRecentTailTurns: report.RequestedRecentTailTurns,
			RecentTailTurns:          report.RecentTailTurns,
			RecentTailTokens:         report.RecentTailTokens,
			RecentTailOverflowTokens: report.RecentTailOverflowTokens,
			RecentTailDegraded:       report.RecentTailDegraded,
			Truncated:                report.Truncated,
			PressureReasons:          append([]string(nil), report.PressureReasons...),
		},
	)
	logger.WarnCF("agent", "absolute context budget pressure", map[string]any{
		"session_key":                 ts.sessionKey,
		"context_window":              report.ContextWindow,
		"output_reserve":              report.OutputReserve,
		"non_history_reserve":         report.NonHistoryReserve,
		"available_context":           report.AvailableContext,
		"history_budget":              report.HistoryBudget,
		"summary_budget":              report.SummaryBudget,
		"source_history_tokens":       report.SourceHistoryTokens,
		"source_summary_tokens":       report.SourceSummaryTokens,
		"selected_history_tokens":     report.SelectedHistoryTokens,
		"selected_summary_tokens":     report.SelectedSummaryTokens,
		"requested_recent_tail_turns": report.RequestedRecentTailTurns,
		"recent_tail_turns":           report.RecentTailTurns,
		"recent_tail_tokens":          report.RecentTailTokens,
		"recent_tail_overflow_tokens": report.RecentTailOverflowTokens,
		"recent_tail_degraded":        report.RecentTailDegraded,
		"truncated":                   report.Truncated,
		"pressure_reasons":            report.PressureReasons,
	})
}

func (p *Pipeline) estimateNonHistoryPromptReserve(
	ts *turnState,
	contextualSkills []string,
	toolDefs []providers.ToolDefinition,
	maxMediaSize int,
) int {
	if ts == nil || ts.agent == nil || ts.agent.ContextBuilder == nil {
		return EstimateToolDefsTokens(toolDefs)
	}
	messages := p.buildTurnMessages(ts, nil, "", ts.userMessage, ts.media, contextualSkills)
	messages = resolveMediaRefs(messages, p.Context.MediaResolver, maxMediaSize)

	tokens := EstimateToolDefsTokens(toolDefs)
	for _, msg := range messages {
		tokens += EstimateMessageTokens(msg)
	}
	return tokens
}
