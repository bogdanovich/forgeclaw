package seahorse

import (
	"context"
	"fmt"
	"sort"

	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/tokenizer"
)

func (a *Assembler) assembleWithAbsoluteBudgets(
	ctx context.Context,
	convID int64,
	resolved []resolvedItem,
	input AssembleInput,
) (*AssembleResult, error) {
	if input.Budget <= 0 {
		return nil, fmt.Errorf("absolute context assembly requires a positive total budget")
	}

	messages, summaries := partitionResolvedItems(resolved)
	summaries, _ = a.dropCoveredSummaries(ctx, summaries)
	sourceHistoryTokens := resolvedItemsTokenCount(messages)
	sourceSummaryTokens := estimateRenderedSummaryTokens(buildAssembleResult(summaries, nil).Summary)
	historyBudget := a.config.historyBudget(input.Budget)
	summaryBudget := a.config.summaryBudget(input.Budget)

	selection, err := selectBoundedMessageTurns(
		messages,
		historyBudget,
		input.Budget,
		a.config.RecentTailTurns,
	)
	if err != nil {
		return nil, err
	}
	selectedMessages := selection.messages
	selectedHistoryTokens := resolvedItemsTokenCount(selectedMessages)
	availableForSummaries := input.Budget - selectedHistoryTokens
	summarySelectionBudget := summaryBudget
	if availableForSummaries < summarySelectionBudget {
		summarySelectionBudget = availableForSummaries
	}
	selectedSummaries := selectNewestResolvedItems(
		summaries,
		summarySelectionBudget,
	)

	final := append(append([]resolvedItem(nil), selectedMessages...), selectedSummaries...)
	sort.Slice(final, func(i, j int) bool { return final[i].ordinal < final[j].ordinal })
	final, droppedCovered := a.dropCoveredSummaries(ctx, final)
	if droppedCovered > 0 {
		logger.InfoCF("seahorse", "assemble: dropped covered summaries", map[string]any{
			"conv_id": convID,
			"dropped": droppedCovered,
		})
	}

	result := buildAssembleResult(final, nil)
	selectedSummaryTokens := estimateRenderedSummaryTokens(result.Summary)
	for selectedSummaryTokens > summaryBudget ||
		selectedHistoryTokens+selectedSummaryTokens > input.Budget {
		var removed bool
		final, removed = removeOldestSummary(final)
		if !removed {
			break
		}
		result = buildAssembleResult(final, nil)
		selectedSummaryTokens = estimateRenderedSummaryTokens(result.Summary)
	}
	if selectedHistoryTokens+selectedSummaryTokens > input.Budget {
		return nil, fmt.Errorf(
			"mandatory recent context cannot fit total context budget: history=%d summary=%d budget=%d",
			selectedHistoryTokens,
			selectedSummaryTokens,
			input.Budget,
		)
	}

	pressureReasons := contextPressureReasons(
		sourceHistoryTokens,
		sourceSummaryTokens,
		historyBudget,
		summaryBudget,
		input.Budget,
	)
	if selection.overflowTokens > 0 {
		pressureReasons = append(pressureReasons, "recent_tail_over_history_budget")
	}
	if selection.degraded {
		pressureReasons = append(pressureReasons, "recent_tail_degraded")
	}
	selectedMessageCount, selectedSummaryCount := countResolvedItemTypes(final)
	report := &AssembleBudgetReport{
		TotalBudget:              input.Budget,
		HistoryBudget:            historyBudget,
		SummaryBudget:            summaryBudget,
		SourceHistoryTokens:      sourceHistoryTokens,
		SourceSummaryTokens:      sourceSummaryTokens,
		SelectedHistoryTokens:    selectedHistoryTokens,
		SelectedSummaryTokens:    selectedSummaryTokens,
		RequestedRecentTailTurns: selection.requestedTurns,
		RecentTailTurns:          selection.selectedTurns,
		RecentTailTokens:         selection.tailTokens,
		RecentTailOverflowTokens: selection.overflowTokens,
		RecentTailDegraded:       selection.degraded,
		Truncated: selectedMessageCount < len(messages) ||
			selectedSummaryCount < len(summaries),
		NeedsCompaction: len(pressureReasons) > 0,
		PressureReasons: pressureReasons,
	}
	result.Budget = report
	logger.InfoCF("seahorse", "assemble: absolute context budget selected", map[string]any{
		"conv_id":                     convID,
		"total_budget":                report.TotalBudget,
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
		"needs_compaction":            report.NeedsCompaction,
		"pressure_reasons":            report.PressureReasons,
	})
	return result, nil
}

func partitionResolvedItems(items []resolvedItem) ([]resolvedItem, []resolvedItem) {
	messages := make([]resolvedItem, 0, len(items))
	summaries := make([]resolvedItem, 0, len(items))
	for _, item := range items {
		switch item.itemType {
		case "message":
			messages = append(messages, item)
		case "summary":
			summaries = append(summaries, item)
		}
	}
	return messages, summaries
}

type boundedTurnSelection struct {
	messages       []resolvedItem
	requestedTurns int
	selectedTurns  int
	tailTokens     int
	overflowTokens int
	degraded       bool
}

func selectBoundedMessageTurns(
	messages []resolvedItem,
	historyTarget int,
	hardBudget int,
	requestedRecentTurns int,
) (boundedTurnSelection, error) {
	if len(messages) == 0 {
		return boundedTurnSelection{}, nil
	}
	turnStarts := resolvedMessageTurnStarts(messages)
	requestedTurns := requestedRecentTurns
	if requestedTurns > len(turnStarts) {
		requestedTurns = len(turnStarts)
	}

	selectionStart := len(messages)
	startIndex := len(turnStarts) - 1
	selectedTurns := requestedTurns
	protectedTokens := 0
	for selectedTurns > 0 {
		startIndex = len(turnStarts) - selectedTurns
		selectionStart = turnStarts[startIndex]
		protectedTokens = resolvedItemsTokenCount(messages[selectionStart:])
		if protectedTokens <= hardBudget {
			startIndex--
			break
		}
		selectedTurns--
		selectionStart = len(messages)
		protectedTokens = 0
	}

	selectionBudget := historyTarget
	if selectionBudget > hardBudget {
		selectionBudget = hardBudget
	}
	if protectedTokens > selectionBudget {
		selectionBudget = protectedTokens
	}
	selectedTokens := protectedTokens
	for startIndex >= 0 {
		candidateStart := turnStarts[startIndex]
		candidateTokens := resolvedItemsTokenCount(messages[candidateStart:selectionStart])
		if selectedTokens+candidateTokens > selectionBudget {
			break
		}
		selectionStart = candidateStart
		selectedTokens += candidateTokens
		startIndex--
	}
	result := boundedTurnSelection{
		requestedTurns: requestedTurns,
		selectedTurns:  selectedTurns,
		tailTokens:     protectedTokens,
		overflowTokens: max(0, protectedTokens-historyTarget),
		degraded:       selectedTurns < requestedTurns,
	}
	if selectionStart == len(messages) {
		return result, nil
	}
	selected := append([]resolvedItem(nil), messages[selectionStart:]...)
	if !isProviderSafeHistoryStart(selected) {
		return boundedTurnSelection{}, fmt.Errorf(
			"selected history does not start at a provider-safe turn boundary",
		)
	}
	result.messages = selected
	return result, nil
}

func resolvedMessageTurnStarts(messages []resolvedItem) []int {
	starts := make([]int, 0)
	for i, item := range messages {
		if item.message != nil && item.message.Role == "user" {
			starts = append(starts, i)
		}
	}
	if len(starts) == 0 {
		return []int{0}
	}
	return starts
}

func selectNewestResolvedItems(items []resolvedItem, budget int) []resolvedItem {
	if budget <= 0 {
		return nil
	}
	start := len(items)
	tokens := 0
	for i := len(items) - 1; i >= 0; i-- {
		if tokens+items[i].tokenCount > budget {
			break
		}
		start = i
		tokens += items[i].tokenCount
	}
	return append([]resolvedItem(nil), items[start:]...)
}

func removeOldestSummary(items []resolvedItem) ([]resolvedItem, bool) {
	for i, item := range items {
		if item.itemType != "summary" {
			continue
		}
		return append(append([]resolvedItem(nil), items[:i]...), items[i+1:]...), true
	}
	return items, false
}

func estimateRenderedSummaryTokens(summary string) int {
	if summary == "" {
		return 0
	}
	return tokenizer.EstimateMessageTokens(providers.Message{Role: "system", Content: summary})
}

func contextPressureReasons(
	sourceHistoryTokens,
	sourceSummaryTokens,
	historyBudget,
	summaryBudget,
	totalBudget int,
) []string {
	reasons := make([]string, 0, 3)
	if sourceHistoryTokens > historyBudget {
		reasons = append(reasons, "history_budget")
	}
	if sourceSummaryTokens > summaryBudget {
		reasons = append(reasons, "summary_budget")
	}
	if sourceHistoryTokens+sourceSummaryTokens > totalBudget {
		reasons = append(reasons, "total_budget")
	}
	return reasons
}

func countResolvedItemTypes(items []resolvedItem) (int, int) {
	messages := 0
	summaries := 0
	for _, item := range items {
		switch item.itemType {
		case "message":
			messages++
		case "summary":
			summaries++
		}
	}
	return messages, summaries
}
