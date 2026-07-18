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

	selectedMessages, recentTailTurns, recentTailTokens, err := selectBoundedMessageTurns(
		messages,
		historyBudget,
		a.config.RecentTailTurns,
	)
	if err != nil {
		return nil, err
	}
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
	selectedMessageCount, selectedSummaryCount := countResolvedItemTypes(final)
	report := &AssembleBudgetReport{
		TotalBudget:           input.Budget,
		HistoryBudget:         historyBudget,
		SummaryBudget:         summaryBudget,
		SourceHistoryTokens:   sourceHistoryTokens,
		SourceSummaryTokens:   sourceSummaryTokens,
		SelectedHistoryTokens: selectedHistoryTokens,
		SelectedSummaryTokens: selectedSummaryTokens,
		RecentTailTurns:       recentTailTurns,
		RecentTailTokens:      recentTailTokens,
		Truncated: selectedMessageCount < len(messages) ||
			selectedSummaryCount < len(summaries),
		NeedsCompaction: len(pressureReasons) > 0,
		PressureReasons: pressureReasons,
	}
	result.Budget = report
	logger.InfoCF("seahorse", "assemble: absolute context budget selected", map[string]any{
		"conv_id":                 convID,
		"total_budget":            report.TotalBudget,
		"history_budget":          report.HistoryBudget,
		"summary_budget":          report.SummaryBudget,
		"source_history_tokens":   report.SourceHistoryTokens,
		"source_summary_tokens":   report.SourceSummaryTokens,
		"selected_history_tokens": report.SelectedHistoryTokens,
		"selected_summary_tokens": report.SelectedSummaryTokens,
		"recent_tail_turns":       report.RecentTailTurns,
		"recent_tail_tokens":      report.RecentTailTokens,
		"truncated":               report.Truncated,
		"needs_compaction":        report.NeedsCompaction,
		"pressure_reasons":        report.PressureReasons,
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

func selectBoundedMessageTurns(
	messages []resolvedItem,
	budget int,
	minimumRecentTurns int,
) ([]resolvedItem, int, int, error) {
	if len(messages) == 0 {
		return nil, 0, 0, nil
	}
	turnStarts := resolvedMessageTurnStarts(messages)
	protectedTurns := minimumRecentTurns
	if protectedTurns > len(turnStarts) {
		protectedTurns = len(turnStarts)
	}

	selectionStart := len(messages)
	startIndex := len(turnStarts) - 1
	protectedTokens := 0
	if protectedTurns > 0 {
		startIndex = len(turnStarts) - protectedTurns
		selectionStart = turnStarts[startIndex]
		protectedTokens = resolvedItemsTokenCount(messages[selectionStart:])
		if protectedTokens > budget {
			return nil, protectedTurns, protectedTokens, fmt.Errorf(
				"mandatory recent tail cannot fit history budget: turns=%d tokens=%d budget=%d",
				protectedTurns,
				protectedTokens,
				budget,
			)
		}
		startIndex--
	}

	selectedTokens := protectedTokens
	for startIndex >= 0 {
		candidateStart := turnStarts[startIndex]
		candidateTokens := resolvedItemsTokenCount(messages[candidateStart:selectionStart])
		if selectedTokens+candidateTokens > budget {
			break
		}
		selectionStart = candidateStart
		selectedTokens += candidateTokens
		startIndex--
	}
	if selectionStart == len(messages) {
		return nil, protectedTurns, protectedTokens, nil
	}
	selected := append([]resolvedItem(nil), messages[selectionStart:]...)
	if !isProviderSafeHistoryStart(selected) {
		return nil, protectedTurns, protectedTokens, fmt.Errorf(
			"selected history does not start at a provider-safe turn boundary",
		)
	}
	return selected, protectedTurns, protectedTokens, nil
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
