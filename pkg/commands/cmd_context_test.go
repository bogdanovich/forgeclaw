package commands

import (
	"strings"
	"testing"
)

func TestFormatContextStats_ShowsStoredAndAssembledSections(t *testing.T) {
	stats := &ContextStats{
		ContextManager:          "seahorse",
		TotalTokens:             131072,
		CompressAtTokens:        122880,
		SummarizeAtTokens:       98304,
		SummaryPrefixTokens:     32000,
		SeahorseHeuristicTokens: 98304,
		StoredUsedTokens:        231900,
		StoredHistoryTokens:     201300,
		StoredUsedPercent:       100,
		StoredMessageCount:      401,
		AssembledUsedTokens:     64200,
		AssembledHistoryTokens:  35800,
		AssembledUsedPercent:    52,
		AssembledMessageCount:   47,
		AssembledFitsBudget:     true,
	}

	got := formatContextStats(stats)

	wants := []string{
		"Manager: seahorse",
		"Stored session",
		"- Messages: 401",
		"- Used: ~231900 / 131072 tokens (176%)",
		"Assembled prompt estimate",
		"- Messages: 47",
		"- Used: ~64200 / 131072 tokens (48%)",
		"Thresholds",
		"- Compress at: 122880 tokens",
		"- Summarize at: heuristic 98304 full-window tokens; effective budget 122880 tokens; summary prefix target 32000 tokens",
	}
	for _, want := range wants {
		if !strings.Contains(got, want) {
			t.Fatalf("formatContextStats missing %q in:\n%s", want, got)
		}
	}
}

func TestFormatContextStats_MarksAssembledEstimateOverBudget(t *testing.T) {
	stats := &ContextStats{
		ContextManager:         "legacy",
		TotalTokens:            1000,
		CompressAtTokens:       900,
		SummarizeAtTokens:      700,
		StoredUsedTokens:       950,
		StoredHistoryTokens:    600,
		StoredUsedPercent:      100,
		StoredMessageCount:     10,
		AssembledUsedTokens:    950,
		AssembledHistoryTokens: 600,
		AssembledUsedPercent:   100,
		AssembledMessageCount:  10,
		AssembledFitsBudget:    false,
	}

	got := formatContextStats(stats)
	if !strings.Contains(got, "Assembled prompt estimate (over budget)") {
		t.Fatalf("formatContextStats missing over-budget label in:\n%s", got)
	}
}
