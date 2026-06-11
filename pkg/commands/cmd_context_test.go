package commands

import (
	"strings"
	"testing"
)

func TestFormatContextStats_ShowsStoredAndAssembledSections(t *testing.T) {
	stats := &ContextStats{
		ContextManager:         "seahorse",
		TotalTokens:            131072,
		CompressAtTokens:       122880,
		SummarizeAtTokens:      98304,
		SummaryPrefixTokens:    32000,
		StoredUsedTokens:       231900,
		StoredHistoryTokens:    201300,
		StoredUsedPercent:      100,
		StoredMessageCount:     401,
		AssembledUsedTokens:    64200,
		AssembledHistoryTokens: 35800,
		AssembledUsedPercent:   52,
		AssembledMessageCount:  47,
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
		"- Summarize at: 98304 assembled tokens; summary prefix target 32000 tokens",
	}
	for _, want := range wants {
		if !strings.Contains(got, want) {
			t.Fatalf("formatContextStats missing %q in:\n%s", want, got)
		}
	}
}
