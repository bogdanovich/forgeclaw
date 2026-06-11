package commands

import (
	"context"
	"fmt"

	"github.com/sipeed/picoclaw/pkg/seahorse"
)

func contextCommand() Definition {
	return Definition{
		Name:        "context",
		Description: "Show current session context and token usage",
		Usage:       "/context",
		Handler: func(_ context.Context, req Request, rt *Runtime) error {
			if rt == nil || rt.GetContextStats == nil {
				return req.Reply(unavailableMsg)
			}
			stats := rt.GetContextStats()
			if stats == nil {
				return req.Reply("No active session context.")
			}
			return req.Reply(formatContextStats(stats))
		},
	}
}

func formatContextStats(s *ContextStats) string {
	storedRemaining := s.CompressAtTokens - s.StoredUsedTokens
	if storedRemaining < 0 {
		storedRemaining = 0
	}
	assembledRemaining := s.CompressAtTokens - s.AssembledUsedTokens
	if assembledRemaining < 0 {
		assembledRemaining = 0
	}
	storedWindowPercent := s.StoredUsedTokens * 100 / max(s.TotalTokens, 1)
	assembledWindowPercent := s.AssembledUsedTokens * 100 / max(s.TotalTokens, 1)
	assembledLabel := "Assembled prompt estimate"
	if !s.AssembledFitsBudget {
		assembledLabel += " (pre-repair over budget)"
	}
	msg := fmt.Sprintf(
		"Context usage  \n"+
			"Manager: %s  \n"+
			"Stored session  \n"+
			"- Messages: %d  \n"+
			"- Used: ~%d / %d tokens (%d%%)  \n"+
			"- History: ~%d tokens  \n"+
			"- Compression progress: %d%%  \n"+
			"- Remaining: ~%d tokens  \n"+
			"%s  \n"+
			"- Messages: %d  \n"+
			"- Used: ~%d / %d tokens (%d%%)  \n"+
			"- History: ~%d tokens  \n"+
			"- Compression progress: %d%%  \n"+
			"- Remaining: ~%d tokens  \n"+
			"Thresholds  \n"+
			"- Compress at: %d tokens  \n"+
			"- Summarize at: %s  \n"+
			"Notes  \n"+
			"- Stored session shows persisted history pressure  \n"+
			"- Assembled prompt estimate is approximate; final requests may differ after runtime shaping and repair",
		s.ContextManager,
		s.StoredMessageCount,
		s.StoredUsedTokens,
		s.TotalTokens,
		storedWindowPercent,
		s.StoredHistoryTokens,
		s.StoredUsedPercent,
		storedRemaining,
		assembledLabel,
		s.AssembledMessageCount,
		s.AssembledUsedTokens,
		s.TotalTokens,
		assembledWindowPercent,
		s.AssembledHistoryTokens,
		s.AssembledUsedPercent,
		assembledRemaining,
		s.CompressAtTokens,
		formatSummarizeThreshold(s),
	)
	return msg
}

func formatSummarizeThreshold(s *ContextStats) string {
	if s == nil {
		return ""
	}
	if s.ContextManager == "seahorse" {
		summaryPrefixTokens := s.SummaryPrefixTokens
		if summaryPrefixTokens <= 0 {
			summaryPrefixTokens = seahorse.SummaryPrefixTokens
		}
		heuristicTokens := s.SeahorseHeuristicTokens
		if heuristicTokens <= 0 {
			heuristicTokens = s.SummarizeAtTokens
		}
		return fmt.Sprintf(
			"heuristic %d full-window tokens; effective budget %d tokens; summary prefix target %d tokens",
			heuristicTokens,
			s.CompressAtTokens,
			summaryPrefixTokens,
		)
	}
	if s.ContextManager == "legacy" && s.SummarizeMessageThreshold > 0 {
		return fmt.Sprintf("%d history tokens or %d messages", s.SummarizeAtTokens, s.SummarizeMessageThreshold)
	}
	return fmt.Sprintf("%d history tokens", s.SummarizeAtTokens)
}
