package commands

import (
	"context"
	"fmt"
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
	msg := fmt.Sprintf(
		"Context usage  \n"+
			"Manager: %s  \n"+
			"Stored session  \n"+
			"- Messages: %d  \n"+
			"- Used: ~%d / %d tokens (%d%%)  \n"+
			"- History: ~%d tokens  \n"+
			"- Compression progress: %d%%  \n"+
			"- Remaining: ~%d tokens  \n"+
			"Assembled prompt estimate  \n"+
			"- Messages: %d  \n"+
			"- Used: ~%d / %d tokens (%d%%)  \n"+
			"- History: ~%d tokens  \n"+
			"- Compression progress: %d%%  \n"+
			"- Remaining: ~%d tokens  \n"+
			"Thresholds  \n"+
			"- Compress at: %d tokens  \n"+
			"- Summarize at: %d history tokens",
		s.ContextManager,
		s.StoredMessageCount,
		s.StoredUsedTokens,
		s.TotalTokens,
		storedWindowPercent,
		s.StoredHistoryTokens,
		s.StoredUsedPercent,
		storedRemaining,
		s.AssembledMessageCount,
		s.AssembledUsedTokens,
		s.TotalTokens,
		assembledWindowPercent,
		s.AssembledHistoryTokens,
		s.AssembledUsedPercent,
		assembledRemaining,
		s.CompressAtTokens,
		s.SummarizeAtTokens,
	)
	return msg
}
