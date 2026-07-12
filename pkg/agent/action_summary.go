package agent

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/tools"
)

type TurnActionRecord struct {
	Source        string `json:"source"`
	Tool          string `json:"tool,omitempty"`
	Text          string `json:"text"`
	Error         bool   `json:"error,omitempty"`
	VerifiedWrite bool   `json:"verified_write,omitempty"`
}

type finalRenderToolCallState struct {
	Tool          string
	VerifiedWrite bool
}

func appendTurnActionRecord(
	records []TurnActionRecord,
	source, tool, text string,
	isError bool,
	verifiedWrite bool,
) []TurnActionRecord {
	text = strings.TrimSpace(text)
	if text == "" {
		return records
	}
	rec := TurnActionRecord{
		Source:        source,
		Tool:          strings.TrimSpace(tool),
		Text:          text,
		Error:         isError,
		VerifiedWrite: verifiedWrite,
	}
	if n := len(records); n > 0 {
		prev := records[n-1]
		if prev.Source == rec.Source && prev.Tool == rec.Tool && prev.Text == rec.Text &&
			prev.Error == rec.Error {
			return records
		}
	}
	return append(records, rec)
}

func hasVerifiedWriteAudit(audit []tools.WriteAuditEntry) bool {
	for _, entry := range audit {
		if !entry.Success {
			continue
		}
		if strings.TrimSpace(entry.Target) == "" {
			continue
		}
		return true
	}
	return false
}

func recordFinalRenderToolCall(
	exec *turnExecution,
	toolCallID, toolName string,
	verifiedWrite bool,
) {
	if exec == nil || strings.TrimSpace(toolCallID) == "" {
		return
	}
	if exec.finalRenderToolCalls == nil {
		exec.finalRenderToolCalls = make(map[string]finalRenderToolCallState)
	}
	state := exec.finalRenderToolCalls[toolCallID]
	if strings.TrimSpace(state.Tool) == "" {
		state.Tool = strings.TrimSpace(toolName)
	}
	state.VerifiedWrite = state.VerifiedWrite || verifiedWrite
	exec.finalRenderToolCalls[toolCallID] = state
}

func filterFinalTurnActionRecords(records []TurnActionRecord) []TurnActionRecord {
	if len(records) == 0 {
		return nil
	}
	filtered := make([]TurnActionRecord, 0, len(records))
	for _, rec := range records {
		if shouldSuppressUnverifiedWriteOutcome(rec.Tool, rec.Text, rec.Error, rec.VerifiedWrite) {
			continue
		}
		filtered = append(filtered, rec)
	}
	return filtered
}

func buildToolCallNameIndex(messages []providers.Message) map[string]string {
	if len(messages) == 0 {
		return nil
	}
	index := make(map[string]string)
	for _, msg := range messages {
		for _, call := range msg.ToolCalls {
			if strings.TrimSpace(call.ID) == "" {
				continue
			}
			toolName := strings.TrimSpace(call.Name)
			if toolName == "" && call.Function != nil {
				toolName = strings.TrimSpace(call.Function.Name)
			}
			if toolName == "" {
				continue
			}
			index[call.ID] = toolName
		}
	}
	return index
}

func buildFinalTurnRenderMessages(exec *turnExecution) []providers.Message {
	if exec == nil || len(exec.messages) == 0 {
		return nil
	}
	messages := append([]providers.Message(nil), exec.messages...)
	toolNames := buildToolCallNameIndex(exec.messages)
	for i := range messages {
		msg := &messages[i]
		if msg.Role != "tool" || strings.TrimSpace(msg.ToolCallID) == "" {
			continue
		}
		state := exec.finalRenderToolCalls[msg.ToolCallID]
		toolName := strings.TrimSpace(state.Tool)
		if toolName == "" {
			toolName = strings.TrimSpace(toolNames[msg.ToolCallID])
		}
		if !shouldSuppressUnverifiedWriteOutcome(toolName, msg.Content, false, state.VerifiedWrite) {
			continue
		}
		msg.Content = "[tool result omitted from final render because it may describe unverified write-side effects]"
		msg.ReasoningContent = ""
	}
	return messages
}

func shouldSuppressUnverifiedWriteOutcome(
	toolName, text string,
	isError, verifiedWrite bool,
) bool {
	if isError || verifiedWrite {
		return false
	}
	lowerText := strings.ToLower(strings.TrimSpace(text))
	if lowerText == "" {
		return false
	}
	if strings.Contains(lowerText, "failed") || strings.Contains(lowerText, "error") {
		return false
	}
	lowerTool := strings.ToLower(strings.TrimSpace(toolName))
	switch lowerTool {
	case "write_file", "append_file", "apply_patch", "cron", "update_plan":
		return true
	}
	for _, phrase := range []string{
		"file written",
		"file edited",
		"file updated",
		"file deleted",
		"saved",
		"updated",
		"created",
		"deleted",
		"removed",
		"appended",
		"patched",
		"recorded",
		"cron job added",
		"cron job updated",
		"cron job removed",
		"set plan",
		"added step",
		"reminder set",
		"note saved",
	} {
		if strings.Contains(lowerText, phrase) {
			return true
		}
	}
	return false
}

func appendTurnWriteAudit(
	records []tools.WriteAuditEntry,
	toolName string,
	audit []tools.WriteAuditEntry,
) []tools.WriteAuditEntry {
	for _, entry := range audit {
		entry.Kind = strings.TrimSpace(entry.Kind)
		entry.Target = strings.TrimSpace(entry.Target)
		entry.Action = strings.TrimSpace(entry.Action)
		entry.Tool = strings.TrimSpace(entry.Tool)
		entry.Summary = strings.TrimSpace(entry.Summary)
		if entry.Target == "" || !entry.Success {
			continue
		}
		if entry.Kind == "" {
			entry.Kind = "file"
		}
		if entry.Action == "" {
			entry.Action = "write"
		}
		if entry.Tool == "" {
			entry.Tool = strings.TrimSpace(toolName)
		}
		duplicate := false
		for _, existing := range records {
			if existing.Kind == entry.Kind &&
				existing.Target == entry.Target &&
				existing.Action == entry.Action &&
				existing.Tool == entry.Tool {
				duplicate = true
				break
			}
		}
		if !duplicate {
			records = append(records, entry)
		}
	}
	return records
}

func finalTurnRenderEligible(al *AgentLoop, exec *turnExecution) bool {
	if al == nil || exec == nil {
		return false
	}
	return finalTurnRenderEligibleForConfig(al.cfg, exec)
}

func finalTurnRenderEligibleForConfig(cfg *config.Config, exec *turnExecution) bool {
	if cfg == nil || exec == nil {
		return false
	}
	if !cfg.Agents.Defaults.UseFinalTurnRender() {
		return false
	}
	return exec.sawSteering
}

func finalTurnRenderModel(ts *turnState, exec *turnExecution) (providers.LLMProvider, string) {
	if exec != nil {
		if exec.model.activeProvider != nil && strings.TrimSpace(exec.model.activeModel) != "" {
			return exec.model.activeProvider, strings.TrimSpace(exec.model.activeModel)
		}
		if exec.model.activeProvider != nil {
			return exec.model.activeProvider, strings.TrimSpace(ts.agent.Model)
		}
	}
	if ts == nil || ts.agent == nil {
		return nil, ""
	}
	return ts.agent.Provider, strings.TrimSpace(ts.agent.Model)
}

func buildFinalTurnRenderInstruction(exec *turnExecution) string {
	var b strings.Builder
	b.WriteString("Write the final user-facing reply for this already-completed turn.\n")
	b.WriteString("Use the same language and general style as the conversation.\n")
	b.WriteString("Do not call tools.\n")
	b.WriteString(
		"Answer the full accumulated user request across this turn, not only the latest follow-up.\n",
	)
	b.WriteString(
		"If a later follow-up clearly corrected, narrowed, or replaced an earlier request, follow the latest clarified intent.\n",
	)
	b.WriteString(
		"If later follow-ups added to earlier requests, include the completed additive results together.\n",
	)
	b.WriteString(
		"Use only the facts already present in the conversation and tool results. Do not invent missing results.\n",
	)
	b.WriteString("Keep the reply concise and natural.\n")

	if exec == nil || len(exec.actionLog) == 0 {
		if exec != nil && len(exec.writeAudit) > 0 {
			b.WriteString("\nVerified write-side effects from tool execution:\n")
			if raw, err := json.MarshalIndent(exec.writeAudit, "", "  "); err == nil {
				_, _ = b.Write(raw)
				b.WriteString(
					"\nOnly claim that files, notes, artifacts, or records were saved/updated when they appear in this verified write list or were explicitly verified earlier in the conversation.",
				)
			}
		}
		return b.String()
	}

	records := make([]TurnActionRecord, 0, len(exec.actionLog))
	for _, rec := range filterFinalTurnActionRecords(exec.actionLog) {
		if strings.TrimSpace(rec.Text) == "" {
			continue
		}
		records = append(records, rec)
	}
	if len(records) == 0 {
		return b.String()
	}

	raw, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return b.String()
	}
	b.WriteString("\nExplicit user-facing outcomes recorded during the turn:\n")
	_, _ = b.Write(raw)
	if len(exec.writeAudit) > 0 {
		writeRaw, err := json.MarshalIndent(exec.writeAudit, "", "  ")
		if err == nil {
			b.WriteString("\n\nVerified write-side effects from tool execution:\n")
			_, _ = b.Write(writeRaw)
			b.WriteString(
				"\nOnly claim that files, notes, artifacts, or records were saved/updated when they appear in this verified write list or were explicitly verified earlier in the conversation.",
			)
		}
	}
	return b.String()
}

func tryRenderFinalTurnReply(
	ctx context.Context,
	al *AgentLoop,
	ts *turnState,
	exec *turnExecution,
	fallback string,
) (string, bool) {
	fallback = strings.TrimSpace(fallback)
	if !finalTurnRenderEligible(al, exec) {
		return fallback, false
	}
	if exec == nil || len(exec.messages) == 0 {
		return fallback, false
	}

	provider, model := finalTurnRenderModel(ts, exec)
	if provider == nil || model == "" {
		return fallback, false
	}

	messages := buildFinalTurnRenderMessages(exec)
	instruction := buildFinalTurnRenderInstruction(exec)
	messages = append(messages, providers.Message{
		Role:    "user",
		Content: instruction,
	})

	opts := map[string]any{
		"max_tokens":       minInt(ts.agent.MaxTokens, 800),
		"temperature":      0.2,
		"prompt_cache_key": ts.agent.ID,
	}

	resp, err := provider.Chat(ctx, messages, nil, model, opts)
	if err != nil || resp == nil {
		if err != nil {
			logger.WarnCF("agent", "Final turn render pass failed", map[string]any{
				"agent_id": ts.agent.ID,
				"error":    err.Error(),
			})
		}
		return fallback, false
	}

	content := strings.TrimSpace(resp.Content)
	if content == "" {
		content = strings.TrimSpace(resp.ReasoningContent)
	}
	if content == "" {
		return fallback, false
	}

	logger.InfoCF("agent", "Rendered final reply from accumulated turn context",
		map[string]any{
			"agent_id":            ts.agent.ID,
			"session_key":         ts.sessionKey,
			"messages_count":      len(messages),
			"action_record_count": len(exec.actionLog),
		})
	return content, true
}

func renderFinalTurnReply(
	ctx context.Context,
	al *AgentLoop,
	ts *turnState,
	exec *turnExecution,
	fallback string,
) string {
	content, ok := tryRenderFinalTurnReply(ctx, al, ts, exec, fallback)
	if ok {
		return content
	}
	return strings.TrimSpace(fallback)
}

func shouldFinalizeAfterToolLoopWithRenderConfig(cfg *config.Config, exec *turnExecution) bool {
	if !finalTurnRenderEligibleForConfig(cfg, exec) {
		return false
	}
	if exec == nil {
		return false
	}
	return !exec.allResponsesHandled
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
