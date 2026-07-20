package agent

import (
	"context"
	"fmt"
	"strings"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/seahorse"
)

// computeContextUsage estimates current context window consumption for the
// given agent and session. Includes history, system prompt (with dynamic context,
// summary, and skills — mirroring BuildMessages composition), and tool definitions.
// The output reserve (MaxTokens) is not counted as "used" but reduces the
// effective budget, matching isOverContextBudget's compression trigger:
//
//	compress when: history + system + tools + maxTokens > contextWindow
//	equivalent to: history + system + tools > contextWindow - maxTokens
//
// Returns nil when the agent or session is unavailable.
func computeContextUsage(agent *AgentInstance, sessionKey string) *bus.ContextUsage {
	if agent == nil || agent.Sessions == nil {
		return nil
	}
	contextWindow := agent.ContextWindow
	if contextWindow <= 0 {
		return nil
	}

	// History tokens
	history := agent.Sessions.GetHistory(sessionKey)
	historyTokens := 0
	for _, m := range history {
		historyTokens += EstimateMessageTokens(m)
	}

	// System message tokens: uses EstimateSystemTokens which mirrors
	// the full system message composition in BuildMessages (static prompt,
	// dynamic context, active skills, summary with wrapping prefix).
	systemTokens := 0
	if agent.ContextBuilder != nil {
		summary := agent.Sessions.GetSummary(sessionKey)
		// Pass nil for active skills: skills are only injected when the user
		// explicitly activates them via /use, which is rare. Using nil matches
		// the common case and avoids over-counting all installed skills.
		systemTokens = agent.ContextBuilder.EstimateSystemTokens(summary, nil)
	}

	// Tool definition tokens
	toolTokens := 0
	if agent.Tools != nil {
		toolTokens = EstimateToolDefsTokens(agent.Tools.ToProviderDefs())
	}

	// Used = history + system (includes summary) + tools
	usedTokens := historyTokens + systemTokens + toolTokens

	// Effective budget = contextWindow minus output reserve (maxTokens)
	effectiveWindow := contextWindow - agent.MaxTokens
	if effectiveWindow < 0 {
		effectiveWindow = contextWindow
	}

	// compressAt = effectiveWindow: aligns with isOverContextBudget's
	// proactive trigger (msgTokens + toolTokens + maxTokens > contextWindow).
	compressAt := effectiveWindow

	// summarizeAt = soft summarization trigger: matches maybeSummarize's
	// threshold (contextWindow * SummarizeTokenPercent / 100).
	//
	// The engine compares this against history-message tokens ONLY (not
	// UsedTokens).  HistoryTokens is exposed alongside UsedTokens so the
	// UI can show both values and avoid user confusion.
	summarizeAt := contextWindow * agent.SummarizeTokenPercent / 100
	if summarizeAt <= 0 {
		summarizeAt = compressAt
	}

	usedPercent := 0
	if compressAt > 0 {
		usedPercent = usedTokens * 100 / compressAt
	}
	if usedPercent > 100 {
		usedPercent = 100
	}

	return &bus.ContextUsage{
		UsedTokens:        usedTokens,
		TotalTokens:       contextWindow,
		HistoryTokens:     historyTokens,
		CompressAtTokens:  compressAt,
		SummarizeAtTokens: summarizeAt,
		UsedPercent:       usedPercent,
	}
}

func estimateNonHistoryPromptReserveForProcessOptions(
	cfg *config.Config,
	agent *AgentInstance,
	opts processOptions,
	summary string,
) int {
	if agent == nil {
		return 0
	}

	var toolDefs []providers.ToolDefinition
	if agent.Tools != nil {
		toolDefs = filterToolsByTurnProfile(agent.Tools.ToProviderDefs(), opts.TurnProfile)
	}
	if agent.ContextBuilder == nil {
		return EstimateToolDefsTokens(toolDefs)
	}

	contextualSkills := activeSkillNames(agent, opts)
	if agent.ContextBuilder != nil {
		contextualSkills = agent.ContextBuilder.ResolveActiveSkillsForContext(contextualSkills)
	}

	req := promptBuildRequestForProcessOptions(cfg, agent, opts, nil, summary, "", nil)
	req.ActiveSkills = append([]string(nil), contextualSkills...)
	messages := agent.ContextBuilder.BuildMessagesFromPrompt(req)

	tokens := EstimateToolDefsTokens(toolDefs)
	for _, msg := range messages {
		tokens += EstimateMessageTokens(msg)
	}
	return tokens
}

func computeAssembledContextUsage(
	ctx context.Context,
	cfg *config.Config,
	agent *AgentInstance,
	cm ContextManager,
	opts processOptions,
	sessionKey string,
) (*bus.ContextUsage, int, bool) {
	if agent == nil || cm == nil {
		return nil, 0, false
	}
	contextWindow := agent.ContextWindow
	if contextWindow <= 0 {
		return nil, 0, false
	}

	if opts.NoHistory {
		usedTokens := estimateNonHistoryPromptReserveForProcessOptions(cfg, agent, opts, "")
		effectiveWindow := contextWindow - agent.MaxTokens
		if effectiveWindow < 0 {
			effectiveWindow = contextWindow
		}

		compressAt := effectiveWindow
		summarizeAt := contextWindow * agent.SummarizeTokenPercent / 100
		if summarizeAt <= 0 {
			summarizeAt = compressAt
		}

		usedPercent := 0
		if compressAt > 0 {
			usedPercent = usedTokens * 100 / compressAt
		}
		if usedPercent > 100 {
			usedPercent = 100
		}

		return &bus.ContextUsage{
			UsedTokens:        usedTokens,
			TotalTokens:       contextWindow,
			HistoryTokens:     0,
			CompressAtTokens:  compressAt,
			SummarizeAtTokens: summarizeAt,
			UsedPercent:       usedPercent,
		}, 0, usedTokens <= compressAt
	}

	resp, err := cm.Assemble(ctx, &AssembleRequest{
		Agent:         agent,
		SessionKey:    sessionKey,
		Budget:        contextWindow,
		MaxTokens:     agent.MaxTokens,
		ReserveTokens: estimateNonHistoryPromptReserveForProcessOptions(cfg, agent, opts, ""),
	})
	if err != nil || resp == nil {
		return nil, 0, false
	}

	historyTokens := 0
	for _, m := range resp.History {
		historyTokens += EstimateMessageTokens(m)
	}

	usedTokens := historyTokens + estimateNonHistoryPromptReserveForProcessOptions(cfg, agent, opts, resp.Summary)

	effectiveWindow := contextWindow - agent.MaxTokens
	if effectiveWindow < 0 {
		effectiveWindow = contextWindow
	}

	compressAt := effectiveWindow
	summarizeAt := contextWindow * agent.SummarizeTokenPercent / 100
	if summarizeAt <= 0 {
		summarizeAt = compressAt
	}

	usedPercent := 0
	if compressAt > 0 {
		usedPercent = usedTokens * 100 / compressAt
	}
	if usedPercent > 100 {
		usedPercent = 100
	}

	fitsBudget := usedTokens <= compressAt

	return &bus.ContextUsage{
		UsedTokens:        usedTokens,
		TotalTokens:       contextWindow,
		HistoryTokens:     historyTokens,
		CompressAtTokens:  compressAt,
		SummarizeAtTokens: summarizeAt,
		UsedPercent:       usedPercent,
	}, len(resp.History), fitsBudget
}

func contextManagerDisplayName(cm ContextManager) string {
	switch cm.(type) {
	case *legacyContextManager:
		return "legacy"
	}
	typeName := fmt.Sprintf("%T", cm)
	if strings.Contains(typeName, "seahorseContextManager") {
		return "seahorse"
	}
	return "custom"
}

func seahorseSummaryPrefixTokens(cm ContextManager) int {
	if contextManagerDisplayName(cm) != "seahorse" {
		return 0
	}
	return seahorse.SummaryPrefixTokens
}
