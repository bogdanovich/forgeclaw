package seahorse

import (
	"fmt"
	"strings"

	"github.com/sipeed/picoclaw/pkg/logger"
)

// ToolResultRetentionMode controls how successful tool output appears in future prompts.
type ToolResultRetentionMode string

const (
	ToolResultRetentionPreserve       ToolResultRetentionMode = "preserve"
	ToolResultRetentionCompactReceipt ToolResultRetentionMode = "compact_receipt"
	ToolResultRetentionTransient      ToolResultRetentionMode = "transient"
	ToolResultRetentionDurable        ToolResultRetentionMode = "durable"

	maxToolResultReceiptBytes = 1024
)

// ToolResultRetentionRule applies to exact tool names. Errors and unresolved
// results always bypass the rule and remain fully preserved.
type ToolResultRetentionRule struct {
	Mode    ToolResultRetentionMode `json:"mode"`
	Receipt string                  `json:"receipt,omitempty"`
}

type toolResultProjectionReport struct {
	Preserved       int
	SafetyPreserved int
	Receipted       int
	Durable         int
	Transient       int
}

type pendingToolCall struct {
	messageIndex int
	partIndex    int
	name         string
}

func (c Config) validateToolResultRetention() error {
	for toolName, rule := range c.ToolResultRetention {
		if strings.TrimSpace(toolName) == "" || toolName != strings.TrimSpace(toolName) {
			return fmt.Errorf("tool names must be non-empty and trimmed")
		}
		switch rule.Mode {
		case ToolResultRetentionPreserve, ToolResultRetentionTransient:
			if rule.Receipt != "" {
				return fmt.Errorf("tool %q mode %q does not accept a receipt", toolName, rule.Mode)
			}
		case ToolResultRetentionCompactReceipt, ToolResultRetentionDurable:
			if strings.TrimSpace(rule.Receipt) == "" {
				return fmt.Errorf("tool %q mode %q requires a receipt", toolName, rule.Mode)
			}
			if rule.Receipt != strings.TrimSpace(rule.Receipt) {
				return fmt.Errorf("tool %q receipt must be trimmed", toolName)
			}
		default:
			return fmt.Errorf("tool %q has unsupported mode %q", toolName, rule.Mode)
		}
		if len(rule.Receipt) > maxToolResultReceiptBytes {
			return fmt.Errorf(
				"tool %q receipt exceeds %d bytes",
				toolName,
				maxToolResultReceiptBytes,
			)
		}
	}
	return nil
}

func projectToolResultMessages(
	messages []Message,
	config Config,
) ([]Message, toolResultProjectionReport) {
	if len(messages) == 0 || len(config.ToolResultRetention) == 0 {
		return append([]Message(nil), messages...), toolResultProjectionReport{}
	}

	projected := cloneSeahorseMessages(messages)
	keep := make([]bool, len(projected))
	for i := range keep {
		keep[i] = true
	}
	pendingCalls := make(map[string]pendingToolCall)
	removeCalls := make(map[int]map[int]struct{})
	report := toolResultProjectionReport{}
	for i := range projected {
		if projected[i].Role == "user" {
			clear(pendingCalls)
		}
		for partIndex, part := range projected[i].Parts {
			if part.Type == "tool_use" && part.ToolCallID != "" {
				pendingCalls[part.ToolCallID] = pendingToolCall{
					messageIndex: i,
					partIndex:    partIndex,
					name:         part.Name,
				}
			}
		}
		resultIndex, ok := singleToolResultPart(projected[i])
		if !ok {
			continue
		}
		result := &projected[i].Parts[resultIndex]
		call, matched := pendingCalls[result.ToolCallID]
		if matched {
			delete(pendingCalls, result.ToolCallID)
		}
		rule, configured := config.ToolResultRetention[call.name]
		if !matched {
			report.Preserved++
			report.SafetyPreserved++
			continue
		}
		if !configured || rule.Mode == ToolResultRetentionPreserve {
			report.Preserved++
			continue
		}
		if result.ToolResultStatus != "success" ||
			len(projected[i].Parts) != 1 ||
			messageHasRetainedArtifact(projected[i]) {
			report.Preserved++
			report.SafetyPreserved++
			continue
		}

		switch rule.Mode {
		case ToolResultRetentionCompactReceipt:
			result.Text = strings.TrimSpace(rule.Receipt)
			projected[i].Content = result.Text
			projected[i].TokenCount = EstimateMessageTokens(projected[i])
			report.Receipted++
		case ToolResultRetentionDurable:
			result.Text = strings.TrimSpace(rule.Receipt)
			projected[i].Content = result.Text
			projected[i].TokenCount = EstimateMessageTokens(projected[i])
			report.Durable++
		case ToolResultRetentionTransient:
			keep[i] = false
			if removeCalls[call.messageIndex] == nil {
				removeCalls[call.messageIndex] = make(map[int]struct{})
			}
			removeCalls[call.messageIndex][call.partIndex] = struct{}{}
			report.Transient++
		}
	}

	for i := range projected {
		removedParts := removeCalls[i]
		if len(removedParts) == 0 || projected[i].Role != "assistant" {
			continue
		}
		originalParts := projected[i].Parts
		filtered := make([]MessagePart, 0, len(originalParts))
		for partIndex, part := range originalParts {
			if _, remove := removedParts[partIndex]; remove {
				continue
			}
			filtered = append(filtered, part)
		}
		if len(filtered) == len(originalParts) {
			continue
		}
		if projected[i].Content == "" || projected[i].Content == partsToReadableContent(originalParts) {
			projected[i].Content = partsToReadableContent(filtered)
		}
		projected[i].Parts = filtered
		projected[i].TokenCount = EstimateMessageTokens(projected[i])
		if len(filtered) == 0 && strings.TrimSpace(projected[i].Content) == "" &&
			strings.TrimSpace(projected[i].ReasoningContent) == "" {
			keep[i] = false
		}
	}

	result := make([]Message, 0, len(projected))
	for i := range projected {
		if keep[i] {
			result = append(result, projected[i])
		}
	}
	return result, report
}

func (a *Assembler) projectResolvedToolResults(
	convID int64,
	items []resolvedItem,
) []resolvedItem {
	if a == nil || len(a.config.ToolResultRetention) == 0 {
		return items
	}
	messages := make([]Message, 0, len(items))
	for _, item := range items {
		if item.itemType == "message" && item.message != nil {
			messages = append(messages, *item.message)
		}
	}
	projected, report := projectToolResultMessages(messages, a.config)
	byID := make(map[int64]Message, len(projected))
	for _, message := range projected {
		byID[message.ID] = message
	}
	result := make([]resolvedItem, 0, len(items))
	for _, item := range items {
		if item.itemType != "message" || item.message == nil {
			result = append(result, item)
			continue
		}
		message, keep := byID[item.message.ID]
		if !keep {
			continue
		}
		item.message = &message
		item.tokenCount = EstimateMessageTokens(message)
		result = append(result, item)
	}
	logToolResultProjection("assemble", convID, report)
	return result
}

func cloneSeahorseMessages(messages []Message) []Message {
	cloned := make([]Message, len(messages))
	for i := range messages {
		cloned[i] = messages[i]
		cloned[i].Parts = append([]MessagePart(nil), messages[i].Parts...)
	}
	return cloned
}

func singleToolResultPart(message Message) (int, bool) {
	if message.Role != "tool" {
		return -1, false
	}
	index := -1
	for i, part := range message.Parts {
		if part.Type != "tool_result" {
			continue
		}
		if index >= 0 {
			return -1, false
		}
		index = i
	}
	return index, index >= 0
}

func messageHasRetainedArtifact(message Message) bool {
	for _, part := range message.Parts {
		if part.Type == "media" {
			return true
		}
	}
	return false
}

func logToolResultProjection(stage string, convID int64, report toolResultProjectionReport) {
	if report.Receipted == 0 && report.Durable == 0 &&
		report.Transient == 0 && report.SafetyPreserved == 0 {
		return
	}
	logger.InfoCF("seahorse", "tool result retention projected", map[string]any{
		"stage":            stage,
		"conv_id":          convID,
		"preserved":        report.Preserved,
		"safety_preserved": report.SafetyPreserved,
		"receipted":        report.Receipted,
		"durable":          report.Durable,
		"transient":        report.Transient,
	})
}
