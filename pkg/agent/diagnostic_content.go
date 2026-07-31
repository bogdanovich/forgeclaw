package agent

import (
	"encoding/json"
	"strings"

	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/diagnostictrace"
	"github.com/bogdanovich/mintclaw/pkg/providers"
)

const (
	diagnosticTurnInputBytes      = 4 * 1024
	diagnosticTurnFinalBytes      = 8 * 1024
	diagnosticModelMessagesBytes  = 10 * 1024
	diagnosticModelResponseBytes  = 6 * 1024
	diagnosticModelReasoningBytes = 3 * 1024
	diagnosticModelToolCallsBytes = 2 * 1024
	diagnosticToolArgumentsBytes  = 6 * 1024
	diagnosticToolResultBytes     = 8 * 1024
	diagnosticErrorBytes          = 2 * 1024
	diagnosticSteeringBytes       = 4 * 1024
	diagnosticSerializedArgsBytes = 4 * 1024
	maxDiagnosticMessages         = 64
	maxDiagnosticToolCalls        = 64
)

func diagnosticContentEnabled(cfg *config.Config) bool {
	return cfg != nil &&
		cfg.Diagnostics.TraceCapture.Enabled &&
		cfg.Diagnostics.TraceCapture.EffectiveContentMode() == string(diagnostictrace.ContentRedacted)
}

func diagnosticTextPreview(cfg *config.Config, value string, maxBytes int) string {
	if !diagnosticContentEnabled(cfg) || strings.TrimSpace(value) == "" {
		return ""
	}
	return diagnostictrace.Redactor{
		Filter: cfg.SensitiveDataReplacer().Replace,
	}.RedactText(value, maxBytes)
}

func diagnosticJSONPreview(cfg *config.Config, value any, maxBytes int) string {
	if !diagnosticContentEnabled(cfg) || value == nil {
		return ""
	}
	return diagnostictrace.Redactor{
		Filter: cfg.SensitiveDataReplacer().Replace,
	}.RedactJSON(value, maxBytes)
}

func diagnosticMessagesPreview(cfg *config.Config, messages []providers.Message) string {
	if !diagnosticContentEnabled(cfg) || len(messages) == 0 {
		return ""
	}
	totalMessages := len(messages)
	toolNames := diagnosticToolNamesByCallID(messages)
	envelope := map[string]any{
		"total_messages": totalMessages,
		"latest_message": diagnosticMessagePreview(messages[len(messages)-1], toolNames),
	}
	selected := 1
	if len(messages) > 1 {
		envelope["origin_message"] = diagnosticMessagePreview(messages[0], toolNames)
		selected++
	}
	recent := make([]any, 0, min(maxDiagnosticMessages-selected, len(messages)-selected))
	for index := len(messages) - 2; index > 0 && selected < maxDiagnosticMessages; index-- {
		recent = append(recent, diagnosticMessagePreview(messages[index], toolNames))
		selected++
	}
	if len(recent) > 0 {
		envelope["recent_messages"] = recent
		envelope["recent_order"] = "newest_first"
	}
	addDiagnosticCount(envelope, "omitted_messages", totalMessages-selected)
	return diagnosticJSONPreview(cfg, envelope, diagnosticModelMessagesBytes)
}

func diagnosticMessagePreview(
	message providers.Message,
	toolNames map[string]string,
) map[string]any {
	item := map[string]any{
		"role": message.Role,
	}
	sensitive := diagnosticMessageContainsNodeEvidence(message, toolNames)
	if sensitive {
		item["content_redacted"] = true
	} else {
		addDiagnosticValue(item, "content", message.Content)
	}
	if sensitive && message.ReasoningContent != "" {
		item["reasoning_content_redacted"] = true
	} else {
		addDiagnosticValue(item, "reasoning_content", message.ReasoningContent)
	}
	addDiagnosticValue(item, "tool_call_id", message.ToolCallID)
	addDiagnosticValue(item, "tool_result_status", message.ToolResultStatus)
	addDiagnosticCount(item, "media_count", len(message.Media))
	addDiagnosticCount(item, "attachment_count", len(message.Attachments))
	calls := message.ToolCalls
	if len(calls) > maxDiagnosticToolCalls {
		calls = calls[:maxDiagnosticToolCalls]
		item["omitted_tool_calls"] = len(message.ToolCalls) - len(calls)
	}
	for _, call := range calls {
		renderedCalls, _ := item["tool_calls"].([]any)
		item["tool_calls"] = append(renderedCalls, diagnosticToolCallFromProvider(call))
	}
	return item
}

func diagnosticMessageContainsNodeEvidence(
	message providers.Message,
	toolNames map[string]string,
) bool {
	if message.Role == "tool" && !diagnosticToolPreviewAllowed(toolNames[message.ToolCallID]) {
		return true
	}
	return len(message.Attachments) > 0 || len(message.Media) > 0 ||
		diagnosticToolCallsContainNodeEvidence(message.ToolCalls) ||
		diagnosticContentContainsArtifactReference(message.Content) ||
		diagnosticContentContainsArtifactReference(message.ReasoningContent)
}

func diagnosticContentContainsArtifactReference(content string) bool {
	if strings.Contains(content, "media://") || strings.Contains(content, "transfer-artifact://") {
		return true
	}
	for _, prefix := range []string{"[image:/", "[audio:/", "[video:/", "[file:/"} {
		if strings.Contains(content, prefix) {
			return true
		}
	}
	return false
}

func diagnosticToolCallsContainNodeEvidence(calls []providers.ToolCall) bool {
	for _, call := range calls {
		name := call.Name
		if name == "" && call.Function != nil {
			name = call.Function.Name
		}
		if !diagnosticToolPreviewAllowed(name) {
			return true
		}
	}
	return false
}

func diagnosticLLMResponseContent(
	response *providers.LLMResponse,
	requestMessages []providers.Message,
) (string, string, bool) {
	if response == nil {
		return "", "", false
	}
	reasoning := firstNonEmptyString(response.ReasoningContent, response.Reasoning)
	if diagnosticToolCallsContainNodeEvidence(response.ToolCalls) ||
		diagnosticContentContainsArtifactReference(response.Content) ||
		diagnosticContentContainsArtifactReference(reasoning) ||
		diagnosticMessagesEndWithNodeResult(requestMessages) {
		return "", "", true
	}
	return response.Content, reasoning, false
}

func diagnosticMessagesEndWithNodeResult(messages []providers.Message) bool {
	if len(messages) == 0 {
		return false
	}
	toolNames := diagnosticToolNamesByCallID(messages)
	for index := len(messages) - 1; index >= 0; index-- {
		message := messages[index]
		if message.Role != "tool" {
			break
		}
		if !diagnosticToolPreviewAllowed(toolNames[message.ToolCallID]) {
			return true
		}
	}
	return false
}

func diagnosticToolCallsPreview(cfg *config.Config, calls []providers.ToolCall) string {
	if !diagnosticContentEnabled(cfg) || len(calls) == 0 {
		return ""
	}
	totalCalls := len(calls)
	omittedCalls := 0
	if len(calls) > maxDiagnosticToolCalls {
		omittedCalls = len(calls) - maxDiagnosticToolCalls
		calls = calls[:maxDiagnosticToolCalls]
	}
	preview := make([]any, 0, len(calls))
	for _, call := range calls {
		preview = append(preview, diagnosticToolCallFromProvider(call))
	}
	envelope := map[string]any{
		"total_tool_calls": totalCalls,
		"tool_calls":       preview,
	}
	addDiagnosticCount(envelope, "omitted_tool_calls", omittedCalls)
	return diagnosticJSONPreview(cfg, envelope, diagnosticModelToolCallsBytes)
}

func diagnosticToolCallFromProvider(call providers.ToolCall) map[string]any {
	name := call.Name
	var arguments any
	if len(call.Arguments) > 0 {
		arguments = call.Arguments
	}
	if call.Function != nil {
		if name == "" {
			name = call.Function.Name
		}
		if arguments == nil && call.Function.Arguments != "" {
			arguments = diagnosticSerializedArguments(call.Function.Arguments)
		}
	}
	item := map[string]any{}
	addDiagnosticValue(item, "id", call.ID)
	addDiagnosticValue(item, "name", name)
	if arguments != nil {
		if diagnosticToolPreviewAllowed(name) {
			item["arguments"] = arguments
		} else {
			item["arguments_redacted"] = true
		}
	}
	return item
}

func diagnosticToolNamesByCallID(messages []providers.Message) map[string]string {
	result := make(map[string]string)
	for _, message := range messages {
		for _, call := range message.ToolCalls {
			name := call.Name
			if name == "" && call.Function != nil {
				name = call.Function.Name
			}
			if call.ID != "" && name != "" {
				result[call.ID] = name
			}
		}
	}
	return result
}

func diagnosticSerializedArguments(value string) any {
	if len(value) > diagnosticSerializedArgsBytes {
		return "[UNSUPPORTED: oversized serialized arguments]"
	}
	var decoded any
	if json.Unmarshal([]byte(value), &decoded) != nil {
		return "[UNSUPPORTED: malformed serialized arguments]"
	}
	return decoded
}

func addDiagnosticValue[T comparable](item map[string]any, key string, value T) {
	var zero T
	if value != zero {
		item[key] = value
	}
}

func addDiagnosticCount(item map[string]any, key string, value int) {
	if value > 0 {
		item[key] = value
	}
}
