package agent

import (
	"strings"
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/providers"
)

func TestDiagnosticContentHelpersAreOptInAndExcludeProviderSecrets(t *testing.T) {
	cfg := config.DefaultConfig()
	message := providers.Message{Role: "user", Content: "inspect sk-1234567890abcdef"}
	if got := diagnosticMessagesPreview(cfg, []providers.Message{message}); got != "" {
		t.Fatalf("disabled preview = %q", got)
	}
	cfg.Diagnostics.TraceCapture.Enabled = true
	if got := diagnosticMessagesPreview(cfg, []providers.Message{message}); got != "" {
		t.Fatalf("metadata-only preview = %q", got)
	}

	cfg.Diagnostics.TraceCapture.ContentMode = "redacted_content"
	configuredSecret := "opaque-config-value-987654"
	cfg.ModelList = config.SecureModelList{&config.ModelConfig{
		ModelName: "diagnostic-test",
		APIKeys:   config.SimpleSecureStrings(configuredSecret),
	}}
	cfg.Tools.FilterSensitiveData = false
	calls := []providers.ToolCall{{
		ID:   "call-1",
		Name: "read_file",
		Arguments: map[string]any{
			"path": "/tmp/report", "token": "arbitrary-secret",
			"note": configuredSecret,
		},
		ThoughtSignature: "provider-thought-secret",
	}}
	got := diagnosticToolCallsPreview(cfg, calls)
	if !strings.Contains(got, "read_file") || !strings.Contains(got, "/tmp/report") {
		t.Fatalf("tool call preview = %q", got)
	}
	for _, forbidden := range []string{
		"arbitrary-secret", "provider-thought-secret", "sk-1234567890abcdef",
		configuredSecret,
	} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("tool call preview leaked %q: %s", forbidden, got)
		}
	}

	messages := diagnosticMessagesPreview(cfg, []providers.Message{message})
	if !strings.Contains(messages, "[REDACTED]") ||
		strings.Contains(messages, "sk-1234567890abcdef") {
		t.Fatalf("message preview = %q", messages)
	}
}

func TestDiagnosticMessagesPreviewBoundsCollectionAndKeepsContextEnds(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Diagnostics.TraceCapture.Enabled = true
	cfg.Diagnostics.TraceCapture.ContentMode = "redacted_content"
	messages := make([]providers.Message, maxDiagnosticMessages+8)
	for i := range messages {
		messages[i] = providers.Message{Role: "user", Content: "middle"}
	}
	messages[0] = providers.Message{Role: "system", Content: "system-policy"}
	messages[len(messages)-1] = providers.Message{Role: "user", Content: "latest-request"}

	got := diagnosticMessagesPreview(cfg, messages)
	for _, expected := range []string{
		"system-policy", "latest-request", `"omitted_messages":8`,
		`"recent_order":"newest_first"`,
	} {
		if !strings.Contains(got, expected) {
			t.Fatalf("message preview lacks %q: %s", expected, got)
		}
	}
	if len(got) > diagnosticModelMessagesBytes {
		t.Fatalf("message preview length = %d", len(got))
	}
}

func TestDiagnosticMessagesPreviewKeepsLatestWhenOriginIsOversized(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Diagnostics.TraceCapture.Enabled = true
	cfg.Diagnostics.TraceCapture.ContentMode = "redacted_content"
	got := diagnosticMessagesPreview(cfg, []providers.Message{
		{Role: "system", Content: strings.Repeat("large-system ", 2000)},
		{Role: "user", Content: "latest-request"},
	})
	if !strings.Contains(got, "latest-request") {
		t.Fatalf("message preview lost latest message: %s", got)
	}
	if len(got) > diagnosticModelMessagesBytes {
		t.Fatalf("message preview length = %d", len(got))
	}
}

func TestDiagnosticToolCallsFailClosedForSerializedArguments(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Diagnostics.TraceCapture.Enabled = true
	cfg.Diagnostics.TraceCapture.ContentMode = "redacted_content"

	tests := []struct {
		name        string
		arguments   string
		placeholder string
	}{
		{
			name: "malformed", arguments: `{"password":"hunter2"`,
			placeholder: "malformed serialized arguments",
		},
		{
			name:        "oversized",
			arguments:   `{"password":"` + strings.Repeat("hunter2", 1024) + `"}`,
			placeholder: "oversized serialized arguments",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := diagnosticToolCallsPreview(cfg, []providers.ToolCall{{
				ID: "call-1",
				Function: &providers.FunctionCall{
					Name: "shell", Arguments: test.arguments,
				},
			}})
			if !strings.Contains(got, test.placeholder) || strings.Contains(got, "hunter2") {
				t.Fatalf("tool call preview = %q", got)
			}
		})
	}
}

func TestDiagnosticNodeFileMessagesRetainStructureWithoutAuthority(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Diagnostics.TraceCapture.Enabled = true
	cfg.Diagnostics.TraceCapture.ContentMode = "redacted_content"
	secretPath := "/private/node/config.json"
	fullDigest := strings.Repeat("a", 64)
	mediaRef := "media://private-artifact"
	transferRef := "transfer-artifact://private-download"
	messages := []providers.Message{
		{
			Role: "user", Content: "upload [file:/private/gateway/source]", Media: []string{mediaRef},
			Attachments: []providers.Attachment{{Ref: mediaRef}},
		},
		{
			Role:             "assistant",
			Content:          "upload " + transferRef,
			ReasoningContent: "Use " + secretPath + " with digest " + fullDigest,
			ToolCalls: []providers.ToolCall{{
				ID: "call-file", Name: "nodes_upload", Arguments: map[string]any{
					"artifact_ref": mediaRef, "destination": secretPath,
				},
			}},
		},
		{
			Role: "tool", ToolCallID: "call-file",
			Content: `{"path":"` + secretPath + `","sha256":"` + fullDigest + `"}`,
		},
	}
	preview := diagnosticMessagesPreview(cfg, messages)
	toolCalls := diagnosticToolCallsPreview(cfg, messages[1].ToolCalls)
	for _, got := range []string{preview, toolCalls} {
		if !strings.Contains(got, "redacted") {
			t.Fatalf("sensitive diagnostic projection = %q", got)
		}
		for _, forbidden := range []string{
			secretPath, fullDigest, mediaRef, transferRef, "/private/gateway/source",
		} {
			if strings.Contains(got, forbidden) {
				t.Fatalf("sensitive diagnostic projection leaked %q: %s", forbidden, got)
			}
		}
	}
}

func TestFormatMessagesForLogRedactsNodeFileHistory(t *testing.T) {
	secretPath := "/private/node/config.json"
	mediaRef := "media://private-upload"
	transferRef := "transfer-artifact://private-download"
	messages := []providers.Message{
		{Role: "user", Content: "upload " + mediaRef, Media: []string{mediaRef}},
		{
			Role: "assistant", Content: "write " + secretPath,
			ToolCalls: []providers.ToolCall{{
				ID: "call-file", Name: "nodes_upload",
				Function: &providers.FunctionCall{
					Name:      "nodes_upload",
					Arguments: `{"destination":"` + secretPath + `","artifact_ref":"` + mediaRef + `"}`,
				},
			}},
		},
		{Role: "tool", ToolCallID: "call-file", Content: transferRef + " " + secretPath},
	}

	got := formatMessagesForLog(messages)
	for _, want := range []string{"nodes_upload", "[REDACTED]"} {
		if !strings.Contains(got, want) {
			t.Fatalf("log projection omitted %q: %s", want, got)
		}
	}
	for _, forbidden := range []string{secretPath, mediaRef, transferRef} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("log projection leaked %q: %s", forbidden, got)
		}
	}
}

func TestHumanInteractionAnswersAreRedactedFromLogsAndTraces(t *testing.T) {
	interactionID := "interaction-private-approval"
	answer := "allow-once-private-answer"
	messages := []providers.Message{
		{
			Role: "assistant",
			ToolCalls: []providers.ToolCall{{
				ID: "call-question", Name: "request_user_input",
				Function: &providers.FunctionCall{
					Name:      "request_user_input",
					Arguments: `{"questions":[{"id":"approval","question":"Reveal private approval?"}]}`,
				},
			}},
		},
		{
			Role:       "tool",
			ToolCallID: "call-question",
			Content: `{"interaction_id":"` + interactionID +
				`","outcome":"answered","answers":{"approval":"` + answer + `"}}`,
		},
		{
			Role:             "assistant",
			Content:          "The private decision was " + answer,
			ReasoningContent: "Use " + answer + " in the next tool call",
			ToolCalls: []providers.ToolCall{{
				ID: "call-question", Name: "read_file",
				Function: &providers.FunctionCall{
					Name: "read_file", Arguments: `{"path":"` + answer + `"}`,
				},
			}},
		},
		{Role: "tool", ToolCallID: "call-question", Content: "public-result"},
	}

	logPreview := formatMessagesForLog(messages)
	cfg := config.DefaultConfig()
	cfg.Diagnostics.TraceCapture.Enabled = true
	cfg.Diagnostics.TraceCapture.ContentMode = "redacted_content"
	tracePreview := diagnosticMessagesPreview(cfg, messages)
	for _, preview := range []string{logPreview, tracePreview} {
		if !strings.Contains(preview, "request_user_input") ||
			!strings.Contains(strings.ToLower(preview), "redact") ||
			!strings.Contains(preview, "public-result") {
			t.Fatalf("interaction preview was not redacted: %s", preview)
		}
		for _, forbidden := range []string{
			interactionID, answer, "Reveal private approval?",
		} {
			if strings.Contains(preview, forbidden) {
				t.Fatalf("interaction preview leaked %q: %s", forbidden, preview)
			}
		}
	}
}

func TestUnmatchedToolResultsAreRedactedFromLogsAndTraces(t *testing.T) {
	answer := "unmatched-private-answer"
	cfg := config.DefaultConfig()
	cfg.Diagnostics.TraceCapture.Enabled = true
	cfg.Diagnostics.TraceCapture.ContentMode = "redacted_content"
	for _, message := range []providers.Message{
		{Role: "tool", ToolCallID: "missing-call", Content: answer},
		{Role: "tool", Content: answer},
	} {
		for _, preview := range []string{
			formatMessagesForLog([]providers.Message{message}),
			diagnosticMessagesPreview(cfg, []providers.Message{message}),
		} {
			if strings.Contains(preview, answer) || !strings.Contains(strings.ToLower(preview), "redact") {
				t.Fatalf("unmatched tool result was not redacted: %s", preview)
			}
		}
	}
}

func TestUnnamedReusedToolCallsInvalidateEarlierCorrelation(t *testing.T) {
	answer := "unnamed-private-answer"
	messages := []providers.Message{
		{
			Role:      "assistant",
			ToolCalls: []providers.ToolCall{{ID: "reused-call", Name: "read_file"}},
		},
		{Role: "tool", ToolCallID: "reused-call", Content: "public-result"},
		{
			Role:      "assistant",
			ToolCalls: []providers.ToolCall{{ID: "reused-call"}},
		},
		{Role: "tool", ToolCallID: "reused-call", Content: answer},
	}

	cfg := config.DefaultConfig()
	cfg.Diagnostics.TraceCapture.Enabled = true
	cfg.Diagnostics.TraceCapture.ContentMode = "redacted_content"
	for _, preview := range []string{
		formatMessagesForLog(messages),
		diagnosticMessagesPreview(cfg, messages),
	} {
		if strings.Contains(preview, answer) || !strings.Contains(preview, "public-result") {
			t.Fatalf("unnamed reused tool call did not fail closed: %s", preview)
		}
	}
}

func TestDuplicateToolCallIDsInOneBatchFailClosed(t *testing.T) {
	answer := "duplicate-private-answer"
	messages := []providers.Message{
		{
			Role: "assistant",
			ToolCalls: []providers.ToolCall{
				{ID: "duplicate-call", Name: "request_user_input"},
				{ID: "duplicate-call", Name: "read_file"},
			},
		},
		{Role: "tool", ToolCallID: "duplicate-call", Content: answer},
		{
			Role:      "assistant",
			ToolCalls: []providers.ToolCall{{ID: "duplicate-call", Name: "read_file"}},
		},
		{Role: "tool", ToolCallID: "duplicate-call", Content: "public-result"},
	}

	cfg := config.DefaultConfig()
	cfg.Diagnostics.TraceCapture.Enabled = true
	cfg.Diagnostics.TraceCapture.ContentMode = "redacted_content"
	for _, preview := range []string{
		formatMessagesForLog(messages),
		diagnosticMessagesPreview(cfg, messages),
	} {
		if strings.Contains(preview, answer) || !strings.Contains(strings.ToLower(preview), "redact") ||
			!strings.Contains(preview, "public-result") {
			t.Fatalf("duplicate tool-call ID did not fail closed: %s", preview)
		}
	}
}

func TestDiagnosticLLMResponseRedactsFollowUpToolArguments(t *testing.T) {
	answer := "private-approval-answer"
	request := []providers.Message{
		{
			Role: "assistant",
			ToolCalls: []providers.ToolCall{{
				ID: "call-question", Name: "request_user_input",
			}},
		},
		{
			Role:       "tool",
			ToolCallID: "call-question",
			Content:    answer,
		},
	}
	response := &providers.LLMResponse{
		Content:   "The private decision was " + answer,
		Reasoning: "Use the private decision in the next tool call",
		ToolCalls: []providers.ToolCall{{
			ID: "call-read", Name: "read_file",
			Function: &providers.FunctionCall{
				Name: "read_file", Arguments: `{"path":"` + answer + `"}`,
			},
		}},
	}

	content, reasoning, sensitive := diagnosticLLMResponseContent(response, request)
	if !sensitive || content != "" || reasoning != "" {
		t.Fatalf("sensitive response projection = (%q, %q, %v)", content, reasoning, sensitive)
	}
	cfg := config.DefaultConfig()
	cfg.Diagnostics.TraceCapture.Enabled = true
	cfg.Diagnostics.TraceCapture.ContentMode = "redacted_content"
	toolCalls := diagnosticToolCallsPreviewWithSensitivity(cfg, response.ToolCalls, sensitive)
	if strings.Contains(toolCalls, answer) || !strings.Contains(toolCalls, "arguments_redacted") {
		t.Fatalf("sensitive response tool arguments were not redacted: %s", toolCalls)
	}
}

func TestDiagnosticLLMResponseSuppressesNodeFileContentAndReasoning(t *testing.T) {
	secretPath := "/private/node/config.json"
	transferRef := "transfer-artifact://private-download"
	content, reasoning, sensitive := diagnosticLLMResponseContent(&providers.LLMResponse{
		Content:          "upload " + secretPath,
		ReasoningContent: "deliver " + transferRef,
		ToolCalls: []providers.ToolCall{{
			ID: "call-file", Name: "nodes_download",
		}},
	}, nil)
	if !sensitive || content != "" || reasoning != "" {
		t.Fatalf("sensitive response projection = (%q, %q, %v)", content, reasoning, sensitive)
	}

	content, reasoning, sensitive = diagnosticLLMResponseContent(&providers.LLMResponse{
		Content: "safe content", Reasoning: "safe reasoning",
	}, nil)
	if sensitive || content != "safe content" || reasoning != "safe reasoning" {
		t.Fatalf("ordinary response projection = (%q, %q, %v)", content, reasoning, sensitive)
	}
}

func TestDiagnosticLLMResponseSuppressesImmediateNodeFileFollowUp(t *testing.T) {
	secretPath := "/private/node/config.json"
	fullDigest := strings.Repeat("b", 64)
	request := []providers.Message{
		{
			Role: "assistant",
			ToolCalls: []providers.ToolCall{{
				ID: "call-info", Name: "nodes_file_info",
			}},
		},
		{
			Role: "tool", ToolCallID: "call-info",
			Content: `{"path":"` + secretPath + `","sha256":"` + fullDigest + `"}`,
		},
	}
	content, reasoning, sensitive := diagnosticLLMResponseContent(&providers.LLMResponse{
		Content:   "The file is at " + secretPath,
		Reasoning: "Its digest is " + fullDigest,
	}, request)
	if !sensitive || content != "" || reasoning != "" {
		t.Fatalf("follow-up response projection = (%q, %q, %v)", content, reasoning, sensitive)
	}

	request = append(request, providers.Message{Role: "user", Content: "new unrelated turn"})
	content, reasoning, sensitive = diagnosticLLMResponseContent(&providers.LLMResponse{
		Content: "safe content", Reasoning: "safe reasoning",
	}, request)
	if sensitive || content != "safe content" || reasoning != "safe reasoning" {
		t.Fatalf("later response projection = (%q, %q, %v)", content, reasoning, sensitive)
	}
}

func TestDiagnosticLLMResponseSuppressesNodeFileGracefulInterrupt(t *testing.T) {
	secretPath := "/private/node/config.json"
	fullDigest := strings.Repeat("c", 64)
	request := []providers.Message{
		{
			Role: "assistant",
			ToolCalls: []providers.ToolCall{{
				ID: "call-download", Name: "nodes_download",
			}},
		},
		{
			Role: "tool", ToolCallID: "call-download",
			Content: `{"path":"` + secretPath + `","sha256":"` + fullDigest + `"}`,
		},
		interruptPromptMessage("Stop scheduling tools and provide a short final summary."),
	}
	content, reasoning, sensitive := diagnosticLLMResponseContent(&providers.LLMResponse{
		Content:   "Downloaded " + secretPath,
		Reasoning: "The digest is " + fullDigest,
	}, request)
	if !sensitive || content != "" || reasoning != "" {
		t.Fatalf("graceful response projection = (%q, %q, %v)", content, reasoning, sensitive)
	}

	request[len(request)-1] = providers.Message{
		Role: "user", Content: "Stop scheduling tools and provide a short final summary.",
	}
	content, reasoning, sensitive = diagnosticLLMResponseContent(&providers.LLMResponse{
		Content: "safe content", Reasoning: "safe reasoning",
	}, request)
	if sensitive || content != "safe content" || reasoning != "safe reasoning" {
		t.Fatalf("untrusted lookalike projection = (%q, %q, %v)", content, reasoning, sensitive)
	}
}
