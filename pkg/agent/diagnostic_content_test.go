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
