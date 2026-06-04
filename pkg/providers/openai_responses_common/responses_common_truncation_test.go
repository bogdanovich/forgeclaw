package openai_responses_common

import (
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/providers/protocoltypes"
)

func TestTranslateMessages_TruncatesOversizedToolOutput(t *testing.T) {
	oversized := strings.Repeat("a", maxFunctionCallOutputBytes+2048)
	input, _ := TranslateMessages([]protocoltypes.Message{{
		Role:       "tool",
		ToolCallID: "call_1",
		Content:    oversized,
	}})
	if len(input) != 1 || input[0].OfFunctionCallOutput == nil {
		t.Fatalf("expected one function_call_output item, got %#v", input)
	}
	out := input[0].OfFunctionCallOutput.Output.OfString.Value
	if len(out) > maxFunctionCallOutputBytes {
		t.Fatalf("truncated output length = %d, want <= %d", len(out), maxFunctionCallOutputBytes)
	}
	if !strings.Contains(out, "tool output truncated") {
		t.Fatalf("expected truncation marker, got prefix %q", out[:80])
	}
}
