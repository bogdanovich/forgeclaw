package tools

import (
	"strings"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/interactions"
	"github.com/bogdanovich/mintclaw/pkg/tools/loopguard"
)

func TestRequestUserInputToolGuidesConversationLanguagePresentation(t *testing.T) {
	tool, err := NewRequestUserInputTool(RequestUserInputToolOptions{})
	if err != nil {
		t.Fatalf("NewRequestUserInputTool() error = %v", err)
	}
	description := tool.Description()
	for _, want := range []string{
		"same language and general style as the conversation",
		"self-contained",
		"enough context for the user to answer directly",
	} {
		if !strings.Contains(description, want) {
			t.Fatalf("Description() missing %q: %s", want, description)
		}
	}
	questions := tool.Parameters()["properties"].(map[string]any)["questions"].(map[string]any)
	items := questions["items"].(map[string]any)
	properties := items["properties"].(map[string]any)
	for _, field := range []string{"header", "question"} {
		fieldDescription := properties[field].(map[string]any)["description"].(string)
		if !strings.Contains(fieldDescription, "conversation's language and style") {
			t.Fatalf("%s description does not assign language ownership: %s", field, fieldDescription)
		}
	}
	options := properties["options"].(map[string]any)["items"].(map[string]any)
	optionProperties := options["properties"].(map[string]any)
	for _, field := range []string{"label", "description"} {
		fieldDescription := optionProperties[field].(map[string]any)["description"].(string)
		if !strings.Contains(fieldDescription, "conversation's language and style") {
			t.Fatalf("option %s description does not assign language ownership: %s", field, fieldDescription)
		}
	}
}

func TestRequestUserInputToolReturnsTypedSuspension(t *testing.T) {
	tool, err := NewRequestUserInputTool(RequestUserInputToolOptions{})
	if err != nil {
		t.Fatalf("NewRequestUserInputTool() error = %v", err)
	}
	result := tool.Execute(t.Context(), map[string]any{
		"questions": []any{
			map[string]any{
				"id":       "deploy_mode",
				"header":   "Deploy",
				"question": "Which deployment mode should be used?",
				"options": []any{
					map[string]any{
						"label":       "Canary",
						"description": "Deploy to one profile first.",
					},
					map[string]any{"label": "All", "description": "Deploy to every profile now."},
				},
			},
		},
	})
	if result.IsError || result.Suspension == nil {
		t.Fatalf("Execute() = %#v, want suspension", result)
	}
	if result.ContentForLLM() != "" {
		t.Fatalf("ContentForLLM() = %q, want empty before resumption", result.ContentForLLM())
	}
	if result.Suspension.Kind != interactions.KindQuestion ||
		result.Suspension.Timeout != time.Hour {
		t.Fatalf("suspension = %#v", result.Suspension)
	}
	if got := result.Suspension.Questions[0].Options[1].Label; got != "All" {
		t.Fatalf("option label = %q, want All", got)
	}
	if got := tool.ToolLoopSemantics(); got != loopguard.SemanticsMutating {
		t.Fatalf("ToolLoopSemantics() = %q", got)
	}
	if got := tool.ToolSteeringSafety(nil); got != SteeringSafetyCancellable {
		t.Fatalf("ToolSteeringSafety() = %q", got)
	}
}

func TestRequestUserInputToolAcceptsLocalizedHeader(t *testing.T) {
	tool, err := NewRequestUserInputTool(RequestUserInputToolOptions{})
	if err != nil {
		t.Fatalf("NewRequestUserInputTool() error = %v", err)
	}
	result := tool.Execute(t.Context(), map[string]any{
		"questions": []any{map[string]any{
			"id":       "confirm_event",
			"header":   "Подтверждение",
			"question": "Создать событие?",
		}},
	})
	if result.IsError || result.Suspension == nil {
		t.Fatalf("Execute() = %#v, want suspension", result)
	}

	result = tool.Execute(t.Context(), map[string]any{
		"questions": []any{map[string]any{
			"id":       "confirm_event",
			"header":   strings.Repeat("я", interactions.MaxHeaderLength+1),
			"question": "Создать событие?",
		}},
	})
	if !result.IsError ||
		!strings.Contains(result.ContentForLLM(), "header exceeds 64 characters") {
		t.Fatalf("Execute() = %#v, want field-specific header bound error", result)
	}
}

func TestRequestUserInputToolUsesBoundedConfiguredTimeout(t *testing.T) {
	tool, err := NewRequestUserInputTool(RequestUserInputToolOptions{
		DefaultTimeout: 5 * time.Minute,
		MaxTimeout:     10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewRequestUserInputTool() error = %v", err)
	}
	args := map[string]any{
		"questions": []any{
			map[string]any{"id": "name", "question": "What name should be used?"},
		},
		"timeout_seconds": float64(600),
	}
	result := tool.Execute(t.Context(), args)
	if result.IsError || result.Suspension.Timeout != 10*time.Minute {
		t.Fatalf("Execute() = %#v", result)
	}
	args["timeout_seconds"] = float64(601)
	if result := tool.Execute(t.Context(), args); !result.IsError {
		t.Fatal("Execute() accepted timeout above configured maximum")
	}
	args["timeout_seconds"] = 60.5
	if result := tool.Execute(t.Context(), args); !result.IsError {
		t.Fatal("Execute() accepted fractional timeout")
	}
	args["timeout_seconds"] = float64(1 << 62)
	if result := tool.Execute(t.Context(), args); !result.IsError {
		t.Fatal("Execute() accepted overflowing timeout")
	}
}

func TestRequestUserInputToolRejectsInvalidQuestionShapes(t *testing.T) {
	tool, err := NewRequestUserInputTool(RequestUserInputToolOptions{})
	if err != nil {
		t.Fatalf("NewRequestUserInputTool() error = %v", err)
	}
	tests := []struct {
		name      string
		questions any
	}{
		{name: "missing", questions: nil},
		{name: "bad id", questions: []any{map[string]any{"id": "Bad ID", "question": "Question?"}}},
		{name: "duplicate ids", questions: []any{
			map[string]any{"id": "same", "question": "One?"},
			map[string]any{"id": "same", "question": "Two?"},
		}},
		{name: "single option", questions: []any{map[string]any{
			"id": "mode", "question": "Mode?", "options": []any{
				map[string]any{"label": "Only", "description": "The only choice."},
			},
		}}},
		{name: "duplicate option", questions: []any{map[string]any{
			"id": "mode", "question": "Mode?", "options": []any{
				map[string]any{"label": "Same", "description": "First."},
				map[string]any{"label": "same", "description": "Second."},
			},
		}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := tool.Execute(t.Context(), map[string]any{"questions": test.questions})
			if !result.IsError || result.Suspension != nil {
				t.Fatalf("Execute() = %#v, want validation error", result)
			}
		})
	}
}

func TestNewRequestUserInputToolRejectsInvalidTimeoutConfiguration(t *testing.T) {
	tests := []RequestUserInputToolOptions{
		{DefaultTimeout: 30 * time.Second},
		{DefaultTimeout: 2 * time.Hour, MaxTimeout: time.Hour},
		{MaxTimeout: 25 * time.Hour},
	}
	for _, options := range tests {
		if _, err := NewRequestUserInputTool(options); err == nil {
			t.Fatalf("NewRequestUserInputTool(%+v) succeeded", options)
		}
	}
}

func TestRequestUserInputToolParametersExposeRuntimeMaximum(t *testing.T) {
	tool, err := NewRequestUserInputTool(RequestUserInputToolOptions{
		DefaultTimeout: 5 * time.Minute,
		MaxTimeout:     10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewRequestUserInputTool() error = %v", err)
	}
	properties := tool.Parameters()["properties"].(map[string]any)
	timeout := properties["timeout_seconds"].(map[string]any)
	if got := timeout["maximum"]; got != 600 {
		t.Fatalf("timeout maximum = %#v, want 600", got)
	}
}

func TestRequestUserInputToolParametersExposeStringBounds(t *testing.T) {
	tool, err := NewRequestUserInputTool(RequestUserInputToolOptions{})
	if err != nil {
		t.Fatalf("NewRequestUserInputTool() error = %v", err)
	}
	questions := tool.Parameters()["properties"].(map[string]any)["questions"].(map[string]any)
	properties := questions["items"].(map[string]any)["properties"].(map[string]any)
	for field, want := range map[string]int{
		"id":       interactions.MaxQuestionIDLength,
		"header":   interactions.MaxHeaderLength,
		"question": interactions.MaxQuestionLength,
	} {
		if got := properties[field].(map[string]any)["maxLength"]; got != want {
			t.Errorf("%s maxLength = %#v, want %d", field, got, want)
		}
	}
	options := properties["options"].(map[string]any)
	optionItems := options["items"].(map[string]any)
	optionProperties := optionItems["properties"].(map[string]any)
	for field, want := range map[string]int{
		"label":       interactions.MaxOptionLabelLength,
		"description": interactions.MaxDescriptionLength,
	} {
		if got := optionProperties[field].(map[string]any)["maxLength"]; got != want {
			t.Errorf("option %s maxLength = %#v, want %d", field, got, want)
		}
	}
}
