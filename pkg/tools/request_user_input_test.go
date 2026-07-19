package tools

import (
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/interactions"
	"github.com/sipeed/picoclaw/pkg/tools/loopguard"
)

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
