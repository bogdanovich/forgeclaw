package tools

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	toolshared "github.com/bogdanovich/mintclaw/pkg/tools/shared"
)

func TestNewToolResult(t *testing.T) {
	result := toolshared.NewToolResult("test content")

	if result.ForLLM != "test content" {
		t.Errorf("Expected ForLLM 'test content', got '%s'", result.ForLLM)
	}
	if result.Silent {
		t.Error("Expected Silent to be false")
	}
	if result.IsError {
		t.Error("Expected IsError to be false")
	}
	if result.Async {
		t.Error("Expected Async to be false")
	}
}

func TestSilentResult(t *testing.T) {
	result := toolshared.SilentResult("silent operation")

	if result.ForLLM != "silent operation" {
		t.Errorf("Expected ForLLM 'silent operation', got '%s'", result.ForLLM)
	}
	if !result.Silent {
		t.Error("Expected Silent to be true")
	}
	if result.IsError {
		t.Error("Expected IsError to be false")
	}
	if result.Async {
		t.Error("Expected Async to be false")
	}
}

func TestDiffResult(t *testing.T) {
	result := toolshared.DiffResult("pkg/tools/fs/edit.go", []byte("hello world\n"), []byte("hello universe\n"))

	if result.Silent {
		t.Error("Expected Silent to be false")
	}
	if result.IsError {
		t.Error("Expected IsError to be false")
	}
	if result.Async {
		t.Error("Expected Async to be false")
	}
	if result.ForLLM == result.ForUser {
		t.Fatalf("Expected ForLLM to omit the full diff, got %q", result.ForLLM)
	}
	if len(result.ForLLM) >= len(result.ForUser) {
		t.Fatalf("Expected ForLLM to stay smaller than ForUser, got %d vs %d", len(result.ForLLM), len(result.ForUser))
	}

	for _, want := range []string{
		"File edited: pkg/tools/fs/edit.go",
		"```diff",
		"--- a/pkg/tools/fs/edit.go",
		"+++ b/pkg/tools/fs/edit.go",
		"-hello world",
		"+hello universe",
	} {
		if !strings.Contains(result.ForUser, want) {
			t.Fatalf("DiffResult output missing %q:\n%s", want, result.ForUser)
		}
	}
}

func TestDiffResult_NormalizesAbsolutePathsAndHandlesNoOpChanges(t *testing.T) {
	result := toolshared.DiffResult("/tmp/test.txt", []byte("same\n"), []byte("same\n"))

	if !strings.Contains(result.ForUser, "File edited: /tmp/test.txt") {
		t.Fatalf("Expected original path in output, got %q", result.ForUser)
	}
	if !strings.Contains(result.ForUser, "(no content change)") {
		t.Fatalf("Expected no-content-change marker, got %q", result.ForUser)
	}
	if !strings.Contains(result.ForLLM, "(no content change)") {
		t.Fatalf("Expected compact no-op summary in ForLLM, got %q", result.ForLLM)
	}
}

func TestAsyncResult(t *testing.T) {
	result := toolshared.AsyncResult("async task started")

	if result.ForLLM != "async task started" {
		t.Errorf("Expected ForLLM 'async task started', got '%s'", result.ForLLM)
	}
	if result.Silent {
		t.Error("Expected Silent to be false")
	}
	if result.IsError {
		t.Error("Expected IsError to be false")
	}
	if !result.Async {
		t.Error("Expected Async to be true")
	}
	if result.AsyncDelivery != "" {
		t.Errorf("Expected empty AsyncDelivery by default, got %q", result.AsyncDelivery)
	}
}

func TestToolResultWithAsyncDelivery(t *testing.T) {
	result := toolshared.AsyncResult("async task started").WithAsyncDelivery(toolshared.AsyncDeliveryUserOnly)

	if result.AsyncDelivery != toolshared.AsyncDeliveryUserOnly {
		t.Fatalf("AsyncDelivery = %q, want %q", result.AsyncDelivery, toolshared.AsyncDeliveryUserOnly)
	}
}

func TestToolResultWithAsyncTaskID(t *testing.T) {
	result := toolshared.AsyncResult("async task started").WithAsyncTaskID(" subagent-7 ")

	if result.AsyncTaskID != "subagent-7" {
		t.Fatalf("AsyncTaskID = %q, want subagent-7", result.AsyncTaskID)
	}
}

func TestToolResultContentForLLMIncludesCompletion(t *testing.T) {
	result := toolshared.NewToolResult("child finished").WithCompletion(&toolshared.CompletionResult{
		Text: "recipe text",
		Media: []toolshared.CompletionMedia{
			{
				Ref:         "media://video",
				Type:        "video",
				Filename:    "clip.mp4",
				ContentType: "video/mp4",
			},
		},
	})

	content := result.ContentForLLM()
	if !strings.Contains(content, "child finished") {
		t.Fatalf("expected base content, got %q", content)
	}
	if !strings.Contains(content, "Structured child completion:") {
		t.Fatalf("expected structured completion note, got %q", content)
	}
	if !strings.Contains(content, `"text":"recipe text"`) {
		t.Fatalf("expected completion text JSON, got %q", content)
	}
	if !strings.Contains(content, `"ref":"media://video"`) {
		t.Fatalf("expected completion media JSON, got %q", content)
	}
	if !strings.Contains(content, `"type":"video"`) {
		t.Fatalf("expected completion media type JSON, got %q", content)
	}
	if result.Deliverable == nil {
		t.Fatalf("expected completion to mirror into deliverable")
	}
	if len(result.Deliverable.Artifacts) != 1 || result.Deliverable.Artifacts[0].Ref != "media://video" {
		t.Fatalf("unexpected deliverable: %+v", result.Deliverable)
	}
}

func TestToolResultContentForLLMIncludesDeliverable(t *testing.T) {
	result := toolshared.NewToolResult("tool finished").WithDeliverable(&toolshared.DeliverableResult{
		Text: "saved recipe",
		Artifacts: []toolshared.DeliverableItem{
			{
				Ref:         "file:/tmp/recipe.md",
				Kind:        "file",
				Filename:    "recipe.md",
				ContentType: "text/markdown",
			},
		},
		Metadata: map[string]string{"source": "instagram"},
	})

	content := result.ContentForLLM()
	for _, want := range []string{
		"tool finished",
		"Structured deliverable:",
		`"text":"saved recipe"`,
		`"ref":"file:/tmp/recipe.md"`,
		`"kind":"file"`,
		`"source":"instagram"`,
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("expected %q in content:\n%s", want, content)
		}
	}
}

func TestToolResultContentForLLMIncludesDeliverableReport(t *testing.T) {
	result := toolshared.NewToolResult("tool finished").WithDeliverable(&toolshared.DeliverableResult{
		Report: &toolshared.DeliverableReport{
			SchemaVersion: "deliverable_report.v1",
			ReportID:      "review-1",
			Summary:       "No high-confidence issues found",
			Claims: []toolshared.ReportClaim{{
				Kind:       "negative_evidence",
				Text:       "No correctness issues found",
				Confidence: "high",
			}},
		},
	})

	content := result.ContentForLLM()
	for _, want := range []string{
		"tool finished",
		"Structured deliverable:",
		`"report_id":"review-1"`,
		`"summary":"No high-confidence issues found"`,
		`"kind":"negative_evidence"`,
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("expected %q in content:\n%s", want, content)
		}
	}
}

func TestMediaResultCreatesDeliverable(t *testing.T) {
	result := toolshared.MediaResult("media ready", []string{"media://one", "media://two"})

	if result.Deliverable == nil {
		t.Fatal("expected media result to include deliverable")
	}
	if len(result.Deliverable.Artifacts) != 2 {
		t.Fatalf("artifact count = %d, want 2", len(result.Deliverable.Artifacts))
	}
	if result.Deliverable.Artifacts[0].Kind != "media" {
		t.Fatalf("artifact kind = %q, want media", result.Deliverable.Artifacts[0].Kind)
	}
}

func TestErrorResult(t *testing.T) {
	result := toolshared.ErrorResult("operation failed")

	if result.ForLLM != "operation failed" {
		t.Errorf("Expected ForLLM 'operation failed', got '%s'", result.ForLLM)
	}
	if result.Silent {
		t.Error("Expected Silent to be false")
	}
	if !result.IsError {
		t.Error("Expected IsError to be true")
	}
	if result.Async {
		t.Error("Expected Async to be false")
	}
}

func TestUserResult(t *testing.T) {
	content := "user visible message"
	result := toolshared.UserResult(content)

	if result.ForLLM != content {
		t.Errorf("Expected ForLLM '%s', got '%s'", content, result.ForLLM)
	}
	if result.ForUser != content {
		t.Errorf("Expected ForUser '%s', got '%s'", content, result.ForUser)
	}
	if result.Silent {
		t.Error("Expected Silent to be false")
	}
	if result.IsError {
		t.Error("Expected IsError to be false")
	}
	if result.Async {
		t.Error("Expected Async to be false")
	}
}

func TestToolResultJSONSerialization(t *testing.T) {
	tests := []struct {
		name   string
		result *toolshared.ToolResult
	}{
		{
			name:   "basic result",
			result: toolshared.NewToolResult("basic content"),
		},
		{
			name:   "silent result",
			result: toolshared.SilentResult("silent content"),
		},
		{
			name:   "async result",
			result: toolshared.AsyncResult("async content"),
		},
		{
			name:   "error result",
			result: toolshared.ErrorResult("error content"),
		},
		{
			name:   "user result",
			result: toolshared.UserResult("user content"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal to JSON
			data, err := json.Marshal(tt.result)
			if err != nil {
				t.Fatalf("Failed to marshal: %v", err)
			}

			// Unmarshal back
			var decoded toolshared.ToolResult
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatalf("Failed to unmarshal: %v", err)
			}

			// Verify fields match (Err should be excluded)
			if decoded.ForLLM != tt.result.ForLLM {
				t.Errorf("ForLLM mismatch: got '%s', want '%s'", decoded.ForLLM, tt.result.ForLLM)
			}
			if decoded.ForUser != tt.result.ForUser {
				t.Errorf("ForUser mismatch: got '%s', want '%s'", decoded.ForUser, tt.result.ForUser)
			}
			if decoded.Silent != tt.result.Silent {
				t.Errorf("Silent mismatch: got %v, want %v", decoded.Silent, tt.result.Silent)
			}
			if decoded.IsError != tt.result.IsError {
				t.Errorf("IsError mismatch: got %v, want %v", decoded.IsError, tt.result.IsError)
			}
			if decoded.Async != tt.result.Async {
				t.Errorf("Async mismatch: got %v, want %v", decoded.Async, tt.result.Async)
			}
		})
	}
}

func TestToolResultWithErrors(t *testing.T) {
	err := errors.New("underlying error")
	result := toolshared.ErrorResult("error message").WithError(err)

	if result.Err == nil {
		t.Error("Expected Err to be set")
	}
	if result.Err.Error() != "underlying error" {
		t.Errorf("Expected Err message 'underlying error', got '%s'", result.Err.Error())
	}

	// Verify Err is not serialized
	data, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		t.Fatalf("Failed to marshal: %v", marshalErr)
	}

	var decoded toolshared.ToolResult
	if unmarshalErr := json.Unmarshal(data, &decoded); unmarshalErr != nil {
		t.Fatalf("Failed to unmarshal: %v", unmarshalErr)
	}

	if decoded.Err != nil {
		t.Error("Expected Err to be nil after JSON round-trip (should not be serialized)")
	}
}

func TestToolResultJSONStructure(t *testing.T) {
	result := toolshared.UserResult("test content")

	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	// Verify JSON structure
	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	// Check expected keys exist
	if _, ok := parsed["for_llm"]; !ok {
		t.Error("Expected 'for_llm' key in JSON")
	}
	if _, ok := parsed["for_user"]; !ok {
		t.Error("Expected 'for_user' key in JSON")
	}
	if _, ok := parsed["silent"]; !ok {
		t.Error("Expected 'silent' key in JSON")
	}
	if _, ok := parsed["is_error"]; !ok {
		t.Error("Expected 'is_error' key in JSON")
	}
	if _, ok := parsed["async"]; !ok {
		t.Error("Expected 'async' key in JSON")
	}

	// Check that 'err' is NOT present (it should have json:"-" tag)
	if _, ok := parsed["err"]; ok {
		t.Error("Expected 'err' key to be excluded from JSON")
	}

	// Verify values
	if parsed["for_llm"] != "test content" {
		t.Errorf("Expected for_llm 'test content', got %v", parsed["for_llm"])
	}
	if parsed["silent"] != false {
		t.Errorf("Expected silent false, got %v", parsed["silent"])
	}
}

func TestToolResultContentForLLM_AppendsHandledDeliveryNote(t *testing.T) {
	result := toolshared.MediaResult("Screenshot attached.", []string{"media://example"}).WithResponseHandled()

	content := result.ContentForLLM()
	if !strings.Contains(content, "Screenshot attached.") {
		t.Fatalf("expected original content in ContentForLLM, got %q", content)
	}
	if !strings.Contains(content, toolshared.HandledToolLLMNote) {
		t.Fatalf("expected handled delivery note in ContentForLLM, got %q", content)
	}
}

func TestToolResultContentForLLM_UsesHandledDeliveryNoteWhenEmpty(t *testing.T) {
	result := (&toolshared.ToolResult{}).WithResponseHandled()

	if got := result.ContentForLLM(); got != toolshared.HandledToolLLMNote {
		t.Fatalf("ContentForLLM() = %q, want %q", got, toolshared.HandledToolLLMNote)
	}
}

func TestToolResultContentForLLM_AppendsArtifactPaths(t *testing.T) {
	result := &toolshared.ToolResult{
		ForLLM:       "Artifact created.",
		ArtifactTags: []string{"[file:/tmp/example.png]"},
	}

	content := result.ContentForLLM()
	if !strings.Contains(content, "Artifact created.") {
		t.Fatalf("expected original content in ContentForLLM, got %q", content)
	}
	if !strings.Contains(content, "Local artifact paths: [file:/tmp/example.png]") {
		t.Fatalf("expected artifact path note in ContentForLLM, got %q", content)
	}
	if !strings.Contains(content, toolshared.ArtifactPathsLLMNote) {
		t.Fatalf("expected artifact guidance note in ContentForLLM, got %q", content)
	}
}
