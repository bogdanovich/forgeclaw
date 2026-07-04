package agent

import (
	"errors"
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/tools"
)

func TestBuildFinalTurnRenderInstructionIncludesWriteAudit(t *testing.T) {
	exec := &turnExecution{
		actionLog: []TurnActionRecord{{
			Source:        "tool_result",
			Tool:          "write_file",
			Text:          "File written: notes/family.md",
			VerifiedWrite: true,
		}},
		writeAudit: []tools.WriteAuditEntry{{
			Kind:    "file",
			Target:  "notes/family.md",
			Action:  "write",
			Tool:    "write_file",
			Success: true,
		}},
	}

	instruction := buildFinalTurnRenderInstruction(exec)
	for _, want := range []string{
		"Verified write-side effects from tool execution",
		`"target": "notes/family.md"`,
		`"action": "write"`,
		"Only claim that files, notes, artifacts, or records were saved/updated",
	} {
		if !strings.Contains(instruction, want) {
			t.Fatalf("instruction missing %q:\n%s", want, instruction)
		}
	}
}

func TestAppendTurnWriteAuditNormalizesAndDedupes(t *testing.T) {
	records := appendTurnWriteAudit(nil, "write_file", []tools.WriteAuditEntry{
		{Target: "notes/a.md", Success: true},
		{Target: "notes/a.md", Success: true},
		{Target: "notes/b.md", Success: false},
	})

	if len(records) != 1 {
		t.Fatalf("records = %+v, want one successful deduped entry", records)
	}
	got := records[0]
	if got.Kind != "file" || got.Action != "write" || got.Tool != "write_file" || got.Target != "notes/a.md" {
		t.Fatalf("unexpected normalized audit entry: %+v", got)
	}
}

func TestBuildFinalTurnRenderInstructionOmitsUnverifiedWriteClaims(t *testing.T) {
	exec := &turnExecution{
		actionLog: []TurnActionRecord{
			{
				Source:        "tool_result",
				Tool:          "write_file",
				Text:          "File written: notes/family.md",
				VerifiedWrite: true,
			},
			{
				Source: "tool_result",
				Tool:   "cron",
				Text:   "Cron job added: remind me tomorrow",
			},
			{
				Source: "tool_result",
				Tool:   "search_tools",
				Text:   "Found 3 matching tools.",
			},
		},
		writeAudit: []tools.WriteAuditEntry{{
			Kind:    "file",
			Target:  "notes/family.md",
			Action:  "write",
			Tool:    "write_file",
			Success: true,
		}},
	}

	instruction := buildFinalTurnRenderInstruction(exec)
	if strings.Contains(instruction, "Cron job added: remind me tomorrow") {
		t.Fatalf("instruction should suppress unverified write claim:\n%s", instruction)
	}
	if !strings.Contains(instruction, "Found 3 matching tools.") {
		t.Fatalf("instruction should keep non-write tool summary:\n%s", instruction)
	}
}

func TestBuildFinalTurnRenderMessagesSuppressesUnverifiedWriteToolResults(t *testing.T) {
	exec := &turnExecution{
		messages: []providers.Message{
			{
				Role: "assistant",
				ToolCalls: []providers.ToolCall{
					{
						ID:   "call-1",
						Name: "cron",
						Function: &providers.FunctionCall{
							Name: "cron",
						},
					},
					{
						ID:   "call-2",
						Name: "write_file",
						Function: &providers.FunctionCall{
							Name: "write_file",
						},
					},
				},
			},
			{
				Role:       "tool",
				ToolCallID: "call-1",
				Content:    "Cron job added: remind me tomorrow",
			},
			{
				Role:       "tool",
				ToolCallID: "call-2",
				Content:    "File written: notes/family.md",
			},
		},
		finalRenderToolCalls: map[string]finalRenderToolCallState{
			"call-1": {Tool: "cron", VerifiedWrite: false},
			"call-2": {Tool: "write_file", VerifiedWrite: true},
		},
	}

	messages := buildFinalTurnRenderMessages(exec)
	if got := messages[1].Content; !strings.Contains(got, "omitted from final render") {
		t.Fatalf("expected unverified write tool result to be sanitized, got %q", got)
	}
	if got := messages[2].Content; got != "File written: notes/family.md" {
		t.Fatalf("expected verified write tool result to remain visible, got %q", got)
	}
}

func TestWrapToolDeliveryErrorPreservesWriteAudit(t *testing.T) {
	original := tools.SilentResult("File written: notes/family.md").
		WithFileWriteAudit("notes/family.md", "write", "write_file")

	wrapped := wrapToolDeliveryError(original, "failed to deliver attachment: boom", errors.New("boom"))
	if !wrapped.IsError {
		t.Fatal("expected wrapped result to be an error")
	}
	if len(wrapped.WriteAudit) != 1 {
		t.Fatalf("expected preserved write audit, got %+v", wrapped.WriteAudit)
	}
	if got := wrapped.WriteAudit[0].Target; got != "notes/family.md" {
		t.Fatalf("write audit target = %q, want notes/family.md", got)
	}
}
