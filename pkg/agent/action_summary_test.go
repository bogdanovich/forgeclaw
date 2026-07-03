package agent

import (
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/tools"
)

func TestBuildFinalTurnRenderInstructionIncludesWriteAudit(t *testing.T) {
	exec := &turnExecution{
		actionLog: []TurnActionRecord{{
			Source: "tool_result",
			Tool:   "write_file",
			Text:   "File written: notes/family.md",
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
