package fstools

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestApplyPatchTool_UpdateFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "main.go")
	if err := os.WriteFile(path, []byte("package main\n\nfunc main() {\n\tprintln(\"old\")\n}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	tool := NewApplyPatchTool(tmpDir, true)
	result := tool.Execute(context.Background(), map[string]any{
		"input": `*** Begin Patch
*** Update File: main.go
@@
 func main() {
-	println("old")
+	println("new")
 }
*** End Patch`,
	})

	if result.IsError {
		t.Fatalf("apply_patch failed: %s", result.ForLLM)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(data); !strings.Contains(got, `println("new")`) || strings.Contains(got, `println("old")`) {
		t.Fatalf("unexpected content:\n%s", got)
	}
	if !strings.Contains(result.ForUser, "```diff") {
		t.Fatalf("expected user diff, got: %s", result.ForUser)
	}
	if len(result.WriteAudit) != 1 {
		t.Fatalf("expected 1 write audit entry, got %+v", result.WriteAudit)
	}
	if got := result.WriteAudit[0]; got.Target != "main.go" || got.Action != "update" || got.Tool != "apply_patch" || !got.Success {
		t.Fatalf("unexpected write audit entry: %+v", got)
	}
}

func TestApplyPatchTool_AddAndDeleteFiles(t *testing.T) {
	tmpDir := t.TempDir()
	deletePath := filepath.Join(tmpDir, "old.txt")
	if err := os.WriteFile(deletePath, []byte("remove me\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	tool := NewApplyPatchTool(tmpDir, true)
	result := tool.Execute(context.Background(), map[string]any{
		"input": `*** Begin Patch
*** Add File: new.txt
+hello
+world
*** Delete File: old.txt
*** End Patch`,
	})

	if result.IsError {
		t.Fatalf("apply_patch failed: %s", result.ForLLM)
	}
	data, err := os.ReadFile(filepath.Join(tmpDir, "new.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello\nworld\n" {
		t.Fatalf("new.txt = %q", string(data))
	}
	if _, err := os.Stat(deletePath); !os.IsNotExist(err) {
		t.Fatalf("old.txt should be deleted, stat err=%v", err)
	}
	if len(result.WriteAudit) != 2 {
		t.Fatalf("expected 2 write audit entries, got %+v", result.WriteAudit)
	}
	got := map[string]string{}
	for _, entry := range result.WriteAudit {
		if entry.Tool != "apply_patch" || !entry.Success {
			t.Fatalf("unexpected write audit entry: %+v", entry)
		}
		got[entry.Target] = entry.Action
	}
	if got["new.txt"] != "add" || got["old.txt"] != "delete" {
		t.Fatalf("unexpected write audit actions: %+v", got)
	}
}

func TestApplyPatchTool_RejectsAmbiguousHunk(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "dup.txt")
	if err := os.WriteFile(path, []byte("same\nsame\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	tool := NewApplyPatchTool(tmpDir, true)
	result := tool.Execute(context.Background(), map[string]any{
		"input": `*** Begin Patch
*** Update File: dup.txt
@@
-same
+other
*** End Patch`,
	})

	if !result.IsError {
		t.Fatalf("expected ambiguous hunk error")
	}
	if !strings.Contains(result.ForLLM, "appears 2 times") {
		t.Fatalf("unexpected error: %s", result.ForLLM)
	}
}

func TestApplyPatchTool_RestrictsOutsideWorkspace(t *testing.T) {
	tmpDir := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.txt")

	tool := NewApplyPatchTool(tmpDir, true)
	result := tool.Execute(context.Background(), map[string]any{
		"input": `*** Begin Patch
*** Add File: ` + outside + `
+blocked
*** End Patch`,
	})

	if !result.IsError {
		t.Fatalf("expected outside workspace write to fail")
	}
	if !strings.Contains(result.ForLLM, "outside the workspace") &&
		!strings.Contains(result.ForLLM, "escapes workspace") {
		t.Fatalf("unexpected error: %s", result.ForLLM)
	}
}
