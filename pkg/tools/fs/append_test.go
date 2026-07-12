package fstools

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestAppendFileTool(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "notes.txt")
	if err := os.WriteFile(path, []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}

	tool := NewAppendFileTool(tmpDir, true)
	result := tool.Execute(context.Background(), map[string]any{
		"path":    "notes.txt",
		"content": " second",
	})
	if result.IsError {
		t.Fatalf("append failed: %s", result.ForLLM)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(content), "first second"; got != want {
		t.Fatalf("content = %q, want %q", got, want)
	}
	if len(result.WriteAudit) != 1 || result.WriteAudit[0].Tool != "append_file" {
		t.Fatalf("unexpected write audit: %#v", result.WriteAudit)
	}
}

func TestAppendFileToolCreatesFile(t *testing.T) {
	tmpDir := t.TempDir()
	tool := NewAppendFileTool(tmpDir, true)
	result := tool.Execute(context.Background(), map[string]any{
		"path":    "new.txt",
		"content": "content",
	})
	if result.IsError {
		t.Fatalf("append failed: %s", result.ForLLM)
	}
	content, err := os.ReadFile(filepath.Join(tmpDir, "new.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(content), "content"; got != want {
		t.Fatalf("content = %q, want %q", got, want)
	}
}

func TestAppendFileToolValidatesArguments(t *testing.T) {
	tool := NewAppendFileTool(t.TempDir(), true)
	for _, args := range []map[string]any{
		{"content": "content"},
		{"path": "notes.txt"},
	} {
		if result := tool.Execute(context.Background(), args); !result.IsError {
			t.Fatalf("Execute(%v) unexpectedly succeeded", args)
		}
	}
}
