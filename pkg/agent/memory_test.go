package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMemoryStore_GetMemoryContext_IncludesUserMemory(t *testing.T) {
	workspace := t.TempDir()
	store := NewMemoryStore(workspace)

	if err := store.WriteLongTerm("Project uses Go."); err != nil {
		t.Fatalf("write long-term memory: %v", err)
	}
	if err := store.WriteUserMemory("User prefers concise replies."); err != nil {
		t.Fatalf("write user memory: %v", err)
	}

	got := store.GetMemoryContext()
	if !strings.Contains(got, "## Long-term Memory") {
		t.Fatalf("memory context missing long-term section: %q", got)
	}
	if !strings.Contains(got, "Project uses Go.") {
		t.Fatalf("memory context missing long-term content: %q", got)
	}
	if !strings.Contains(got, "## User Memory") {
		t.Fatalf("memory context missing user memory section: %q", got)
	}
	if !strings.Contains(got, "User prefers concise replies.") {
		t.Fatalf("memory context missing user memory content: %q", got)
	}
}

func TestMemoryStore_GetRecentDailyNotes_PrefersCanonicalThenLegacy(t *testing.T) {
	workspace := t.TempDir()
	store := NewMemoryStore(workspace)

	today := time.Now()
	canonicalPath := store.dailyFileForDate(today)
	if err := os.MkdirAll(filepath.Dir(canonicalPath), 0o755); err != nil {
		t.Fatalf("mkdir canonical daily dir: %v", err)
	}
	if err := os.WriteFile(canonicalPath, []byte("# today\n\ncanonical"), 0o600); err != nil {
		t.Fatalf("write canonical daily note: %v", err)
	}

	yesterday := today.AddDate(0, 0, -1)
	legacyPath := store.legacyDailyFileForDate(yesterday)
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o755); err != nil {
		t.Fatalf("mkdir legacy daily dir: %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte("# yesterday\n\nlegacy"), 0o600); err != nil {
		t.Fatalf("write legacy daily note: %v", err)
	}

	got := store.GetRecentDailyNotes(2)
	if !strings.Contains(got, "canonical") {
		t.Fatalf("recent daily notes missing canonical content: %q", got)
	}
	if !strings.Contains(got, "legacy") {
		t.Fatalf("recent daily notes missing legacy content: %q", got)
	}
}

func TestMemoryStore_TrackedPathsIncludeUserMemoryAndCanonicalDailyNotes(t *testing.T) {
	workspace := t.TempDir()
	store := NewMemoryStore(workspace)

	paths := store.TrackedPaths()
	want := []string{
		filepath.Join(workspace, "memory", "MEMORY.md"),
		filepath.Join(workspace, "memory", "USER_MEMORY.md"),
		store.dailyFileForDate(time.Now()),
	}

	for _, expected := range want {
		found := false
		for _, path := range paths {
			if path == expected {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("tracked paths missing %q: %#v", expected, paths)
		}
	}
}
