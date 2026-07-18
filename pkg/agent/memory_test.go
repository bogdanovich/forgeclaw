package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/config"
)

func TestMemoryContextBoundsLongTermAndPreservesEdges(t *testing.T) {
	workspace := t.TempDir()
	memoryDir := filepath.Join(workspace, "memory")
	if err := os.MkdirAll(memoryDir, 0o755); err != nil {
		t.Fatal(err)
	}
	content := "IMPORTANT-HEAD\n" + strings.Repeat("середина-", 100) + "\nRECENT-TAIL"
	if err := os.WriteFile(filepath.Join(memoryDir, "MEMORY.md"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	const budget = 180
	store := newMemoryStore(workspace, config.PromptMemoryConfig{
		LongTermMaxBytes:   budget,
		DailyNotesMaxBytes: 256,
		RecentDays:         1,
	}, time.Now)
	context := store.GetMemoryContext()
	section := strings.TrimPrefix(context, "## Long-term Memory\n\n")

	if len(section) > budget {
		t.Fatalf("long-term memory section is %d bytes, want <= %d", len(section), budget)
	}
	if !strings.Contains(section, "IMPORTANT-HEAD") || !strings.Contains(section, "RECENT-TAIL") {
		t.Fatalf("truncated memory must preserve both edges: %q", section)
	}
	if !strings.Contains(section, "stable memory truncated") {
		t.Fatalf("truncated memory must contain a visible marker: %q", section)
	}
	if !strings.Contains(section, "с") {
		t.Fatalf("truncated memory should retain valid UTF-8 content: %q", section)
	}
}

func TestMemoryContextBoundsDailyNotesNewestFirst(t *testing.T) {
	workspace := t.TempDir()
	now := time.Date(2026, time.July, 18, 12, 0, 0, 0, time.Local)
	for i, label := range []string{"NEWEST", "MIDDLE", "OLDEST"} {
		date := now.AddDate(0, 0, -i).Format("20060102")
		path := filepath.Join(workspace, "memory", date[:6], date+".md")
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		content := label + "-HEAD\n" + strings.Repeat(label+" content ", 20) + "\n" + label + "-TAIL"
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	const budget = 220
	store := newMemoryStore(workspace, config.PromptMemoryConfig{
		LongTermMaxBytes:   256,
		DailyNotesMaxBytes: budget,
		RecentDays:         3,
	}, func() time.Time { return now })
	context := store.GetMemoryContext()
	section := strings.TrimPrefix(context, "## Recent Daily Notes\n\n")

	if len(section) > budget {
		t.Fatalf("daily-note section is %d bytes, want <= %d", len(section), budget)
	}
	if !strings.Contains(section, "NEWEST-TAIL") {
		t.Fatalf("newest content at the end of today's note was not retained: %q", section)
	}
	if strings.Contains(section, "OLDEST") {
		t.Fatalf("oldest note should be truncated before the newest: %q", section)
	}
	if !strings.Contains(section, "daily notes truncated") {
		t.Fatalf("truncated daily notes must contain a visible marker: %q", section)
	}
}

func TestMemoryPromptSourcePathsIncludeAbsentRecentNotes(t *testing.T) {
	now := time.Date(2026, time.July, 18, 12, 0, 0, 0, time.Local)
	store := newMemoryStore(t.TempDir(), config.PromptMemoryConfig{RecentDays: 2}, func() time.Time {
		return now
	})
	paths := store.promptSourcePaths()

	wantSuffixes := []string{
		filepath.Join("memory", "MEMORY.md"),
		filepath.Join("memory", "202607", "20260718.md"),
		filepath.Join("memory", "202607", "20260717.md"),
	}
	if len(paths) != len(wantSuffixes) {
		t.Fatalf("prompt source path count = %d, want %d: %#v", len(paths), len(wantSuffixes), paths)
	}
	for i, suffix := range wantSuffixes {
		if !strings.HasSuffix(paths[i], suffix) {
			t.Errorf("path[%d] = %q, want suffix %q", i, paths[i], suffix)
		}
	}
}

func TestMemoryTruncationHonorsTinyByteLimit(t *testing.T) {
	tests := map[string]string{
		"middle": truncateMiddleBytes("много текста", 5, "memory"),
		"daily":  joinDailyNotesWithinBudget([]string{"много текста"}, 5),
	}
	for name, result := range tests {
		t.Run(name, func(t *testing.T) {
			if len(result) > 5 {
				t.Fatalf("result is %d bytes, want <= 5: %q", len(result), result)
			}
		})
	}
}
