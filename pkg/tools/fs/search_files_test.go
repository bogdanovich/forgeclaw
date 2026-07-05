package fstools

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestSearchFilesTool_ContentSearch(t *testing.T) {
	tmpDir := t.TempDir()
	mustWriteSearchFile(
		t,
		tmpDir,
		"main.go",
		"package main\n\nfunc main() {\n\tprintln(\"needle\")\n}\n",
	)
	mustWriteSearchFile(t, tmpDir, "README.md", "needle in docs\n")

	tool := NewSearchFilesTool(tmpDir, true, 0)
	result := tool.Execute(context.Background(), map[string]any{
		"pattern":   "needle",
		"path":      ".",
		"file_glob": "*.go",
	})

	if result.IsError {
		t.Fatalf("search_files failed: %s", result.ForLLM)
	}
	if !strings.Contains(result.ForLLM, "main.go:") {
		t.Fatalf("expected Go match, got:\n%s", result.ForLLM)
	}
	if strings.Contains(result.ForLLM, "README.md") {
		t.Fatalf("file_glob should exclude README.md, got:\n%s", result.ForLLM)
	}
	assertNoSearchTruncation(t, result.ForLLM)
}

func TestSearchFilesTool_FilesSearch(t *testing.T) {
	tmpDir := t.TempDir()
	mustWriteSearchFile(t, tmpDir, "cmd/app/main.go", "package main\n")
	mustWriteSearchFile(t, tmpDir, "pkg/app/app_test.go", "package app\n")
	mustWriteSearchFile(t, tmpDir, "notes.txt", "ignore\n")

	tool := NewSearchFilesTool(tmpDir, true, 0)
	result := tool.Execute(context.Background(), map[string]any{
		"pattern": "*.go",
		"target":  "files",
		"path":    ".",
	})

	if result.IsError {
		t.Fatalf("search_files failed: %s", result.ForLLM)
	}
	if !strings.Contains(result.ForLLM, "cmd/app/main.go") ||
		!strings.Contains(result.ForLLM, "pkg/app/app_test.go") {
		t.Fatalf("expected Go files, got:\n%s", result.ForLLM)
	}
	if strings.Contains(result.ForLLM, "notes.txt") {
		t.Fatalf("unexpected notes.txt match:\n%s", result.ForLLM)
	}
	assertNoSearchTruncation(t, result.ForLLM)
}

func TestSearchFilesTool_FilesSearchDoesNotReportIgnoredNonCandidates(t *testing.T) {
	tmpDir := t.TempDir()
	mustWriteSearchFile(t, tmpDir, ".gitignore", ".env\n*.log\n")
	mustWriteSearchFile(t, tmpDir, ".env", "secret\n")
	mustWriteSearchFile(t, tmpDir, "debug.log", "log\n")
	mustWriteSearchFile(t, tmpDir, "notes.txt", "notes\n")

	tool := NewSearchFilesTool(tmpDir, true, 0)
	result := tool.Execute(context.Background(), map[string]any{
		"pattern": "*.go",
		"target":  "files",
		"path":    ".",
	})

	if result.IsError {
		t.Fatalf("search_files failed: %s", result.ForLLM)
	}
	if result.ForLLM != "No files matched." {
		t.Fatalf("expected complete no-match result, got:\n%s", result.ForLLM)
	}
	assertNoSearchTruncation(t, result.ForLLM)
}

func TestSearchFilesTool_FilesSearchReportsIgnoredCandidates(t *testing.T) {
	tmpDir := t.TempDir()
	mustWriteSearchFile(t, tmpDir, ".gitignore", "ignored.go\n")
	mustWriteSearchFile(t, tmpDir, "ignored.go", "package ignored\n")

	tool := NewSearchFilesTool(tmpDir, true, 0)
	result := tool.Execute(context.Background(), map[string]any{
		"pattern": "*.go",
		"target":  "files",
		"path":    ".",
	})

	if result.IsError {
		t.Fatalf("search_files failed: %s", result.ForLLM)
	}
	if strings.Contains(result.ForLLM, "ignored.go") {
		t.Fatalf("expected ignored file to stay hidden by default, got:\n%s", result.ForLLM)
	}
	assertSearchTruncation(
		t,
		result.ForLLM,
		"reason=ignored_paths",
		"returned_count=0",
		"omitted_count=unknown",
		"skipped_ignored_count=1",
	)
}

func TestSearchFilesTool_CountMode(t *testing.T) {
	tmpDir := t.TempDir()
	mustWriteSearchFile(t, tmpDir, "a.txt", "needle\nneedle\n")
	mustWriteSearchFile(t, tmpDir, "b.txt", "needle\n")

	tool := NewSearchFilesTool(tmpDir, true, 0)
	result := tool.Execute(context.Background(), map[string]any{
		"pattern":     "needle",
		"output_mode": "count",
	})

	if result.IsError {
		t.Fatalf("search_files failed: %s", result.ForLLM)
	}
	if !strings.Contains(result.ForLLM, "a.txt: 2") ||
		!strings.Contains(result.ForLLM, "b.txt: 1") {
		t.Fatalf("expected count output, got:\n%s", result.ForLLM)
	}
	assertNoSearchTruncation(t, result.ForLLM)
}

func TestSearchFilesTool_CountModeReportsLimitTruncation(t *testing.T) {
	tmpDir := t.TempDir()
	for i := 0; i < 5; i++ {
		mustWriteSearchFile(t, tmpDir, fmt.Sprintf("file-%d.txt", i), "needle\n")
	}

	tool := NewSearchFilesTool(tmpDir, true, 0)
	result := tool.Execute(context.Background(), map[string]any{
		"pattern":     "needle",
		"output_mode": "count",
		"limit":       2,
	})

	if result.IsError {
		t.Fatalf("search_files failed: %s", result.ForLLM)
	}
	assertSearchTruncation(
		t,
		result.ForLLM,
		"reason=count_limit",
		"returned_count=2",
		"limit=2",
		"omitted_count=3",
		"suggested_narrowing.path=set a narrower path",
		"suggested_narrowing.file_glob=set file_glob such as *.go or *.md",
		"suggested_narrowing.pattern=make pattern more specific",
	)
}

func TestSearchFilesTool_CountModeKeepsLogicalOmittedCountWhenByteTruncated(t *testing.T) {
	tmpDir := t.TempDir()
	for i := 0; i < 520; i++ {
		mustWriteSearchFile(
			t,
			tmpDir,
			filepath.Join(
				"long-count-paths",
				fmt.Sprintf("file-%03d-%s.txt", i, strings.Repeat("longname", 8)),
			),
			"needle\n",
		)
	}

	tool := NewSearchFilesTool(tmpDir, true, 0)
	result := tool.Execute(context.Background(), map[string]any{
		"pattern":     "needle",
		"output_mode": "count",
		"limit":       500,
	})

	if result.IsError {
		t.Fatalf("search_files failed: %s", result.ForLLM)
	}
	if len(result.ForLLM) > maxSearchFilesResultBytes {
		t.Fatalf(
			"expected truncated result <= %d bytes, got %d",
			maxSearchFilesResultBytes,
			len(result.ForLLM),
		)
	}
	assertSearchTruncation(
		t,
		result.ForLLM,
		"reason=byte_limit,count_limit",
		"returned_count=152",
		"limit=500",
		"omitted_count=368",
		"count_limit_omitted_count=20",
		"rendered_omitted_count=348",
	)
}

func TestSearchFilesTool_RespectsGitignoreByDefault(t *testing.T) {
	tmpDir := t.TempDir()
	mustWriteSearchFile(t, tmpDir, ".gitignore", ".env\nignored/\n*.log\n")
	mustWriteSearchFile(t, tmpDir, ".env", "secret needle\n")
	mustWriteSearchFile(t, tmpDir, "ignored/file.txt", "ignored needle\n")
	mustWriteSearchFile(t, tmpDir, "debug.log", "logged needle\n")
	mustWriteSearchFile(t, tmpDir, "visible.txt", "visible needle\n")

	tool := NewSearchFilesTool(tmpDir, true, 0)
	result := tool.Execute(context.Background(), map[string]any{
		"pattern": "needle",
		"path":    ".",
	})

	if result.IsError {
		t.Fatalf("search_files failed: %s", result.ForLLM)
	}
	if !strings.Contains(result.ForLLM, "visible.txt") {
		t.Fatalf("expected visible match, got:\n%s", result.ForLLM)
	}
	for _, ignored := range []string{".env", "ignored/file.txt", "debug.log"} {
		if strings.Contains(result.ForLLM, ignored) {
			t.Fatalf("expected %s to be ignored, got:\n%s", ignored, result.ForLLM)
		}
	}
	assertSearchTruncation(
		t,
		result.ForLLM,
		"truncated=true",
		"reason=ignored_paths",
		"returned_count=1",
		"omitted_count=unknown",
		"skipped_ignored_count=3",
		"suggested_narrowing.include_ignored=true only if ignored/runtime files are needed",
	)
}

func TestSearchFilesTool_IncludeIgnoredFindsGitignoredFiles(t *testing.T) {
	tmpDir := t.TempDir()
	mustWriteSearchFile(t, tmpDir, ".gitignore", ".env\nignored/\n*.log\n")
	mustWriteSearchFile(t, tmpDir, ".env", "secret needle\n")
	mustWriteSearchFile(t, tmpDir, "ignored/file.txt", "ignored needle\n")
	mustWriteSearchFile(t, tmpDir, "debug.log", "logged needle\n")
	mustWriteSearchFile(t, tmpDir, "visible.txt", "visible needle\n")

	tool := NewSearchFilesTool(tmpDir, true, 0)
	result := tool.Execute(context.Background(), map[string]any{
		"pattern":         "needle",
		"path":            ".",
		"include_ignored": true,
	})

	if result.IsError {
		t.Fatalf("search_files failed: %s", result.ForLLM)
	}
	for _, expected := range []string{".env", "ignored/file.txt", "debug.log", "visible.txt"} {
		if !strings.Contains(result.ForLLM, expected) {
			t.Fatalf("expected %s with include_ignored, got:\n%s", expected, result.ForLLM)
		}
	}
	assertNoSearchTruncation(t, result.ForLLM)
}

func TestSearchFilesTool_ExplicitIgnoredFilePathStillRespectsGitignore(t *testing.T) {
	tmpDir := t.TempDir()
	mustWriteSearchFile(t, tmpDir, ".gitignore", ".env\n")
	mustWriteSearchFile(t, tmpDir, ".env", "secret needle\n")

	tool := NewSearchFilesTool(tmpDir, true, 0)
	result := tool.Execute(context.Background(), map[string]any{
		"pattern": "needle",
		"path":    ".env",
	})

	if result.IsError {
		t.Fatalf("search_files failed: %s", result.ForLLM)
	}
	if strings.Contains(result.ForLLM, ".env") {
		t.Fatalf(
			"expected explicit ignored file to be skipped without include_ignored, got:\n%s",
			result.ForLLM,
		)
	}

	result = tool.Execute(context.Background(), map[string]any{
		"pattern":         "needle",
		"path":            ".env",
		"include_ignored": true,
	})
	if result.IsError {
		t.Fatalf("search_files with include_ignored failed: %s", result.ForLLM)
	}
	if !strings.Contains(result.ForLLM, ".env") {
		t.Fatalf("expected explicit ignored file with include_ignored, got:\n%s", result.ForLLM)
	}
	assertNoSearchTruncation(t, result.ForLLM)
}

func TestSearchFilesTool_RespectsNestedGitignore(t *testing.T) {
	tmpDir := t.TempDir()
	mustWriteSearchFile(t, tmpDir, "sub/.gitignore", "secret.txt\n")
	mustWriteSearchFile(t, tmpDir, "sub/secret.txt", "hidden needle\n")
	mustWriteSearchFile(t, tmpDir, "sub/public.txt", "visible needle\n")

	tool := NewSearchFilesTool(tmpDir, true, 0)
	result := tool.Execute(context.Background(), map[string]any{
		"pattern": "needle",
		"path":    ".",
	})

	if result.IsError {
		t.Fatalf("search_files failed: %s", result.ForLLM)
	}
	if !strings.Contains(result.ForLLM, "sub/public.txt") {
		t.Fatalf("expected public match, got:\n%s", result.ForLLM)
	}
	if strings.Contains(result.ForLLM, "sub/secret.txt") {
		t.Fatalf("expected nested ignored file to be skipped, got:\n%s", result.ForLLM)
	}
	assertSearchTruncation(t, result.ForLLM, "reason=ignored_paths")
}

func TestSearchFilesTool_RestrictsOutsideWorkspace(t *testing.T) {
	tmpDir := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outside, []byte("needle\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	tool := NewSearchFilesTool(tmpDir, true, 0)
	result := tool.Execute(context.Background(), map[string]any{
		"pattern": "needle",
		"path":    outside,
	})

	if !result.IsError {
		t.Fatalf("expected outside workspace search to fail")
	}
	if !strings.Contains(result.ForLLM, "outside the workspace") &&
		!strings.Contains(result.ForLLM, "access denied") &&
		!strings.Contains(result.ForLLM, "escapes workspace") {
		t.Fatalf("unexpected error: %s", result.ForLLM)
	}
}

func TestSearchFilesTool_TruncatesOversizedContentResults(t *testing.T) {
	tmpDir := t.TempDir()
	var huge strings.Builder
	for i := 0; i < 1200; i++ {
		fmt.Fprintf(&huge, "line %04d: NO_REPLY runtime payload for context overflow testing\n", i)
	}
	mustWriteSearchFile(t, tmpDir, "sessions/runtime.jsonl", huge.String())

	tool := NewSearchFilesTool(tmpDir, true, 0)
	result := tool.Execute(context.Background(), map[string]any{
		"pattern":     "NO_REPLY",
		"path":        ".",
		"output_mode": "content",
		"limit":       500,
	})

	if result.IsError {
		t.Fatalf("search_files failed: %s", result.ForLLM)
	}
	if len(result.ForLLM) > maxSearchFilesResultBytes {
		t.Fatalf(
			"expected truncated result <= %d bytes, got %d",
			maxSearchFilesResultBytes,
			len(result.ForLLM),
		)
	}
	assertSearchTruncation(
		t,
		result.ForLLM,
		"truncated=true",
		"reason=byte_limit",
		"limit=500",
		"omitted_count=",
		"suggested_narrowing.path=set a narrower path",
		"suggested_narrowing.file_glob=set file_glob such as *.go or *.md",
		"suggested_narrowing.pattern=make pattern more specific",
	)
	if !strings.Contains(result.ForLLM, "sessions/runtime.jsonl") {
		t.Fatalf("expected runtime file matches to remain searchable, got:\n%s", result.ForLLM)
	}
	if !utf8.ValidString(result.ForLLM) {
		t.Fatal("expected truncated result to remain valid UTF-8")
	}
	lines := strings.Split(result.ForLLM, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "sessions/runtime.jsonl") && !strings.HasSuffix(line, "context overflow testing") {
			t.Fatalf("expected complete match rows only, got partial line %q", line)
		}
	}
}

func TestSearchFilesTool_ByteTruncationWithIgnoredPathsKeepsOmittedUnknown(t *testing.T) {
	tmpDir := t.TempDir()
	mustWriteSearchFile(t, tmpDir, ".gitignore", "ignored/\n")
	mustWriteSearchFile(t, tmpDir, "ignored/hidden.txt", "NO_REPLY hidden\n")
	var huge strings.Builder
	for i := 0; i < 1200; i++ {
		fmt.Fprintf(&huge, "line %04d: NO_REPLY visible payload for byte limit testing\n", i)
	}
	mustWriteSearchFile(t, tmpDir, "visible/runtime.txt", huge.String())

	tool := NewSearchFilesTool(tmpDir, true, 0)
	result := tool.Execute(context.Background(), map[string]any{
		"pattern":     "NO_REPLY",
		"path":        ".",
		"output_mode": "content",
		"limit":       500,
	})

	if result.IsError {
		t.Fatalf("search_files failed: %s", result.ForLLM)
	}
	if len(result.ForLLM) > maxSearchFilesResultBytes {
		t.Fatalf(
			"expected truncated result <= %d bytes, got %d",
			maxSearchFilesResultBytes,
			len(result.ForLLM),
		)
	}
	assertSearchTruncation(
		t,
		result.ForLLM,
		"reason=byte_limit,count_limit,ignored_paths",
		"omitted_count=unknown",
		"rendered_omitted_count=",
		"skipped_ignored_count=1",
	)
}

func TestSearchFilesTool_TruncatesOversizedFilesOnlyResults(t *testing.T) {
	tmpDir := t.TempDir()
	for i := 0; i < 2000; i++ {
		mustWriteSearchFile(
			t,
			tmpDir,
			filepath.Join("sessions", fmt.Sprintf("session-%04d.jsonl", i)),
			"runtime marker\n",
		)
	}

	tool := NewSearchFilesTool(tmpDir, true, 0)
	result := tool.Execute(context.Background(), map[string]any{
		"pattern":     "session-*.jsonl",
		"target":      "files",
		"path":        ".",
		"output_mode": "files_only",
		"limit":       500,
	})

	if result.IsError {
		t.Fatalf("search_files failed: %s", result.ForLLM)
	}
	if len(result.ForLLM) > maxSearchFilesResultBytes {
		t.Fatalf(
			"expected truncated result <= %d bytes, got %d",
			maxSearchFilesResultBytes,
			len(result.ForLLM),
		)
	}
	if !strings.Contains(result.ForLLM, "truncated at limit 500") {
		t.Fatalf("expected logical limit header, got:\n%s", result.ForLLM)
	}
	assertSearchTruncation(
		t,
		result.ForLLM,
		"truncated=true",
		"reason=count_limit",
		"returned_count=500",
		"limit=500",
		"omitted_count=unknown",
	)
	for _, line := range strings.Split(result.ForLLM, "\n") {
		if strings.HasPrefix(line, "sessions/session-") && !strings.HasSuffix(line, ".jsonl") {
			t.Fatalf("expected complete file path rows only, got partial line %q", line)
		}
	}
	if !strings.Contains(result.ForLLM, "sessions/session-") {
		t.Fatalf("expected matching session paths, got:\n%s", result.ForLLM)
	}
}

func TestSearchFilesTool_FilesSearchKeepsUnknownOmittedCountWhenByteTruncated(t *testing.T) {
	tmpDir := t.TempDir()
	for i := 0; i < 700; i++ {
		mustWriteSearchFile(
			t,
			tmpDir,
			filepath.Join(
				"long-file-paths",
				fmt.Sprintf("session-%03d-%s.jsonl", i, strings.Repeat("longname", 8)),
			),
			"runtime marker\n",
		)
	}

	tool := NewSearchFilesTool(tmpDir, true, 0)
	result := tool.Execute(context.Background(), map[string]any{
		"pattern": "session-*.jsonl",
		"target":  "files",
		"path":    ".",
		"limit":   500,
	})

	if result.IsError {
		t.Fatalf("search_files failed: %s", result.ForLLM)
	}
	if len(result.ForLLM) > maxSearchFilesResultBytes {
		t.Fatalf(
			"expected truncated result <= %d bytes, got %d",
			maxSearchFilesResultBytes,
			len(result.ForLLM),
		)
	}
	assertSearchTruncation(
		t,
		result.ForLLM,
		"reason=byte_limit,count_limit",
		"omitted_count=unknown",
		"rendered_omitted_count=",
	)
}

func TestSearchFilesTool_ReportsMaxFileSizeSkip(t *testing.T) {
	tmpDir := t.TempDir()
	mustWriteSearchFile(t, tmpDir, "small.txt", "needle\n")
	mustWriteSearchFile(t, tmpDir, "large.txt", strings.Repeat("needle\n", 20))

	tool := NewSearchFilesTool(tmpDir, true, 32)
	result := tool.Execute(context.Background(), map[string]any{
		"pattern": "needle",
		"path":    ".",
	})

	if result.IsError {
		t.Fatalf("search_files failed: %s", result.ForLLM)
	}
	if !strings.Contains(result.ForLLM, "small.txt") {
		t.Fatalf("expected small file match, got:\n%s", result.ForLLM)
	}
	if strings.Contains(result.ForLLM, "large.txt") {
		t.Fatalf("expected large file to be skipped, got:\n%s", result.ForLLM)
	}
	assertSearchTruncation(
		t,
		result.ForLLM,
		"truncated=true",
		"reason=max_file_size",
		"returned_count=1",
		"omitted_count=unknown",
		"skipped_max_file_size_count=1",
	)
}

func assertSearchTruncation(t *testing.T, output string, wants ...string) {
	t.Helper()
	if !strings.Contains(output, "Search truncation:") {
		t.Fatalf("expected Search truncation block, got:\n%s", output)
	}
	for _, want := range wants {
		if !strings.Contains(output, want) {
			t.Fatalf("expected truncation block to contain %q, got:\n%s", want, output)
		}
	}
}

func assertNoSearchTruncation(t *testing.T, output string) {
	t.Helper()
	if strings.Contains(output, "Search truncation:") {
		t.Fatalf("did not expect Search truncation block, got:\n%s", output)
	}
}

func mustWriteSearchFile(t *testing.T, root string, rel string, content string) {
	t.Helper()
	path := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
