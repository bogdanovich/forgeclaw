package fstools

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	defaultSearchFilesLimit       = 100
	maxSearchFilesLimit           = 500
	maxSearchFilesContext         = 10
	defaultSearchFilesMaxFileSize = 1024 * 1024
)

// SearchFilesTool searches workspace files without requiring shell grep/rg.
type SearchFilesTool struct {
	fs          fileSystem
	maxFileSize int
}

func NewSearchFilesTool(
	workspace string,
	restrict bool,
	maxFileSize int,
	allowPaths ...[]*regexp.Regexp,
) *SearchFilesTool {
	var patterns []*regexp.Regexp
	if len(allowPaths) > 0 {
		patterns = allowPaths[0]
	}
	if maxFileSize <= 0 {
		maxFileSize = defaultSearchFilesMaxFileSize
	}
	return &SearchFilesTool{
		fs:          buildFs(workspace, restrict, patterns),
		maxFileSize: maxFileSize,
	}
}

func (t *SearchFilesTool) Name() string {
	return "search_files"
}

func (t *SearchFilesTool) Description() string {
	return "Search file contents or find files by name within the configured workspace. Use this instead of shell grep/rg/find/ls for routine file discovery."
}

func (t *SearchFilesTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"pattern": map[string]any{
				"type":        "string",
				"description": "Regex pattern for content search, or glob/name pattern for files search.",
			},
			"target": map[string]any{
				"type":        "string",
				"enum":        []string{"content", "files"},
				"description": "Search file contents or file paths.",
			},
			"path": map[string]any{
				"type":        "string",
				"description": "Directory or file to search. Defaults to current workspace.",
			},
			"file_glob": map[string]any{
				"type":        "string",
				"description": "Optional glob to restrict content search to matching file names, for example *.go.",
			},
			"output_mode": map[string]any{
				"type":        "string",
				"enum":        []string{"content", "files_only", "count"},
				"description": "For content search: matching lines, file paths only, or match counts.",
			},
			"context": map[string]any{
				"type":        "integer",
				"description": "Number of context lines before and after each content match. Default 0, max 10.",
			},
			"limit": map[string]any{
				"type":        "integer",
				"description": "Maximum number of returned matches/files. Default 100, max 500.",
			},
		},
		"required": []string{"pattern"},
	}
}

func (t *SearchFilesTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	opts, err := parseSearchFilesOptions(args)
	if err != nil {
		return ErrorResult(err.Error())
	}

	switch opts.target {
	case "files":
		return t.searchFileNames(ctx, opts)
	default:
		return t.searchContent(ctx, opts)
	}
}

type searchFilesOptions struct {
	pattern    string
	target     string
	path       string
	fileGlob   string
	outputMode string
	context    int
	limit      int
}

type contentMatch struct {
	path       string
	lineNumber int
	line       string
	before     []numberedLine
	after      []numberedLine
}

type numberedLine struct {
	number int
	text   string
}

func parseSearchFilesOptions(args map[string]any) (searchFilesOptions, error) {
	pattern, _ := args["pattern"].(string)
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return searchFilesOptions{}, fmt.Errorf("pattern is required")
	}

	target, _ := args["target"].(string)
	if target == "" {
		target = "content"
	}
	if target != "content" && target != "files" {
		return searchFilesOptions{}, fmt.Errorf("target must be content or files")
	}

	path, _ := args["path"].(string)
	if strings.TrimSpace(path) == "" {
		path = "."
	}

	outputMode, _ := args["output_mode"].(string)
	if outputMode == "" {
		outputMode = "content"
	}
	if outputMode != "content" && outputMode != "files_only" && outputMode != "count" {
		return searchFilesOptions{}, fmt.Errorf("output_mode must be content, files_only, or count")
	}

	contextLines := intArg(args["context"], 0)
	if contextLines < 0 {
		contextLines = 0
	}
	if contextLines > maxSearchFilesContext {
		contextLines = maxSearchFilesContext
	}

	limit := intArg(args["limit"], defaultSearchFilesLimit)
	if limit <= 0 {
		limit = defaultSearchFilesLimit
	}
	if limit > maxSearchFilesLimit {
		limit = maxSearchFilesLimit
	}

	fileGlob, _ := args["file_glob"].(string)
	return searchFilesOptions{
		pattern:    pattern,
		target:     target,
		path:       path,
		fileGlob:   strings.TrimSpace(fileGlob),
		outputMode: outputMode,
		context:    contextLines,
		limit:      limit,
	}, nil
}

func intArg(value any, fallback int) int {
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return fallback
	}
}

func (t *SearchFilesTool) searchFileNames(ctx context.Context, opts searchFilesOptions) *ToolResult {
	var matches []string
	truncated := false

	err := walkSearchFiles(ctx, t.fs, opts.path, func(path string, entry os.DirEntry) error {
		if entry.IsDir() {
			return nil
		}
		name := entry.Name()
		matched, err := matchFilePattern(opts.pattern, path, name)
		if err != nil {
			return err
		}
		if matched {
			matches = append(matches, cleanDisplayPath(path))
			if len(matches) >= opts.limit {
				truncated = true
				return errSearchLimitReached
			}
		}
		return nil
	})
	if err != nil && err != errSearchLimitReached {
		return ErrorResult(err.Error())
	}
	sort.Strings(matches)

	if len(matches) == 0 {
		return NewToolResult("No files matched.")
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Matched files: %d", len(matches))
	if truncated {
		fmt.Fprintf(&b, " (truncated at limit %d)", opts.limit)
	}
	b.WriteString("\n\n")
	for _, match := range matches {
		b.WriteString(match)
		b.WriteByte('\n')
	}
	return NewToolResult(strings.TrimRight(b.String(), "\n"))
}

func (t *SearchFilesTool) searchContent(ctx context.Context, opts searchFilesOptions) *ToolResult {
	re, err := regexp.Compile(opts.pattern)
	if err != nil {
		return ErrorResult(fmt.Sprintf("invalid regex pattern: %v", err))
	}

	var matches []contentMatch
	fileCounts := map[string]int{}
	filesOnly := map[string]bool{}
	truncated := false
	filesScanned := 0
	filesSkipped := 0

	err = walkSearchFiles(ctx, t.fs, opts.path, func(path string, entry os.DirEntry) error {
		if entry.IsDir() {
			return nil
		}
		if opts.fileGlob != "" && !matchGlob(opts.fileGlob, entry.Name(), path) {
			return nil
		}
		data, err := t.fs.ReadFile(path)
		if err != nil {
			filesSkipped++
			return nil
		}
		if len(data) > t.maxFileSize || looksBinary(data) {
			filesSkipped++
			return nil
		}

		filesScanned++
		lines := strings.Split(string(data), "\n")
		for i, line := range lines {
			if !re.MatchString(line) {
				continue
			}

			displayPath := cleanDisplayPath(path)
			fileCounts[displayPath]++
			filesOnly[displayPath] = true

			switch opts.outputMode {
			case "files_only", "count":
				if len(filesOnly) >= opts.limit && opts.outputMode == "files_only" {
					truncated = true
					return errSearchLimitReached
				}
			default:
				matches = append(matches, contentMatch{
					path:       displayPath,
					lineNumber: i + 1,
					line:       line,
					before:     contextBefore(lines, i, opts.context),
					after:      contextAfter(lines, i, opts.context),
				})
				if len(matches) >= opts.limit {
					truncated = true
					return errSearchLimitReached
				}
			}
		}
		return nil
	})
	if err != nil && err != errSearchLimitReached {
		return ErrorResult(err.Error())
	}

	return formatContentSearchResult(opts, matches, filesOnly, fileCounts, filesScanned, filesSkipped, truncated)
}

func formatContentSearchResult(
	opts searchFilesOptions,
	matches []contentMatch,
	filesOnly map[string]bool,
	fileCounts map[string]int,
	filesScanned int,
	filesSkipped int,
	truncated bool,
) *ToolResult {
	var b strings.Builder

	switch opts.outputMode {
	case "files_only":
		paths := sortedMapKeys(filesOnly)
		if len(paths) == 0 {
			return NewToolResult(searchNoMatches(filesScanned, filesSkipped))
		}
		if len(paths) > opts.limit {
			paths = paths[:opts.limit]
			truncated = true
		}
		fmt.Fprintf(&b, "Matched files: %d", len(paths))
		if truncated {
			fmt.Fprintf(&b, " (truncated at limit %d)", opts.limit)
		}
		fmt.Fprintf(&b, "\nFiles scanned: %d, skipped: %d\n\n", filesScanned, filesSkipped)
		for _, path := range paths {
			b.WriteString(path)
			b.WriteByte('\n')
		}
	case "count":
		paths := sortedMapKeys(fileCounts)
		if len(paths) == 0 {
			return NewToolResult(searchNoMatches(filesScanned, filesSkipped))
		}
		fmt.Fprintf(&b, "Matched files: %d\nFiles scanned: %d, skipped: %d\n\n", len(paths), filesScanned, filesSkipped)
		for _, path := range paths {
			fmt.Fprintf(&b, "%s: %d\n", path, fileCounts[path])
		}
	default:
		if len(matches) == 0 {
			return NewToolResult(searchNoMatches(filesScanned, filesSkipped))
		}
		fmt.Fprintf(&b, "Matches: %d", len(matches))
		if truncated {
			fmt.Fprintf(&b, " (truncated at limit %d)", opts.limit)
		}
		fmt.Fprintf(&b, "\nFiles scanned: %d, skipped: %d\n\n", filesScanned, filesSkipped)
		for idx, match := range matches {
			if idx > 0 {
				b.WriteByte('\n')
			}
			for _, line := range match.before {
				fmt.Fprintf(&b, "%s-%d-%s\n", match.path, line.number, line.text)
			}
			fmt.Fprintf(&b, "%s:%d:%s\n", match.path, match.lineNumber, match.line)
			for _, line := range match.after {
				fmt.Fprintf(&b, "%s-%d-%s\n", match.path, line.number, line.text)
			}
		}
	}

	return NewToolResult(strings.TrimRight(b.String(), "\n"))
}

func searchNoMatches(filesScanned int, filesSkipped int) string {
	return fmt.Sprintf("No matches found. Files scanned: %d, skipped: %d", filesScanned, filesSkipped)
}

func sortedMapKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func contextBefore(lines []string, idx int, count int) []numberedLine {
	if count <= 0 {
		return nil
	}
	start := idx - count
	if start < 0 {
		start = 0
	}
	out := make([]numberedLine, 0, idx-start)
	for i := start; i < idx; i++ {
		out = append(out, numberedLine{number: i + 1, text: lines[i]})
	}
	return out
}

func contextAfter(lines []string, idx int, count int) []numberedLine {
	if count <= 0 {
		return nil
	}
	end := idx + count
	if end >= len(lines) {
		end = len(lines) - 1
	}
	out := make([]numberedLine, 0, end-idx)
	for i := idx + 1; i <= end; i++ {
		out = append(out, numberedLine{number: i + 1, text: lines[i]})
	}
	return out
}

var errSearchLimitReached = fmt.Errorf("search limit reached")

func walkSearchFiles(ctx context.Context, sysFs fileSystem, root string, fn func(path string, entry os.DirEntry) error) error {
	entries, err := sysFs.ReadDir(root)
	if err != nil {
		data, readErr := sysFs.ReadFile(root)
		if readErr != nil {
			return fmt.Errorf("failed to search path: %w", err)
		}
		return fn(root, fakeFileEntry{name: filepath.Base(root), size: int64(len(data))})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, entry := range entries {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if shouldSkipSearchEntry(entry) {
			continue
		}

		path := joinSearchPath(root, entry.Name())
		if err := fn(path, entry); err != nil {
			return err
		}
		if entry.IsDir() {
			if err := walkSearchFiles(ctx, sysFs, path, fn); err != nil {
				return err
			}
		}
	}
	return nil
}

func joinSearchPath(root string, name string) string {
	if root == "" || root == "." {
		return name
	}
	return filepath.Join(root, name)
}

func shouldSkipSearchEntry(entry os.DirEntry) bool {
	name := entry.Name()
	if name == ".git" || name == "node_modules" || name == ".cache" || name == "vendor" {
		return true
	}
	return entry.Type()&os.ModeSymlink != 0
}

func matchFilePattern(pattern string, path string, name string) (bool, error) {
	if matched := matchGlob(pattern, name, path); matched {
		return true, nil
	}
	if strings.ContainsAny(pattern, "*?[") {
		return false, nil
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return false, fmt.Errorf("invalid file search pattern: %v", err)
	}
	return re.MatchString(path) || re.MatchString(name), nil
}

func matchGlob(pattern string, name string, path string) bool {
	if pattern == "" {
		return true
	}
	if ok, _ := filepath.Match(pattern, name); ok {
		return true
	}
	if ok, _ := filepath.Match(pattern, path); ok {
		return true
	}
	return strings.Contains(name, pattern) || strings.Contains(path, pattern)
}

func looksBinary(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	sample := data
	if len(sample) > 8000 {
		sample = sample[:8000]
	}
	for _, b := range sample {
		if b == 0 {
			return true
		}
	}
	return false
}

func cleanDisplayPath(path string) string {
	cleaned := filepath.Clean(path)
	if cleaned == "." {
		return cleaned
	}
	return filepath.ToSlash(cleaned)
}

type fakeFileEntry struct {
	name string
	size int64
}

func (f fakeFileEntry) Name() string               { return f.name }
func (f fakeFileEntry) IsDir() bool                { return false }
func (f fakeFileEntry) Type() os.FileMode          { return 0 }
func (f fakeFileEntry) Info() (os.FileInfo, error) { return fakeFileInfo(f), nil }

type fakeFileInfo fakeFileEntry

func (f fakeFileInfo) Name() string       { return f.name }
func (f fakeFileInfo) Size() int64        { return f.size }
func (f fakeFileInfo) Mode() os.FileMode  { return 0o600 }
func (f fakeFileInfo) ModTime() time.Time { return time.Time{} }
func (f fakeFileInfo) IsDir() bool        { return false }
func (f fakeFileInfo) Sys() any           { return nil }
