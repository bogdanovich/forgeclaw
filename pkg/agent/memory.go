// PicoClaw - Ultra-lightweight personal AI agent
// Inspired by and based on nanobot: https://github.com/HKUDS/nanobot
// License: MIT
//
// Copyright (c) 2026 PicoClaw contributors

package agent

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/fileutil"
)

const dailyAppendMarkerPrefix = "<!-- picoclaw:append_daily:v1:"

// MemoryStore manages persistent memory for the agent.
// - Long-term memory: memory/MEMORY.md
// - Daily notes: memory/YYYYMM/YYYYMMDD.md
type MemoryStore struct {
	workspace  string
	memoryDir  string
	memoryFile string
	prompt     config.PromptMemoryConfig
	now        func() time.Time
}

// NewMemoryStore creates a new MemoryStore with the given workspace path.
// It ensures the memory directory exists.
func NewMemoryStore(workspace string) *MemoryStore {
	return newMemoryStore(workspace, config.PromptMemoryConfig{}, time.Now)
}

func newMemoryStore(
	workspace string,
	prompt config.PromptMemoryConfig,
	now func() time.Time,
) *MemoryStore {
	memoryDir := filepath.Join(workspace, "memory")
	memoryFile := filepath.Join(memoryDir, "MEMORY.md")

	// Ensure memory directory exists
	os.MkdirAll(memoryDir, 0o755)

	return &MemoryStore{
		workspace:  workspace,
		memoryDir:  memoryDir,
		memoryFile: memoryFile,
		prompt:     prompt,
		now:        now,
	}
}

func (ms *MemoryStore) configurePrompt(prompt config.PromptMemoryConfig) {
	ms.prompt = prompt
}

func (ms *MemoryStore) currentDateKey() string {
	return ms.now().Format("20060102")
}

// getTodayFile returns the path to today's daily note file (memory/YYYYMM/YYYYMMDD.md).
func (ms *MemoryStore) getTodayFile() string {
	today := ms.currentDateKey() // YYYYMMDD
	monthDir := today[:6]        // YYYYMM
	filePath := filepath.Join(ms.memoryDir, monthDir, today+".md")
	return filePath
}

// ReadLongTerm reads the long-term memory (MEMORY.md).
// Returns empty string if the file doesn't exist.
func (ms *MemoryStore) ReadLongTerm() string {
	if data, err := os.ReadFile(ms.memoryFile); err == nil {
		return string(data)
	}
	return ""
}

// WriteLongTerm writes content to the long-term memory file (MEMORY.md).
func (ms *MemoryStore) WriteLongTerm(content string) error {
	// Use unified atomic write utility with explicit sync for flash storage reliability.
	// Using 0o600 (owner read/write only) for secure default permissions.
	return fileutil.WriteFileAtomic(ms.memoryFile, []byte(content), 0o600)
}

// ReadToday reads today's daily note.
// Returns empty string if the file doesn't exist.
func (ms *MemoryStore) ReadToday() string {
	todayFile := ms.getTodayFile()
	if data, err := os.ReadFile(todayFile); err == nil {
		return stripDailyAppendMarkers(string(data))
	}
	return ""
}

// AppendToday appends content to today's daily note.
// If the file doesn't exist, it creates a new file with a date header.
func (ms *MemoryStore) AppendToday(content string) error {
	todayFile := ms.getTodayFile()

	// Ensure month directory exists
	monthDir := filepath.Dir(todayFile)
	if err := os.MkdirAll(monthDir, 0o755); err != nil {
		return err
	}

	var existingContent string
	if data, err := os.ReadFile(todayFile); err == nil {
		existingContent = string(data)
	}

	var newContent string
	if existingContent == "" {
		// Add header for new day
		header := fmt.Sprintf("# %s\n\n", ms.now().Format("2006-01-02"))
		newContent = header + content
	} else {
		// Append to existing content
		newContent = existingContent + "\n" + content
	}

	// Use unified atomic write utility with explicit sync for flash storage reliability.
	return fileutil.WriteFileAtomic(todayFile, []byte(newContent), 0o600)
}

// GetRecentDailyNotes returns daily notes from the last N days.
// Contents are joined with "---" separator.
func (ms *MemoryStore) GetRecentDailyNotes(days int) string {
	return ms.getRecentDailyNotes(ms.recentDailyPaths(days))
}

func (ms *MemoryStore) recentDailyPaths(days int) []string {
	if days <= 0 {
		return nil
	}
	now := ms.now()
	paths := make([]string, 0, days)
	for i := range days {
		dateStr := now.AddDate(0, 0, -i).Format("20060102")
		paths = append(paths, filepath.Join(ms.memoryDir, dateStr[:6], dateStr+".md"))
	}
	return paths
}

func (ms *MemoryStore) promptSourcePaths() []string {
	paths := []string{ms.memoryFile}
	return append(paths, ms.recentDailyPaths(ms.prompt.EffectiveRecentDays())...)
}

func (ms *MemoryStore) getRecentDailyNotes(paths []string) string {
	var sb strings.Builder
	first := true

	for _, filePath := range paths {
		if data, err := os.ReadFile(filePath); err == nil {
			if !first {
				sb.WriteString("\n\n---\n\n")
			}
			sb.WriteString(stripDailyAppendMarkers(string(data)))
			first = false
		}
	}

	return sb.String()
}

// GetMemoryContext returns formatted memory context for the agent prompt.
// Includes long-term memory and recent daily notes.
func (ms *MemoryStore) GetMemoryContext() string {
	longTerm := truncateMiddleBytes(
		ms.ReadLongTerm(),
		ms.prompt.EffectiveLongTermMaxBytes(),
		"stable memory",
	)
	recentNotes := ms.getBoundedRecentDailyNotes(
		ms.recentDailyPaths(ms.prompt.EffectiveRecentDays()),
		ms.prompt.EffectiveDailyNotesMaxBytes(),
	)

	if longTerm == "" && recentNotes == "" {
		return ""
	}

	var sb strings.Builder

	if longTerm != "" {
		sb.WriteString("## Long-term Memory\n\n")
		sb.WriteString(longTerm)
	}

	if recentNotes != "" {
		if longTerm != "" {
			sb.WriteString("\n\n---\n\n")
		}
		sb.WriteString("## Recent Daily Notes\n\n")
		sb.WriteString(recentNotes)
	}

	return sb.String()
}

func truncateMiddleBytes(text string, maxBytes int, label string) string {
	if maxBytes <= 0 || len(text) <= maxBytes {
		return text
	}
	marker := ""
	for range 4 {
		if len(marker) >= maxBytes {
			return validUTF8Prefix(marker, maxBytes)
		}
		available := maxBytes - len(marker)
		head := validUTF8Prefix(text, available/2)
		tail := validUTF8Suffix(text, available-len(head))
		omitted := len(text) - len(head) - len(tail)
		nextMarker := fmt.Sprintf("\n\n... [%s truncated: %d bytes omitted] ...\n\n", label, omitted)
		if nextMarker == marker {
			return head + marker + tail
		}
		marker = nextMarker
	}
	if len(marker) >= maxBytes {
		return validUTF8Prefix(marker, maxBytes)
	}
	available := maxBytes - len(marker)
	head := validUTF8Prefix(text, max(available/2, 0))
	tail := validUTF8Suffix(text, max(available-len(head), 0))
	return head + marker + tail
}

func (ms *MemoryStore) getBoundedRecentDailyNotes(paths []string, maxBytes int) string {
	notes := make([]string, 0, len(paths))
	for _, path := range paths {
		if data, err := os.ReadFile(path); err == nil {
			notes = append(notes, stripDailyAppendMarkers(string(data)))
		}
	}
	return joinDailyNotesWithinBudget(notes, maxBytes)
}

func stripDailyAppendMarkers(content string) string {
	lines := strings.SplitAfter(content, "\n")
	var cleaned strings.Builder
	cleaned.Grow(len(content))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, dailyAppendMarkerPrefix) && strings.HasSuffix(trimmed, " -->") {
			continue
		}
		cleaned.WriteString(line)
	}
	return cleaned.String()
}

func joinDailyNotesWithinBudget(notes []string, maxBytes int) string {
	const separator = "\n\n---\n\n"
	all := strings.Join(notes, separator)
	if maxBytes <= 0 || len(all) <= maxBytes {
		return all
	}

	marker := ""
	for range 4 {
		if len(marker) >= maxBytes {
			return validUTF8Prefix(marker, maxBytes)
		}
		retained := retainDailyNotes(notes, maxBytes-len(marker), separator)
		omitted := len(all) - len(retained)
		nextMarker := fmt.Sprintf("\n\n... [daily notes truncated: %d bytes omitted]", omitted)
		if nextMarker == marker {
			return retained + marker
		}
		marker = nextMarker
	}
	if len(marker) >= maxBytes {
		return validUTF8Prefix(marker, maxBytes)
	}
	return retainDailyNotes(notes, maxBytes-len(marker), separator) + marker
}

func retainDailyNotes(notes []string, maxBytes int, separator string) string {
	var retained strings.Builder
	for _, note := range notes {
		separatorBytes := 0
		if retained.Len() > 0 {
			separatorBytes = len(separator)
		}
		if retained.Len()+separatorBytes+len(note) <= maxBytes {
			if separatorBytes > 0 {
				retained.WriteString(separator)
			}
			retained.WriteString(note)
			continue
		}

		available := maxBytes - retained.Len() - separatorBytes
		if available > 0 {
			if separatorBytes > 0 {
				retained.WriteString(separator)
			}
			retained.WriteString(validUTF8Suffix(note, available))
		}
		break
	}
	return retained.String()
}

func validUTF8Prefix(text string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(text) <= maxBytes {
		return text
	}
	prefix := text[:maxBytes]
	for !utf8.ValidString(prefix) {
		prefix = prefix[:len(prefix)-1]
	}
	return prefix
}

func validUTF8Suffix(text string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(text) <= maxBytes {
		return text
	}
	start := len(text) - maxBytes
	for start < len(text) && !utf8.RuneStart(text[start]) {
		start++
	}
	return text[start:]
}
