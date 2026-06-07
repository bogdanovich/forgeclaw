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

	"github.com/sipeed/picoclaw/pkg/fileutil"
)

// MemoryStore manages persistent memory for the agent.
// - Long-term memory: memory/MEMORY.md
// - User memory: memory/USER_MEMORY.md
// - Daily notes: memory/YYYY/MM/YYYY-MM-DD.md
type MemoryStore struct {
	workspace      string
	memoryDir      string
	memoryFile     string
	userMemoryFile string
}

// NewMemoryStore creates a new MemoryStore with the given workspace path.
// It ensures the memory directory exists.
func NewMemoryStore(workspace string) *MemoryStore {
	memoryDir := filepath.Join(workspace, "memory")
	memoryFile := filepath.Join(memoryDir, "MEMORY.md")
	userMemoryFile := filepath.Join(memoryDir, "USER_MEMORY.md")

	// Ensure memory directory exists
	os.MkdirAll(memoryDir, 0o755)

	return &MemoryStore{
		workspace:      workspace,
		memoryDir:      memoryDir,
		memoryFile:     memoryFile,
		userMemoryFile: userMemoryFile,
	}
}

// getTodayFile returns the path to today's canonical daily note file
// (memory/YYYY/MM/YYYY-MM-DD.md).
func (ms *MemoryStore) getTodayFile() string {
	return ms.dailyFileForDate(time.Now())
}

func (ms *MemoryStore) dailyFileForDate(date time.Time) string {
	yearDir := date.Format("2006")
	monthDir := date.Format("01")
	return filepath.Join(ms.memoryDir, yearDir, monthDir, date.Format("2006-01-02")+".md")
}

func (ms *MemoryStore) legacyDailyFileForDate(date time.Time) string {
	dateStr := date.Format("20060102") // YYYYMMDD
	monthDir := dateStr[:6]            // YYYYMM
	return filepath.Join(ms.memoryDir, monthDir, dateStr+".md")
}

func (ms *MemoryStore) recentDailyCandidatePaths(days int) []string {
	paths := make([]string, 0, days*2)
	for i := range days {
		date := time.Now().AddDate(0, 0, -i)
		paths = append(paths, ms.dailyFileForDate(date), ms.legacyDailyFileForDate(date))
	}
	return paths
}

// TrackedPaths returns memory files whose changes should invalidate prompt
// cache baselines.
func (ms *MemoryStore) TrackedPaths() []string {
	paths := []string{ms.memoryFile, ms.userMemoryFile}
	paths = append(paths, ms.recentDailyCandidatePaths(3)...)
	return paths
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

// ReadUserMemory reads mutable user/operator memory (USER_MEMORY.md).
// Returns empty string if the file doesn't exist.
func (ms *MemoryStore) ReadUserMemory() string {
	if data, err := os.ReadFile(ms.userMemoryFile); err == nil {
		return string(data)
	}
	return ""
}

// WriteUserMemory writes content to USER_MEMORY.md.
func (ms *MemoryStore) WriteUserMemory(content string) error {
	return fileutil.WriteFileAtomic(ms.userMemoryFile, []byte(content), 0o600)
}

// ReadToday reads today's daily note.
// Returns empty string if the file doesn't exist.
func (ms *MemoryStore) ReadToday() string {
	todayFile := ms.getTodayFile()
	if data, err := os.ReadFile(todayFile); err == nil {
		return string(data)
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
		header := fmt.Sprintf("# %s\n\n", time.Now().Format("2006-01-02"))
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
	var sb strings.Builder
	first := true

	for i := range days {
		date := time.Now().AddDate(0, 0, -i)
		filePath := ms.dailyFileForDate(date)
		data, err := os.ReadFile(filePath)
		if err != nil {
			data, err = os.ReadFile(ms.legacyDailyFileForDate(date))
		}
		if err == nil {
			if !first {
				sb.WriteString("\n\n---\n\n")
			}
			sb.Write(data)
			first = false
		}
	}

	return sb.String()
}

// GetMemoryContext returns formatted memory context for the agent prompt.
// Includes long-term memory and recent daily notes.
func (ms *MemoryStore) GetMemoryContext() string {
	longTerm := ms.ReadLongTerm()
	userMemory := ms.ReadUserMemory()
	recentNotes := ms.GetRecentDailyNotes(3)

	if longTerm == "" && userMemory == "" && recentNotes == "" {
		return ""
	}

	var sb strings.Builder

	if longTerm != "" {
		sb.WriteString("## Long-term Memory\n\n")
		sb.WriteString(longTerm)
	}

	if userMemory != "" {
		if sb.Len() > 0 {
			sb.WriteString("\n\n---\n\n")
		}
		sb.WriteString("## User Memory\n\n")
		sb.WriteString(userMemory)
	}

	if recentNotes != "" {
		if sb.Len() > 0 {
			sb.WriteString("\n\n---\n\n")
		}
		sb.WriteString("## Recent Daily Notes\n\n")
		sb.WriteString(recentNotes)
	}

	return sb.String()
}
