package evaltrace

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/fileutil"
)

const (
	DefaultRetention = 24 * time.Hour
	DefaultMaxTraces = 100
)

type Store struct {
	Root      string
	Retention time.Duration
	MaxTraces int
	Now       func() time.Time
}

func (s Store) Save(trace Trace) (string, error) {
	if err := Validate(trace); err != nil {
		return "", err
	}
	root, err := s.safeRoot()
	if err != nil {
		return "", err
	}
	if mkdirErr := os.MkdirAll(root, 0o700); mkdirErr != nil {
		return "", fmt.Errorf("create trace store: %w", mkdirErr)
	}
	if symlinkErr := rejectSymlinkPath(root); symlinkErr != nil {
		return "", symlinkErr
	}
	if chmodErr := os.Chmod(root, 0o700); chmodErr != nil {
		return "", fmt.Errorf("secure trace store: %w", chmodErr)
	}
	data, err := json.MarshalIndent(trace, "", "  ")
	if err != nil {
		return "", fmt.Errorf("encode trace: %w", err)
	}
	path := filepath.Join(root, trace.TraceID+".json")
	if err := fileutil.WriteFileAtomic(path, append(data, '\n'), 0o600); err != nil {
		return "", fmt.Errorf("write trace: %w", err)
	}
	return path, nil
}

func (s Store) Load(traceID string) (Trace, error) {
	if !safeIDPattern.MatchString(traceID) {
		return Trace{}, fmt.Errorf("invalid trace id")
	}
	root, err := s.safeRoot()
	if err != nil {
		return Trace{}, err
	}
	if symlinkErr := rejectSymlinkPath(root); symlinkErr != nil {
		return Trace{}, symlinkErr
	}
	path := filepath.Join(root, traceID+".json")
	if symlinkErr := rejectSymlinkPath(path); symlinkErr != nil {
		return Trace{}, symlinkErr
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Trace{}, err
	}
	if len(data) > HardMaxTraceBytes {
		return Trace{}, fmt.Errorf("trace exceeds hard byte limit")
	}
	var trace Trace
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&trace); err != nil {
		return Trace{}, fmt.Errorf("decode trace: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Trace{}, fmt.Errorf("decode trace: trailing JSON data")
	}
	if err := Validate(trace); err != nil {
		return Trace{}, err
	}
	return trace, nil
}

func (s Store) Prune() (int, error) {
	root, err := s.safeRoot()
	if err != nil {
		return 0, err
	}
	if symlinkErr := rejectSymlinkPath(root); symlinkErr != nil {
		return 0, symlinkErr
	}
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	retention := s.Retention
	if retention <= 0 {
		retention = DefaultRetention
	}
	maxTraces := s.MaxTraces
	if maxTraces <= 0 {
		maxTraces = DefaultMaxTraces
	}
	now := time.Now()
	if s.Now != nil {
		now = s.Now()
	}
	type candidate struct {
		path string
		mod  time.Time
	}
	candidates := make([]candidate, 0, len(entries))
	for _, entry := range entries {
		if entry.Type()&os.ModeSymlink != 0 || entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			return 0, infoErr
		}
		candidates = append(candidates, candidate{filepath.Join(root, entry.Name()), info.ModTime()})
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].mod.Before(candidates[j].mod) })
	removed := 0
	for len(candidates)-removed > maxTraces || (removed < len(candidates) && now.Sub(candidates[removed].mod) > retention) {
		if err := os.Remove(candidates[removed].path); err != nil && !os.IsNotExist(err) {
			return removed, err
		}
		removed++
	}
	return removed, nil
}

func (s Store) safeRoot() (string, error) {
	root := strings.TrimSpace(s.Root)
	if root == "" {
		return "", fmt.Errorf("trace store root is required")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	return filepath.Clean(abs), nil
}

func rejectSymlinkPath(path string) error {
	current := filepath.Clean(path)
	for {
		info, err := os.Lstat(current)
		if err == nil && info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("trace store path must not traverse a symlink")
		}
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return nil
		}
		current = parent
	}
}
