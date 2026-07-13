package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigTaskRegistryDefaultsAndOverrides(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{
  "version": 3,
  "task_registry": {
    "terminal_retention_hours": 24,
    "max_records": 12,
    "max_events": 34,
    "max_snapshot_bytes": 5678
  }
}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error: %v", err)
	}
	got := cfg.Tasks
	if got.TerminalRetentionHours != 24 || got.MaxRecords != 12 || got.MaxEvents != 34 || got.MaxSnapshotBytes != 5678 {
		t.Fatalf("unexpected task registry config: %#v", got)
	}
}
