package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaultDiagnosticTraceCaptureIsDisabledAndBounded(t *testing.T) {
	cfg := DefaultConfig().Diagnostics.TraceCapture
	if cfg.Enabled {
		t.Fatal("trace capture must be disabled by default")
	}
	if cfg.ContentMode != "metadata_only" {
		t.Fatalf("content mode = %q", cfg.ContentMode)
	}
	if cfg.MaxTraceBytes != DefaultDiagnosticTraceMaxBytes ||
		cfg.MaxRecords != DefaultDiagnosticTraceMaxRecords ||
		cfg.MaxRecordBytes != DefaultDiagnosticTraceMaxRecordBytes ||
		cfg.RetentionHours != DefaultDiagnosticTraceRetentionHours ||
		cfg.MaxTraces != DefaultDiagnosticTraceMaxTraces {
		t.Fatalf("unexpected trace defaults: %#v", cfg)
	}
}

func TestDefaultTaskRegistryIsBounded(t *testing.T) {
	cfg := DefaultConfig().Tasks
	if cfg.MaxSnapshotBytes != 2*1024*1024 || cfg.MaxRecords != 1000 || cfg.MaxEvents != 5000 ||
		cfg.TerminalRetentionHours != 168 {
		t.Fatalf("unexpected task registry defaults: %#v", cfg)
	}
}

func TestDiagnosticTraceCaptureUsesOnlySupportedRuntimeModes(t *testing.T) {
	cfg := DiagnosticTraceCaptureConfig{Enabled: true, ContentMode: "fixture"}
	if got := cfg.EffectiveContentMode(); got != "metadata_only" {
		t.Fatalf("effective content mode = %q", got)
	}
	cfg.ContentMode = "redacted_content"
	if got := cfg.EffectiveContentMode(); got != "redacted_content" {
		t.Fatalf("effective content mode = %q", got)
	}
}

func TestLoadConfigDiagnosticTraceCapture(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{
  "version": 3,
  "diagnostics": {
    "trace_capture": {
      "enabled": true,
      "content_mode": "redacted_content",
      "state_dir": "custom",
      "retention_hours": 168,
      "max_traces": 250
    }
  }
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error: %v", err)
	}
	got := cfg.Diagnostics.TraceCapture
	if !got.Enabled || got.EffectiveContentMode() != "redacted_content" ||
		got.StateDir != "custom" || got.RetentionHours != 168 || got.MaxTraces != 250 {
		t.Fatalf("diagnostic trace config = %#v", got)
	}
}

func TestLoadConfigRejectsRemovedEvaluationTraceConfig(t *testing.T) {
	tests := map[string]string{
		"evaluation root": `{"version":3,"evaluation":{"trace_capture":{"enabled":true}}}`,
		"max corrections": `{"version":3,"diagnostics":{"trace_capture":{"max_corrections":8}}}`,
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := LoadConfig(path)
			if err == nil || !strings.Contains(err.Error(), "unknown field") {
				t.Fatalf("LoadConfig() error = %v", err)
			}
		})
	}
}
