package config

import "testing"

func TestDefaultEvaluationTraceCaptureIsDisabledAndBounded(t *testing.T) {
	cfg := DefaultConfig().Evaluation.TraceCapture
	if cfg.Enabled {
		t.Fatal("trace capture must be disabled by default")
	}
	if cfg.ContentMode != "metadata_only" {
		t.Fatalf("content mode = %q", cfg.ContentMode)
	}
	if cfg.MaxTraceBytes != DefaultEvaluationTraceMaxBytes ||
		cfg.MaxRecords != DefaultEvaluationTraceMaxRecords ||
		cfg.MaxRecordBytes != DefaultEvaluationTraceMaxRecordBytes ||
		cfg.MaxCorrections != DefaultEvaluationTraceMaxCorrections ||
		cfg.RetentionHours != DefaultEvaluationTraceRetentionHours ||
		cfg.MaxTraces != DefaultEvaluationTraceMaxTraces {
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

func TestEvaluationTraceCaptureRejectsFixtureModeForRuntime(t *testing.T) {
	cfg := EvaluationTraceCaptureConfig{Enabled: true, ContentMode: "fixture"}
	if got := cfg.EffectiveContentMode(); got != "metadata_only" {
		t.Fatalf("effective content mode = %q", got)
	}
	cfg.ContentMode = "redacted_content"
	if got := cfg.EffectiveContentMode(); got != "redacted_content" {
		t.Fatalf("effective content mode = %q", got)
	}
}
