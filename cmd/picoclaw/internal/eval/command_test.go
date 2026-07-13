package eval

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/evaltrace"
)

func TestEvalCommandWritesStableJSON(t *testing.T) {
	trace := evaltrace.Trace{
		SchemaVersion: evaltrace.SchemaVersionV1, TraceID: "cli-trace",
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Source:    evaltrace.Source{FixtureID: "cli-trace", FixtureSource: "command_test.go"},
		Policy:    evaltrace.CapturePolicy{ContentMode: evaltrace.ContentFixture},
		Limits:    evaltrace.NormalizeLimits(evaltrace.AppliedLimits{}),
		Records: []evaltrace.Record{
			fixtureCLIRecord(
				t,
				1,
				evaltrace.RecordDeliveryOutcome,
				evaltrace.DeliveryPayload{Mode: "user_only", TargetHash: "u1", Status: "delivered"},
			),
		},
	}
	trace.Records[0].Scope.TargetHash = "u1"
	trace.Records[0].Correlation.CompletionID = "c1"
	trace, err := evaltrace.Finalize(trace)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "trace.json")
	data, _ := json.Marshal(trace)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := NewEvalCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetArgs([]string{"--json", "--evaluator", "duplicate_response.v1", path})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"schema_version": "forgeclaw.eval_report.v1"`) ||
		!strings.Contains(output.String(), `"status": "pass"`) {
		t.Fatalf("output = %s", output.String())
	}
}

func TestEvalEvolutionCommandWritesStableJSON(t *testing.T) {
	manifest := map[string]any{
		"schema_version": "forgeclaw.evolution_eval_manifest.v1",
		"source":         "command-test", "sanitized": true, "why": "synthetic fixture",
		"policy": map[string]any{
			"min_trials": 1, "min_score_delta": 0.25, "min_useful_yield": 0.5,
			"min_coverage": 1.0, "min_evaluated_candidates": 1,
		},
		"candidates": []any{map[string]any{
			"id": "candidate-1", "source_record_ids": []string{"source-1"},
			"cases": []any{map[string]any{
				"id": "case-1", "source": "command_test.go",
				"held_out_record_ids": []string{"held-out-1"},
				"criteria":            []any{map[string]any{"name": "correct", "weight": 1, "required": true}},
				"protected":           []string{"delivery_once"},
				"baseline_trials":     []any{cliEvolutionTrial(false)},
				"candidate_trials":    []any{cliEvolutionTrial(true)},
			}},
		}},
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "evolution.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := NewEvalCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetArgs([]string{"evolution", "--json", path})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"schema_version": "forgeclaw.evolution_eval_report.v1"`) ||
		!strings.Contains(output.String(), `"recommendation": "retain_experiment"`) {
		t.Fatalf("output = %s", output.String())
	}
}

func cliEvolutionTrial(correct bool) map[string]any {
	return map[string]any{
		"seed": "seed-1", "criteria": map[string]bool{"correct": correct},
		"protected":     map[string]bool{"delivery_once": true},
		"evidence_refs": []string{"trace:seed-1"},
	}
}

func fixtureCLIRecord(t *testing.T, sequence uint64, kind evaltrace.RecordKind, payload any) evaltrace.Record {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return evaltrace.Record{
		Sequence: sequence,
		Kind:     kind,
		Origin:   evaltrace.Origin{Kind: "fixture", ID: "record-1"},
		Data:     data,
	}
}
