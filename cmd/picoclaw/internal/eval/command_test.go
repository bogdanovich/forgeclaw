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
