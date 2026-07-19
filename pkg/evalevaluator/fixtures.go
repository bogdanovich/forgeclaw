package evalevaluator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/sipeed/picoclaw/pkg/evaltrace"
)

const FixtureManifestV1 = "forgeclaw.eval_fixture_manifest.v1"

type FixtureManifest struct {
	SchemaVersion string    `json:"schema_version"`
	Fixtures      []Fixture `json:"fixtures"`
}

type Fixture struct {
	ID        string          `json:"id"`
	Evaluator string          `json:"evaluator"`
	Source    string          `json:"source"`
	Sanitized bool            `json:"sanitized"`
	Why       string          `json:"why"`
	Expected  Status          `json:"expected"`
	Records   []FixtureRecord `json:"records"`
}

type FixtureRecord struct {
	Kind          evaltrace.RecordKind `json:"kind"`
	Data          json.RawMessage      `json:"data"`
	TaskID        string               `json:"task_id,omitempty"`
	TargetHash    string               `json:"target_hash,omitempty"`
	ToolCallID    string               `json:"tool_call_id,omitempty"`
	CompletionID  string               `json:"completion_id,omitempty"`
	InteractionID string               `json:"interaction_id,omitempty"`
}

func LoadFixtureManifest(path string) (FixtureManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return FixtureManifest{}, err
	}
	if len(data) > evaltrace.HardMaxTraceBytes {
		return FixtureManifest{}, fmt.Errorf("fixture manifest exceeds byte limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest FixtureManifest
	if err := decoder.Decode(&manifest); err != nil {
		return FixtureManifest{}, fmt.Errorf("decode fixture manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return FixtureManifest{}, fmt.Errorf("decode fixture manifest: trailing JSON data")
	}
	if manifest.SchemaVersion != FixtureManifestV1 {
		return FixtureManifest{}, fmt.Errorf("unsupported fixture manifest %q", manifest.SchemaVersion)
	}
	seen := make(map[string]bool, len(manifest.Fixtures))
	for _, fixture := range manifest.Fixtures {
		if seen[fixture.ID] {
			return FixtureManifest{}, fmt.Errorf("duplicate fixture id %q", fixture.ID)
		}
		seen[fixture.ID] = true
		if !fixture.Sanitized || fixture.Why == "" {
			return FixtureManifest{}, fmt.Errorf(
				"fixture %q requires sanitization attestation and rationale",
				fixture.ID,
			)
		}
		if fixture.Expected != StatusPass && fixture.Expected != StatusFail {
			return FixtureManifest{}, fmt.Errorf("fixture %q expected status must be pass or fail", fixture.ID)
		}
	}
	return manifest, nil
}

func (fixture Fixture) Trace() (evaltrace.Trace, error) {
	if fixture.ID == "" || fixture.Source == "" {
		return evaltrace.Trace{}, fmt.Errorf("fixture id and source are required")
	}
	records := make([]evaltrace.Record, 0, len(fixture.Records))
	for i, item := range fixture.Records {
		sequence := uint64(i + 1)
		records = append(records, evaltrace.Record{
			Sequence: sequence, OffsetNanos: int64(i), Kind: item.Kind,
			Origin: evaltrace.Origin{Kind: "fixture", ID: fmt.Sprintf("%s-%d", fixture.ID, sequence)},
			Scope:  evaltrace.Scope{TaskID: item.TaskID, TargetHash: item.TargetHash},
			Correlation: evaltrace.Correlation{
				ToolCallID: item.ToolCallID, CompletionID: item.CompletionID,
				InteractionID: item.InteractionID,
			},
			Data: append(json.RawMessage(nil), item.Data...),
		})
	}
	return evaltrace.Finalize(evaltrace.Trace{
		SchemaVersion: evaltrace.SchemaVersionV1,
		TraceID:       fixture.ID,
		CreatedAt:     time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Source:        evaltrace.Source{FixtureID: fixture.ID, FixtureSource: fixture.Source},
		Policy:        evaltrace.CapturePolicy{ContentMode: evaltrace.ContentFixture},
		Limits:        evaltrace.NormalizeLimits(evaltrace.AppliedLimits{}),
		Records:       records,
	})
}
