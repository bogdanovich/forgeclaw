package evalreplay

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/evaltrace"
)

func TestReplayProjectsHealthyTraceDeterministically(t *testing.T) {
	trace := replayTrace(
		t,
		replayRecord(
			t,
			1,
			evaltrace.RecordTurnStart,
			"turn-start",
			evaltrace.TurnPayload{InputHash: "input"},
		),
		replayRecordWithCorrelation(
			t,
			2,
			evaltrace.RecordSteeringEnqueued,
			"steer-enqueued",
			evaltrace.Correlation{},
			evaltrace.SteeringPayload{MessageHash: "steer-1"},
		),
		replayRecordWithCorrelation(
			t,
			3,
			evaltrace.RecordSteeringInjected,
			"steer-injected",
			evaltrace.Correlation{},
			evaltrace.SteeringPayload{MessageHash: "steer-1"},
		),
		replayRecordWithCorrelation(
			t,
			4,
			evaltrace.RecordToolCall,
			"tool-call",
			evaltrace.Correlation{ToolCallID: "call-1"},
			evaltrace.ToolPayload{Tool: "fixture_lookup"},
		),
		replayRecordWithCorrelation(
			t,
			5,
			evaltrace.RecordToolResult,
			"tool-result",
			evaltrace.Correlation{ToolCallID: "call-1"},
			evaltrace.ToolPayload{Tool: "fixture_lookup", Executed: true},
		),
		replayRecord(
			t,
			6,
			evaltrace.RecordToolLoopDecision,
			"loop-decision",
			evaltrace.ToolPayload{
				Tool: "fixture_lookup", Action: "warn", DecisionCode: "repeat", Count: 2, Threshold: 3,
			},
		),
		replayRecord(
			t,
			7,
			evaltrace.RecordModelFallbackAttempt,
			"fallback",
			evaltrace.ModelPayload{IdentityKey: "provider:model", Status: "succeeded"},
		),
		replayRecordWithCorrelation(
			t,
			8,
			evaltrace.RecordDeliveryDecision,
			"delivery-decision",
			evaltrace.Correlation{CompletionID: "completion-1"},
			evaltrace.DeliveryPayload{Mode: "user_only", TargetHash: "target", WillUser: true},
		),
		replayRecordWithCorrelation(
			t,
			9,
			evaltrace.RecordDeliveryAttempt,
			"delivery-attempt",
			evaltrace.Correlation{CompletionID: "completion-1"},
			evaltrace.DeliveryPayload{Mode: "user_only", TargetHash: "target"},
		),
		replayRecordWithCorrelation(
			t,
			10,
			evaltrace.RecordDeliveryOutcome,
			"delivery-outcome",
			evaltrace.Correlation{CompletionID: "completion-1"},
			evaltrace.DeliveryPayload{Mode: "user_only", TargetHash: "target", Status: "sent"},
		),
		replayRecord(
			t,
			11,
			evaltrace.RecordTurnEnd,
			"turn-end",
			evaltrace.TurnPayload{Status: "completed"},
		),
	)
	trace.Outcome = &evaltrace.Outcome{Status: "completed"}
	trace = finalizeReplayTrace(t, trace)

	first, err := Replay(trace)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Replay(trace)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Canonical, second.Canonical) {
		t.Fatalf("canonical replay changed:\n%s\n%s", first.Canonical, second.Canonical)
	}
	if len(first.Projection.Diagnostics) != 0 {
		t.Fatalf("diagnostics = %#v", first.Projection.Diagnostics)
	}
	if !first.Projection.Terminal || !first.Projection.Tools["call-1"].Result {
		t.Fatalf("projection = %#v", first.Projection)
	}
	if first.Projection.Providers.SelectedIdentity != "provider:model" {
		t.Fatalf("selected identity = %q", first.Projection.Providers.SelectedIdentity)
	}
	if len(first.Projection.ToolLoop.Decisions) != 1 ||
		first.Projection.ToolLoop.Decisions[0].Code != "repeat" {
		t.Fatalf("tool-loop projection = %#v", first.Projection.ToolLoop)
	}
}

func TestReplayReportsUnresolvedToolAndMissingTurnEnd(t *testing.T) {
	trace := finalizeReplayTrace(t, replayTrace(
		t,
		replayRecord(t, 1, evaltrace.RecordTurnStart, "turn-start", evaltrace.TurnPayload{}),
		replayRecordWithCorrelation(
			t,
			2,
			evaltrace.RecordToolCall,
			"tool-call",
			evaltrace.Correlation{ToolCallID: "call-1"},
			evaltrace.ToolPayload{Tool: "fixture_lookup"},
		),
	))

	result, err := Replay(trace)
	if err != nil {
		t.Fatal(err)
	}
	codes := make(map[string]bool)
	for _, diagnostic := range result.Projection.Diagnostics {
		codes[diagnostic.Code] = true
	}
	if !codes["turn_not_terminal"] || !codes["tool_call_unresolved"] {
		t.Fatalf("diagnostics = %#v", result.Projection.Diagnostics)
	}
}

func TestReplayRejectsPayloadOutsideTypedContract(t *testing.T) {
	record := replayRecord(
		t,
		1,
		evaltrace.RecordTaskTransition,
		"task",
		map[string]any{"event_type": "created", "unexpected": true},
	)
	record.Scope.TaskID = "task-1"
	trace := finalizeReplayTrace(t, replayTrace(t, record))
	result, err := Replay(trace)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Projection.Diagnostics) != 1 ||
		result.Projection.Diagnostics[0].Code != "payload_invalid" {
		t.Fatalf("diagnostics = %#v", result.Projection.Diagnostics)
	}
}

func TestReplayRejectsInvalidTraceBeforeProjection(t *testing.T) {
	trace := replayTrace(
		t,
		replayRecord(t, 1, evaltrace.RecordTurnStart, "turn", evaltrace.TurnPayload{}),
	)
	trace.Records[0].Digest = strings.Repeat("0", 64)
	if _, err := Replay(trace); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("Replay() error = %v", err)
	}
}

func replayTrace(t *testing.T, records ...evaltrace.Record) evaltrace.Trace {
	t.Helper()
	return evaltrace.Trace{
		SchemaVersion: evaltrace.SchemaVersionV1,
		TraceID:       "fixture-replay",
		CreatedAt:     time.Date(2026, 7, 12, 20, 0, 0, 0, time.UTC),
		Source: evaltrace.Source{
			FixtureID:     "fixture-replay",
			FixtureSource: "pkg/evalreplay/reducer_test.go",
		},
		Policy:  evaltrace.CapturePolicy{ContentMode: evaltrace.ContentFixture},
		Limits:  evaltrace.NormalizeLimits(evaltrace.AppliedLimits{}),
		Records: records,
	}
}

func replayRecord(
	t *testing.T,
	sequence uint64,
	kind evaltrace.RecordKind,
	originID string,
	payload any,
) evaltrace.Record {
	t.Helper()
	return replayRecordWithCorrelation(
		t,
		sequence,
		kind,
		originID,
		evaltrace.Correlation{},
		payload,
	)
}

func replayRecordWithCorrelation(
	t *testing.T,
	sequence uint64,
	kind evaltrace.RecordKind,
	originID string,
	correlation evaltrace.Correlation,
	payload any,
) evaltrace.Record {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return evaltrace.Record{
		Sequence:    sequence,
		OffsetNanos: int64(sequence),
		Kind:        kind,
		Origin:      evaltrace.Origin{Kind: "fixture", ID: originID},
		Correlation: correlation,
		Data:        data,
	}
}

func finalizeReplayTrace(t *testing.T, trace evaltrace.Trace) evaltrace.Trace {
	t.Helper()
	finalized, err := evaltrace.Finalize(trace)
	if err != nil {
		t.Fatal(err)
	}
	return finalized
}
