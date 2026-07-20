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

func TestReplayProjectsToolSteeringDecision(t *testing.T) {
	trace := replayTrace(
		t,
		replayRecord(
			t,
			1,
			evaltrace.RecordTurnStart,
			"turn-start",
			evaltrace.TurnPayload{},
		),
		replayRecordWithCorrelation(
			t,
			2,
			evaltrace.RecordToolSteeringDecision,
			"steering-decision",
			evaltrace.Correlation{ToolCallID: "call-1"},
			evaltrace.ToolPayload{
				Tool: "write_file", Action: "skip", Classification: "cancellable", Cause: "queued_user_steering",
			},
		),
		replayRecord(
			t,
			3,
			evaltrace.RecordTurnEnd,
			"turn-end",
			evaltrace.TurnPayload{Status: "completed"},
		),
	)
	trace.Outcome = &evaltrace.Outcome{Status: "completed"}
	trace = finalizeReplayTrace(t, trace)

	result, err := Replay(trace)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Projection.Diagnostics) != 0 {
		t.Fatalf("diagnostics = %#v", result.Projection.Diagnostics)
	}
	if len(result.Projection.Steering.ToolDecisions) != 1 {
		t.Fatalf("tool decisions = %#v", result.Projection.Steering.ToolDecisions)
	}
	decision := result.Projection.Steering.ToolDecisions[0]
	if decision.Action != "skip" || decision.Classification != "cancellable" ||
		decision.Cause != "queued_user_steering" {
		t.Fatalf("decision = %#v", decision)
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

func TestReplayProjectsCorrectionRecords(t *testing.T) {
	trace := finalizeReplayTrace(t, replayTrace(
		t,
		replayRecord(t, 1, evaltrace.RecordTurnStart, "turn-start", evaltrace.TurnPayload{}),
		replayRecord(t, 2, evaltrace.RecordUserCorrection, "correction", evaltrace.Correction{
			CorrectionID: "correction-1", RecordRefs: []uint64{1}, Category: "wrong_outcome", Note: "fixture note",
		}),
		replayRecord(
			t,
			3,
			evaltrace.RecordTurnEnd,
			"turn-end",
			evaltrace.TurnPayload{Status: "completed"},
		),
	))

	result, err := Replay(trace)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Projection.Corrections) != 1 {
		t.Fatalf("corrections = %#v", result.Projection.Corrections)
	}
	correction := result.Projection.Corrections[0]
	if correction.CorrectionID != "correction-1" || correction.Sequence != 2 ||
		correction.Category != "wrong_outcome" {
		t.Fatalf("correction = %#v", correction)
	}
	if strings.Contains(string(result.Canonical), "fixture note") {
		t.Fatalf("canonical projection leaked correction note: %s", result.Canonical)
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

func TestReplayRejectsInteractionWaitingWithoutSuccessfulPrompt(t *testing.T) {
	created := replayRecordWithCorrelation(
		t,
		1,
		evaltrace.RecordInteractionTransition,
		"interaction-created",
		evaltrace.Correlation{InteractionID: "interaction-1", ToolCallID: "call-1"},
		evaltrace.InteractionPayload{
			EventType: "interaction.created", Kind: "question", Status: "created",
			Revision: 1, Sequence: 1,
		},
	)
	waiting := replayRecordWithCorrelation(
		t,
		2,
		evaltrace.RecordInteractionTransition,
		"interaction-waiting",
		evaltrace.Correlation{InteractionID: "interaction-1", ToolCallID: "call-1"},
		evaltrace.InteractionPayload{
			EventType: "interaction.waiting", Kind: "question", From: "created", Status: "waiting",
			Revision: 2, Sequence: 2,
		},
	)
	trace := replayTrace(t, created, waiting)
	trace.Metadata.TraceKind = evaltrace.TraceKindInteraction
	result, err := Replay(finalizeReplayTrace(t, trace))
	if err != nil {
		t.Fatal(err)
	}
	if !hasDiagnostic(
		result.Projection.Diagnostics,
		"interaction_waiting_without_prompt_evidence",
	) {
		t.Fatalf("diagnostics = %#v", result.Projection.Diagnostics)
	}
	diagnostic := findDiagnostic(
		result.Projection.Diagnostics,
		"interaction_waiting_without_prompt_evidence",
	)
	if diagnostic.Evidence != EvidenceRequiresCompleteHistory {
		t.Fatalf("diagnostic evidence = %q", diagnostic.Evidence)
	}
}

func TestReplayInteractionProtocolRejectsMalformedEventShapes(t *testing.T) {
	tests := []struct {
		name string
		want string
		data evaltrace.InteractionPayload
	}{
		{
			name: "created", want: "interaction_duplicate_or_invalid_create",
			data: evaltrace.InteractionPayload{
				EventType: "interaction.created", Kind: "invalid", From: "waiting",
				Status: "created", Revision: 1, Sequence: 1,
			},
		},
		{
			name: "prompt delivery", want: "interaction_prompt_delivery_invalid",
			data: evaltrace.InteractionPayload{
				EventType: "interaction.delivery_attempted", Kind: "question", From: "waiting",
				Status: "waiting", Revision: 1, Sequence: 1,
			},
		},
		{
			name: "waiting", want: "interaction_waiting_transition_invalid",
			data: evaltrace.InteractionPayload{
				EventType: "interaction.waiting", Kind: "question", From: "waiting",
				Status: "created", Revision: 1, Sequence: 1,
			},
		},
		{
			name: "answer", want: "interaction_answer_transition_invalid",
			data: evaltrace.InteractionPayload{
				EventType: "interaction.answer_claimed", Kind: "question", From: "created",
				Status: "waiting", Outcome: "allowed", Revision: 1, Sequence: 1,
			},
		},
		{
			name: "resume", want: "interaction_resume_transition_invalid",
			data: evaltrace.InteractionPayload{
				EventType: "interaction.resume_started", Kind: "question", From: "waiting",
				Status: "waiting", Revision: 1, Sequence: 1,
			},
		},
		{
			name: "approval consumption", want: "interaction_approval_consumption_invalid",
			data: evaltrace.InteractionPayload{
				EventType: "interaction.approval_consumed", Kind: "question", From: "created",
				Status: "created", Outcome: "denied", Code: "wrong", Revision: 1, Sequence: 1,
			},
		},
		{
			name: "approval expiry", want: "interaction_approval_expiry_invalid",
			data: evaltrace.InteractionPayload{
				EventType: "interaction.approval_expired", Kind: "question", From: "created",
				Status: "created", Outcome: "allowed", Code: "wrong", Revision: 1, Sequence: 1,
			},
		},
		{
			name: "final delivery", want: "interaction_final_delivery_invalid",
			data: evaltrace.InteractionPayload{
				EventType: "interaction.final_delivery_attempted", Kind: "question", From: "waiting",
				Status: "waiting", Revision: 1, Sequence: 1,
			},
		},
		{
			name: "canceling", want: "interaction_canceling_transition_invalid",
			data: evaltrace.InteractionPayload{
				EventType: "interaction.canceling", Kind: "question", From: "resolved",
				Status: "waiting", Revision: 1, Sequence: 1,
			},
		},
		{
			name: "recovery", want: "interaction_recovery_transition_invalid",
			data: evaltrace.InteractionPayload{
				EventType: "interaction.recovery_observed", Kind: "question", From: "waiting",
				Status: "waiting", Code: "wrong", Revision: 1, Sequence: 1,
			},
		},
		{
			name: "resolved", want: "interaction_terminal_transition_invalid",
			data: evaltrace.InteractionPayload{
				EventType: "interaction.resolved", Kind: "question", From: "created",
				Status: "created", Revision: 1, Sequence: 1,
			},
		},
		{
			name: "canceled", want: "interaction_terminal_transition_invalid",
			data: evaltrace.InteractionPayload{
				EventType: "interaction.cancelled", Kind: "question", From: "resolved",
				Status: "resolved", Revision: 1, Sequence: 1,
			},
		},
		{
			name: "failed", want: "interaction_terminal_transition_invalid",
			data: evaltrace.InteractionPayload{
				EventType: "interaction.failed", Kind: "question", From: "resolved",
				Status: "resolved", Revision: 1, Sequence: 1,
			},
		},
		{
			name: "unknown", want: "interaction_event_unknown",
			data: evaltrace.InteractionPayload{
				EventType: "interaction.unknown", Kind: "question", Status: "created",
				Revision: 1, Sequence: 1,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := replayRecordWithCorrelation(
				t,
				1,
				evaltrace.RecordInteractionTransition,
				"interaction-event",
				evaltrace.Correlation{InteractionID: "interaction-1", ToolCallID: "call-1"},
				test.data,
			)
			trace := replayTrace(t, record)
			trace.Metadata.TraceKind = evaltrace.TraceKindInteraction
			result, err := Replay(finalizeReplayTrace(t, trace))
			if err != nil {
				t.Fatal(err)
			}
			diagnostic := findDiagnostic(result.Projection.Diagnostics, test.want)
			if diagnostic.Code == "" {
				t.Fatalf("diagnostics = %#v", result.Projection.Diagnostics)
			}
			if diagnostic.Evidence != EvidenceConclusive {
				t.Fatalf("diagnostic = %#v", diagnostic)
			}
		})
	}
}

func TestReplayInteractionProtocolAcceptsValidLifecycleMatrix(t *testing.T) {
	success := true
	tests := []struct {
		name   string
		events []evaltrace.InteractionPayload
	}{
		{
			name: "answered question with final delivery",
			events: []evaltrace.InteractionPayload{
				{EventType: "interaction.created", Kind: "question", Status: "created"},
				{
					EventType: "interaction.delivery_attempted", Kind: "question", From: "created",
					Status: "created", Success: &success,
				},
				{
					EventType: "interaction.waiting",
					Kind:      "question",
					From:      "created",
					Status:    "waiting",
				},
				{
					EventType: "interaction.answer_claimed",
					Kind:      "question",
					From:      "waiting",
					Status:    "answer_claimed",
					Outcome:   "answered",
				},
				{
					EventType: "interaction.resume_started",
					Kind:      "question",
					From:      "answer_claimed",
					Status:    "resuming",
					Outcome:   "answered",
				},
				{
					EventType: "interaction.final_delivery_attempted",
					Kind:      "question",
					From:      "resuming",
					Status:    "resuming",
					Outcome:   "answered",
					Success:   &success,
				},
				{
					EventType: "interaction.resolved",
					Kind:      "question",
					From:      "resuming",
					Status:    "resolved",
					Outcome:   "answered",
				},
			},
		},
		{
			name: "allowed approval consumed once",
			events: []evaltrace.InteractionPayload{
				{EventType: "interaction.created", Kind: "approval", Status: "created"},
				{
					EventType: "interaction.delivery_attempted",
					Kind:      "approval",
					From:      "created",
					Status:    "created",
					Success:   &success,
				},
				{
					EventType: "interaction.waiting",
					Kind:      "approval",
					From:      "created",
					Status:    "waiting",
				},
				{
					EventType: "interaction.answer_claimed",
					Kind:      "approval",
					From:      "waiting",
					Status:    "answer_claimed",
					Outcome:   "allowed",
				},
				{
					EventType: "interaction.resume_started",
					Kind:      "approval",
					From:      "answer_claimed",
					Status:    "resuming",
					Outcome:   "allowed",
				},
				{
					EventType: "interaction.approval_consumed",
					Kind:      "approval",
					From:      "resuming",
					Status:    "resuming",
					Outcome:   "allowed",
					Code:      "allow_once_consumed",
				},
				{
					EventType: "interaction.resolved",
					Kind:      "approval",
					From:      "resuming",
					Status:    "resolved",
					Outcome:   "allowed",
				},
			},
		},
		{
			name: "allowed approval expires before consumption",
			events: []evaltrace.InteractionPayload{
				{EventType: "interaction.created", Kind: "approval", Status: "created"},
				{
					EventType: "interaction.delivery_attempted",
					Kind:      "approval",
					From:      "created",
					Status:    "created",
					Success:   &success,
				},
				{
					EventType: "interaction.waiting",
					Kind:      "approval",
					From:      "created",
					Status:    "waiting",
				},
				{
					EventType: "interaction.answer_claimed",
					Kind:      "approval",
					From:      "waiting",
					Status:    "answer_claimed",
					Outcome:   "allowed",
				},
				{
					EventType: "interaction.resume_started",
					Kind:      "approval",
					From:      "answer_claimed",
					Status:    "resuming",
					Outcome:   "allowed",
				},
				{
					EventType: "interaction.approval_expired",
					Kind:      "approval",
					From:      "resuming",
					Status:    "resuming",
					Outcome:   "timed_out",
					Code:      "timeout_at_approval_consumption",
				},
				{
					EventType: "interaction.failed",
					Kind:      "approval",
					From:      "resuming",
					Status:    "failed",
					Outcome:   "timed_out",
				},
			},
		},
		{
			name: "canceled before delivery",
			events: []evaltrace.InteractionPayload{
				{EventType: "interaction.created", Kind: "question", Status: "created"},
				{
					EventType: "interaction.canceling",
					Kind:      "question",
					From:      "created",
					Status:    "canceling",
				},
				{
					EventType: "interaction.cancel" + "led", Kind: "question", From: "canceling",
					Status: "cancel" + "led",
				},
			},
		},
		{
			name: "recovery observation then failure",
			events: []evaltrace.InteractionPayload{
				{EventType: "interaction.created", Kind: "question", Status: "created"},
				{
					EventType: "interaction.answer_claimed",
					Kind:      "question",
					From:      "created",
					Status:    "answer_claimed",
					Outcome:   "timed_out",
					Code:      "timeout",
				},
				{
					EventType: "interaction.recovery_observed",
					Kind:      "question",
					From:      "answer_claimed",
					Status:    "answer_claimed",
					Outcome:   "timed_out",
					Code:      "resume_failed",
				},
				{
					EventType: "interaction.failed",
					Kind:      "question",
					From:      "answer_claimed",
					Status:    "failed",
					Outcome:   "timed_out",
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			records := make([]evaltrace.Record, 0, len(test.events))
			for index, payload := range test.events {
				sequence := index + 1
				payload.Revision = int64(sequence)
				payload.Sequence = int64(sequence)
				records = append(records, replayRecordWithCorrelation(
					t,
					uint64(sequence),
					evaltrace.RecordInteractionTransition,
					"interaction-event-"+payload.EventType,
					evaltrace.Correlation{InteractionID: "interaction-1", ToolCallID: "call-1"},
					payload,
				))
			}
			trace := replayTrace(t, records...)
			trace.Metadata.TraceKind = evaltrace.TraceKindInteraction
			result, err := Replay(finalizeReplayTrace(t, trace))
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Projection.Diagnostics) != 0 {
				t.Fatalf("diagnostics = %#v", result.Projection.Diagnostics)
			}
		})
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

func hasDiagnostic(diagnostics []Diagnostic, code string) bool {
	for _, diagnostic := range diagnostics {
		if diagnostic.Code == code {
			return true
		}
	}
	return false
}

func findDiagnostic(diagnostics []Diagnostic, code string) Diagnostic {
	for _, diagnostic := range diagnostics {
		if diagnostic.Code == code {
			return diagnostic
		}
	}
	return Diagnostic{}
}
