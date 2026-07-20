package evalevaluator

import (
	"encoding/json"
	"testing"

	"github.com/sipeed/picoclaw/pkg/evaltrace"
)

func TestEvaluateReportsMissingEvidenceAsNotEvaluable(t *testing.T) {
	trace, err := (Fixture{ID: "missing-evidence", Source: "evaluators_test.go"}).Trace()
	if err != nil {
		t.Fatal(err)
	}
	report, err := Evaluate(trace)
	if err != nil {
		t.Fatal(err)
	}
	if report.Skipped != len(DefaultEvaluators()) || report.Passed != 0 || report.Failed != 0 ||
		report.Errors != 0 {
		t.Fatalf("report = %#v", report)
	}
}

func TestDeliveryEvaluatorReportsMalformedTypedEvidenceAsError(t *testing.T) {
	trace, err := (Fixture{
		ID: "malformed-delivery", Source: "evaluators_test.go",
		Records: []FixtureRecord{
			{
				Kind: evaltrace.RecordDeliveryDecision, CompletionID: "c1", TargetHash: "u1",
				Data: json.RawMessage(`{"mode":"user_only","will_user":true}`),
			},
			{
				Kind: evaltrace.RecordDeliveryOutcome, CompletionID: "c1", TargetHash: "u1",
				Data: json.RawMessage(`{"status":42}`),
			},
		},
	}).Trace()
	if err != nil {
		t.Fatal(err)
	}
	evaluator, _ := EvaluatorByName("delivery_reliability.v1")
	report, err := Evaluate(trace, evaluator)
	if err != nil {
		t.Fatal(err)
	}
	if report.Errors != 1 || report.Findings[0].Status != StatusError {
		t.Fatalf("report = %#v", report)
	}
}

func TestDurableInteractionEvaluatorDefersToLifecycleTrace(t *testing.T) {
	trace, err := (Fixture{
		ID: "suspended-turn", Source: "evaluators_test.go", TraceKind: evaltrace.TraceKindTurn,
		Records: []FixtureRecord{{
			Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
			Data: json.RawMessage(
				`{"event_type":"interaction.created","kind":"question","status":"created","revision":1,"sequence":1}`,
			),
		}},
	}).Trace()
	if err != nil {
		t.Fatal(err)
	}
	evaluator, _ := EvaluatorByName("durable_interaction.v1")
	report, err := Evaluate(trace, evaluator)
	if err != nil {
		t.Fatal(err)
	}
	if report.Skipped != 1 || report.Findings[0].Status != StatusNotEvaluable {
		t.Fatalf("report = %#v", report)
	}
}

func TestDurableInteractionEvaluatorAcceptsCreatedStateTimeoutClaim(t *testing.T) {
	trace, err := (Fixture{
		ID: "created-timeout", Source: "evaluators_test.go", TraceKind: evaltrace.TraceKindInteraction,
		Records: []FixtureRecord{
			{
				Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
				Data: json.RawMessage(
					`{"event_type":"interaction.created","kind":"question","status":"created","revision":1,"sequence":1}`,
				),
			},
			{
				Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
				Data: json.RawMessage(
					`{"event_type":"interaction.answer_claimed","kind":"question","from":"created","status":"answer_claimed","outcome":"timed_out","revision":2,"sequence":2,"code":"timeout"}`,
				),
			},
			{
				Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
				Data: json.RawMessage(
					`{"event_type":"interaction.resume_started","kind":"question","from":"answer_claimed","status":"resuming","outcome":"timed_out","revision":3,"sequence":3}`,
				),
			},
			{
				Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
				Data: json.RawMessage(
					`{"event_type":"interaction.resolved","kind":"question","from":"resuming","status":"resolved","outcome":"timed_out","revision":4,"sequence":4}`,
				),
			},
		},
	}).Trace()
	if err != nil {
		t.Fatal(err)
	}
	evaluator, _ := EvaluatorByName("durable_interaction.v1")
	report, err := Evaluate(trace, evaluator)
	if err != nil {
		t.Fatal(err)
	}
	if report.Passed != 1 || report.Findings[0].Status != StatusPass {
		t.Fatalf("report = %#v", report)
	}
}

func TestDurableInteractionEvaluatorRejectsMalformedApprovalExpiry(t *testing.T) {
	trace, err := (Fixture{
		ID: "malformed-approval-expiry", Source: "evaluators_test.go",
		TraceKind: evaltrace.TraceKindInteraction,
		Records: []FixtureRecord{
			{
				Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
				Data: json.RawMessage(
					`{"event_type":"interaction.created","kind":"approval","status":"created","revision":1,"sequence":1}`,
				),
			},
			{
				Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
				Data: json.RawMessage(
					`{"event_type":"interaction.approval_expired","kind":"approval","from":"created","status":"created","outcome":"allowed","revision":2,"sequence":2}`,
				),
			},
		},
	}).Trace()
	if err != nil {
		t.Fatal(err)
	}
	evaluator, _ := EvaluatorByName("durable_interaction.v1")
	report, err := Evaluate(trace, evaluator)
	if err != nil {
		t.Fatal(err)
	}
	if report.Failed != 1 || report.Findings[0].Status != StatusFail {
		t.Fatalf("report = %#v", report)
	}
}

func TestDurableInteractionEvaluatorRejectsMalformedTerminalTransitions(t *testing.T) {
	tests := []struct {
		name      string
		eventType string
	}{
		{name: "resolved", eventType: "interaction.resolved"},
		{name: "canceled", eventType: "interaction.cancel" + "led"},
		{name: "failed", eventType: "interaction.failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			trace, err := (Fixture{
				ID: "malformed-terminal-" + test.name, Source: "evaluators_test.go",
				TraceKind: evaltrace.TraceKindInteraction,
				Records: []FixtureRecord{
					{
						Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
						Data: json.RawMessage(
							`{"event_type":"interaction.created","kind":"question","status":"created","revision":1,"sequence":1}`,
						),
					},
					{
						Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
						Data: json.RawMessage(
							`{"event_type":"` + test.eventType + `","kind":"question","from":"created","status":"created","revision":2,"sequence":2}`,
						),
					},
				},
			}).Trace()
			if err != nil {
				t.Fatal(err)
			}
			evaluator, _ := EvaluatorByName("durable_interaction.v1")
			report, err := Evaluate(trace, evaluator)
			if err != nil {
				t.Fatal(err)
			}
			if report.Failed != 1 || report.Findings[0].Status != StatusFail {
				t.Fatalf("report = %#v", report)
			}
		})
	}
}

func TestDurableInteractionEvaluatorSkipsPartialContinuationTurn(t *testing.T) {
	trace, err := (Fixture{
		ID: "continuation-turn", Source: "evaluators_test.go", TraceKind: evaltrace.TraceKindTurn,
		Records: []FixtureRecord{
			{
				Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
				Data: json.RawMessage(
					`{"event_type":"interaction.resume_started","kind":"question","from":"answer_claimed","status":"resuming","outcome":"answered","revision":4,"sequence":4}`,
				),
			},
			{
				Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
				Data: json.RawMessage(
					`{"event_type":"interaction.resolved","kind":"question","from":"resuming","status":"resolved","outcome":"answered","revision":5,"sequence":5}`,
				),
			},
		},
	}).Trace()
	if err != nil {
		t.Fatal(err)
	}
	evaluator, _ := EvaluatorByName("durable_interaction.v1")
	report, err := Evaluate(trace, evaluator)
	if err != nil {
		t.Fatal(err)
	}
	if report.Skipped != 1 || report.Findings[0].Status != StatusNotEvaluable {
		t.Fatalf("report = %#v", report)
	}
}

func TestDurableInteractionEvaluatorAllowsUnconsumedApprovalThatFailsClosed(t *testing.T) {
	tests := []struct {
		name      string
		eventType string
		status    string
	}{
		{name: "failed", eventType: "interaction.failed", status: "failed"},
		{name: "canceled", eventType: "interaction.cancel" + "led", status: "cancel" + "led"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			trace, err := (Fixture{
				ID: "allowed-approval-" + test.name, Source: "evaluators_test.go",
				TraceKind: evaltrace.TraceKindInteraction,
				Records: []FixtureRecord{
					{
						Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
						Data: json.RawMessage(
							`{"event_type":"interaction.created","kind":"approval","status":"created","revision":1,"sequence":1}`,
						),
					},
					{
						Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
						Data: json.RawMessage(
							`{"event_type":"interaction.delivery_attempted","kind":"approval","from":"created","status":"created","revision":2,"sequence":2,"success":true}`,
						),
					},
					{
						Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
						Data: json.RawMessage(
							`{"event_type":"interaction.waiting","kind":"approval","from":"created","status":"waiting","revision":3,"sequence":3}`,
						),
					},
					{
						Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
						Data: json.RawMessage(
							`{"event_type":"interaction.answer_claimed","kind":"approval","from":"waiting","status":"answer_claimed","outcome":"allowed","revision":4,"sequence":4}`,
						),
					},
					{
						Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
						Data: json.RawMessage(
							`{"event_type":"` + test.eventType + `","kind":"approval","from":"answer_claimed","status":"` + test.status + `","outcome":"allowed","revision":5,"sequence":5}`,
						),
					},
				},
			}).Trace()
			if err != nil {
				t.Fatal(err)
			}
			evaluator, _ := EvaluatorByName("durable_interaction.v1")
			report, err := Evaluate(trace, evaluator)
			if err != nil {
				t.Fatal(err)
			}
			if report.Passed != 1 || report.Findings[0].Status != StatusPass {
				t.Fatalf("report = %#v", report)
			}
		})
	}
}

func TestDurableInteractionEvaluatorFailsOnDuplicateEvidenceInIncompleteTrace(t *testing.T) {
	trace, err := (Fixture{
		ID: "incomplete-duplicate-answer", Source: "evaluators_test.go",
		TraceKind: evaltrace.TraceKindInteraction,
		Records: []FixtureRecord{
			{
				Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
				Data: json.RawMessage(
					`{"event_type":"interaction.created","kind":"question","status":"created","revision":1,"sequence":1}`,
				),
			},
			{
				Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
				Data: json.RawMessage(
					`{"event_type":"interaction.delivery_attempted","kind":"question","from":"created","status":"created","revision":2,"sequence":2,"success":true}`,
				),
			},
			{
				Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
				Data: json.RawMessage(
					`{"event_type":"interaction.waiting","kind":"question","from":"created","status":"waiting","revision":3,"sequence":3}`,
				),
			},
			{
				Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
				Data: json.RawMessage(
					`{"event_type":"interaction.answer_claimed","kind":"question","from":"waiting","status":"answer_claimed","outcome":"answered","revision":4,"sequence":4}`,
				),
			},
			{
				Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
				Data: json.RawMessage(
					`{"event_type":"interaction.answer_claimed","kind":"question","from":"waiting","status":"answer_claimed","outcome":"answered","revision":5,"sequence":5}`,
				),
			},
		},
	}).Trace()
	if err != nil {
		t.Fatal(err)
	}
	trace.Truncation.Incomplete = true
	trace.Truncation.Reasons = []string{"fixture_noncritical_record_dropped"}
	evaluator, _ := EvaluatorByName("durable_interaction.v1")
	report, err := Evaluate(trace, evaluator)
	if err != nil {
		t.Fatal(err)
	}
	if report.Failed != 1 || report.Findings[0].Status != StatusFail {
		t.Fatalf("report = %#v", report)
	}
}

func TestDurableInteractionEvaluatorSkipsIncompleteTraceWithoutConclusiveViolation(t *testing.T) {
	trace, err := (Fixture{
		ID: "incomplete-lifecycle", Source: "evaluators_test.go",
		TraceKind: evaltrace.TraceKindInteraction,
		Records: []FixtureRecord{{
			Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
			Data: json.RawMessage(
				`{"event_type":"interaction.created","kind":"question","status":"created","revision":1,"sequence":1}`,
			),
		}},
	}).Trace()
	if err != nil {
		t.Fatal(err)
	}
	trace.Truncation.Incomplete = true
	trace.Truncation.Reasons = []string{"fixture_history_missing"}
	evaluator, _ := EvaluatorByName("durable_interaction.v1")
	report, err := Evaluate(trace, evaluator)
	if err != nil {
		t.Fatal(err)
	}
	if report.Skipped != 1 || report.Findings[0].Status != StatusNotEvaluable {
		t.Fatalf("report = %#v", report)
	}
}

func TestDurableInteractionEvaluatorFailsCompleteNonterminalLifecycle(t *testing.T) {
	trace, err := (Fixture{
		ID: "complete-nonterminal-lifecycle", Source: "evaluators_test.go",
		TraceKind: evaltrace.TraceKindInteraction,
		Records: []FixtureRecord{{
			Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
			Data: json.RawMessage(
				`{"event_type":"interaction.created","kind":"question","status":"created","revision":1,"sequence":1}`,
			),
		}},
	}).Trace()
	if err != nil {
		t.Fatal(err)
	}
	evaluator, _ := EvaluatorByName("durable_interaction.v1")
	report, err := Evaluate(trace, evaluator)
	if err != nil {
		t.Fatal(err)
	}
	if report.Failed != 1 || report.Findings[0].Status != StatusFail {
		t.Fatalf("report = %#v", report)
	}
}

func TestDurableInteractionEvaluatorFailsOnMalformedApprovalEvidenceInIncompleteTrace(t *testing.T) {
	tests := []struct {
		name      string
		eventType string
		payload   string
	}{
		{
			name:      "consumption",
			eventType: "interaction.approval_consumed",
			payload:   `{"event_type":"interaction.approval_consumed","kind":"question","from":"created","status":"created","outcome":"denied","code":"wrong","revision":2,"sequence":2}`,
		},
		{
			name:      "expiry",
			eventType: "interaction.approval_expired",
			payload:   `{"event_type":"interaction.approval_expired","kind":"question","from":"created","status":"created","outcome":"allowed","code":"wrong","revision":2,"sequence":2}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			trace, err := (Fixture{
				ID:     "incomplete-malformed-approval-" + test.name,
				Source: "evaluators_test.go", TraceKind: evaltrace.TraceKindInteraction,
				Records: []FixtureRecord{
					{
						Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
						Data: json.RawMessage(
							`{"event_type":"interaction.created","kind":"approval","status":"created","revision":1,"sequence":1}`,
						),
					},
					{
						Kind: evaltrace.RecordInteractionTransition, InteractionID: "i1",
						Data: json.RawMessage(test.payload),
					},
				},
			}).Trace()
			if err != nil {
				t.Fatal(err)
			}
			trace.Truncation.Incomplete = true
			trace.Truncation.Reasons = []string{"fixture_history_missing"}
			evaluator, _ := EvaluatorByName("durable_interaction.v1")
			report, err := Evaluate(trace, evaluator)
			if err != nil {
				t.Fatal(err)
			}
			if report.Failed != 1 || report.Findings[0].Status != StatusFail {
				t.Fatalf("%s report = %#v", test.eventType, report)
			}
		})
	}
}
