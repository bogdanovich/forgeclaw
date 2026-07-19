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
	if report.Skipped != len(DefaultEvaluators()) || report.Passed != 0 || report.Failed != 0 || report.Errors != 0 {
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
		ID: "suspended-turn", Source: "evaluators_test.go",
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
