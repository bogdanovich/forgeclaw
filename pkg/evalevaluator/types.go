// Package evalevaluator runs deterministic correctness checks over evaluation
// traces and their replay projections.
package evalevaluator

import (
	"github.com/sipeed/picoclaw/pkg/evalreplay"
	"github.com/sipeed/picoclaw/pkg/evaltrace"
)

type Status string

const (
	StatusPass         Status = "pass"
	StatusFail         Status = "fail"
	StatusError        Status = "error"
	StatusNotEvaluable Status = "not_evaluable"
)

type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

type Finding struct {
	Evaluator   string   `json:"evaluator"`
	Version     string   `json:"version"`
	Status      Status   `json:"status"`
	Severity    Severity `json:"severity"`
	RecordRefs  []uint64 `json:"record_refs,omitempty"`
	Expected    string   `json:"expected"`
	Observed    string   `json:"observed"`
	Remediation string   `json:"remediation,omitempty"`
}

type Report struct {
	TraceID  string    `json:"trace_id"`
	Findings []Finding `json:"findings"`
	Passed   int       `json:"passed"`
	Failed   int       `json:"failed"`
	Errors   int       `json:"errors"`
	Skipped  int       `json:"not_evaluable"`
}

type Input struct {
	Trace      evaltrace.Trace
	Projection evalreplay.Projection
}

type Evaluator interface {
	Name() string
	Version() string
	Evaluate(Input) Finding
}

func Evaluate(trace evaltrace.Trace, evaluators ...Evaluator) (Report, error) {
	replayed, err := evalreplay.Replay(trace)
	if err != nil {
		return Report{}, err
	}
	if len(evaluators) == 0 {
		evaluators = DefaultEvaluators()
	}
	report := Report{TraceID: trace.TraceID, Findings: make([]Finding, 0, len(evaluators))}
	input := Input{Trace: trace, Projection: replayed.Projection}
	for _, evaluator := range evaluators {
		finding := evaluator.Evaluate(input)
		finding.Evaluator = evaluator.Name()
		finding.Version = evaluator.Version()
		report.Findings = append(report.Findings, finding)
		switch finding.Status {
		case StatusPass:
			report.Passed++
		case StatusFail:
			report.Failed++
		case StatusError:
			report.Errors++
		case StatusNotEvaluable:
			report.Skipped++
		}
	}
	return report, nil
}

func DefaultEvaluators() []Evaluator {
	return []Evaluator{
		deliveryReliability{},
		duplicateResponse{},
		steeringCorrectness{},
		restartRecovery{},
		durableInteraction{},
		compactionRetention{},
		toolLoopRecovery{},
		providerFailover{},
	}
}

func EvaluatorByName(name string) (Evaluator, bool) {
	for _, evaluator := range DefaultEvaluators() {
		if evaluator.Name() == name {
			return evaluator, true
		}
	}
	return nil, false
}

func finding(status Status, severity Severity, expected, observed, remediation string, refs ...uint64) Finding {
	return Finding{
		Status:      status,
		Severity:    severity,
		RecordRefs:  refs,
		Expected:    expected,
		Observed:    observed,
		Remediation: remediation,
	}
}
