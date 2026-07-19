package evalevaluator

import (
	"path/filepath"
	"testing"
)

func TestHistoricalFixturesMatchExpectedFindings(t *testing.T) {
	manifest, err := LoadFixtureManifest(filepath.Join("testdata", "historical_failures.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Fixtures) < len(DefaultEvaluators())*2 {
		t.Fatalf("fixture count = %d, want at least %d", len(manifest.Fixtures), len(DefaultEvaluators())*2)
	}
	coverage := make(map[string]map[Status]bool)
	for _, fixture := range manifest.Fixtures {
		t.Run(fixture.ID, func(t *testing.T) {
			evaluator, ok := EvaluatorByName(fixture.Evaluator)
			if !ok {
				t.Fatalf("unknown evaluator %q", fixture.Evaluator)
			}
			trace, err := fixture.Trace()
			if err != nil {
				t.Fatal(err)
			}
			report, err := Evaluate(trace, evaluator)
			if err != nil {
				t.Fatal(err)
			}
			if len(report.Findings) != 1 || report.Findings[0].Status != fixture.Expected {
				t.Fatalf("findings = %#v, want %s", report.Findings, fixture.Expected)
			}
		})
		if coverage[fixture.Evaluator] == nil {
			coverage[fixture.Evaluator] = make(map[Status]bool)
		}
		coverage[fixture.Evaluator][fixture.Expected] = true
	}
	for _, evaluator := range DefaultEvaluators() {
		if !coverage[evaluator.Name()][StatusPass] || !coverage[evaluator.Name()][StatusFail] {
			t.Fatalf("evaluator %s lacks pass/fail fixture coverage", evaluator.Name())
		}
	}
}
