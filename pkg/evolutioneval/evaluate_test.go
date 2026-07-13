package evolutioneval_test

import (
	"testing"

	"github.com/sipeed/picoclaw/pkg/evolutioneval"
)

func TestEvaluateRequiresPairedHeldOutImprovement(t *testing.T) {
	manifest := testManifest()
	report := evolutioneval.Evaluate(manifest)
	if report.Candidates[0].Status != evolutioneval.StatusBeneficial {
		t.Fatalf("candidate = %#v", report.Candidates[0])
	}
	if report.Candidates[0].Cases[0].CandidateMeanTokens != 100 {
		t.Fatalf("case metrics = %#v", report.Candidates[0].Cases[0])
	}
	if report.Summary.Recommendation != evolutioneval.RecommendationRetain {
		t.Fatalf("summary = %#v", report.Summary)
	}
}

func TestEvaluateRejectsGenerationEvidenceLeakage(t *testing.T) {
	manifest := testManifest()
	manifest.Candidates[0].Cases[0].HeldOutRecordIDs = []string{"source-task-1"}
	report := evolutioneval.Evaluate(manifest)
	if report.Candidates[0].Status != evolutioneval.StatusInvalid ||
		report.Summary.Recommendation != evolutioneval.RecommendationInsufficient {
		t.Fatalf("report = %#v", report)
	}
}

func TestEvaluateRejectsProtectedRegressionDespiteHigherScore(t *testing.T) {
	manifest := testManifest()
	manifest.Candidates[0].Cases[0].CandidateTrials[1].Protected["delivery_once"] = false
	report := evolutioneval.Evaluate(manifest)
	result := report.Candidates[0].Cases[0]
	if result.Status != evolutioneval.StatusRegression || len(result.ProtectedFailures) != 1 {
		t.Fatalf("case = %#v", result)
	}
}

func TestEvaluateRequiresMatchingSeedsAndEvidence(t *testing.T) {
	manifest := testManifest()
	manifest.Candidates[0].Cases[0].CandidateTrials[1].Seed = "other-seed"
	manifest.Candidates[0].Cases[0].CandidateTrials[0].EvidenceRefs = nil
	report := evolutioneval.Evaluate(manifest)
	if report.Candidates[0].Status != evolutioneval.StatusInvalid {
		t.Fatalf("candidate = %#v", report.Candidates[0])
	}
}

func TestEvaluateRejectsMissingHeldOutProvenanceAndUnknownRubricKeys(t *testing.T) {
	manifest := testManifest()
	manifest.Candidates[0].Cases[0].HeldOutRecordIDs = nil
	manifest.Candidates[0].Cases[0].CandidateTrials[0].Criteria["typo"] = true
	report := evolutioneval.Evaluate(manifest)
	if report.Candidates[0].Status != evolutioneval.StatusInvalid {
		t.Fatalf("candidate = %#v", report.Candidates[0])
	}
}

func TestEvaluateRecommendsRemovalWhenMeasuredCandidatesDoNotImprove(t *testing.T) {
	manifest := testManifest()
	manifest.Policy.MinEvaluatedCandidates = 1
	for i := range manifest.Candidates[0].Cases[0].CandidateTrials {
		manifest.Candidates[0].Cases[0].CandidateTrials[i].Criteria["correct_state"] = false
	}
	report := evolutioneval.Evaluate(manifest)
	if report.Summary.Recommendation != evolutioneval.RecommendationRemove {
		t.Fatalf("summary = %#v", report.Summary)
	}
}

func testManifest() evolutioneval.Manifest {
	baseline := []evolutioneval.Trial{
		trial("seed-1", false, true),
		trial("seed-2", false, true),
	}
	candidate := []evolutioneval.Trial{
		trial("seed-1", true, true),
		trial("seed-2", true, true),
	}
	return evolutioneval.Manifest{
		SchemaVersion: evolutioneval.ManifestSchemaV1,
		Source:        "sanitized-test-fixture",
		Sanitized:     true,
		Why:           "synthetic held-out evidence",
		Policy: evolutioneval.Policy{
			MinTrials: 2, MinScoreDelta: 0.25, MinUsefulYield: 0.5,
			MinCoverage: 1, MinEvaluatedCandidates: 1,
		},
		Candidates: []evolutioneval.Candidate{{
			ID: "candidate-1", SourceRecordIDs: []string{"source-task-1"},
			Cases: []evolutioneval.Case{{
				ID: "held-out-case-1", Source: "evaluate_test.go",
				HeldOutRecordIDs: []string{"held-out-task-1"},
				Criteria: []evolutioneval.Criterion{
					{Name: "correct_state", Weight: 3, Required: true},
					{Name: "concise_response", Weight: 1},
				},
				Protected:       []string{"delivery_once", "no_secret_output"},
				BaselineTrials:  baseline,
				CandidateTrials: candidate,
			}},
		}},
	}
}

func trial(seed string, correct, protected bool) evolutioneval.Trial {
	return evolutioneval.Trial{
		Seed: seed,
		Criteria: map[string]bool{
			"correct_state": correct, "concise_response": true,
		},
		Protected: map[string]bool{
			"delivery_once": protected, "no_secret_output": true,
		},
		EvidenceRefs: []string{"trace:" + seed},
		ToolCalls:    1, Tokens: 100, LatencyMS: 10,
	}
}
