package evolutioneval

import (
	"fmt"
	"sort"
	"strings"
)

func Evaluate(manifest Manifest) Report {
	report := Report{
		SchemaVersion: ReportSchemaV1,
		Source:        manifest.Source,
		Candidates:    make([]CandidateResult, 0, len(manifest.Candidates)),
	}
	for _, candidate := range manifest.Candidates {
		result := evaluateCandidate(manifest.Policy, candidate)
		report.Candidates = append(report.Candidates, result)
	}
	report.Summary = summarize(manifest.Policy, report.Candidates)
	return report
}

func evaluateCandidate(policy Policy, candidate Candidate) CandidateResult {
	result := CandidateResult{ID: candidate.ID, Cases: make([]CaseResult, 0, len(candidate.Cases))}
	for _, evalCase := range candidate.Cases {
		result.Cases = append(result.Cases, evaluateCase(policy, candidate.SourceRecordIDs, evalCase))
	}
	result.Status = StatusBeneficial
	result.Reason = "every held-out case improved without a protected or required-criterion regression"
	for _, evalCase := range result.Cases {
		switch evalCase.Status {
		case StatusInvalid:
			result.Status = StatusInvalid
			result.Reason = "one or more held-out cases have invalid or incomplete evidence"
			return result
		case StatusRegression:
			result.Status = StatusRegression
			result.Reason = "one or more held-out cases regressed"
		case StatusNotBeneficial:
			if result.Status == StatusBeneficial {
				result.Status = StatusNotBeneficial
				result.Reason = "one or more held-out cases did not meet the improvement threshold"
			}
		}
	}
	return result
}

func evaluateCase(policy Policy, sourceIDs []string, evalCase Case) CaseResult {
	result := CaseResult{ID: evalCase.ID, Status: StatusInvalid}
	if !safeID.MatchString(evalCase.ID) || strings.TrimSpace(evalCase.Source) == "" {
		result.Reason = "case requires a safe id and source"
		return result
	}
	if len(sourceIDs) == 0 || len(evalCase.HeldOutRecordIDs) == 0 {
		result.Reason = "candidate generation and held-out record ids are required"
		return result
	}
	if hasBlankOrDuplicate(sourceIDs) || hasBlankOrDuplicate(evalCase.HeldOutRecordIDs) {
		result.Reason = "record ids must be non-empty and unique"
		return result
	}
	if overlaps(sourceIDs, evalCase.HeldOutRecordIDs) {
		result.Reason = "held-out evidence overlaps candidate generation records"
		return result
	}
	if len(evalCase.Criteria) == 0 || len(evalCase.Protected) == 0 || hasBlankOrDuplicate(evalCase.Protected) {
		result.Reason = "case requires task criteria and protected invariants"
		return result
	}
	if len(evalCase.BaselineTrials) < policy.MinTrials || len(evalCase.CandidateTrials) < policy.MinTrials {
		result.Reason = fmt.Sprintf("case requires at least %d paired trials", policy.MinTrials)
		return result
	}
	criteria, totalWeight, criteriaErr := criterionMap(evalCase.Criteria)
	if criteriaErr != "" {
		result.Reason = criteriaErr
		return result
	}
	baseline, baselineErr := indexTrials(evalCase.BaselineTrials, criteria, evalCase.Protected)
	if baselineErr != "" {
		result.Reason = "baseline: " + baselineErr
		return result
	}
	candidate, candidateErr := indexTrials(evalCase.CandidateTrials, criteria, evalCase.Protected)
	if candidateErr != "" {
		result.Reason = "candidate: " + candidateErr
		return result
	}
	if !sameSeeds(baseline, candidate) {
		result.Reason = "baseline and candidate trials must use identical unique seeds"
		return result
	}
	result.BaselineScore = meanScore(baseline, evalCase.Criteria, totalWeight)
	result.CandidateScore = meanScore(candidate, evalCase.Criteria, totalWeight)
	result.ScoreDelta = result.CandidateScore - result.BaselineScore
	result.BaselineMeanToolCalls, result.BaselineMeanTokens, result.BaselineMeanLatencyMS = meanResources(baseline)
	result.CandidateMeanToolCalls, result.CandidateMeanTokens, result.CandidateMeanLatencyMS = meanResources(candidate)
	result.ToolCallDelta = result.CandidateMeanToolCalls - result.BaselineMeanToolCalls
	result.TokenDelta = result.CandidateMeanTokens - result.BaselineMeanTokens
	result.LatencyDeltaMS = result.CandidateMeanLatencyMS - result.BaselineMeanLatencyMS
	result.ProtectedFailures = protectedFailures(baseline, candidate, evalCase.Protected)
	result.RequiredRegressions = requiredRegressions(baseline, candidate, evalCase.Criteria)
	if len(result.ProtectedFailures) > 0 || len(result.RequiredRegressions) > 0 {
		result.Status = StatusRegression
		result.Reason = "candidate violated a protected invariant or regressed a required criterion"
		return result
	}
	if result.ScoreDelta+1e-12 < policy.MinScoreDelta {
		result.Status = StatusNotBeneficial
		result.Reason = fmt.Sprintf("score delta %.4f is below required %.4f", result.ScoreDelta, policy.MinScoreDelta)
		return result
	}
	result.Status = StatusBeneficial
	result.Reason = "candidate met the score threshold without protected regressions"
	return result
}

func criterionMap(criteria []Criterion) (map[string]Criterion, float64, string) {
	result := make(map[string]Criterion, len(criteria))
	total := 0.0
	for _, criterion := range criteria {
		name := strings.TrimSpace(criterion.Name)
		if name == "" || criterion.Weight <= 0 {
			return nil, 0, "criteria require unique names and positive weights"
		}
		if _, exists := result[name]; exists {
			return nil, 0, "duplicate criterion " + name
		}
		result[name] = criterion
		total += criterion.Weight
	}
	return result, total, ""
}

func indexTrials(trials []Trial, criteria map[string]Criterion, protected []string) (map[string]Trial, string) {
	result := make(map[string]Trial, len(trials))
	for _, trial := range trials {
		if !safeID.MatchString(trial.Seed) {
			return nil, "trial seed must be a safe identifier"
		}
		if _, exists := result[trial.Seed]; exists {
			return nil, "duplicate trial seed " + trial.Seed
		}
		if len(trial.EvidenceRefs) == 0 {
			return nil, "trial " + trial.Seed + " has no evidence references"
		}
		if hasBlankOrDuplicate(trial.EvidenceRefs) {
			return nil, "trial " + trial.Seed + " has blank or duplicate evidence references"
		}
		if len(trial.Criteria) != len(criteria) {
			return nil, "trial " + trial.Seed + " has unknown or duplicate criterion keys"
		}
		for name := range criteria {
			if _, exists := trial.Criteria[name]; !exists {
				return nil, "trial " + trial.Seed + " is missing criterion " + name
			}
		}
		if len(trial.Protected) != len(protected) {
			return nil, "trial " + trial.Seed + " has unknown or duplicate protected invariant keys"
		}
		for _, name := range protected {
			if _, exists := trial.Protected[name]; !exists {
				return nil, "trial " + trial.Seed + " is missing protected invariant " + name
			}
		}
		if trial.ToolCalls < 0 || trial.Tokens < 0 || trial.LatencyMS < 0 {
			return nil, "trial resource measurements cannot be negative"
		}
		result[trial.Seed] = trial
	}
	return result, ""
}

func meanScore(trials map[string]Trial, criteria []Criterion, totalWeight float64) float64 {
	total := 0.0
	for _, trial := range trials {
		for _, criterion := range criteria {
			if trial.Criteria[criterion.Name] {
				total += criterion.Weight
			}
		}
	}
	return total / (float64(len(trials)) * totalWeight)
}

func meanResources(trials map[string]Trial) (toolCalls, tokens, latencyMS float64) {
	for _, trial := range trials {
		toolCalls += float64(trial.ToolCalls)
		tokens += float64(trial.Tokens)
		latencyMS += float64(trial.LatencyMS)
	}
	count := float64(len(trials))
	return toolCalls / count, tokens / count, latencyMS / count
}

func protectedFailures(baseline, candidate map[string]Trial, names []string) []string {
	failures := make(map[string]struct{})
	for seed, candidateTrial := range candidate {
		baselineTrial := baseline[seed]
		for _, name := range names {
			if !candidateTrial.Protected[name] || (baselineTrial.Protected[name] && !candidateTrial.Protected[name]) {
				failures[name] = struct{}{}
			}
		}
	}
	return sortedKeys(failures)
}

func requiredRegressions(baseline, candidate map[string]Trial, criteria []Criterion) []string {
	regressions := make(map[string]struct{})
	for _, criterion := range criteria {
		if !criterion.Required {
			continue
		}
		for seed, baselineTrial := range baseline {
			if baselineTrial.Criteria[criterion.Name] && !candidate[seed].Criteria[criterion.Name] {
				regressions[criterion.Name] = struct{}{}
			}
		}
	}
	return sortedKeys(regressions)
}

func sameSeeds(left, right map[string]Trial) bool {
	if len(left) != len(right) {
		return false
	}
	for seed := range left {
		if _, exists := right[seed]; !exists {
			return false
		}
	}
	return true
}

func overlaps(left, right []string) bool {
	seen := make(map[string]struct{}, len(left))
	for _, value := range left {
		seen[value] = struct{}{}
	}
	for _, value := range right {
		if _, exists := seen[value]; exists {
			return true
		}
	}
	return false
}

func hasBlankOrDuplicate(values []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return true
		}
		if _, exists := seen[value]; exists {
			return true
		}
		seen[value] = struct{}{}
	}
	return false
}

func sortedKeys(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func summarize(policy Policy, candidates []CandidateResult) Summary {
	summary := Summary{TotalCandidates: len(candidates)}
	for _, candidate := range candidates {
		switch candidate.Status {
		case StatusBeneficial:
			summary.EvaluatedCandidates++
			summary.UsefulCandidates++
		case StatusNotBeneficial:
			summary.EvaluatedCandidates++
		case StatusRegression:
			summary.EvaluatedCandidates++
			summary.RegressedCandidates++
		case StatusInvalid:
			summary.InvalidCandidates++
		}
	}
	if summary.TotalCandidates > 0 {
		summary.Coverage = float64(summary.EvaluatedCandidates) / float64(summary.TotalCandidates)
	}
	if summary.EvaluatedCandidates > 0 {
		summary.UsefulYield = float64(summary.UsefulCandidates) / float64(summary.EvaluatedCandidates)
	}
	switch {
	case summary.InvalidCandidates > 0 || summary.EvaluatedCandidates < policy.MinEvaluatedCandidates || summary.Coverage < policy.MinCoverage:
		summary.Recommendation = RecommendationInsufficient
		summary.Reason = "evaluation coverage or evidence validity is below policy"
	case summary.RegressedCandidates > 0:
		summary.Recommendation = RecommendationRedesign
		summary.Reason = "one or more candidates caused a protected regression"
	case summary.UsefulCandidates == 0:
		summary.Recommendation = RecommendationRemove
		summary.Reason = "no evaluated candidate improved held-out outcomes"
	case summary.UsefulYield < policy.MinUsefulYield:
		summary.Recommendation = RecommendationRedesign
		summary.Reason = "measured useful yield is below policy"
	default:
		summary.Recommendation = RecommendationRetain
		summary.Reason = "held-out useful yield meets policy without protected regressions"
	}
	return summary
}
