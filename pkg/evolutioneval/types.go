// Package evolutioneval evaluates self-evolution candidates against paired,
// held-out baseline and candidate observations.
package evolutioneval

const (
	ManifestSchemaV1 = "forgeclaw.evolution_eval_manifest.v1"
	ReportSchemaV1   = "forgeclaw.evolution_eval_report.v1"
)

type Manifest struct {
	SchemaVersion string      `json:"schema_version"`
	Source        string      `json:"source"`
	Sanitized     bool        `json:"sanitized"`
	Why           string      `json:"why"`
	Policy        Policy      `json:"policy"`
	Candidates    []Candidate `json:"candidates"`
}

type Policy struct {
	MinTrials              int     `json:"min_trials"`
	MinScoreDelta          float64 `json:"min_score_delta"`
	MinUsefulYield         float64 `json:"min_useful_yield"`
	MinCoverage            float64 `json:"min_coverage"`
	MinEvaluatedCandidates int     `json:"min_evaluated_candidates"`
}

type Candidate struct {
	ID              string   `json:"id"`
	SourceRecordIDs []string `json:"source_record_ids"`
	Cases           []Case   `json:"cases"`
}

type Case struct {
	ID               string      `json:"id"`
	Source           string      `json:"source"`
	HeldOutRecordIDs []string    `json:"held_out_record_ids,omitempty"`
	Criteria         []Criterion `json:"criteria"`
	Protected        []string    `json:"protected"`
	BaselineTrials   []Trial     `json:"baseline_trials"`
	CandidateTrials  []Trial     `json:"candidate_trials"`
}

type Criterion struct {
	Name     string  `json:"name"`
	Weight   float64 `json:"weight"`
	Required bool    `json:"required,omitempty"`
}

type Trial struct {
	Seed         string          `json:"seed"`
	Criteria     map[string]bool `json:"criteria"`
	Protected    map[string]bool `json:"protected"`
	EvidenceRefs []string        `json:"evidence_refs"`
	ToolCalls    int             `json:"tool_calls,omitempty"`
	Tokens       int             `json:"tokens,omitempty"`
	LatencyMS    int64           `json:"latency_ms,omitempty"`
}

type Status string

const (
	StatusBeneficial           Status = "beneficial"
	StatusNotBeneficial        Status = "not_beneficial"
	StatusRegression           Status = "regression"
	StatusInvalid              Status = "invalid"
	RecommendationRetain       string = "retain_experiment"
	RecommendationRedesign     string = "redesign"
	RecommendationRemove       string = "remove"
	RecommendationInsufficient string = "insufficient_evidence"
)

type Report struct {
	SchemaVersion string            `json:"schema_version"`
	Source        string            `json:"source"`
	Candidates    []CandidateResult `json:"candidates"`
	Summary       Summary           `json:"summary"`
}

type CandidateResult struct {
	ID     string       `json:"id"`
	Status Status       `json:"status"`
	Cases  []CaseResult `json:"cases"`
	Reason string       `json:"reason"`
}

type CaseResult struct {
	ID                     string   `json:"id"`
	Status                 Status   `json:"status"`
	BaselineScore          float64  `json:"baseline_score"`
	CandidateScore         float64  `json:"candidate_score"`
	ScoreDelta             float64  `json:"score_delta"`
	BaselineMeanToolCalls  float64  `json:"baseline_mean_tool_calls"`
	CandidateMeanToolCalls float64  `json:"candidate_mean_tool_calls"`
	ToolCallDelta          float64  `json:"tool_call_delta"`
	BaselineMeanTokens     float64  `json:"baseline_mean_tokens"`
	CandidateMeanTokens    float64  `json:"candidate_mean_tokens"`
	TokenDelta             float64  `json:"token_delta"`
	BaselineMeanLatencyMS  float64  `json:"baseline_mean_latency_ms"`
	CandidateMeanLatencyMS float64  `json:"candidate_mean_latency_ms"`
	LatencyDeltaMS         float64  `json:"latency_delta_ms"`
	ProtectedFailures      []string `json:"protected_failures,omitempty"`
	RequiredRegressions    []string `json:"required_regressions,omitempty"`
	Reason                 string   `json:"reason"`
}

type Summary struct {
	TotalCandidates     int     `json:"total_candidates"`
	EvaluatedCandidates int     `json:"evaluated_candidates"`
	UsefulCandidates    int     `json:"useful_candidates"`
	RegressedCandidates int     `json:"regressed_candidates"`
	InvalidCandidates   int     `json:"invalid_candidates"`
	Coverage            float64 `json:"coverage"`
	UsefulYield         float64 `json:"useful_yield"`
	Recommendation      string  `json:"recommendation"`
	Reason              string  `json:"reason"`
}
