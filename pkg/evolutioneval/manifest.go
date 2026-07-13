package evolutioneval

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

const hardMaxManifestBytes = 8 << 20

var safeID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

func LoadManifest(path string) (Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, err
	}
	if len(data) > hardMaxManifestBytes {
		return Manifest{}, errors.New("evolution evaluation manifest exceeds byte limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode evolution evaluation manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Manifest{}, errors.New("decode evolution evaluation manifest: trailing JSON data")
	}
	if err := ValidateManifest(manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func ValidateManifest(manifest Manifest) error {
	if manifest.SchemaVersion != ManifestSchemaV1 {
		return fmt.Errorf("unsupported evolution evaluation manifest %q", manifest.SchemaVersion)
	}
	if strings.TrimSpace(manifest.Source) == "" || !manifest.Sanitized || strings.TrimSpace(manifest.Why) == "" {
		return errors.New("manifest requires source, sanitization attestation, and rationale")
	}
	if err := validatePolicy(manifest.Policy); err != nil {
		return err
	}
	if len(manifest.Candidates) == 0 {
		return errors.New("manifest requires at least one candidate")
	}
	seen := make(map[string]struct{}, len(manifest.Candidates))
	for _, candidate := range manifest.Candidates {
		if !safeID.MatchString(candidate.ID) {
			return fmt.Errorf("candidate id %q is not a safe identifier", candidate.ID)
		}
		if _, exists := seen[candidate.ID]; exists {
			return fmt.Errorf("duplicate candidate id %q", candidate.ID)
		}
		seen[candidate.ID] = struct{}{}
		if len(candidate.Cases) == 0 {
			return fmt.Errorf("candidate %q requires at least one held-out case", candidate.ID)
		}
	}
	return nil
}

func validatePolicy(policy Policy) error {
	if policy.MinTrials < 1 || policy.MinTrials > 100 {
		return errors.New("policy min_trials must be between 1 and 100")
	}
	if policy.MinScoreDelta <= 0 || policy.MinScoreDelta > 1 {
		return errors.New("policy min_score_delta must be in (0,1]")
	}
	if policy.MinUsefulYield <= 0 || policy.MinUsefulYield > 1 ||
		policy.MinCoverage <= 0 || policy.MinCoverage > 1 {
		return errors.New("policy yield and coverage thresholds must be in (0,1]")
	}
	if policy.MinEvaluatedCandidates < 1 {
		return errors.New("policy min_evaluated_candidates must be positive")
	}
	return nil
}
