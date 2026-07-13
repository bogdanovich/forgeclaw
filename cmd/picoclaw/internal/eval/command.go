package eval

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/sipeed/picoclaw/pkg/evalevaluator"
	"github.com/sipeed/picoclaw/pkg/evaltrace"
	"github.com/sipeed/picoclaw/pkg/evolutioneval"
)

const outputSchemaV1 = "forgeclaw.eval_report.v1"

var ErrFindings = errors.New("evaluation findings failed")

type commandOutput struct {
	SchemaVersion string                 `json:"schema_version"`
	Reports       []evalevaluator.Report `json:"reports"`
}

func NewEvalCommand() *cobra.Command {
	var jsonOutput bool
	var evaluatorNames []string
	cmd := &cobra.Command{
		Use:   "eval TRACE...",
		Short: "Evaluate captured execution traces",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			evaluators, err := selectedEvaluators(evaluatorNames)
			if err != nil {
				return err
			}
			output := commandOutput{SchemaVersion: outputSchemaV1, Reports: make([]evalevaluator.Report, 0, len(args))}
			failed := false
			for _, path := range args {
				trace, err := loadTrace(path)
				if err != nil {
					return fmt.Errorf("load %s: %w", path, err)
				}
				report, err := evalevaluator.Evaluate(trace, evaluators...)
				if err != nil {
					return fmt.Errorf("evaluate %s: %w", path, err)
				}
				failed = failed || report.Failed > 0 || report.Errors > 0
				output.Reports = append(output.Reports, report)
			}
			if jsonOutput {
				encoder := json.NewEncoder(cmd.OutOrStdout())
				encoder.SetIndent("", "  ")
				if err := encoder.Encode(output); err != nil {
					return err
				}
			} else {
				writeHuman(cmd.OutOrStdout(), output)
			}
			if failed {
				return ErrFindings
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Write stable machine-readable JSON")
	cmd.Flags().StringSliceVar(&evaluatorNames, "evaluator", nil, "Run only the named evaluator (repeatable)")
	cmd.AddCommand(newFixturesCommand())
	cmd.AddCommand(newEvolutionCommand())
	return cmd
}

func selectedEvaluators(names []string) ([]evalevaluator.Evaluator, error) {
	if len(names) == 0 {
		return evalevaluator.DefaultEvaluators(), nil
	}
	result := make([]evalevaluator.Evaluator, 0, len(names))
	seen := make(map[string]bool)
	for _, name := range names {
		name = strings.TrimSpace(name)
		if seen[name] {
			continue
		}
		evaluator, ok := evalevaluator.EvaluatorByName(name)
		if !ok {
			return nil, fmt.Errorf("unknown evaluator %q", name)
		}
		seen[name] = true
		result = append(result, evaluator)
	}
	return result, nil
}

func loadTrace(path string) (evaltrace.Trace, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return evaltrace.Trace{}, err
	}
	if len(data) > evaltrace.HardMaxTraceBytes {
		return evaltrace.Trace{}, errors.New("trace exceeds hard byte limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var trace evaltrace.Trace
	if err := decoder.Decode(&trace); err != nil {
		return evaltrace.Trace{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return evaltrace.Trace{}, errors.New("trailing JSON data")
	}
	if err := evaltrace.Validate(trace); err != nil {
		return evaltrace.Trace{}, err
	}
	return trace, nil
}

func writeHuman(writer io.Writer, output commandOutput) {
	for _, report := range output.Reports {
		fmt.Fprintf(
			writer,
			"%s: %d passed, %d failed, %d errors, %d not evaluable\n",
			report.TraceID,
			report.Passed,
			report.Failed,
			report.Errors,
			report.Skipped,
		)
		for _, finding := range report.Findings {
			if finding.Status == evalevaluator.StatusPass || finding.Status == evalevaluator.StatusNotEvaluable {
				continue
			}
			fmt.Fprintf(writer, "  %s %s: %s\n", finding.Status, finding.Evaluator, finding.Observed)
		}
	}
}

func newFixturesCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "fixtures MANIFEST",
		Short: "Validate deterministic evaluation fixtures",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			manifest, err := evalevaluator.LoadFixtureManifest(args[0])
			if err != nil {
				return err
			}
			failures := make([]string, 0)
			for _, fixture := range manifest.Fixtures {
				evaluator, ok := evalevaluator.EvaluatorByName(fixture.Evaluator)
				if !ok {
					failures = append(failures, fixture.ID+": unknown evaluator")
					continue
				}
				trace, err := fixture.Trace()
				if err != nil {
					failures = append(failures, fixture.ID+": "+err.Error())
					continue
				}
				report, err := evalevaluator.Evaluate(trace, evaluator)
				if err != nil || len(report.Findings) != 1 || report.Findings[0].Status != fixture.Expected {
					failures = append(failures, fixture.ID+": unexpected finding")
				}
			}
			sort.Strings(failures)
			if len(failures) > 0 {
				return fmt.Errorf("fixture validation failed: %s", strings.Join(failures, "; "))
			}
			fmt.Fprintf(cmd.OutOrStdout(), "validated %d fixtures\n", len(manifest.Fixtures))
			return nil
		},
	}
}

func newEvolutionCommand() *cobra.Command {
	var jsonOutput bool
	cmd := &cobra.Command{
		Use:   "evolution MANIFEST",
		Short: "Evaluate self-evolution candidates against paired held-out trials",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			manifest, err := evolutioneval.LoadManifest(args[0])
			if err != nil {
				return err
			}
			report := evolutioneval.Evaluate(manifest)
			if jsonOutput {
				encoder := json.NewEncoder(cmd.OutOrStdout())
				encoder.SetIndent("", "  ")
				return encoder.Encode(report)
			}
			writeEvolutionHuman(cmd.OutOrStdout(), report)
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Write stable machine-readable JSON")
	return cmd
}

func writeEvolutionHuman(writer io.Writer, report evolutioneval.Report) {
	fmt.Fprintf(
		writer,
		"self-evolution: %s (%d/%d evaluated, %d useful, %d regressed, %d invalid)\n",
		report.Summary.Recommendation,
		report.Summary.EvaluatedCandidates,
		report.Summary.TotalCandidates,
		report.Summary.UsefulCandidates,
		report.Summary.RegressedCandidates,
		report.Summary.InvalidCandidates,
	)
	for _, candidate := range report.Candidates {
		fmt.Fprintf(writer, "  %s %s: %s\n", candidate.Status, candidate.ID, candidate.Reason)
	}
}
