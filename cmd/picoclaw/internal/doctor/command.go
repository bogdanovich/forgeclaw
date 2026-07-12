package doctor

import (
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/sipeed/picoclaw/cmd/picoclaw/internal"
	doctorpkg "github.com/sipeed/picoclaw/pkg/doctor"
)

// ExitError lets the root command preserve doctor's documented exit status
// without terminating from inside Cobra's RunE lifecycle.
type ExitError struct {
	Code int
}

func (e *ExitError) Error() string {
	return fmt.Sprintf("doctor found issues (exit code %d)", e.Code)
}

func NewDoctorCommand() *cobra.Command {
	var configPath string
	var jsonOutput bool
	var strict bool

	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Run read-only configuration safety checks",
		Long: `Run read-only configuration safety checks.

Exit codes:
  0: no error/fail findings; warnings are allowed unless --strict is set
  1: command or config loading error
  2: error/fail findings, or warning findings when --strict is set`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if strings.TrimSpace(configPath) == "" {
				configPath = internal.GetConfigPath()
			}
			report, err := doctorpkg.Run(doctorpkg.Options{ConfigPath: configPath})
			if err != nil {
				return err
			}

			if jsonOutput {
				data, err := doctorpkg.MarshalJSON(report)
				if err != nil {
					return err
				}
				fmt.Fprintln(cmd.OutOrStdout(), string(data))
			} else {
				renderHuman(cmd.OutOrStdout(), report, strict)
			}

			if code := ExitCode(report, strict); code != 0 {
				return &ExitError{Code: code}
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&configPath, "config", "", "Path to config.json (default: active PicoClaw config)")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Emit stable JSON report")
	cmd.Flags().BoolVar(&strict, "strict", false, "Exit non-zero on warning findings")
	return cmd
}

func ExitCode(report *doctorpkg.Report, strict bool) int {
	if report == nil {
		return 1
	}
	if report.Summary.Error > 0 || report.Summary.Fail > 0 {
		return 2
	}
	if strict && report.Summary.Warning > 0 {
		return 2
	}
	return 0
}

func renderHuman(w io.Writer, report *doctorpkg.Report, strict bool) {
	fmt.Fprintf(w, "PicoClaw doctor: %s\n", report.ConfigPath)
	fmt.Fprintf(
		w,
		"Findings: error=%d fail=%d warning=%d info=%d total=%d\n",
		report.Summary.Error,
		report.Summary.Fail,
		report.Summary.Warning,
		report.Summary.Info,
		report.Summary.Total,
	)
	if strict {
		fmt.Fprintln(w, "Strict mode: warnings cause exit code 2")
	}
	if len(report.Findings) == 0 {
		fmt.Fprintln(w, "No findings.")
		return
	}
	for _, finding := range report.Findings {
		fmt.Fprintf(w, "\n[%s] %s %s\n", finding.Severity, finding.ID, finding.Title)
		fmt.Fprintf(w, "  Rationale: %s\n", finding.Rationale)
		fmt.Fprintf(w, "  Remediation: %s\n", finding.Remediation)
		for _, evidence := range finding.Evidence {
			fmt.Fprintf(w, "  Evidence: %s — %s\n", evidence.Path, evidence.Summary)
		}
	}
}
