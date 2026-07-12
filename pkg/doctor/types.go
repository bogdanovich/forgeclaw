package doctor

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/sipeed/picoclaw/pkg/config"
)

type Severity string

const (
	SeverityError   Severity = "error"
	SeverityFail    Severity = "fail"
	SeverityWarning Severity = "warning"
	SeverityInfo    Severity = "info"
)

type Status string

const (
	StatusPass Status = "pass"
	StatusWarn Status = "warn"
	StatusFail Status = "fail"
)

type Evidence struct {
	Path    string `json:"path"`
	Summary string `json:"summary"`
}

type Finding struct {
	ID          string     `json:"id"`
	Severity    Severity   `json:"severity"`
	Status      Status     `json:"status"`
	Title       string     `json:"title"`
	Rationale   string     `json:"rationale"`
	Remediation string     `json:"remediation"`
	Evidence    []Evidence `json:"evidence,omitempty"`
}

type Report struct {
	SchemaVersion string    `json:"schema_version"`
	GeneratedBy   string    `json:"generated_by"`
	ConfigPath    string    `json:"config_path"`
	Summary       Summary   `json:"summary"`
	Findings      []Finding `json:"findings"`
}

type Summary struct {
	Error   int `json:"error"`
	Fail    int `json:"fail"`
	Warning int `json:"warning"`
	Info    int `json:"info"`
	Pass    int `json:"pass"`
	Total   int `json:"total"`
}

type Options struct {
	ConfigPath string
}

type rawDocuments struct {
	ConfigPath      string
	ConfigJSON      []byte
	SecurityPath    string
	SecurityYAML    []byte
	ConfigAvailable bool
}

const SchemaVersion = "doctor.v1"

func Run(opts Options) (*Report, error) {
	configPath := strings.TrimSpace(opts.ConfigPath)
	if configPath == "" {
		return nil, fmt.Errorf("config path is required")
	}

	cfg, err := config.LoadConfigReadOnly(configPath)
	if err != nil {
		return nil, err
	}
	raw, err := readRawDocuments(configPath)
	if err != nil {
		return nil, err
	}

	findings := runChecks(cfg, raw)
	sortFindings(findings)

	report := &Report{
		SchemaVersion: SchemaVersion,
		GeneratedBy:   "picoclaw doctor",
		ConfigPath:    configPath,
		Findings:      findings,
	}
	report.Summary = summarize(findings)
	return report, nil
}

func readRawDocuments(configPath string) (rawDocuments, error) {
	raw := rawDocuments{
		ConfigPath:   configPath,
		SecurityPath: filepath.Join(filepath.Dir(configPath), config.SecurityConfigFile),
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return raw, err
		}
	} else {
		raw.ConfigJSON = data
		raw.ConfigAvailable = true
	}

	securityData, err := os.ReadFile(raw.SecurityPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return raw, err
		}
	} else {
		raw.SecurityYAML = securityData
	}

	return raw, nil
}

func summarize(findings []Finding) Summary {
	var summary Summary
	for _, finding := range findings {
		switch finding.Severity {
		case SeverityError:
			summary.Error++
		case SeverityFail:
			summary.Fail++
		case SeverityWarning:
			summary.Warning++
		case SeverityInfo:
			summary.Info++
		}
		if finding.Status == StatusPass {
			summary.Pass++
		}
	}
	summary.Total = len(findings)
	return summary
}

func sortFindings(findings []Finding) {
	sort.SliceStable(findings, func(i, j int) bool {
		left, right := findings[i], findings[j]
		if severityRank(left.Severity) != severityRank(right.Severity) {
			return severityRank(left.Severity) < severityRank(right.Severity)
		}
		if left.ID != right.ID {
			return left.ID < right.ID
		}
		return firstEvidencePath(left) < firstEvidencePath(right)
	})
}

func severityRank(severity Severity) int {
	switch severity {
	case SeverityError:
		return 0
	case SeverityFail:
		return 1
	case SeverityWarning:
		return 2
	case SeverityInfo:
		return 3
	default:
		return 4
	}
}

func firstEvidencePath(finding Finding) string {
	if len(finding.Evidence) == 0 {
		return ""
	}
	return finding.Evidence[0].Path
}

func MarshalJSON(report *Report) ([]byte, error) {
	return json.MarshalIndent(report, "", "  ")
}

func parseSecurityYAML(data []byte) (*yaml.Node, error) {
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil, nil
	}
	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, err
	}
	return &root, nil
}
