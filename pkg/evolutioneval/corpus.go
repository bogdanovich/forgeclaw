package evolutioneval

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/sipeed/picoclaw/pkg/evolution"
)

const (
	CorpusReportSchemaV1 = "forgeclaw.evolution_corpus_report.v1"
	maxCorpusFileBytes   = 64 << 20
	maxCorpusLineBytes   = 2 << 20
	oversizedDraftChars  = 5000
)

var orderedStep = regexp.MustCompile(`(?m)^\s*(?:[0-9]+[.)]|[-*]\s+\[[ xX]\])\s+`)

type CorpusReport struct {
	SchemaVersion string           `json:"schema_version"`
	Sources       []string         `json:"sources"`
	Summary       CorpusSummary    `json:"summary"`
	Candidates    []CandidateAudit `json:"candidates"`
}

type CorpusSummary struct {
	Records                 int `json:"records"`
	Candidates              int `json:"candidates"`
	UniqueTargets           int `json:"unique_targets"`
	TargetCollisionDrafts   int `json:"target_collision_drafts"`
	GenericTemplateDrafts   int `json:"generic_template_drafts"`
	CopiedFinalOutputDrafts int `json:"copied_final_output_drafts"`
	OversizedDrafts         int `json:"oversized_drafts"`
	MissingProvenanceDrafts int `json:"missing_provenance_drafts"`
	NoOrderedStepsDrafts    int `json:"no_ordered_steps_drafts"`
	ExcessiveSkillRefs      int `json:"excessive_skill_refs_drafts"`
}

type CandidateAudit struct {
	ID          string   `json:"id"`
	Target      string   `json:"target"`
	Status      string   `json:"status"`
	SignalCodes []string `json:"signal_codes,omitempty"`
}

func LoadAndAuditCorpus(recordPaths []string, draftPath string) (CorpusReport, error) {
	if len(recordPaths) == 0 || strings.TrimSpace(draftPath) == "" {
		return CorpusReport{}, errors.New("record paths and draft path are required")
	}
	records := make([]evolution.LearningRecord, 0)
	for _, path := range recordPaths {
		loaded, err := loadRecordFile(path)
		if err != nil {
			return CorpusReport{}, fmt.Errorf("load records %s: %w", path, err)
		}
		records = append(records, loaded...)
	}
	drafts, err := loadDraftFile(draftPath)
	if err != nil {
		return CorpusReport{}, fmt.Errorf("load drafts %s: %w", draftPath, err)
	}
	sources := append([]string(nil), recordPaths...)
	sources = append(sources, draftPath)
	return AuditCorpus(sources, records, drafts), nil
}

func AuditCorpus(
	sources []string,
	records []evolution.LearningRecord,
	drafts []evolution.SkillDraft,
) CorpusReport {
	recordByID := make(map[string]evolution.LearningRecord, len(records))
	for _, record := range records {
		recordByID[record.ID] = record
	}
	targetCounts := make(map[string]int)
	for _, draft := range drafts {
		targetCounts[draft.TargetSkillName]++
	}

	report := CorpusReport{
		SchemaVersion: CorpusReportSchemaV1,
		Sources:       append([]string(nil), sources...),
		Summary: CorpusSummary{
			Records: len(records), Candidates: len(drafts), UniqueTargets: len(targetCounts),
		},
		Candidates: make([]CandidateAudit, 0, len(drafts)),
	}
	for _, draft := range drafts {
		signals := auditDraft(draft, recordByID, targetCounts)
		candidate := CandidateAudit{
			ID: draft.ID, Target: draft.TargetSkillName, Status: string(draft.Status), SignalCodes: signals,
		}
		report.Candidates = append(report.Candidates, candidate)
		for _, signal := range signals {
			switch signal {
			case "target_collision":
				report.Summary.TargetCollisionDrafts++
			case "generic_shortcut_template":
				report.Summary.GenericTemplateDrafts++
			case "copies_prior_final_output":
				report.Summary.CopiedFinalOutputDrafts++
			case "oversized_body":
				report.Summary.OversizedDrafts++
			case "missing_provenance":
				report.Summary.MissingProvenanceDrafts++
			case "no_ordered_steps":
				report.Summary.NoOrderedStepsDrafts++
			case "excessive_skill_refs":
				report.Summary.ExcessiveSkillRefs++
			}
		}
	}
	sort.Slice(report.Candidates, func(i, j int) bool { return report.Candidates[i].ID < report.Candidates[j].ID })
	return report
}

func auditDraft(
	draft evolution.SkillDraft,
	recordByID map[string]evolution.LearningRecord,
	targetCounts map[string]int,
) []string {
	signals := make([]string, 0, 7)
	if targetCounts[draft.TargetSkillName] > 1 {
		signals = append(signals, "target_collision")
	}
	body := strings.TrimSpace(draft.BodyOrPatch)
	procedure := markdownSection(body, "Procedure")
	if (strings.Contains(body, "Start from the learned path for") &&
		strings.Contains(body, "Prefer the pattern summarized as")) ||
		(strings.Contains(body, "as a direct shortcut instead of replaying") &&
			strings.Contains(body, "Follow the source skill guidance below as one compact procedure")) {
		signals = append(signals, "generic_shortcut_template")
	}
	if len([]rune(body)) > oversizedDraftChars {
		signals = append(signals, "oversized_body")
	}
	if !orderedStep.MatchString(procedure) {
		signals = append(signals, "no_ordered_steps")
	}
	if len(draft.MatchedSkillRefs) > 3 {
		signals = append(signals, "excessive_skill_refs")
	}
	source, exists := recordByID[draft.SourceRecordID]
	if draft.SourceRecordID == "" || !exists {
		signals = append(signals, "missing_provenance")
	} else if strings.Contains(procedure, "Use the same operation demonstrated by the source task result:") ||
		copiesFinalOutput(procedure, source, recordByID) {
		signals = append(signals, "copies_prior_final_output")
	}
	sort.Strings(signals)
	return signals
}

func markdownSection(body, heading string) string {
	marker := "## " + heading
	start := strings.Index(body, marker)
	if start < 0 {
		return ""
	}
	section := body[start+len(marker):]
	if end := strings.Index(section, "\n## "); end >= 0 {
		section = section[:end]
	}
	return strings.TrimSpace(section)
}

func copiesFinalOutput(
	body string,
	source evolution.LearningRecord,
	recordByID map[string]evolution.LearningRecord,
) bool {
	if containsNormalized(body, source.FinalOutput) {
		return true
	}
	ids := append(append([]string(nil), source.TaskRecordIDs...), source.SourceRecordIDs...)
	for _, id := range ids {
		if record, exists := recordByID[id]; exists && containsNormalized(body, record.FinalOutput) {
			return true
		}
	}
	return false
}

func containsNormalized(body, excerpt string) bool {
	body = strings.Join(strings.Fields(body), " ")
	excerpt = strings.Join(strings.Fields(excerpt), " ")
	return len([]rune(excerpt)) >= 40 && strings.Contains(body, excerpt)
}

func loadRecordFile(path string) ([]evolution.LearningRecord, error) {
	file, err := openBounded(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), maxCorpusLineBytes)
	records := make([]evolution.LearningRecord, 0)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var record evolution.LearningRecord
		if err := json.Unmarshal(line, &record); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func loadDraftFile(path string) ([]evolution.SkillDraft, error) {
	file, err := openBounded(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	var drafts []evolution.SkillDraft
	if err := decoder.Decode(&drafts); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errors.New("draft file has trailing JSON data")
	}
	return drafts, nil
}

func openBounded(path string) (*os.File, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("corpus input must be a regular file")
	}
	if info.Size() > maxCorpusFileBytes {
		return nil, errors.New("corpus input exceeds byte limit")
	}
	return os.Open(path)
}
