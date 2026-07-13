package evolutioneval_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/evolution"
	"github.com/sipeed/picoclaw/pkg/evolutioneval"
)

func TestAuditCorpusReportsContentFreeFailureSignals(t *testing.T) {
	finalOutput := "A sufficiently long prior task result that must not become a reusable procedure."
	records := []evolution.LearningRecord{
		{ID: "task-1", Kind: evolution.RecordKindTask, FinalOutput: finalOutput},
		{ID: "pattern-1", Kind: evolution.RecordKindPattern, TaskRecordIDs: []string{"task-1"}},
	}
	drafts := []evolution.SkillDraft{
		{
			ID: "draft-1", SourceRecordID: "pattern-1", TargetSkillName: "shared-target",
			Status: evolution.DraftStatusCandidate, MatchedSkillRefs: []string{"a", "b", "c", "d"},
			BodyOrPatch: "## Start\nStart from the learned path for x.\n" +
				"Prefer the pattern summarized as x.\n## Procedure\n" + finalOutput,
		},
		{
			ID: "draft-generic", SourceRecordID: "pattern-1", TargetSkillName: "generic-target",
			BodyOrPatch: "## Procedure\nUse it as a direct shortcut instead of replaying old steps.\n" +
				"Follow the source skill guidance below as one compact procedure.\n" +
				"## Expected Result\n" + finalOutput,
		},
		{ID: "draft-2", SourceRecordID: "missing", TargetSkillName: "shared-target", BodyOrPatch: "short"},
	}
	report := evolutioneval.AuditCorpus([]string{"fixture"}, records, drafts)
	if report.Summary.TargetCollisionDrafts != 2 || report.Summary.GenericTemplateDrafts != 2 ||
		report.Summary.CopiedFinalOutputDrafts != 1 || report.Summary.MissingProvenanceDrafts != 1 ||
		report.Summary.ExcessiveSkillRefs != 1 || report.Summary.NoOrderedStepsDrafts != 3 {
		t.Fatalf("summary = %#v", report.Summary)
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) == "" || strings.Contains(string(encoded), finalOutput) {
		t.Fatalf("report leaked content: %s", encoded)
	}
}

func TestLoadAndAuditCorpusReadsJSONLAndDraftArray(t *testing.T) {
	dir := t.TempDir()
	recordPath := filepath.Join(dir, "records.jsonl")
	draftPath := filepath.Join(dir, "drafts.json")
	record := evolution.LearningRecord{
		ID: "pattern-1", Kind: evolution.RecordKindPattern, CreatedAt: time.Now(), Summary: "redacted",
	}
	recordData, _ := json.Marshal(record)
	if err := os.WriteFile(recordPath, append(recordData, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	draftData, _ := json.Marshal([]evolution.SkillDraft{{
		ID: "draft-1", SourceRecordID: "pattern-1", TargetSkillName: "target", BodyOrPatch: "1. Do work",
	}})
	if err := os.WriteFile(draftPath, draftData, 0o600); err != nil {
		t.Fatal(err)
	}
	report, err := evolutioneval.LoadAndAuditCorpus([]string{recordPath}, draftPath)
	if err != nil || report.Summary.Candidates != 1 || report.Summary.MissingProvenanceDrafts != 0 {
		t.Fatalf("report=%#v err=%v", report, err)
	}
}
