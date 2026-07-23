package tasks

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRegistryRestoresLoadedStateWhenStartupPruneWriteFails(t *testing.T) {
	store := filepath.Join(t.TempDir(), "state", "task_registry.json")
	initial := NewRegistry(store)
	if err := initial.Upsert(Record{
		TaskID: "expired", Task: "test", Status: StatusSucceeded,
		DeliveryStatus: DeliveryDelivered,
		EndedAt:        time.Now().Add(-time.Hour).UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(store)
	if err != nil {
		t.Fatal(err)
	}
	var persisted Snapshot
	if err = json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	persisted.Tasks[0].CleanupAfter = time.Now().Add(-time.Minute).UnixMilli()
	data, err = json.MarshalIndent(persisted, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(store, data, 0o600); err != nil {
		t.Fatal(err)
	}
	registry := &Registry{
		store: store,
		options: Options{
			TerminalRetention: time.Millisecond,
			MaxRecords:        DefaultMaxRecords,
			MaxEvents:         DefaultMaxEvents,
			MaxSnapshotBytes:  DefaultMaxSnapshotBytes,
		},
		records: make(map[string]Record),
		events:  make([]TaskEvent, 0),
		writeAtomic: func(string, []byte, os.FileMode) error {
			return errors.New("pre-rename failure")
		},
	}
	if err = registry.load(); err != nil {
		t.Fatal(err)
	}
	registry.pruneLoadedState(time.Now().UnixMilli())
	if registry.LastLoadError() == nil {
		t.Fatal("startup prune persistence failure was not surfaced")
	}
	if _, ok := registry.Get("expired"); !ok {
		t.Fatal("failed startup prune removed in-memory record")
	}
	data, err = os.ReadFile(store)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Tasks) != 1 || snapshot.Tasks[0].TaskID != "expired" {
		t.Fatal("failed startup prune changed durable record")
	}
}

func TestRegistryPersistsAndReloadsRecords(t *testing.T) {
	store := filepath.Join(t.TempDir(), "state", "task_registry.json")

	registry := NewRegistry(store)
	if err := registry.Upsert(Record{
		TaskID:         "subagent-7",
		Runtime:        RuntimeSubagent,
		Task:           "download media",
		Status:         StatusRunning,
		DeliveryStatus: DeliveryPending,
	}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	if err := registry.Update("subagent-7", func(rec *Record) {
		rec.Status = StatusSucceeded
		rec.DeliveryStatus = DeliveryDelivered
		rec.LastCompletionID = "completion-7"
		rec.DeliveredAt = 123
		rec.TerminalSummary = "done"
	}); err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	reloaded := NewRegistry(store)
	rec, ok := reloaded.Get("subagent-7")
	if !ok {
		t.Fatal("expected persisted task after reload")
	}
	if rec.Status != StatusSucceeded {
		t.Fatalf("Status = %q, want %q", rec.Status, StatusSucceeded)
	}
	if rec.DeliveryStatus != DeliveryDelivered {
		t.Fatalf("DeliveryStatus = %q, want %q", rec.DeliveryStatus, DeliveryDelivered)
	}
	if rec.TerminalSummary != "done" {
		t.Fatalf("TerminalSummary = %q, want done", rec.TerminalSummary)
	}
	if rec.LastCompletionID != "completion-7" {
		t.Fatalf("LastCompletionID = %q, want completion-7", rec.LastCompletionID)
	}
	if rec.DeliveredAt != 123 {
		t.Fatalf("DeliveredAt = %d, want 123", rec.DeliveredAt)
	}
}

func TestRegistryOwnsDurableGenerationIdentity(t *testing.T) {
	store := filepath.Join(t.TempDir(), "state", "task_registry.json")
	registry := NewRegistry(store)
	if err := registry.Upsert(Record{
		TaskID: "task-generation", GenerationID: "caller-controlled", Task: "test",
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	record, ok := registry.Get("task-generation")
	if !ok || record.GenerationID == "" || record.GenerationID == "caller-controlled" {
		t.Fatalf("runtime-owned generation = %#v", record)
	}
	generationID := record.GenerationID
	events := registry.ListEvents("task-generation")
	if len(events) != 1 || events[0].GenerationID != generationID {
		t.Fatalf("initial events = %#v", events)
	}
	if !strings.Contains(events[0].EventID, generationID) {
		t.Fatalf("event ID %q does not bind generation %q", events[0].EventID, generationID)
	}
	if err := registry.Update("task-generation", func(record *Record) {
		record.GenerationID = "mutated"
		record.ProgressSummary = "working"
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	record, _ = registry.Get("task-generation")
	if record.GenerationID != generationID {
		t.Fatalf("generation after mutation = %q, want %q", record.GenerationID, generationID)
	}
	reloaded := NewRegistry(store)
	if err := reloaded.LastLoadError(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	record, _ = reloaded.Get("task-generation")
	if record.GenerationID != generationID {
		t.Fatalf("reloaded generation = %q, want %q", record.GenerationID, generationID)
	}
	for _, event := range reloaded.ListEvents("task-generation") {
		if event.GenerationID != generationID {
			t.Fatalf("event generation = %q, want %q", event.GenerationID, generationID)
		}
	}
}

func TestRegistrySeparatesReusedTaskGenerationsAfterEventRetention(t *testing.T) {
	registry := NewRegistryWithOptions("", Options{MaxEvents: 1})
	upsert := func() (Record, TaskEvent) {
		t.Helper()
		if err := registry.Upsert(Record{
			TaskID: "reused", CreatedAt: 1_000, Task: "test",
		}); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		record, _ := registry.Get("reused")
		events := registry.ListEvents("reused")
		return record, events[len(events)-1]
	}
	firstRecord, firstUpsert := upsert()
	if err := registry.AppendEvent("reused", EventTaskProgress, nil); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	secondRecord, secondUpsert := upsert()

	if firstRecord.GenerationID == secondRecord.GenerationID {
		t.Fatalf("reused task generations share ID %q", firstRecord.GenerationID)
	}
	if firstUpsert.EventID == secondUpsert.EventID {
		t.Fatalf("reused task upserts share event ID %q", firstUpsert.EventID)
	}
	if firstUpsert.Seq != 1 || secondUpsert.Seq != 1 {
		t.Fatalf("generation-local sequences = %d, %d; want 1, 1", firstUpsert.Seq, secondUpsert.Seq)
	}
	if secondUpsert.GenerationID != secondRecord.GenerationID {
		t.Fatalf("second upsert generation = %q, want %q", secondUpsert.GenerationID, secondRecord.GenerationID)
	}
}

func TestRegistryRejectsSnapshotsWithoutGenerationIdentity(t *testing.T) {
	store := filepath.Join(t.TempDir(), "task_registry.json")
	data, err := json.Marshal(Snapshot{Tasks: []Record{{TaskID: "legacy"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(store, data, 0o600); err != nil {
		t.Fatal(err)
	}
	registry := NewRegistry(store)
	if err := registry.LastLoadError(); err == nil || !strings.Contains(err.Error(), "generation_id") {
		t.Fatalf("LastLoadError = %v, want missing generation_id", err)
	}
}

func TestRegistryRejectsSnapshotTransactionally(t *testing.T) {
	store := filepath.Join(t.TempDir(), "task_registry.json")
	snapshot := Snapshot{Tasks: []Record{
		{TaskID: "valid-prefix", GenerationID: "generation-valid", LastEventSeq: 1},
		{TaskID: "invalid-suffix"},
	}}
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if writeErr := os.WriteFile(store, data, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}

	registry := NewRegistry(store)
	if loadErr := registry.LastLoadError(); loadErr == nil {
		t.Fatal("LastLoadError = nil, want invalid snapshot")
	}
	if records := registry.List(); len(records) != 0 {
		t.Fatalf("partially published records = %#v", records)
	}
	if upsertErr := registry.Upsert(Record{TaskID: "must-not-overwrite", Task: "test"}); upsertErr == nil ||
		!strings.Contains(upsertErr.Error(), "read-only after load failure") {
		t.Fatalf("Upsert error = %v, want read-only load failure", upsertErr)
	}
	after, err := os.ReadFile(store)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(data) {
		t.Fatal("invalid snapshot was rewritten after partial load")
	}
}

func TestRegistrySequenceSurvivesEventRetentionAndReload(t *testing.T) {
	store := filepath.Join(t.TempDir(), "task_registry.json")
	registry := NewRegistryWithOptions(store, Options{MaxEvents: 1})
	if err := registry.Upsert(Record{TaskID: "retained-generation", Task: "test"}); err != nil {
		t.Fatal(err)
	}
	if err := registry.AppendEvent("retained-generation", EventTaskProgress, nil); err != nil {
		t.Fatal(err)
	}
	before := registry.ListEvents("retained-generation")
	if len(before) != 1 || before[0].Seq != 2 {
		t.Fatalf("events before reload = %#v, want retained sequence 2", before)
	}

	reloaded := NewRegistryWithOptions(store, Options{MaxEvents: 1})
	if err := reloaded.LastLoadError(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if err := reloaded.AppendEvent("retained-generation", EventTaskProgress, nil); err != nil {
		t.Fatal(err)
	}
	after := reloaded.ListEvents("retained-generation")
	if len(after) != 1 || after[0].Seq != 3 {
		t.Fatalf("events after reload = %#v, want retained sequence 3", after)
	}
	if after[0].EventID == before[0].EventID {
		t.Fatalf("event ID reused after retention: %q", after[0].EventID)
	}
	if after[0].Fingerprint == before[0].Fingerprint {
		t.Fatalf("fingerprint reused after retention: %q", after[0].Fingerprint)
	}
	record, _ := reloaded.Get("retained-generation")
	if record.LastEventSeq != 3 {
		t.Fatalf("LastEventSeq = %d, want 3", record.LastEventSeq)
	}
}

func TestRegistryProjectsDurableInteractionLifecycle(t *testing.T) {
	registry := NewRegistry("")
	if err := registry.Upsert(Record{
		TaskID: "task-1", Status: StatusRunning, DeliveryStatus: DeliveryPending,
	}); err != nil {
		t.Fatal(err)
	}

	if err := registry.MarkWaitingForInput(
		"task-1", "interaction-1", "abc123", strings.Repeat("summary ", 100),
	); err != nil {
		t.Fatalf("MarkWaitingForInput() error = %v", err)
	}
	rec, _ := registry.Get("task-1")
	if rec.Status != StatusWaitingForInput || rec.InteractionID != "interaction-1" ||
		rec.InteractionShortID != "abc123" || len([]rune(rec.InteractionSummary)) != 500 {
		t.Fatalf("waiting record = %#v", rec)
	}
	if err := registry.MarkWaitingForInput(
		"task-1", "interaction-2", "other", "other prompt",
	); err == nil {
		t.Fatal("replaced a task's active interaction")
	}
	if err := registry.MarkInteractionRunning("task-1", "interaction-1"); err != nil {
		t.Fatalf("MarkInteractionRunning() error = %v", err)
	}
	rec, _ = registry.Get("task-1")
	if rec.Status != StatusRunning || rec.InteractionID != "interaction-1" ||
		rec.InteractionShortID != "" || rec.InteractionSummary != "" {
		t.Fatalf("resumed record = %#v", rec)
	}
	if err := registry.FinishInteraction(
		"task-1", "interaction-1", StatusTimedOut, "human input timed out",
	); err != nil {
		t.Fatalf("FinishInteraction() error = %v", err)
	}
	rec, _ = registry.Get("task-1")
	if rec.Status != StatusTimedOut || rec.Error != "human input timed out" {
		t.Fatalf("terminal record = %#v", rec)
	}
}

func TestWaitingForInputSurvivesReloadAndActiveReconciliation(t *testing.T) {
	store := filepath.Join(t.TempDir(), "state", "task_registry.json")
	registry := NewRegistry(store)
	if err := registry.Upsert(Record{
		TaskID: "task-waiting", Status: StatusRunning, DeliveryStatus: DeliveryPending,
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.MarkWaitingForInput(
		"task-waiting", "interaction-1", "abc123", "Choose a deployment mode",
	); err != nil {
		t.Fatal(err)
	}

	reloaded := NewRegistry(store)
	active := reloaded.ListActive()
	if len(active) != 1 || active[0].Status != StatusWaitingForInput {
		t.Fatalf("active after reload = %#v", active)
	}
	count, err := reloaded.MarkActiveLost("runtime restarted")
	if err != nil || count != 0 {
		t.Fatalf("MarkActiveLost() = (%d, %v), want waiting task preserved", count, err)
	}
	rec, _ := reloaded.Get("task-waiting")
	if rec.Status != StatusWaitingForInput || rec.InteractionShortID != "abc123" {
		t.Fatalf("record after reconciliation = %#v", rec)
	}
}

func TestCorrelatedResumingTaskSurvivesRestartAndRepairsLostState(t *testing.T) {
	registry := NewRegistry("")
	if err := registry.Upsert(Record{
		TaskID: "task-resuming", Status: StatusRunning,
		DeliveryStatus: DeliveryPending, InteractionID: "interaction-1",
	}); err != nil {
		t.Fatal(err)
	}
	count, err := registry.MarkActiveLost("runtime restarted")
	if err != nil || count != 0 {
		t.Fatalf("MarkActiveLost() = (%d, %v)", count, err)
	}
	rec, _ := registry.Get("task-resuming")
	if rec.Status != StatusRunning {
		t.Fatalf("correlated task status = %q", rec.Status)
	}
	if err := registry.Update("task-resuming", func(rec *Record) {
		rec.Status = StatusLost
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.MarkInteractionRunning("task-resuming", "interaction-1"); err != nil {
		t.Fatalf("MarkInteractionRunning() repair error = %v", err)
	}
	if err := registry.CompleteInteractionTask(
		"task-resuming", "interaction-1", "done", DeliveryDelivered,
	); err != nil {
		t.Fatalf("CompleteInteractionTask() error = %v", err)
	}
	rec, _ = registry.Get("task-resuming")
	if rec.Status != StatusSucceeded || rec.DeliveryStatus != DeliveryDelivered {
		t.Fatalf("repaired task = %#v", rec)
	}
}

func TestRegistryPersistsAndReloadsTaskEvents(t *testing.T) {
	store := filepath.Join(t.TempDir(), "state", "task_registry.json")
	registry := NewRegistry(store)

	if err := registry.Upsert(Record{
		TaskID:         "subagent-7",
		Runtime:        RuntimeSubagent,
		Task:           "download media",
		Status:         StatusRunning,
		DeliveryStatus: DeliveryPending,
	}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	if err := registry.Update("subagent-7", func(rec *Record) {
		rec.Status = StatusSucceeded
		rec.DeliveryStatus = DeliveryDelivered
		rec.ProgressSummary = "done"
	}); err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	reloaded := NewRegistry(store)
	events := reloaded.ListEvents("subagent-7")
	if len(events) != 4 {
		t.Fatalf("event count = %d, want 4: %+v", len(events), events)
	}
	wantTypes := []EventType{
		EventTaskUpserted,
		EventTaskStatusChanged,
		EventTaskDeliveryChanged,
		EventTaskProgress,
	}
	for i, want := range wantTypes {
		if events[i].Type != want {
			t.Fatalf("events[%d].Type = %q, want %q; events=%+v", i, events[i].Type, want, events)
		}
		if events[i].SchemaVersion != TaskEventSchemaVersion {
			t.Fatalf("events[%d].SchemaVersion = %q, want %q", i, events[i].SchemaVersion, TaskEventSchemaVersion)
		}
		if events[i].Seq != int64(i+1) {
			t.Fatalf("events[%d].Seq = %d, want %d", i, events[i].Seq, i+1)
		}
		if events[i].Fingerprint == "" {
			t.Fatalf("events[%d].Fingerprint is empty", i)
		}
	}
	if events[1].Payload["from"] != string(StatusRunning) || events[1].Payload["to"] != string(StatusSucceeded) {
		t.Fatalf("status event payload = %+v", events[1].Payload)
	}
	if events[2].Payload["from"] != string(DeliveryPending) ||
		events[2].Payload["to"] != string(DeliveryDelivered) {
		t.Fatalf("delivery event payload = %+v", events[2].Payload)
	}
	if events[3].Payload["summary"] != "done" {
		t.Fatalf("progress event payload = %+v", events[3].Payload)
	}
}

func TestRegistryListEventsCanReturnAllTasks(t *testing.T) {
	registry := NewRegistry("")
	for _, id := range []string{"task-a", "task-b"} {
		if err := registry.Upsert(Record{TaskID: id, Runtime: RuntimeTool, Task: id}); err != nil {
			t.Fatalf("Upsert(%s) error = %v", id, err)
		}
	}

	events := registry.ListEvents("")
	if len(events) != 2 {
		t.Fatalf("event count = %d, want 2: %+v", len(events), events)
	}
	if events[0].TaskID != "task-a" || events[1].TaskID != "task-b" {
		t.Fatalf("events = %+v, want task-a then task-b", events)
	}
}

func TestRegistryAppendEventPersistsSemanticPayload(t *testing.T) {
	store := filepath.Join(t.TempDir(), "state", "task_registry.json")
	registry := NewRegistry(store)
	if err := registry.Upsert(Record{
		TaskID:         "task-1",
		Runtime:        RuntimeTool,
		Task:           "deliver result",
		Status:         StatusRunning,
		DeliveryStatus: DeliveryPending,
	}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	if err := registry.AppendEvent("task-1", EventTaskDeliveryDecision, map[string]string{
		"mode":          "user_only",
		"completion_id": "completion-1",
	}); err != nil {
		t.Fatalf("AppendEvent() error = %v", err)
	}

	reloaded := NewRegistry(store)
	events := reloaded.ListEvents("task-1")
	if len(events) != 2 {
		t.Fatalf("event count = %d, want 2: %+v", len(events), events)
	}
	if events[1].Type != EventTaskDeliveryDecision {
		t.Fatalf("event type = %q, want %q", events[1].Type, EventTaskDeliveryDecision)
	}
	if events[1].Payload["mode"] != "user_only" ||
		events[1].Payload["completion_id"] != "completion-1" {
		t.Fatalf("event payload = %+v", events[1].Payload)
	}
}

func TestRegistryPersistsDeliverableFields(t *testing.T) {
	store := filepath.Join(t.TempDir(), "state", "task_registry.json")
	registry := NewRegistry(store)

	if err := registry.Upsert(Record{
		TaskID:       "delegate-1",
		Runtime:      RuntimeDelegate,
		TaskKind:     "delegate",
		ParentTaskID: "root-1",
		Task:         "download the reel",
		Status:       StatusSucceeded,
		Deliverable: &DeliverablePayload{
			Text: "video downloaded",
			Artifacts: []DeliverableItem{
				{
					Ref:         "media://video",
					Kind:        "video",
					Filename:    "source.mp4",
					ContentType: "video/mp4",
					Delivered:   true,
				},
			},
			Metadata: map[string]string{"source": "instagram"},
		},
	}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	reloaded := NewRegistry(store)
	rec, ok := reloaded.Get("delegate-1")
	if !ok {
		t.Fatal("expected persisted task after reload")
	}
	if rec.ParentTaskID != "root-1" {
		t.Fatalf("ParentTaskID = %q, want root-1", rec.ParentTaskID)
	}
	if rec.Deliverable == nil || len(rec.Deliverable.Artifacts) != 1 {
		t.Fatalf("unexpected deliverable: %+v", rec.Deliverable)
	}
	if rec.Deliverable.Artifacts[0].Ref != "media://video" {
		t.Fatalf("artifact ref = %q, want media://video", rec.Deliverable.Artifacts[0].Ref)
	}
	if rec.Deliverable.Metadata["source"] != "instagram" {
		t.Fatalf("metadata source = %q, want instagram", rec.Deliverable.Metadata["source"])
	}
	if rec.Deliverable.Report == nil {
		t.Fatal("expected deliverable report projection")
	}
	if rec.Deliverable.Report.SchemaVersion != DeliverableReportV1 {
		t.Fatalf(
			"report schema = %q, want %q",
			rec.Deliverable.Report.SchemaVersion,
			DeliverableReportV1,
		)
	}
	if rec.Deliverable.Report.Summary != "video downloaded" {
		t.Fatalf("report summary = %q, want video downloaded", rec.Deliverable.Report.Summary)
	}
	if rec.Deliverable.Report.ContentHash == "" || rec.Deliverable.Report.ReportID == "" {
		t.Fatalf("report identity not populated: %+v", rec.Deliverable.Report)
	}
	if len(rec.Deliverable.Report.Claims) != 1 || rec.Deliverable.Report.Claims[0].Kind != "fact" {
		t.Fatalf("unexpected report claims: %+v", rec.Deliverable.Report.Claims)
	}
}

func TestRegistryPreservesExplicitDeliverableReport(t *testing.T) {
	registry := NewRegistry("")
	report := &DeliverableReport{
		SchemaVersion: DeliverableReportV1,
		ReportID:      "review-1",
		ContentHash:   "abc123",
		Summary:       "No findings",
		Claims: []ReportClaim{{
			Kind:       "negative_evidence",
			Text:       "No high-confidence issues found",
			Confidence: "high",
			SourceRefs: []string{"diff"},
			Metadata:   map[string]string{"path": "pkg/review.go"},
		}},
		FieldDeltas: []ReportFieldDelta{{
			Field: "review_status",
			To:    "clean",
		}},
		Provenance: map[string]string{"producer": "reviewer"},
		Extra: map[string]any{
			"nested": map[string]any{"key": "value"},
		},
	}
	if err := registry.Upsert(Record{
		TaskID: "delegate-1",
		Task:   "review code",
		Deliverable: &DeliverablePayload{
			Text:   "reviewed",
			Report: report,
		},
	}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	report.ReportID = "mutated"
	report.Claims[0].Kind = "fact"
	report.Claims[0].SourceRefs[0] = "mutated"
	report.Claims[0].Metadata["path"] = "mutated"
	report.FieldDeltas[0].To = "mutated"
	report.Provenance["producer"] = "mutated"
	report.Extra["nested"].(map[string]any)["key"] = "mutated"

	rec, ok := registry.Get("delegate-1")
	if !ok {
		t.Fatal("expected task")
	}
	storedReport := rec.Deliverable.Report
	if storedReport.ReportID != "review-1" || storedReport.ContentHash != "abc123" {
		t.Fatalf("explicit report identity changed: %+v", storedReport)
	}
	if storedReport.GeneratedAt == 0 {
		t.Fatalf("expected GeneratedAt to be filled: %+v", storedReport)
	}
	if storedReport.Provenance["producer"] != "reviewer" {
		t.Fatalf("explicit provenance lost: %+v", storedReport.Provenance)
	}
	if storedReport.Claims[0].SourceRefs[0] != "diff" {
		t.Fatalf("source refs aliased: %+v", storedReport.Claims[0].SourceRefs)
	}
	if storedReport.Claims[0].Metadata["path"] != "pkg/review.go" {
		t.Fatalf("claim metadata aliased: %+v", storedReport.Claims[0].Metadata)
	}
	if storedReport.FieldDeltas[0].To != "clean" {
		t.Fatalf("field deltas aliased: %+v", storedReport.FieldDeltas)
	}
	nested := storedReport.Extra["nested"].(map[string]any)
	if nested["key"] != "value" {
		t.Fatalf("extra map aliased: %+v", storedReport.Extra)
	}
}

func TestRegistryMaxNumericSuffix(t *testing.T) {
	registry := NewRegistry("")
	for _, id := range []string{"subagent-2", "subagent-10", "other-99"} {
		if err := registry.Upsert(Record{TaskID: id, Runtime: RuntimeSubagent, Task: "t"}); err != nil {
			t.Fatalf("Upsert(%s) error = %v", id, err)
		}
	}

	if got := registry.MaxNumericSuffix("subagent-"); got != 10 {
		t.Fatalf("MaxNumericSuffix() = %d, want 10", got)
	}
}

func TestRegistryStampsCleanupAfterForTerminalTasks(t *testing.T) {
	registry := NewRegistryWithOptions("", Options{TerminalRetention: time.Hour})
	endedAt := time.Now().UnixMilli()
	if err := registry.Upsert(Record{
		TaskID:         "task-1",
		Runtime:        RuntimeSubagent,
		Task:           "done",
		Status:         StatusSucceeded,
		DeliveryStatus: DeliveryDelivered,
		EndedAt:        endedAt,
	}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	rec, ok := registry.Get("task-1")
	if !ok {
		t.Fatal("expected task")
	}
	if rec.CleanupAfter != endedAt+int64(time.Hour/time.Millisecond) {
		t.Fatalf("CleanupAfter = %d, want %d", rec.CleanupAfter, endedAt+int64(time.Hour/time.Millisecond))
	}
}

func TestRegistryPrunesExpiredTerminalTasks(t *testing.T) {
	store := filepath.Join(t.TempDir(), "state", "task_registry.json")
	registry := NewRegistryWithOptions(store, Options{TerminalRetention: time.Millisecond})

	if err := registry.Upsert(Record{
		TaskID:         "old-done",
		Runtime:        RuntimeSubagent,
		Task:           "old",
		Status:         StatusSucceeded,
		DeliveryStatus: DeliveryDelivered,
		EndedAt:        time.Now().Add(-time.Hour).UnixMilli(),
	}); err != nil {
		t.Fatalf("Upsert(old) error = %v", err)
	}
	if err := registry.Upsert(Record{
		TaskID:         "active",
		Runtime:        RuntimeSubagent,
		Task:           "active",
		Status:         StatusRunning,
		DeliveryStatus: DeliveryPending,
		CreatedAt:      time.Now().Add(-time.Hour).UnixMilli(),
	}); err != nil {
		t.Fatalf("Upsert(active) error = %v", err)
	}

	if _, ok := registry.Get("old-done"); ok {
		t.Fatal("expected expired terminal task to be pruned")
	}
	if _, ok := registry.Get("active"); !ok {
		t.Fatal("expected active task to be preserved")
	}
}

func TestRegistryPrunesOldestTerminalTasksAboveMaxRecords(t *testing.T) {
	registry := NewRegistryWithOptions("", Options{
		TerminalRetention: 24 * time.Hour,
		MaxRecords:        3,
	})

	records := []Record{
		{TaskID: "active-1", Status: StatusRunning, CreatedAt: time.Now().Add(-4 * time.Minute).UnixMilli()},
		{TaskID: "active-2", Status: StatusRunning, CreatedAt: time.Now().Add(-3 * time.Minute).UnixMilli()},
		{
			TaskID:    "old-terminal",
			Status:    StatusSucceeded,
			CreatedAt: time.Now().Add(-2 * time.Minute).UnixMilli(),
			EndedAt:   time.Now().Add(-2 * time.Minute).UnixMilli(),
		},
		{
			TaskID:    "new-terminal",
			Status:    StatusSucceeded,
			CreatedAt: time.Now().Add(-time.Minute).UnixMilli(),
			EndedAt:   time.Now().Add(-time.Minute).UnixMilli(),
		},
	}
	for _, rec := range records {
		rec.Runtime = RuntimeSubagent
		rec.Task = rec.TaskID
		if isTerminalStatus(rec.Status) {
			rec.DeliveryStatus = DeliveryDelivered
		} else {
			rec.DeliveryStatus = DeliveryPending
		}
		if err := registry.Upsert(rec); err != nil {
			t.Fatalf("Upsert(%s) error = %v", rec.TaskID, err)
		}
	}

	if _, ok := registry.Get("old-terminal"); ok {
		t.Fatal("expected oldest terminal task to be pruned")
	}
	for _, id := range []string{"active-1", "active-2", "new-terminal"} {
		if _, ok := registry.Get(id); !ok {
			t.Fatalf("expected %s to be preserved", id)
		}
	}
}

func TestRegistryPreservesPendingTerminalTasksAboveMaxRecords(t *testing.T) {
	registry := NewRegistryWithOptions("", Options{MaxRecords: 1})
	for _, rec := range []Record{
		{TaskID: "pending-done", Status: StatusSucceeded, DeliveryStatus: DeliveryPending},
		{TaskID: "running", Status: StatusRunning, DeliveryStatus: DeliveryPending},
	} {
		rec.Runtime = RuntimeSubagent
		rec.Task = rec.TaskID
		if err := registry.Upsert(rec); err != nil {
			t.Fatalf("Upsert(%s) error = %v", rec.TaskID, err)
		}
	}
	for _, id := range []string{"pending-done", "running"} {
		if _, ok := registry.Get(id); !ok {
			t.Fatalf("expected protected record %q to be preserved", id)
		}
	}
}

func TestRegistryPrunesSnapshotBytesWithoutDroppingProtectedTasks(t *testing.T) {
	store := filepath.Join(t.TempDir(), "state", "task_registry.json")
	registry := NewRegistryWithOptions(store, Options{
		MaxRecords:       10,
		MaxEvents:        10,
		MaxSnapshotBytes: 200,
	})
	for _, rec := range []Record{
		{
			TaskID:         "running",
			Runtime:        RuntimeTool,
			Task:           strings.Repeat("running task ", 30),
			Status:         StatusRunning,
			DeliveryStatus: DeliveryPending,
		},
		{
			TaskID:         "delivered-terminal",
			Runtime:        RuntimeTool,
			Task:           strings.Repeat("terminal task ", 50),
			Status:         StatusSucceeded,
			DeliveryStatus: DeliveryDelivered,
			Deliverable:    &DeliverablePayload{Text: strings.Repeat("deliverable ", 80)},
		},
	} {
		if err := registry.Upsert(rec); err != nil {
			t.Fatalf("Upsert(%s) error = %v", rec.TaskID, err)
		}
	}
	if _, ok := registry.Get("running"); !ok {
		t.Fatal("running task must be preserved")
	}
	if _, ok := registry.Get("delivered-terminal"); ok {
		t.Fatal("delivered terminal task should be pruned to satisfy byte budget")
	}
	data, err := os.ReadFile(store)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) <= 200 {
		t.Fatal("protected running task should be allowed to exceed the byte budget")
	}
}

func TestRegistryPrunesEventsBelowMaxRecordLimit(t *testing.T) {
	store := filepath.Join(t.TempDir(), "state", "task_registry.json")
	registry := NewRegistryWithOptions(store, Options{MaxRecords: 10, MaxEvents: 2})
	if err := registry.Upsert(Record{
		TaskID:         "task-1",
		Runtime:        RuntimeTool,
		Task:           "test event retention",
		Status:         StatusRunning,
		DeliveryStatus: DeliveryPending,
	}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	for _, eventType := range []EventType{EventTaskProgress, EventTaskUpdated, EventTaskReconciled} {
		if err := registry.AppendEvent("task-1", eventType, nil); err != nil {
			t.Fatalf("AppendEvent(%s) error = %v", eventType, err)
		}
	}

	data, err := os.ReadFile(store)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Tasks) != 1 || snapshot.Tasks[0].TaskID != "task-1" {
		t.Fatalf("tasks = %#v", snapshot.Tasks)
	}
	if len(snapshot.Events) != 2 {
		t.Fatalf("events = %d, want 2: %#v", len(snapshot.Events), snapshot.Events)
	}
	if snapshot.Events[0].Type != EventTaskUpdated || snapshot.Events[1].Type != EventTaskReconciled {
		t.Fatalf("retained events = %#v", snapshot.Events)
	}
}

func TestRegistryPrunesOrphanEventsWhenSnapshotHasNoTasks(t *testing.T) {
	store := filepath.Join(t.TempDir(), "state", "task_registry.json")
	snapshot := Snapshot{Events: []TaskEvent{{
		EventID:       "event-orphan",
		SchemaVersion: TaskEventSchemaVersion,
		TaskID:        "deleted-task",
		GenerationID:  "generation-orphan",
		Type:          EventTaskUpdated,
		EmittedAt:     time.Now().UnixMilli(),
	}}}
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Dir(store), 0o700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(store, data, 0o600); err != nil {
		t.Fatal(err)
	}

	reloaded := NewRegistry(store)
	if events := reloaded.ListEvents(""); len(events) != 0 {
		t.Fatalf("orphan events = %#v, want none", events)
	}
	data, err = os.ReadFile(store)
	if err != nil {
		t.Fatal(err)
	}
	snapshot = Snapshot{}
	if err := json.Unmarshal(data, &snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Events) != 0 {
		t.Fatalf("persisted orphan events = %#v, want none", snapshot.Events)
	}
}

func TestRegistryListPendingTerminalDelivery(t *testing.T) {
	registry := NewRegistry("")
	records := []Record{
		{TaskID: "pending-done", Status: StatusSucceeded, DeliveryStatus: DeliveryPending},
		{TaskID: "pending-failed", Status: StatusFailed, DeliveryStatus: DeliveryPending},
		{TaskID: "pending-running", Status: StatusRunning, DeliveryStatus: DeliveryPending},
		{TaskID: "delivered-done", Status: StatusSucceeded, DeliveryStatus: DeliveryDelivered},
	}
	for _, rec := range records {
		rec.Runtime = RuntimeSubagent
		rec.Task = rec.TaskID
		if err := registry.Upsert(rec); err != nil {
			t.Fatalf("Upsert(%s) error = %v", rec.TaskID, err)
		}
	}

	got := registry.ListPendingTerminalDelivery()
	if len(got) != 2 {
		t.Fatalf("pending terminal count = %d, want 2: %+v", len(got), got)
	}
	if got[0].TaskID != "pending-done" || got[1].TaskID != "pending-failed" {
		t.Fatalf(
			"pending terminal tasks = %v, want pending-done,pending-failed",
			[]string{got[0].TaskID, got[1].TaskID},
		)
	}
}

func TestRegistryListActive(t *testing.T) {
	registry := NewRegistry("")
	for _, rec := range []Record{
		{TaskID: "queued", Status: StatusQueued},
		{TaskID: "running", Status: StatusRunning},
		{TaskID: "done", Status: StatusSucceeded},
		{TaskID: "lost", Status: StatusLost},
	} {
		rec.Runtime = RuntimeTool
		rec.Task = rec.TaskID
		if err := registry.Upsert(rec); err != nil {
			t.Fatalf("Upsert(%s) error = %v", rec.TaskID, err)
		}
	}

	got := registry.ListActive()
	if len(got) != 2 {
		t.Fatalf("active count = %d, want 2: %+v", len(got), got)
	}
	if got[0].TaskID != "queued" || got[1].TaskID != "running" {
		t.Fatalf("active tasks = %+v, want queued,running", got)
	}
}

func TestRegistryMarkStaleActiveLost(t *testing.T) {
	registry := NewRegistry("")
	old := time.Now().Add(-time.Hour).UnixMilli()
	recent := time.Now().UnixMilli()
	for _, rec := range []Record{
		{TaskID: "old-running", Status: StatusRunning, CreatedAt: old, LastEventAt: old},
		{TaskID: "recent-running", Status: StatusRunning, CreatedAt: recent, LastEventAt: recent},
		{TaskID: "done", Status: StatusSucceeded, CreatedAt: old, LastEventAt: old},
	} {
		rec.Runtime = RuntimeTool
		rec.Task = rec.TaskID
		rec.DeliveryStatus = DeliveryPending
		if err := registry.Upsert(rec); err != nil {
			t.Fatalf("Upsert(%s) error = %v", rec.TaskID, err)
		}
	}

	count, err := registry.MarkStaleActiveLost(30*time.Minute, "stale owner")
	if err != nil {
		t.Fatalf("MarkStaleActiveLost() error = %v", err)
	}
	if count != 1 {
		t.Fatalf("changed count = %d, want 1", count)
	}
	oldRec, _ := registry.Get("old-running")
	if oldRec.Status != StatusLost {
		t.Fatalf("old-running status = %q, want lost", oldRec.Status)
	}
	if oldRec.DeliveryStatus != DeliveryNotApplicable {
		t.Fatalf("old-running delivery = %q, want not_applicable", oldRec.DeliveryStatus)
	}
	if oldRec.Error != "stale owner" {
		t.Fatalf("old-running error = %q, want stale owner", oldRec.Error)
	}
	recentRec, _ := registry.Get("recent-running")
	if recentRec.Status != StatusRunning {
		t.Fatalf("recent-running status = %q, want running", recentRec.Status)
	}
	doneRec, _ := registry.Get("done")
	if doneRec.Status != StatusSucceeded {
		t.Fatalf("done status = %q, want succeeded", doneRec.Status)
	}
}

func TestRegistryHeartbeatUpdatesOnlyActiveTasks(t *testing.T) {
	registry := NewRegistry("")
	old := time.Now().Add(-time.Hour).UnixMilli()
	for _, rec := range []Record{
		{
			TaskID:         "running",
			Runtime:        RuntimeDelegate,
			Task:           "running task",
			Status:         StatusRunning,
			DeliveryStatus: DeliveryPending,
			CreatedAt:      old,
			LastEventAt:    old,
		},
		{
			TaskID:         "done",
			Runtime:        RuntimeDelegate,
			Task:           "done task",
			Status:         StatusSucceeded,
			DeliveryStatus: DeliveryDelivered,
			CreatedAt:      old,
			LastEventAt:    old,
			EndedAt:        old,
		},
	} {
		if err := registry.Upsert(rec); err != nil {
			t.Fatalf("Upsert(%s) error = %v", rec.TaskID, err)
		}
	}

	if err := registry.Heartbeat("running", "still working"); err != nil {
		t.Fatalf("Heartbeat(running) error = %v", err)
	}
	if err := registry.Heartbeat("done", "should not change"); err != nil {
		t.Fatalf("Heartbeat(done) error = %v", err)
	}

	running, _ := registry.Get("running")
	if running.LastEventAt <= old {
		t.Fatalf("running LastEventAt = %d, want > %d", running.LastEventAt, old)
	}
	if running.ProgressSummary != "still working" {
		t.Fatalf("running ProgressSummary = %q, want still working", running.ProgressSummary)
	}
	done, _ := registry.Get("done")
	if done.LastEventAt != old {
		t.Fatalf("done LastEventAt = %d, want unchanged %d", done.LastEventAt, old)
	}
	if done.ProgressSummary != "" {
		t.Fatalf("done ProgressSummary = %q, want empty", done.ProgressSummary)
	}
}

func TestRegistryMarkActiveLost(t *testing.T) {
	registry := NewRegistry("")
	now := time.Now().UnixMilli()
	for _, rec := range []Record{
		{TaskID: "queued", Status: StatusQueued, CreatedAt: now, LastEventAt: now},
		{TaskID: "running", Status: StatusRunning, CreatedAt: now, LastEventAt: now},
		{TaskID: "done", Status: StatusSucceeded, CreatedAt: now, LastEventAt: now},
	} {
		rec.Runtime = RuntimeTool
		rec.Task = rec.TaskID
		rec.DeliveryStatus = DeliveryPending
		if err := registry.Upsert(rec); err != nil {
			t.Fatalf("Upsert(%s) error = %v", rec.TaskID, err)
		}
	}

	count, err := registry.MarkActiveLost("runtime restarted")
	if err != nil {
		t.Fatalf("MarkActiveLost() error = %v", err)
	}
	if count != 2 {
		t.Fatalf("changed count = %d, want 2", count)
	}
	for _, id := range []string{"queued", "running"} {
		rec, _ := registry.Get(id)
		if rec.Status != StatusLost {
			t.Fatalf("%s status = %q, want lost", id, rec.Status)
		}
		if rec.DeliveryStatus != DeliveryNotApplicable {
			t.Fatalf("%s delivery = %q, want not_applicable", id, rec.DeliveryStatus)
		}
		if rec.Error != "runtime restarted" {
			t.Fatalf("%s error = %q, want runtime restarted", id, rec.Error)
		}
	}
	done, _ := registry.Get("done")
	if done.Status != StatusSucceeded {
		t.Fatalf("done status = %q, want succeeded", done.Status)
	}
}

func TestRegistryMarkActiveLostEmitsTransitionEvents(t *testing.T) {
	registry := NewRegistry("")
	if err := registry.Upsert(Record{
		TaskID:         "running",
		Runtime:        RuntimeSubagent,
		Task:           "running task",
		Status:         StatusRunning,
		DeliveryStatus: DeliveryPending,
	}); err != nil {
		t.Fatalf("Upsert(running) error = %v", err)
	}

	changed, err := registry.MarkActiveLost("runtime restarted")
	if err != nil {
		t.Fatalf("MarkActiveLost error = %v", err)
	}
	if changed != 1 {
		t.Fatalf("changed = %d, want 1", changed)
	}
	events := registry.ListEvents("running")
	wantTypes := []EventType{
		EventTaskUpserted,
		EventTaskStatusChanged,
		EventTaskDeliveryChanged,
		EventTaskReconciled,
	}
	if len(events) != len(wantTypes) {
		t.Fatalf("event count = %d, want %d: %+v", len(events), len(wantTypes), events)
	}
	for i, want := range wantTypes {
		if events[i].Type != want {
			t.Fatalf("events[%d].Type = %q, want %q: %+v", i, events[i].Type, want, events)
		}
	}
	if events[3].Payload["reason"] != "runtime restarted" {
		t.Fatalf("reconciled payload = %+v", events[3].Payload)
	}
}
