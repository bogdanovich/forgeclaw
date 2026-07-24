package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/evalcapture"
	"github.com/sipeed/picoclaw/pkg/evaltrace"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	taskregistry "github.com/sipeed/picoclaw/pkg/tasks"
)

func TestTaskTraceProjectorSeparatesIdenticalIDsAcrossWorkspaces(t *testing.T) {
	workspaceA, workspaceB := t.TempDir(), t.TempDir()
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(traceTestConfig(workspaceA), eventBus)
	t.Cleanup(func() {
		manager.close()
		_ = eventBus.Close()
	})
	registryA := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspaceA))
	registryB := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspaceB))
	manager.attachTaskRegistry(workspaceA, registryA)
	manager.attachTaskRegistry(workspaceB, registryB)

	finishTaskForTrace(t, registryA, "shared-task", "session-a", 0)
	finishTaskForTrace(t, registryB, "shared-task", "session-b", 0)

	traceA := readCapturedTrace(t, waitForTraceFile(t, workspaceA))
	traceB := readCapturedTrace(t, waitForTraceFile(t, workspaceB))
	if traceA.TraceID == traceB.TraceID {
		t.Fatalf("workspace traces share id %q", traceA.TraceID)
	}
	if traceA.Metadata.SessionHash == traceB.Metadata.SessionHash {
		t.Fatalf("workspace traces share session hash %q", traceA.Metadata.SessionHash)
	}
}

func TestTaskTraceProjectorReconcilesTerminalSnapshotWithoutNewEvent(t *testing.T) {
	workspace := t.TempDir()
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	finishTaskForTrace(t, registry, "terminal-before-attach", "session", 0)

	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(traceTestConfig(workspace), eventBus)
	t.Cleanup(func() {
		manager.close()
		_ = eventBus.Close()
	})
	manager.attachTaskRegistry(workspace, registry)

	trace := readCapturedTrace(t, waitForTraceFile(t, workspace))
	if trace.Outcome == nil || trace.Outcome.Status != string(taskregistry.StatusSucceeded) {
		t.Fatalf("snapshot outcome = %#v", trace.Outcome)
	}
	if len(trace.Records) != int(registryRecord(t, registry, "terminal-before-attach").LastEventSeq) {
		t.Fatalf("snapshot trace records = %d", len(trace.Records))
	}
}

func TestTaskTraceProjectorDoesNotDowngradeCompleteTraceOnStartup(t *testing.T) {
	workspace := t.TempDir()
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(traceTestConfig(workspace), eventBus)
	manager.attachTaskRegistry(workspace, registry)
	record := finishTaskForTrace(t, registry, "durable", "session", 0)
	path := waitForTraceFile(t, workspace)
	complete := readCapturedTrace(t, path)
	manager.close()

	pruned := taskregistry.NewRegistryWithOptions(
		taskregistry.WorkspaceStorePath(workspace),
		taskregistry.Options{MaxEvents: 1},
	)
	if got := registryRecord(t, pruned, "durable"); got.GenerationID != record.GenerationID {
		t.Fatalf("reloaded generation = %q, want %q", got.GenerationID, record.GenerationID)
	}
	restarted := newTraceCaptureManager(traceTestConfig(workspace), eventBus)
	restarted.attachTaskRegistry(workspace, pruned)
	restarted.close()
	t.Cleanup(func() { _ = eventBus.Close() })

	after := readCapturedTrace(t, path)
	if after.Truncation.Incomplete {
		t.Fatalf("complete trace was downgraded: %+v", after.Truncation)
	}
	if len(after.Records) != len(complete.Records) {
		t.Fatalf("records after restart = %d, want %d", len(after.Records), len(complete.Records))
	}
}

func TestTaskTraceProjectorReplacesCorruptStoredTrace(t *testing.T) {
	workspace := t.TempDir()
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	record := finishTaskForTrace(t, registry, "corrupt", "session", 0)
	settings := traceCaptureSettingsFromConfig(traceTestConfig(workspace))
	state := newTaskTraceState(
		settings,
		workspace,
		record,
		firstTaskEvent(registry.ListEvents("corrupt")),
	)
	root := traceStoreRoot(settings, workspace)
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, state.trace.builder.TraceID()+".json")
	if err := os.WriteFile(path, []byte(`{"truncated":`), 0o600); err != nil {
		t.Fatal(err)
	}

	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(traceTestConfig(workspace), eventBus)
	manager.attachTaskRegistry(workspace, registry)
	manager.close()
	t.Cleanup(func() { _ = eventBus.Close() })

	recovered := readCapturedTrace(t, path)
	if recovered.TraceID != state.trace.builder.TraceID() ||
		recovered.Truncation.Incomplete {
		t.Fatalf("recovered trace = %#v", recovered)
	}
	if len(recovered.Records) != int(record.LastEventSeq) {
		t.Fatalf("recovered records = %d, want %d", len(recovered.Records), record.LastEventSeq)
	}
}

func TestTaskTraceReconciliationExtendsPersistedFailureAfterPrunedRestart(t *testing.T) {
	workspace := t.TempDir()
	settings := traceCaptureSettingsFromConfig(traceTestConfig(workspace))
	record := taskregistry.Record{
		TaskID: "recovery", GenerationID: "generation", CreatedAt: 1,
		Status:         taskregistry.StatusSucceeded,
		DeliveryStatus: taskregistry.DeliveryPending,
	}
	firstTraces, firstSubmit := collectTaskTraces(t)
	first := newTaskTraceProjector(settings, firstSubmit)
	first.observe(workspace, taskregistry.EventObservation{
		Event:        taskEventFixture(record, 1, 1, taskregistry.EventTaskUpserted),
		Record:       record,
		FinalForTask: true,
	})
	record.DeliveryStatus = taskregistry.DeliveryFailed
	first.observe(workspace, taskregistry.EventObservation{
		Event:        taskEventFixture(record, 2, 2, taskregistry.EventTaskDeliveryChanged),
		Record:       record,
		FinalForTask: true,
	})
	existing := firstTraces()[0]
	first.close()

	candidateTraces, candidateSubmit := collectTaskTraces(t)
	restarted := newTaskTraceProjector(settings, candidateSubmit)
	record.InteractionID = "interaction-1"
	restarted.observe(workspace, taskregistry.EventObservation{
		Event:        taskEventFixture(record, 2, 2, taskregistry.EventTaskDeliveryChanged),
		Record:       record,
		FinalForTask: true,
	})
	record.DeliveryStatus = taskregistry.DeliveryDelivered
	restarted.observe(workspace, taskregistry.EventObservation{
		Event:        taskEventFixture(record, 3, 3, taskregistry.EventTaskDeliveryChanged),
		Record:       record,
		FinalForTask: true,
	})
	candidate := candidateTraces()[0]
	restarted.close()

	merged, persist := reconcileTaskTraceCandidate(existing, candidate)
	if !persist {
		t.Fatal("delivery recovery did not extend persisted trace")
	}
	if merged.Truncation.Incomplete || len(merged.Records) != 3 {
		t.Fatalf("merged trace = records:%d truncation:%+v", len(merged.Records), merged.Truncation)
	}
	if merged.Outcome == nil || merged.Outcome.ErrorCode != "" {
		t.Fatalf("merged outcome = %#v", merged.Outcome)
	}
}

func TestTaskTraceProjectorExtendsCompleteTraceAfterCompletionIDChanges(t *testing.T) {
	workspace := t.TempDir()
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(traceTestConfig(workspace), eventBus)
	manager.attachTaskRegistry(workspace, registry)
	if err := registry.Upsert(taskregistry.Record{
		TaskID: "completion-recovery", Task: "test",
		Status:         taskregistry.StatusRunning,
		DeliveryStatus: taskregistry.DeliveryPending,
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Update("completion-recovery", func(record *taskregistry.Record) {
		record.Status = taskregistry.StatusSucceeded
		record.DeliveryStatus = taskregistry.DeliveryFailed
		record.LastCompletionID = "completion-1"
	}); err != nil {
		t.Fatal(err)
	}
	path := waitForTraceFile(t, workspace)
	failed := readCapturedTrace(t, path)
	manager.close()

	if err := registry.Update("completion-recovery", func(record *taskregistry.Record) {
		record.InteractionID = "interaction-1"
		record.LastCompletionID = "completion-2"
	}); err != nil {
		t.Fatal(err)
	}
	restarted := newTraceCaptureManager(traceTestConfig(workspace), eventBus)
	restarted.attachTaskRegistry(workspace, registry)
	if err := registry.Update("completion-recovery", func(record *taskregistry.Record) {
		record.DeliveryStatus = taskregistry.DeliveryDelivered
		record.DeliveryError = ""
		record.LastCompletionID = "completion-2"
	}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	var recovered evaltrace.Trace
	for {
		recovered = readCapturedTrace(t, path)
		if len(recovered.Records) > len(failed.Records) &&
			recovered.Outcome != nil &&
			recovered.Outcome.ErrorCode == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("canonical trace was not extended: %#v", recovered)
		}
		time.Sleep(time.Millisecond)
	}
	restarted.close()
	t.Cleanup(func() { _ = eventBus.Close() })

	completions := taskDeliveryCompletionIDs(recovered)
	if !slices.Equal(completions, []string{"completion-1", "completion-2"}) {
		t.Fatalf("delivery completion IDs = %v", completions)
	}
}

func TestTaskTraceProjectorEnablesAfterRegistryAttachment(t *testing.T) {
	workspace := t.TempDir()
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(nil, eventBus)
	t.Cleanup(func() {
		manager.close()
		_ = eventBus.Close()
	})
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	manager.attachTaskRegistry(workspace, registry)
	finishTaskForTrace(t, registry, "while-disabled", "session-disabled", 0)

	manager.updateConfig(traceTestConfig(workspace))
	trace := readCapturedTrace(t, waitForTraceFile(t, workspace))
	if trace.Outcome == nil || trace.Outcome.Status != string(taskregistry.StatusSucceeded) {
		t.Fatalf("enabled snapshot outcome = %#v", trace.Outcome)
	}
}

func TestTaskTraceProjectorSeparatesReusedTaskIDGenerations(t *testing.T) {
	workspace := t.TempDir()
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(traceTestConfig(workspace), eventBus)
	t.Cleanup(func() {
		manager.close()
		_ = eventBus.Close()
	})
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	manager.attachTaskRegistry(workspace, registry)

	const createdAt = int64(1_000)
	first := finishTaskForTrace(t, registry, "reused", "session", createdAt)
	waitForTraceFiles(t, workspace, 1)
	second := finishTaskForTrace(t, registry, "reused", "session", createdAt)
	paths := waitForTraceFiles(t, workspace, 2)

	if first.GenerationID == second.GenerationID {
		t.Fatalf("reused task generations share id %q", first.GenerationID)
	}
	firstTrace := readCapturedTrace(t, paths[0])
	secondTrace := readCapturedTrace(t, paths[1])
	if firstTrace.TraceID == secondTrace.TraceID {
		t.Fatalf("reused task generations share trace id %q", firstTrace.TraceID)
	}
	for _, trace := range []evaltrace.Trace{firstTrace, secondTrace} {
		generations := taskTraceGenerations(t, trace)
		if len(generations) != 1 {
			t.Fatalf("trace mixes generations: %v", generations)
		}
	}
}

func TestTaskTraceProjectorFiltersStartupHistoryToCurrentGeneration(t *testing.T) {
	workspace := t.TempDir()
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	first := finishTaskForTrace(t, registry, "reused-snapshot", "session", 1_000)
	second := finishTaskForTrace(t, registry, "reused-snapshot", "session", 1_000)
	if first.GenerationID == second.GenerationID {
		t.Fatal("task reuse did not create a generation")
	}

	traces, submit := collectTaskTraces(t)
	projector := newTaskTraceProjector(
		traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
		submit,
	)
	t.Cleanup(projector.close)
	projector.attach(workspace, registry)

	got := traces()
	if len(got) != 1 {
		t.Fatalf("startup reconciled %d traces, want current generation only", len(got))
	}
	if generations := taskTraceGenerations(t, got[0]); !slices.Equal(
		generations,
		[]string{second.GenerationID},
	) {
		t.Fatalf("startup trace generations = %v", generations)
	}
}

func TestTaskTraceProjectorBuffersCommitsAcrossSnapshotApplication(t *testing.T) {
	workspace := t.TempDir()
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	finishTaskForTrace(t, registry, "snapshot", "session", 0)

	snapshotSubmit := make(chan struct{})
	releaseSnapshot := make(chan struct{})
	var mu sync.Mutex
	var traces []evaltrace.Trace
	projector := newTaskTraceProjector(
		traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
		func(_ traceCaptureSettings, active *activeTraceCapture) error {
			trace, err := active.builder.Finalize()
			if err != nil {
				return err
			}
			if trace.Records[0].Scope.TaskID == "snapshot" {
				select {
				case <-snapshotSubmit:
				default:
					close(snapshotSubmit)
					<-releaseSnapshot
				}
			}
			mu.Lock()
			traces = append(traces, trace)
			mu.Unlock()
			return nil
		},
	)
	t.Cleanup(projector.close)
	attached := make(chan struct{})
	go func() {
		projector.attach(workspace, registry)
		close(attached)
	}()
	<-snapshotSubmit
	finishTaskForTrace(t, registry, "post-boundary", "session", 0)
	close(releaseSnapshot)
	<-attached

	deadline := time.Now().Add(time.Second)
	for {
		mu.Lock()
		count := len(traces)
		mu.Unlock()
		if count == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("persisted %d traces, want snapshot and post-boundary", count)
		}
		time.Sleep(time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	taskIDs := []string{
		traces[0].Records[0].Scope.TaskID,
		traces[1].Records[0].Scope.TaskID,
	}
	sort.Strings(taskIDs)
	if !slices.Equal(taskIDs, []string{"post-boundary", "snapshot"}) {
		t.Fatalf("trace task IDs = %v", taskIDs)
	}
}

func TestTaskTraceProjectorKeepsLiveHistoryAcrossRetention(t *testing.T) {
	workspace := t.TempDir()
	registry := taskregistry.NewRegistryWithOptions(
		taskregistry.WorkspaceStorePath(workspace),
		taskregistry.Options{MaxEvents: 2},
	)
	var traces []evaltrace.Trace
	projector := newTaskTraceProjector(
		traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
		func(_ traceCaptureSettings, active *activeTraceCapture) error {
			trace, err := active.builder.Finalize()
			if err != nil {
				return err
			}
			traces = append(traces, trace)
			return nil
		},
	)
	t.Cleanup(projector.close)
	projector.attach(workspace, registry)

	if err := registry.Upsert(taskregistry.Record{
		TaskID: "retained", Task: "test", Status: taskregistry.StatusRunning,
		DeliveryStatus: taskregistry.DeliveryPending,
	}); err != nil {
		t.Fatal(err)
	}
	upsert := registry.ListEvents("retained")[0]
	for i := range 5 {
		if err := registry.Update("retained", func(record *taskregistry.Record) {
			record.ProgressSummary = fmt.Sprintf("step %d", i)
		}); err != nil {
			t.Fatal(err)
		}
	}
	if slices.ContainsFunc(registry.ListEvents("retained"), func(event taskregistry.TaskEvent) bool {
		return event.EventID == upsert.EventID
	}) {
		t.Fatal("upsert event was not evicted from registry retention")
	}
	if err := registry.Update("retained", func(record *taskregistry.Record) {
		record.Status = taskregistry.StatusSucceeded
		record.DeliveryStatus = taskregistry.DeliveryDelivered
	}); err != nil {
		t.Fatal(err)
	}

	if len(traces) != 1 {
		t.Fatalf("persisted %d traces, want 1", len(traces))
	}
	if !slices.ContainsFunc(traces[0].Records, func(record evaltrace.Record) bool {
		return record.Origin.ID == upsert.EventID
	}) {
		t.Fatalf("terminal trace lost retained prefix: %#v", traces[0].Records)
	}
	record := registryRecord(t, registry, "retained")
	projector.observe(workspace, taskregistry.EventObservation{
		Event: upsert, Record: record, FinalForTask: true,
	})
	if len(traces) != 1 {
		t.Fatalf("stale observation created duplicate trace: %d", len(traces))
	}
}

func TestTaskTraceProjectorKeepsRecoverableLostTransitionInGeneration(t *testing.T) {
	workspace := t.TempDir()
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	var traces []evaltrace.Trace
	projector := newTaskTraceProjector(
		traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
		func(_ traceCaptureSettings, active *activeTraceCapture) error {
			trace, err := active.builder.Finalize()
			if err != nil {
				return err
			}
			traces = append(traces, trace)
			return nil
		},
	)
	t.Cleanup(projector.close)
	projector.attach(workspace, registry)
	if err := registry.Upsert(taskregistry.Record{
		TaskID: "recovered", Task: "test", Status: taskregistry.StatusRunning,
		DeliveryStatus: taskregistry.DeliveryPending,
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.MarkWaitingForInput(
		"recovered", "interaction-1", "short-1", "approval required",
	); err != nil {
		t.Fatal(err)
	}
	if err := registry.Update("recovered", func(record *taskregistry.Record) {
		record.Status = taskregistry.StatusLost
		record.DeliveryStatus = taskregistry.DeliveryNotApplicable
		record.Error = "runtime restarted"
	}); err != nil {
		t.Fatal(err)
	}
	if len(traces) != 0 {
		t.Fatalf("recoverable lost state prematurely persisted %d traces", len(traces))
	}
	if err := registry.MarkInteractionRunning("recovered", "interaction-1"); err != nil {
		t.Fatal(err)
	}
	if err := registry.CompleteInteractionTask(
		"recovered", "interaction-1", "approved", taskregistry.DeliveryDelivered,
	); err != nil {
		t.Fatal(err)
	}

	if len(traces) != 1 {
		t.Fatalf("persisted %d traces, want one generation trace", len(traces))
	}
	statuses := taskTransitionStatuses(t, traces[0])
	for _, want := range []string{
		string(taskregistry.StatusLost),
		string(taskregistry.StatusRunning),
		string(taskregistry.StatusSucceeded),
	} {
		if !slices.Contains(statuses, want) {
			t.Fatalf("trace statuses = %v, missing %q", statuses, want)
		}
	}
}

func TestTaskTraceProjectorExtendsFailedInteractionDeliveryThroughRecovery(t *testing.T) {
	workspace := t.TempDir()
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	traces, submit := collectTaskTraces(t)
	projector := newTaskTraceProjector(
		traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
		submit,
	)
	t.Cleanup(projector.close)
	projector.attach(workspace, registry)

	if err := registry.Upsert(taskregistry.Record{
		TaskID: "delivery-recovery", Task: "test", Status: taskregistry.StatusRunning,
		DeliveryStatus: taskregistry.DeliveryPending,
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.MarkWaitingForInput(
		"delivery-recovery", "interaction-1", "short-1", "approval required",
	); err != nil {
		t.Fatal(err)
	}
	if err := registry.MarkInteractionRunning("delivery-recovery", "interaction-1"); err != nil {
		t.Fatal(err)
	}
	if err := registry.CompleteInteractionTask(
		"delivery-recovery", "interaction-1", "approved", taskregistry.DeliveryPending,
	); err != nil {
		t.Fatal(err)
	}
	if err := registry.Update("delivery-recovery", func(record *taskregistry.Record) {
		record.DeliveryStatus = taskregistry.DeliveryFailed
		record.DeliveryError = "definitely not sent"
	}); err != nil {
		t.Fatal(err)
	}
	if got := traces(); len(got) != 0 {
		t.Fatalf("failed retryable delivery persisted %d traces", len(got))
	}
	if err := registry.Update("delivery-recovery", func(record *taskregistry.Record) {
		record.DeliveryStatus = taskregistry.DeliveryDelivered
		record.DeliveryError = ""
	}); err != nil {
		t.Fatal(err)
	}

	got := traces()
	if len(got) != 1 {
		t.Fatalf("recovered delivery persisted %d traces, want 1", len(got))
	}
	statuses := taskDeliveryStatuses(t, got[0])
	if !slices.Contains(statuses, string(taskregistry.DeliveryFailed)) ||
		!slices.Contains(statuses, string(taskregistry.DeliveryDelivered)) {
		t.Fatalf("delivery statuses = %v", statuses)
	}
	if got[0].Outcome == nil || got[0].Outcome.ErrorCode != "" {
		t.Fatalf("recovered outcome = %#v", got[0].Outcome)
	}
}

func TestTaskTraceProjectorRetriesCapacityRejectionWithoutNewEvent(t *testing.T) {
	workspace := t.TempDir()
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	var attempts atomic.Int32
	admitted := make(chan evaltrace.Trace, 1)
	projector := newTaskTraceProjector(
		traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
		func(_ traceCaptureSettings, active *activeTraceCapture) error {
			if attempts.Add(1) == 1 {
				return &evalcapture.AdmissionError{
					Reason: evalcapture.ReasonCapacity,
					Class:  evalcapture.ClassCritical,
				}
			}
			trace, err := active.builder.Finalize()
			if err != nil {
				return err
			}
			admitted <- trace
			return nil
		},
	)
	t.Cleanup(projector.close)
	projector.attach(workspace, registry)
	finishTaskForTrace(t, registry, "retry", "session", 0)

	select {
	case trace := <-admitted:
		if trace.Outcome == nil ||
			trace.Outcome.Status != string(taskregistry.StatusSucceeded) {
			t.Fatalf("admitted outcome = %#v", trace.Outcome)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("capacity rejection was not retried")
	}
	if attempts.Load() < 2 {
		t.Fatalf("admission attempts = %d", attempts.Load())
	}
}

func TestTaskTraceProjectorReopenReleasesRetrySlot(t *testing.T) {
	workspace := t.TempDir()
	var attempts atomic.Int32
	projector := newTaskTraceProjector(
		traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
		func(_ traceCaptureSettings, trace *activeTraceCapture) error {
			if attempts.Add(1) == 1 {
				return &evalcapture.AdmissionError{
					Reason: evalcapture.ReasonCapacity,
					Class:  evalcapture.ClassCritical,
				}
			}
			_, err := trace.builder.Finalize()
			return err
		},
	)
	t.Cleanup(projector.close)
	record := taskregistry.Record{
		TaskID: "reopen", GenerationID: "generation", CreatedAt: 1,
		Status:         taskregistry.StatusSucceeded,
		DeliveryStatus: taskregistry.DeliveryFailed,
	}
	projector.observe(workspace, taskregistry.EventObservation{
		Event:        taskEventFixture(record, 1, 1, taskregistry.EventTaskDeliveryChanged),
		Record:       record,
		FinalForTask: true,
	})
	if got := projector.stats().PendingAdmissions; got != 1 {
		t.Fatalf("pending admissions after rejection = %d, want 1", got)
	}

	record.InteractionID = "interaction-1"
	record.DeliveryStatus = taskregistry.DeliveryPending
	projector.observe(workspace, taskregistry.EventObservation{
		Event:        taskEventFixture(record, 2, 2, taskregistry.EventTaskDeliveryChanged),
		Record:       record,
		FinalForTask: true,
	})
	if got := projector.stats().PendingAdmissions; got != 0 {
		t.Fatalf("pending admissions after reopen = %d, want 0", got)
	}
	record.DeliveryStatus = taskregistry.DeliveryDelivered
	projector.observe(workspace, taskregistry.EventObservation{
		Event:        taskEventFixture(record, 3, 3, taskregistry.EventTaskDeliveryChanged),
		Record:       record,
		FinalForTask: true,
	})
	if got := projector.stats().PendingAdmissions; got != 0 {
		t.Fatalf("pending admissions after recovery = %d, want 0", got)
	}
}

func TestTaskTraceProjectorRetriesPermanentWriterFailure(t *testing.T) {
	workspace := t.TempDir()
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	var attempts atomic.Int32
	projector := newTaskTraceProjector(
		traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
		func(_ traceCaptureSettings, trace *activeTraceCapture) error {
			attempts.Add(1)
			_, err := trace.builder.Finalize()
			return err
		},
	)
	projector.awaitPersistence = true
	t.Cleanup(projector.close)
	projector.attach(workspace, registry)
	record := finishTaskForTrace(t, registry, "storage-retry", "session", 1)
	key := newTaskTraceKey(workspace, record.TaskID, record.GenerationID)
	projector.mu.Lock()
	traceID := projector.traces[key].trace.builder.TraceID()
	projector.mu.Unlock()
	projector.observeWriterEvent(evalcapture.Event{
		Kind:    evalcapture.EventPermanentlyFailed,
		Reason:  evalcapture.ReasonStorageFailure,
		TraceID: traceID,
		Class:   evalcapture.ClassCritical,
	})
	if got := projector.stats().PendingAdmissions; got != 1 {
		t.Fatalf("pending admissions after storage failure = %d, want 1", got)
	}
	if current := registryRecord(t, registry, record.TaskID); !current.TraceCapturePending {
		t.Fatal("storage failure released durable trace retry marker")
	}
	projector.mu.Lock()
	if projector.retryTimer != nil {
		projector.retryTimer.Stop()
		projector.retryTimer = nil
	}
	projector.mu.Unlock()
	projector.retryPending()
	projector.observeWriterEvent(evalcapture.Event{
		Kind:    evalcapture.EventPersisted,
		TraceID: traceID,
		Class:   evalcapture.ClassCritical,
	})

	if attempts.Load() != 2 {
		t.Fatalf("submission attempts = %d, want 2", attempts.Load())
	}
	if got := projector.stats().PendingAdmissions; got != 0 {
		t.Fatalf("pending admissions after persistence = %d, want 0", got)
	}
	if current := registryRecord(t, registry, record.TaskID); current.TraceCapturePending {
		t.Fatal("successful persistence retained trace retry marker")
	}
	projector.mu.Lock()
	remaining := len(projector.traces)
	projector.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("remaining traces = %d, want 0", remaining)
	}
}

func TestTaskTraceProjectorRecoversDeferredCapacityOverflowFromRegistry(t *testing.T) {
	workspace := t.TempDir()
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	var accepting atomic.Bool
	var admitted atomic.Int32
	projector := newTaskTraceProjector(
		traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
		func(_ traceCaptureSettings, trace *activeTraceCapture) error {
			if !accepting.Load() {
				return &evalcapture.AdmissionError{
					Reason: evalcapture.ReasonCapacity,
					Class:  evalcapture.ClassCritical,
				}
			}
			if _, err := trace.builder.Finalize(); err != nil {
				return err
			}
			admitted.Add(1)
			return nil
		},
	)
	projector.awaitPersistence = true
	t.Cleanup(projector.close)
	projector.attach(workspace, registry)
	for i := range maxPendingTaskTraceAdmissions + 17 {
		finishTaskForTrace(
			t,
			registry,
			fmt.Sprintf("capacity-%03d", i),
			"session",
			int64(i+1),
		)
	}

	stats := projector.stats()
	if stats.PendingAdmissions != maxPendingTaskTraceAdmissions {
		t.Fatalf("pending admissions = %d, want %d", stats.PendingAdmissions, maxPendingTaskTraceAdmissions)
	}
	if stats.OverflowDeferrals != 17 {
		t.Fatalf("overflow deferrals = %d, want 17", stats.OverflowDeferrals)
	}
	projector.mu.Lock()
	retained := len(projector.traces)
	projector.mu.Unlock()
	if retained != maxPendingTaskTraceAdmissions {
		t.Fatalf("retained terminal traces = %d, want %d", retained, maxPendingTaskTraceAdmissions)
	}
	for _, record := range registry.List() {
		if !record.TraceCapturePending {
			t.Fatalf("task %q lacks durable trace retry marker", record.TaskID)
		}
	}

	accepting.Store(true)
	deadline := time.Now().Add(3 * time.Second)
	want := int32(maxPendingTaskTraceAdmissions + 17)
	for admitted.Load() != want {
		if time.Now().After(deadline) {
			t.Fatalf("admitted traces = %d, want %d", admitted.Load(), want)
		}
		time.Sleep(time.Millisecond)
	}
	if stats := projector.stats(); stats.PendingAdmissions != 0 {
		t.Fatalf("pending admissions after recovery = %d", stats.PendingAdmissions)
	}
}

func TestTaskTraceProjectorCloseWaitsForPendingAdmission(t *testing.T) {
	workspace := t.TempDir()
	release := make(chan struct{})
	waitStarted := make(chan struct{})
	projector := newTaskTraceProjector(
		traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
		func(traceCaptureSettings, *activeTraceCapture) error {
			return &evalcapture.AdmissionError{
				Reason: evalcapture.ReasonCapacity,
				Class:  evalcapture.ClassCritical,
			}
		},
		func(context.Context, traceCaptureSettings, *activeTraceCapture) error {
			close(waitStarted)
			<-release
			return nil
		},
	)
	record := taskregistry.Record{
		TaskID: "shutdown", GenerationID: "shutdown-generation", CreatedAt: 1,
		Status:         taskregistry.StatusSucceeded,
		DeliveryStatus: taskregistry.DeliveryDelivered,
	}
	projector.observe(workspace, taskregistry.EventObservation{
		Event:        taskEventFixture(record, 1, 1, taskregistry.EventTaskUpserted),
		Record:       record,
		FinalForTask: true,
	})

	closed := make(chan struct{})
	go func() {
		projector.close()
		close(closed)
	}()
	<-waitStarted
	select {
	case <-closed:
		t.Fatal("close returned before pending trace admission")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close did not finish after pending trace admission")
	}
}

func TestTaskTraceProjectorShutdownDeadlineLeavesRegistryRecoverable(t *testing.T) {
	workspace := t.TempDir()
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	projector := newTaskTraceProjector(
		traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
		func(traceCaptureSettings, *activeTraceCapture) error {
			return &evalcapture.AdmissionError{
				Reason: evalcapture.ReasonCapacity,
				Class:  evalcapture.ClassCritical,
			}
		},
		func(
			ctx context.Context,
			_ traceCaptureSettings,
			_ *activeTraceCapture,
		) error {
			<-ctx.Done()
			return ctx.Err()
		},
	)
	projector.attach(workspace, registry)
	finishTaskForTrace(t, registry, "shutdown-timeout", "session", 1)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := projector.closeWithContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("closeWithContext error = %v", err)
	}

	traces, submit := collectTaskTraces(t)
	reloaded := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	restarted := newTaskTraceProjector(
		traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
		submit,
	)
	t.Cleanup(restarted.close)
	restarted.attach(workspace, reloaded)
	if got := traces(); len(got) != 1 {
		t.Fatalf("reconciled traces after shutdown timeout = %d, want 1", len(got))
	}
}

func TestTaskTraceProjectorMarksPrunedStartupHistoryIncomplete(t *testing.T) {
	workspace := t.TempDir()
	registry := taskregistry.NewRegistryWithOptions(
		taskregistry.WorkspaceStorePath(workspace),
		taskregistry.Options{MaxEvents: 1},
	)
	record := finishTaskForTrace(t, registry, "pruned", "session", 0)
	if events := registry.ListEvents("pruned"); len(events) != 1 || events[0].Seq <= 1 {
		t.Fatalf("retained events = %#v", events)
	}

	traces, submit := collectTaskTraces(t)
	projector := newTaskTraceProjector(
		traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
		submit,
	)
	t.Cleanup(projector.close)
	projector.attach(workspace, registry)
	got := traces()
	if len(got) != 1 {
		t.Fatalf("reconciled %d traces, want 1", len(got))
	}
	if !got[0].Truncation.Incomplete ||
		!slices.Contains(got[0].Truncation.Reasons, "task_event_sequence_gap") ||
		got[0].Truncation.DroppedRecords != int(record.LastEventSeq-1) {
		t.Fatalf("truncation = %+v", got[0].Truncation)
	}
}

func TestTaskTraceProjectorClampsClockRollbackOffsets(t *testing.T) {
	workspace := t.TempDir()
	traces, submit := collectTaskTraces(t)
	projector := newTaskTraceProjector(
		traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
		submit,
	)
	t.Cleanup(projector.close)
	record := taskregistry.Record{
		TaskID: "clock", GenerationID: "generation-clock", CreatedAt: 100,
		Status: taskregistry.StatusRunning, DeliveryStatus: taskregistry.DeliveryPending,
	}
	projector.observe(workspace, taskregistry.EventObservation{
		Event:  taskEventFixture(record, 1, 200, taskregistry.EventTaskUpserted),
		Record: record, FinalForTask: true,
	})
	record.Status = taskregistry.StatusSucceeded
	record.DeliveryStatus = taskregistry.DeliveryDelivered
	projector.observe(workspace, taskregistry.EventObservation{
		Event:  taskEventFixture(record, 2, 150, taskregistry.EventTaskStatusChanged),
		Record: record, FinalForTask: true,
	})

	got := traces()
	if len(got) != 1 {
		t.Fatalf("persisted %d traces, want 1", len(got))
	}
	if got[0].Records[1].OffsetNanos < got[0].Records[0].OffsetNanos {
		t.Fatalf("offsets moved backward: %#v", got[0].Records)
	}
}

func TestTaskTraceProjectorPersistsIncompleteTraceOnClose(t *testing.T) {
	workspace := t.TempDir()
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(traceTestConfig(workspace), eventBus)
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	manager.attachTaskRegistry(workspace, registry)
	if err := registry.Upsert(taskregistry.Record{
		TaskID: "active", Task: "test", Status: taskregistry.StatusRunning,
		DeliveryStatus: taskregistry.DeliveryPending,
	}); err != nil {
		t.Fatal(err)
	}
	manager.close()
	t.Cleanup(func() { _ = eventBus.Close() })

	trace := readCapturedTrace(t, waitForTraceFile(t, workspace))
	if !trace.Truncation.Incomplete ||
		!slices.Contains(
			trace.Truncation.Reasons,
			"runtime_closed_before_terminal_task_delivery",
		) {
		t.Fatalf("truncation = %+v", trace.Truncation)
	}
}

func finishTaskForTrace(
	t *testing.T,
	registry *taskregistry.Registry,
	taskID, session string,
	createdAt int64,
) taskregistry.Record {
	t.Helper()
	if err := registry.Upsert(taskregistry.Record{
		TaskID: taskID, Task: "test", RequesterSessionKey: session,
		CreatedAt: createdAt, Status: taskregistry.StatusRunning,
		DeliveryStatus: taskregistry.DeliveryPending,
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Update(taskID, func(record *taskregistry.Record) {
		record.Status = taskregistry.StatusSucceeded
		record.DeliveryStatus = taskregistry.DeliveryDelivered
	}); err != nil {
		t.Fatal(err)
	}
	return registryRecord(t, registry, taskID)
}

func registryRecord(
	t *testing.T,
	registry *taskregistry.Registry,
	taskID string,
) taskregistry.Record {
	t.Helper()
	record, ok := registry.Get(taskID)
	if !ok {
		t.Fatalf("task %q not found", taskID)
	}
	return record
}

func collectTaskTraces(
	t *testing.T,
) (func() []evaltrace.Trace, func(traceCaptureSettings, *activeTraceCapture) error) {
	t.Helper()
	var mu sync.Mutex
	var traces []evaltrace.Trace
	return func() []evaltrace.Trace {
			mu.Lock()
			defer mu.Unlock()
			return append([]evaltrace.Trace(nil), traces...)
		}, func(_ traceCaptureSettings, active *activeTraceCapture) error {
			trace, err := active.builder.Finalize()
			if err != nil {
				return err
			}
			mu.Lock()
			traces = append(traces, trace)
			mu.Unlock()
			return nil
		}
}

func taskTraceGenerations(t *testing.T, trace evaltrace.Trace) []string {
	t.Helper()
	var generations []string
	for _, record := range trace.Records {
		if record.Kind != evaltrace.RecordTaskTransition {
			continue
		}
		var payload evaltrace.TaskPayload
		if err := json.Unmarshal(record.Data, &payload); err != nil {
			t.Fatal(err)
		}
		if payload.GenerationID != "" && !slices.Contains(generations, payload.GenerationID) {
			generations = append(generations, payload.GenerationID)
		}
	}
	return generations
}

func taskTransitionStatuses(t *testing.T, trace evaltrace.Trace) []string {
	t.Helper()
	var statuses []string
	for _, record := range trace.Records {
		if record.Kind != evaltrace.RecordTaskTransition {
			continue
		}
		var payload evaltrace.TaskPayload
		if err := json.Unmarshal(record.Data, &payload); err != nil {
			t.Fatal(err)
		}
		if payload.Status != "" {
			statuses = append(statuses, payload.Status)
		}
	}
	return statuses
}

func taskDeliveryStatuses(t *testing.T, trace evaltrace.Trace) []string {
	t.Helper()
	var statuses []string
	for _, record := range trace.Records {
		if record.Kind != evaltrace.RecordDeliveryOutcome {
			continue
		}
		var payload evaltrace.DeliveryPayload
		if err := json.Unmarshal(record.Data, &payload); err != nil {
			t.Fatal(err)
		}
		statuses = append(statuses, payload.Status)
	}
	return statuses
}

func taskDeliveryCompletionIDs(trace evaltrace.Trace) []string {
	var completionIDs []string
	for _, record := range trace.Records {
		if record.Kind == evaltrace.RecordDeliveryOutcome {
			completionIDs = append(completionIDs, record.Correlation.CompletionID)
		}
	}
	return completionIDs
}

func taskEventFixture(
	record taskregistry.Record,
	sequence, emittedAt int64,
	eventType taskregistry.EventType,
) taskregistry.TaskEvent {
	return taskregistry.TaskEvent{
		SchemaVersion: taskregistry.TaskEventSchemaVersion,
		EventID: fmt.Sprintf(
			"%s:%s:%06d:%s",
			record.TaskID,
			record.GenerationID,
			sequence,
			eventType,
		),
		TaskID: record.TaskID, GenerationID: record.GenerationID,
		Type: eventType, Status: record.Status, DeliveryStatus: record.DeliveryStatus,
		Seq: sequence, EmittedAt: emittedAt,
	}
}
