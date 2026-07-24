package agent

import (
	"encoding/json"
	"fmt"
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
