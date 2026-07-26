package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/evalcapture"
	"github.com/sipeed/picoclaw/pkg/evaltrace"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	taskregistry "github.com/sipeed/picoclaw/pkg/tasks"
)

func TestTaskTraceProjectionKeyRoundTrip(t *testing.T) {
	taskID := "task.with spaces/and:punctuation"
	generationID := "generation.with spaces/and:punctuation"
	key := encodeTaskTraceProjectionKey(taskID, generationID)
	gotTask, gotGeneration, err := decodeTaskTraceProjectionKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if gotTask != taskID || gotGeneration != generationID {
		t.Fatalf("decoded key = (%q, %q)", gotTask, gotGeneration)
	}
	for _, invalid := range []string{"", "one-part", ".missing", "missing."} {
		if _, _, err := decodeTaskTraceProjectionKey(invalid); err == nil {
			t.Fatalf("decodeTaskTraceProjectionKey(%q) succeeded", invalid)
		}
	}
}

func TestTaskTraceWorkspaceAliasesShareIdentity(t *testing.T) {
	workspace := t.TempDir()
	alias := workspace + string(os.PathSeparator) + "."
	settings := traceCaptureSettingsFromConfig(traceTestConfig(workspace))
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	record := finishTaskForTrace(t, registry, "alias", "session", 1)
	history := registry.ListEvents(record.TaskID)

	canonical := buildTaskTrace(settings, workspace, record, history)
	aliased := buildTaskTrace(settings, alias, record, history)
	if canonical.builder.TraceID() != aliased.builder.TraceID() ||
		taskTraceSourceID(workspace) != taskTraceSourceID(alias) {
		t.Fatal("workspace alias changed task trace identity")
	}
}

func TestTaskTraceSourcePendingIsBoundedAndTerminalOnly(t *testing.T) {
	workspace := t.TempDir()
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	if err := registry.SetTraceCaptureProtection(true, 2000); err != nil {
		t.Fatal(err)
	}
	first := finishTaskForTrace(t, registry, "a", "session", 1)
	_ = finishTaskForTrace(t, registry, "b", "session", 2)
	if err := registry.Upsert(taskregistry.Record{
		TaskID: "active", Task: "test", Status: taskregistry.StatusRunning,
		DeliveryStatus: taskregistry.DeliveryPending,
	}); err != nil {
		t.Fatal(err)
	}
	source := &taskTraceSource{workspace: workspace, registry: registry}
	keys, err := source.Pending(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 {
		t.Fatalf("pending keys = %d, want 1", len(keys))
	}
	taskID, generationID, err := decodeTaskTraceProjectionKey(keys[0])
	if err != nil {
		t.Fatal(err)
	}
	if taskID != first.TaskID || generationID != first.GenerationID {
		t.Fatalf("first pending key = (%q, %q)", taskID, generationID)
	}
}

func TestTaskTraceSourceSharesInteractionTerminalPredicateWithRegistry(t *testing.T) {
	workspace := t.TempDir()
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	if err := registry.SetTraceCaptureProtection(true, 100); err != nil {
		t.Fatal(err)
	}
	if err := registry.Upsert(taskregistry.Record{
		TaskID: "interaction-delivery", InteractionID: "interaction-1",
		Status:         taskregistry.StatusSucceeded,
		DeliveryStatus: taskregistry.DeliveryFailed,
	}); err != nil {
		t.Fatal(err)
	}
	source := &taskTraceSource{workspace: workspace, registry: registry}
	keys, err := source.Pending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	record := registryRecord(t, registry, "interaction-delivery")
	if record.TraceCapturePending || len(keys) != 0 {
		t.Fatalf("retryable interaction record=%#v pending keys=%v", record, keys)
	}

	if err = registry.Update(record.TaskID, func(current *taskregistry.Record) {
		current.DeliveryStatus = taskregistry.DeliveryDelivered
	}); err != nil {
		t.Fatal(err)
	}
	keys, err = source.Pending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	record = registryRecord(t, registry, record.TaskID)
	if !record.TraceCapturePending || len(keys) != 1 {
		t.Fatalf("terminal interaction record=%#v pending keys=%v", record, keys)
	}
}

func TestTaskTraceSourceConfirmationIsRevisionBound(t *testing.T) {
	workspace := t.TempDir()
	settings := traceCaptureSettingsFromConfig(traceTestConfig(workspace))
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	if err := registry.SetTraceCaptureProtection(true, 2000); err != nil {
		t.Fatal(err)
	}
	record := finishTaskForTrace(t, registry, "task", "session", 1)
	source := &taskTraceSource{
		workspace: workspace, registry: registry, settings: settings,
	}
	key := encodeTaskTraceProjectionKey(record.TaskID, record.GenerationID)
	candidate, found, err := source.LoadLatest(context.Background(), key)
	if err != nil || !found {
		t.Fatalf("LoadLatest found=%v err=%v", found, err)
	}
	if !candidate.Persist {
		t.Fatal("pending task trace did not require a writer receipt")
	}
	if updateErr := registry.Update(record.TaskID, func(current *taskregistry.Record) {
		current.DeliveryStatus = taskregistry.DeliveryFailed
	}); updateErr != nil {
		t.Fatal(updateErr)
	}
	confirmation, err := source.Confirm(
		context.Background(),
		key,
		candidate.Revision,
	)
	if err != nil {
		t.Fatal(err)
	}
	if confirmation != evalcapture.ConfirmationStale {
		t.Fatalf("confirmation = %q, want stale", confirmation)
	}
	current := registryRecord(t, registry, record.TaskID)
	if !current.TraceCapturePending {
		t.Fatal("stale confirmation cleared the pending marker")
	}
}

func TestTaskTraceProjectorCapturesLiveTerminalTask(t *testing.T) {
	workspace := t.TempDir()
	manager, eventBus, registry := newTaskTraceTestRuntime(t, workspace)
	record := finishTaskForTrace(t, registry, "live", "session", 1)
	waitForTraceMarkerCleared(
		t,
		registry,
		record.TaskID,
		record.GenerationID,
	)
	trace := readCapturedTrace(t, waitForTraceFile(t, workspace))
	if trace.Outcome == nil ||
		trace.Outcome.Status != string(taskregistry.StatusSucceeded) {
		t.Fatalf("outcome = %#v", trace.Outcome)
	}
	if generations := taskTraceGenerations(t, trace); !slices.Equal(
		generations,
		[]string{record.GenerationID},
	) {
		t.Fatalf("trace generations = %v", generations)
	}
	manager.close()
	_ = eventBus.Close()
}

func TestTaskTraceProjectorPreservesHistoryAcrossRegistryPruning(t *testing.T) {
	workspace := t.TempDir()
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(traceTestConfig(workspace), eventBus)
	registry := taskregistry.NewRegistryWithOptions(
		taskregistry.WorkspaceStorePath(workspace),
		taskregistry.Options{
			MaxRecords: 10, MaxEvents: 2, MaxSnapshotBytes: 2 * 1024 * 1024,
		},
	)
	manager.attachTaskRegistry(workspace, registry)
	if err := registry.Upsert(taskregistry.Record{
		TaskID: "retention", Task: "test", CreatedAt: 1,
		Status:         taskregistry.StatusRunning,
		DeliveryStatus: taskregistry.DeliveryPending,
	}); err != nil {
		t.Fatal(err)
	}
	for _, progress := range []string{"one", "two", "three", "four"} {
		if err := registry.Update("retention", func(record *taskregistry.Record) {
			record.ProgressSummary = progress
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := registry.Update("retention", func(record *taskregistry.Record) {
		record.Status = taskregistry.StatusSucceeded
		record.DeliveryStatus = taskregistry.DeliveryDelivered
	}); err != nil {
		t.Fatal(err)
	}
	record := registryRecord(t, registry, "retention")
	waitForTraceMarkerCleared(
		t,
		registry,
		record.TaskID,
		record.GenerationID,
	)
	trace := readCapturedTrace(t, waitForTraceFile(t, workspace))
	if trace.Truncation.Incomplete {
		t.Fatalf("trace unexpectedly incomplete: %+v", trace.Truncation)
	}
	if got := maxTaskTraceEventSequence(trace.Records); got != record.LastEventSeq {
		t.Fatalf("trace revision = %d, want %d", got, record.LastEventSeq)
	}
	if len(trace.Records) != int(record.LastEventSeq) {
		t.Fatalf(
			"trace records = %d, want %d",
			len(trace.Records),
			record.LastEventSeq,
		)
	}
	if global := registry.ListEvents(record.TaskID); len(global) > 2 {
		t.Fatalf("global retained events = %d, want at most 2", len(global))
	}
	manager.close()
	_ = eventBus.Close()
}

func TestTaskTraceProjectorPreservesActiveJournalAcrossShutdown(t *testing.T) {
	workspace := t.TempDir()
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(traceTestConfig(workspace), eventBus)
	registry := taskregistry.NewRegistryWithOptions(
		taskregistry.WorkspaceStorePath(workspace),
		taskregistry.Options{
			MaxRecords: 10, MaxEvents: 2, MaxSnapshotBytes: 2 * 1024 * 1024,
		},
	)
	manager.attachTaskRegistry(workspace, registry)
	if err := registry.Upsert(taskregistry.Record{
		TaskID: "shutdown-active", Task: "test",
		Status:         taskregistry.StatusRunning,
		DeliveryStatus: taskregistry.DeliveryPending,
	}); err != nil {
		t.Fatal(err)
	}
	for _, progress := range []string{"one", "two", "three", "four"} {
		if err := registry.Update("shutdown-active", func(record *taskregistry.Record) {
			record.ProgressSummary = progress
		}); err != nil {
			t.Fatal(err)
		}
	}
	before := registryRecord(t, registry, "shutdown-active")
	manager.close()
	_ = eventBus.Close()

	reloaded := taskregistry.NewRegistryWithOptions(
		taskregistry.WorkspaceStorePath(workspace),
		taskregistry.Options{
			MaxRecords: 10, MaxEvents: 2, MaxSnapshotBytes: 2 * 1024 * 1024,
		},
	)
	active := registryRecord(t, reloaded, before.TaskID)
	if active.TraceCapturePending {
		t.Fatal("active task unexpectedly awaits terminal trace persistence")
	}
	if len(active.TraceCaptureEvents) != int(active.LastEventSeq) {
		t.Fatalf(
			"reloaded active journal events = %d, want %d",
			len(active.TraceCaptureEvents),
			active.LastEventSeq,
		)
	}

	restartBus := runtimeevents.NewBus()
	restarted := newTraceCaptureManager(traceTestConfig(workspace), restartBus)
	restarted.attachTaskRegistry(workspace, reloaded)
	if err := reloaded.Update(active.TaskID, func(record *taskregistry.Record) {
		record.Status = taskregistry.StatusSucceeded
		record.DeliveryStatus = taskregistry.DeliveryDelivered
	}); err != nil {
		t.Fatal(err)
	}
	terminal := registryRecord(t, reloaded, active.TaskID)
	waitForTraceMarkerCleared(
		t,
		reloaded,
		terminal.TaskID,
		terminal.GenerationID,
	)
	trace := readCapturedTrace(t, waitForTraceFile(t, workspace))
	if trace.Truncation.Incomplete ||
		maxTaskTraceEventSequence(trace.Records) != terminal.LastEventSeq {
		t.Fatalf(
			"restarted trace revision=%d truncation=%+v, want revision=%d",
			maxTaskTraceEventSequence(trace.Records),
			trace.Truncation,
			terminal.LastEventSeq,
		)
	}
	restarted.close()
	_ = restartBus.Close()
}

func TestTaskTraceProjectorAppliesUpdatedJournalLimit(t *testing.T) {
	workspace := t.TempDir()
	eventBus := runtimeevents.NewBus()
	cfg := traceTestConfig(workspace)
	manager := newTraceCaptureManager(cfg, eventBus)
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	manager.attachTaskRegistry(workspace, registry)
	if err := registry.Upsert(taskregistry.Record{
		TaskID: "limit-update", Task: "test",
		Status:         taskregistry.StatusRunning,
		DeliveryStatus: taskregistry.DeliveryPending,
	}); err != nil {
		t.Fatal(err)
	}
	for _, progress := range []string{"one", "two", "three", "four"} {
		if err := registry.Update("limit-update", func(record *taskregistry.Record) {
			record.ProgressSummary = progress
		}); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(registryRecord(t, registry, "limit-update").TraceCaptureEvents); got < 4 {
		t.Fatalf("initial journal events = %d, want at least 4", got)
	}

	updated := *cfg
	updated.Evaluation.TraceCapture.MaxRecords = 2
	manager.updateConfig(&updated)
	record := registryRecord(t, registry, "limit-update")
	if got := len(record.TraceCaptureEvents); got != 2 {
		t.Fatalf("updated journal events = %d, want 2", got)
	}
	if got, want := record.TraceCaptureDropped,
		int(record.TraceCaptureEvents[0].Seq-1); got != want {
		t.Fatalf("updated journal dropped = %d, want %d", got, want)
	}
	manager.close()
	_ = eventBus.Close()
}

func TestTaskTraceProjectorRecoversPendingTaskAfterRestart(t *testing.T) {
	workspace := t.TempDir()
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	if err := registry.SetTraceCaptureProtection(true, 2000); err != nil {
		t.Fatal(err)
	}
	record := finishTaskForTrace(t, registry, "restart", "session", 1)
	if !record.TraceCapturePending {
		t.Fatal("terminal record is not protected")
	}

	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(traceTestConfig(workspace), eventBus)
	manager.attachTaskRegistry(workspace, registry)
	waitForTraceMarkerCleared(
		t,
		registry,
		record.TaskID,
		record.GenerationID,
	)
	trace := readCapturedTrace(t, waitForTraceFile(t, workspace))
	if highest := maxTaskTraceEventSequence(trace.Records); highest != record.LastEventSeq {
		t.Fatalf("highest sequence = %d, want %d", highest, record.LastEventSeq)
	}
	manager.close()
	_ = eventBus.Close()
}

func TestTaskTraceProjectorRetriesFailedProtectionInstallation(t *testing.T) {
	workspace := t.TempDir()
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	record := finishTaskForTrace(t, registry, "protection-retry", "session", 1)
	stateDir := filepath.Dir(taskregistry.WorkspaceStorePath(workspace))
	if err := os.Chmod(stateDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(stateDir, 0o700) })

	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(traceTestConfig(workspace), eventBus)
	manager.attachTaskRegistry(workspace, registry)
	manager.tasks.mu.Lock()
	_, subscribed := manager.tasks.subs[workspace]
	retryScheduled := manager.tasks.retryTimer != nil
	manager.tasks.mu.Unlock()
	if subscribed || !retryScheduled {
		t.Fatalf(
			"failed installation subscribed=%v retry=%v",
			subscribed,
			retryScheduled,
		)
	}
	if registryRecord(t, registry, record.TaskID).TraceCapturePending {
		t.Fatal("failed installation retained an unowned marker")
	}

	if err := os.Chmod(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	waitForTraceMarkerCleared(
		t,
		registry,
		record.TaskID,
		record.GenerationID,
	)
	_ = waitForTraceFile(t, workspace)
	manager.close()
	_ = eventBus.Close()
}

func TestTaskTraceProjectorExtendsCanonicalTraceAfterDeliveryRecovery(
	t *testing.T,
) {
	workspace := t.TempDir()
	manager, eventBus, registry := newTaskTraceTestRuntime(t, workspace)
	if err := registry.Upsert(taskregistry.Record{
		TaskID: "delivery", Task: "test", CreatedAt: 1,
		Status:         taskregistry.StatusRunning,
		DeliveryStatus: taskregistry.DeliveryPending,
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Update("delivery", func(record *taskregistry.Record) {
		record.Status = taskregistry.StatusSucceeded
		record.DeliveryStatus = taskregistry.DeliveryFailed
		record.LastCompletionID = "completion-1"
	}); err != nil {
		t.Fatal(err)
	}
	failed := registryRecord(t, registry, "delivery")
	waitForTraceMarkerCleared(
		t,
		registry,
		failed.TaskID,
		failed.GenerationID,
	)
	firstPath := waitForTraceFile(t, workspace)
	first := readCapturedTrace(t, firstPath)

	if err := registry.Update("delivery", func(record *taskregistry.Record) {
		record.DeliveryStatus = taskregistry.DeliveryPending
		record.LastCompletionID = "completion-2"
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Update("delivery", func(record *taskregistry.Record) {
		record.DeliveryStatus = taskregistry.DeliveryDelivered
	}); err != nil {
		t.Fatal(err)
	}
	delivered := registryRecord(t, registry, "delivery")
	waitForTraceMarkerCleared(
		t,
		registry,
		delivered.TaskID,
		delivered.GenerationID,
	)
	second := waitForTraceRevision(t, firstPath, delivered.LastEventSeq)
	if len(second.Records) <= len(first.Records) {
		t.Fatalf(
			"recovered records = %d, want more than %d",
			len(second.Records),
			len(first.Records),
		)
	}
	if got := deliveryStatuses(t, second); !slices.Contains(got, "failed") ||
		!slices.Contains(got, "delivered") {
		t.Fatalf("delivery statuses = %v", got)
	}
	manager.close()
	_ = eventBus.Close()
}

func TestTaskTraceProjectorRepairsCorruptCanonicalTrace(t *testing.T) {
	workspace := t.TempDir()
	settings := traceCaptureSettingsFromConfig(traceTestConfig(workspace))
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	if err := registry.SetTraceCaptureProtection(true, 2000); err != nil {
		t.Fatal(err)
	}
	record := finishTaskForTrace(t, registry, "corrupt", "session", 1)
	active := buildTaskTrace(
		settings,
		workspace,
		record,
		registry.ListEvents(record.TaskID),
	)
	trace, policy, err := prepareTrace(settings, active)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(policy.Root, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(policy.Root, trace.TraceID+".json")
	if err := os.WriteFile(path, []byte("{truncated"), 0o600); err != nil {
		t.Fatal(err)
	}

	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(traceTestConfig(workspace), eventBus)
	manager.attachTaskRegistry(workspace, registry)
	waitForTraceMarkerCleared(
		t,
		registry,
		record.TaskID,
		record.GenerationID,
	)
	repaired := readCapturedTrace(t, path)
	if repaired.TraceID != trace.TraceID {
		t.Fatalf("repaired trace ID = %q, want %q", repaired.TraceID, trace.TraceID)
	}
	manager.close()
	_ = eventBus.Close()
}

func TestTaskTraceProjectorDisablePreservesInflightMarker(t *testing.T) {
	workspace := t.TempDir()
	settings := traceCaptureSettingsFromConfig(traceTestConfig(workspace))
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	storage := newBlockingTaskTraceStorage()
	coordinator := evalcapture.NewCoordinator(evalcapture.CoordinatorOptions{
		PendingCapacity: 4,
		RetryDelay:      time.Millisecond,
		Writer: evalcapture.Options{
			Capacity:    4,
			MaxAttempts: 1,
			RetryDelay:  -1,
			StorageFactory: func(policy evalcapture.Policy) evalcapture.Storage {
				storage.setPolicy(policy)
				return storage
			},
		},
	})
	projector := newTaskTraceProjector(settings, coordinator)
	projector.attach(workspace, registry)
	record := finishTaskForTrace(t, registry, "disabled", "session", 1)
	select {
	case <-storage.started:
	case <-time.After(time.Second):
		t.Fatal("task trace did not reach storage")
	}
	disabled := settings
	disabled.enabled = false
	projector.updateSettings(disabled)
	current := registryRecord(t, registry, record.TaskID)
	if !current.TraceCapturePending {
		t.Fatal("disable cleared an in-flight durable marker")
	}
	close(storage.release)
	closeCoordinatorForTaskTraceTest(t, coordinator)
	projector.stop()
	projector.finish()
}

func TestTaskTraceProjectorShutdownDrainsRegisteredSource(t *testing.T) {
	workspace := t.TempDir()
	settings := traceCaptureSettingsFromConfig(traceTestConfig(workspace))
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	storage := newBlockingTaskTraceStorage()
	coordinator := evalcapture.NewCoordinator(evalcapture.CoordinatorOptions{
		PendingCapacity: 4,
		RetryDelay:      time.Millisecond,
		Writer: evalcapture.Options{
			Capacity:    4,
			MaxAttempts: 1,
			RetryDelay:  -1,
			StorageFactory: func(policy evalcapture.Policy) evalcapture.Storage {
				storage.setPolicy(policy)
				return storage
			},
		},
	})
	projector := newTaskTraceProjector(settings, coordinator)
	projector.attach(workspace, registry)
	record := finishTaskForTrace(t, registry, "shutdown", "session", 1)
	select {
	case <-storage.started:
	case <-time.After(time.Second):
		t.Fatal("task trace did not reach storage")
	}
	projector.stop()
	closed := make(chan error, 1)
	go func() {
		admissionCtx, cancelAdmission := context.WithTimeout(
			context.Background(),
			time.Second,
		)
		defer cancelAdmission()
		closed <- coordinator.Close(admissionCtx, time.Second)
	}()
	close(storage.release)
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	waitForTraceMarkerCleared(
		t,
		registry,
		record.TaskID,
		record.GenerationID,
	)
	projector.finish()
}

func TestTaskTraceReconciliationDoesNotDowngradeCompleteTrace(t *testing.T) {
	workspace := t.TempDir()
	settings := traceCaptureSettingsFromConfig(traceTestConfig(workspace))
	record := taskregistry.Record{
		TaskID: "task", GenerationID: "generation", CreatedAt: 1,
		Status:         taskregistry.StatusSucceeded,
		DeliveryStatus: taskregistry.DeliveryDelivered,
		LastEventSeq:   2,
	}
	complete := buildTaskTrace(settings, workspace, record, []taskregistry.TaskEvent{
		taskTraceTestEvent(record, 1, taskregistry.EventTaskUpserted),
		taskTraceTestEvent(record, 2, taskregistry.EventTaskStatusChanged),
	})
	existing, _, err := prepareTrace(settings, complete)
	if err != nil {
		t.Fatal(err)
	}
	incomplete := buildTaskTrace(settings, workspace, record, []taskregistry.TaskEvent{
		taskTraceTestEvent(record, 2, taskregistry.EventTaskStatusChanged),
	})
	candidate, _, err := prepareTrace(settings, incomplete)
	if err != nil {
		t.Fatal(err)
	}
	selected, persist := reconcileTaskTraceCandidate(existing, candidate)
	if persist {
		t.Fatal("incomplete reconstruction replaced complete canonical trace")
	}
	if selected.TraceID != existing.TraceID ||
		len(selected.Records) != len(existing.Records) ||
		selected.Truncation.Incomplete {
		t.Fatalf("selected trace was downgraded: %+v", selected.Truncation)
	}
}

func TestTaskTraceJournalTruncationCountsMissingPrefixOnce(t *testing.T) {
	workspace := t.TempDir()
	settings := traceCaptureSettingsFromConfig(traceTestConfig(workspace))
	record := taskregistry.Record{
		TaskID: "task", GenerationID: "generation", CreatedAt: 1,
		Status: taskregistry.StatusSucceeded, LastEventSeq: 4,
		DeliveryStatus:      taskregistry.DeliveryDelivered,
		TraceCaptureDropped: 2,
	}
	trace, _, err := prepareTrace(
		settings,
		buildTaskTrace(settings, workspace, record, []taskregistry.TaskEvent{
			taskTraceTestEvent(record, 3, taskregistry.EventTaskProgress),
			taskTraceTestEvent(record, 4, taskregistry.EventTaskStatusChanged),
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !trace.Truncation.Incomplete ||
		trace.Truncation.DroppedRecords != 2 {
		t.Fatalf("truncation = %+v, want exactly 2 dropped records", trace.Truncation)
	}
}

func TestTaskTraceReconciliationPersistsNewerRevisionWithoutHistory(
	t *testing.T,
) {
	workspace := t.TempDir()
	settings := traceCaptureSettingsFromConfig(traceTestConfig(workspace))
	record := taskregistry.Record{
		TaskID: "task", GenerationID: "generation", CreatedAt: 1,
		Status:         taskregistry.StatusSucceeded,
		DeliveryStatus: taskregistry.DeliveryDelivered,
		LastEventSeq:   2,
	}
	existing, _, err := prepareTrace(
		settings,
		buildTaskTrace(settings, workspace, record, []taskregistry.TaskEvent{
			taskTraceTestEvent(record, 1, taskregistry.EventTaskUpserted),
			taskTraceTestEvent(record, 2, taskregistry.EventTaskStatusChanged),
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	record.LastEventSeq = 5
	candidate, _, err := prepareTrace(
		settings,
		buildTaskTrace(settings, workspace, record, nil),
	)
	if err != nil {
		t.Fatal(err)
	}
	selected, persist := reconcileTaskTraceCandidate(existing, candidate)
	if !persist {
		t.Fatal("newer projection revision was treated as already durable")
	}
	if selected.Metadata.ProjectionRevision != 5 ||
		!selected.Truncation.Incomplete {
		t.Fatalf(
			"selected revision=%d truncation=%+v",
			selected.Metadata.ProjectionRevision,
			selected.Truncation,
		)
	}
}

func newTaskTraceTestRuntime(
	t *testing.T,
	workspace string,
) (*traceCaptureManager, *runtimeevents.EventBus, *taskregistry.Registry) {
	t.Helper()
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(traceTestConfig(workspace), eventBus)
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	manager.attachTaskRegistry(workspace, registry)
	return manager, eventBus, registry
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

func waitForTraceMarkerCleared(
	t *testing.T,
	registry *taskregistry.Registry,
	taskID, generationID string,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		record := registryRecord(t, registry, taskID)
		if record.GenerationID != generationID {
			t.Fatalf(
				"task %q generation = %q, want %q",
				taskID,
				record.GenerationID,
				generationID,
			)
		}
		if !record.TraceCapturePending {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("task %q trace marker was not cleared", taskID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForTraceRevision(
	t *testing.T,
	path string,
	revision int64,
) evaltrace.Trace {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		trace := readCapturedTrace(t, path)
		if maxTaskTraceEventSequence(trace.Records) >= revision {
			return trace
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"trace revision = %d, want at least %d",
				maxTaskTraceEventSequence(trace.Records),
				revision,
			)
		}
		time.Sleep(10 * time.Millisecond)
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
		if payload.GenerationID != "" &&
			!slices.Contains(generations, payload.GenerationID) {
			generations = append(generations, payload.GenerationID)
		}
	}
	return generations
}

func deliveryStatuses(t *testing.T, trace evaltrace.Trace) []string {
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

func taskTraceTestEvent(
	record taskregistry.Record,
	sequence int64,
	eventType taskregistry.EventType,
) taskregistry.TaskEvent {
	return taskregistry.TaskEvent{
		EventID: fmt.Sprintf(
			"%s:%s:%06d:%s",
			record.TaskID,
			record.GenerationID,
			sequence,
			eventType,
		),
		TaskID: record.TaskID, GenerationID: record.GenerationID,
		Seq: sequence, EmittedAt: sequence,
		Type: eventType, Status: record.Status,
		DeliveryStatus: record.DeliveryStatus,
	}
}

type blockingTaskTraceStorage struct {
	once    sync.Once
	started chan struct{}
	release chan struct{}

	mu     sync.Mutex
	policy evalcapture.Policy
}

func newBlockingTaskTraceStorage() *blockingTaskTraceStorage {
	return &blockingTaskTraceStorage{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *blockingTaskTraceStorage) setPolicy(policy evalcapture.Policy) {
	s.mu.Lock()
	s.policy = policy
	s.mu.Unlock()
}

func (s *blockingTaskTraceStorage) Save(
	trace evaltrace.Trace,
) (string, error) {
	s.once.Do(func() { close(s.started) })
	<-s.release
	s.mu.Lock()
	policy := s.policy
	s.mu.Unlock()
	return (evaltrace.Store{
		Root: policy.Root, Retention: policy.Retention,
		MaxTraces: policy.MaxTraces,
	}).Save(trace)
}

func (s *blockingTaskTraceStorage) Prune() (int, error) {
	return 0, nil
}

func closeCoordinatorForTaskTraceTest(
	t *testing.T,
	coordinator *evalcapture.Coordinator,
) {
	t.Helper()
	admissionCtx, cancelAdmission := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancelAdmission()
	if err := coordinator.Close(admissionCtx, time.Second); err != nil {
		t.Fatal(err)
	}
}
