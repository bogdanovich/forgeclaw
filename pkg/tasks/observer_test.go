package tasks

import (
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

func TestRegistryEventObserverRunsAfterDurableWriteAndCanUnsubscribe(t *testing.T) {
	store := filepath.Join(t.TempDir(), "tasks.json")
	registry := NewRegistry(store)
	observations := make(chan EventObservation, 4)
	unsubscribe := registry.SubscribeEvents(func(observation EventObservation) {
		reloaded := NewRegistry(store)
		if len(reloaded.ListEvents(observation.Event.TaskID)) == 0 {
			t.Error("observer ran before durable event write")
		}
		observations <- observation
	})
	if err := registry.Upsert(Record{TaskID: "task-1", Task: "test"}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	first := <-observations
	if first.Event.Type != EventTaskUpserted || first.Record.TaskID != "task-1" {
		t.Fatalf("observation = %#v", first)
	}
	unsubscribe()
	if err := registry.Update("task-1", func(record *Record) { record.Status = StatusSucceeded }); err != nil {
		t.Fatalf("Update: %v", err)
	}
	select {
	case observation := <-observations:
		t.Fatalf("observer ran after unsubscribe: %#v", observation)
	default:
	}
}

func TestRegistryDoesNotNotifyObserverWhenPersistenceFails(t *testing.T) {
	registry := NewRegistry(filepath.Join(t.TempDir(), "initial", "tasks.json"))
	badParent := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(badParent, []byte("blocked"), 0o600); err != nil {
		t.Fatal(err)
	}
	registry.store = filepath.Join(badParent, "tasks.json")
	called := false
	registry.SubscribeEvents(func(EventObservation) { called = true })
	if err := registry.Upsert(Record{TaskID: "task-1", Task: "test"}); err == nil {
		t.Fatal("expected persistence error")
	}
	if called {
		t.Fatal("observer called for failed append")
	}
	if records := registry.List(); len(records) != 0 {
		t.Fatalf("failed write left records in memory: %#v", records)
	}
	if events := registry.ListEvents(""); len(events) != 0 {
		t.Fatalf("failed write left events in memory: %#v", events)
	}
	snapshot, activate, unsubscribe := registry.SubscribeSnapshot(func(observation EventObservation) {
		if observation.Event.TaskID == "task-1" {
			t.Error("failed write became a live observation")
		}
	})
	t.Cleanup(unsubscribe)
	if len(snapshot.Records) != 0 || len(snapshot.Events) != 0 {
		t.Fatalf("failed write leaked through snapshot: %#v", snapshot)
	}
	activate()
	registry.store = filepath.Join(t.TempDir(), "recovered", "tasks.json")
	if err := registry.Upsert(Record{TaskID: "task-2", Task: "test"}); err != nil {
		t.Fatal(err)
	}
	if _, exists := registry.Get("task-1"); exists {
		t.Fatal("failed task became durable after later successful write")
	}
}

func TestRegistryRollsBackNestedUpdateAfterPersistenceFailure(t *testing.T) {
	store := filepath.Join(t.TempDir(), "tasks.json")
	registry := NewRegistry(store)
	if err := registry.Upsert(Record{
		TaskID: "existing", Task: "test", Status: StatusRunning,
		Deliverable: &DeliverablePayload{Metadata: map[string]string{"state": "original"}},
	}); err != nil {
		t.Fatal(err)
	}
	badParent := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(badParent, []byte("blocked"), 0o600); err != nil {
		t.Fatal(err)
	}
	registry.store = filepath.Join(badParent, "tasks.json")
	if err := registry.Update("existing", func(record *Record) {
		record.Status = StatusSucceeded
		record.Deliverable.Metadata["state"] = "speculative"
	}); err == nil {
		t.Fatal("expected persistence error")
	}
	record, _ := registry.Get("existing")
	events := registry.ListEvents("existing")
	if record.Status != StatusRunning || record.Deliverable.Metadata["state"] != "original" ||
		len(events) != 1 || events[0].Type != EventTaskUpserted {
		t.Fatalf("state after rollback: record=%#v events=%#v", record, events)
	}
}

func TestRegistryObserverPanicDoesNotFailDurableUpdate(t *testing.T) {
	registry := NewRegistry(filepath.Join(t.TempDir(), "tasks.json"))
	registry.SubscribeEvents(func(EventObservation) { panic("observer failed") })
	if err := registry.Upsert(Record{TaskID: "task-1", Task: "test"}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	reloaded := NewRegistry(registry.store)
	if _, ok := reloaded.Get("task-1"); !ok {
		t.Fatal("durable update was lost after observer panic")
	}
}

func TestRegistrySubscribeSnapshotCreatesAtomicBoundary(t *testing.T) {
	registry := NewRegistry(filepath.Join(t.TempDir(), "tasks.json"))
	if err := registry.Upsert(Record{
		TaskID: "before", Task: "test", TaskKind: "fixture",
		Deliverable: &DeliverablePayload{Metadata: map[string]string{"source": "original"}},
	}); err != nil {
		t.Fatal(err)
	}
	var observed []EventObservation
	snapshot, activate, unsubscribe := registry.SubscribeSnapshot(func(observation EventObservation) {
		observed = append(observed, observation)
	})
	t.Cleanup(unsubscribe)
	if len(snapshot.Records) != 1 || snapshot.Records[0].TaskID != "before" ||
		len(snapshot.Events) != 1 || snapshot.Events[0].Type != EventTaskUpserted {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if len(observed) != 0 {
		t.Fatalf("snapshot event was also delivered live: %#v", observed)
	}
	activate()

	snapshot.Records[0].Deliverable.Metadata["source"] = "mutated"
	snapshot.Events[0].Payload["task_kind"] = "mutated"
	stored, _ := registry.Get("before")
	storedEvents := registry.ListEvents("before")
	if stored.Deliverable.Metadata["source"] != "original" ||
		storedEvents[0].Payload["task_kind"] == "mutated" {
		t.Fatal("snapshot mutation changed registry state")
	}

	if err := registry.AppendEvent("before", EventTaskProgress, nil); err != nil {
		t.Fatal(err)
	}
	if len(observed) != 1 || observed[0].Event.Type != EventTaskProgress {
		t.Fatalf("post-boundary observations = %#v", observed)
	}
}

func TestRegistrySubscribeSnapshotExcludesQueuedPreBoundaryEvents(t *testing.T) {
	registry := NewRegistry("")
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	registry.SubscribeEvents(func(observation EventObservation) {
		if observation.Event.TaskID == "first" {
			close(firstEntered)
			<-releaseFirst
		}
	})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- registry.Upsert(Record{TaskID: "first", Task: "test"})
	}()
	<-firstEntered
	if err := registry.Upsert(Record{TaskID: "second", Task: "test"}); err != nil {
		t.Fatal(err)
	}
	var observed []string
	snapshot, activate, unsubscribe := registry.SubscribeSnapshot(func(observation EventObservation) {
		observed = append(observed, observation.Event.TaskID)
	})
	t.Cleanup(unsubscribe)
	if len(snapshot.Records) != 2 || len(snapshot.Events) != 2 {
		t.Fatalf("snapshot boundary = %#v", snapshot)
	}
	if err := registry.Upsert(Record{TaskID: "third", Task: "test"}); err != nil {
		t.Fatal(err)
	}
	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if len(observed) != 0 {
		t.Fatalf("pre-activation observations = %#v", observed)
	}
	activate()
	if len(observed) != 1 || observed[0] != "third" {
		t.Fatalf("post-boundary observations = %#v", observed)
	}
}

func TestRegistrySubscribeSnapshotPreservesGenerationSequenceAcrossClockRollback(t *testing.T) {
	registry := NewRegistry("")
	if err := registry.Upsert(Record{TaskID: "clock", Task: "test"}); err != nil {
		t.Fatal(err)
	}
	if err := registry.AppendEvent("clock", EventTaskProgress, nil); err != nil {
		t.Fatal(err)
	}
	registry.mu.Lock()
	registry.events[0].EmittedAt = 200
	registry.events[1].EmittedAt = 100
	registry.mu.Unlock()

	snapshot, activate, unsubscribe := registry.SubscribeSnapshot(func(EventObservation) {})
	t.Cleanup(unsubscribe)
	activate()
	if len(snapshot.Events) != 2 || snapshot.Events[0].Seq != 1 || snapshot.Events[1].Seq != 2 {
		t.Fatalf("snapshot events = %#v", snapshot.Events)
	}
}

func TestRegistrySubscribeSnapshotActivationSerializesConcurrentCommits(t *testing.T) {
	registry := NewRegistry("")
	if err := registry.Upsert(Record{TaskID: "concurrent", Task: "test"}); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var observed []int64
	_, activate, unsubscribe := registry.SubscribeSnapshot(func(observation EventObservation) {
		mu.Lock()
		observed = append(observed, observation.Event.Seq)
		mu.Unlock()
	})
	t.Cleanup(unsubscribe)

	const commits = 32
	start := make(chan struct{})
	errors := make(chan error, commits)
	var writers sync.WaitGroup
	for range commits {
		writers.Add(1)
		go func() {
			defer writers.Done()
			<-start
			errors <- registry.AppendEvent("concurrent", EventTaskProgress, nil)
		}()
	}
	close(start)
	activate()
	writers.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(observed) != commits {
		t.Fatalf("observed %d commits, want %d: %v", len(observed), commits, observed)
	}
	if !sort.SliceIsSorted(observed, func(i, j int) bool { return observed[i] < observed[j] }) {
		t.Fatalf("observations reordered across activation: %v", observed)
	}
	for i, sequence := range observed {
		if want := int64(i + 2); sequence != want {
			t.Fatalf("observation %d sequence = %d, want %d: %v", i, sequence, want, observed)
		}
	}
}

func TestRegistrySerializesCommittedObservationSnapshots(t *testing.T) {
	registry := NewRegistry("")
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	var mu sync.Mutex
	var observed []EventObservation
	registry.SubscribeEvents(func(observation EventObservation) {
		if observation.Event.Seq == 1 {
			close(firstEntered)
			<-releaseFirst
		}
		mu.Lock()
		observed = append(observed, observation)
		mu.Unlock()
	})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- registry.Upsert(Record{
			TaskID: "ordered", Task: "test", Status: StatusRunning,
		})
	}()
	<-firstEntered
	if err := registry.Update("ordered", func(record *Record) {
		record.Status = StatusSucceeded
	}); err != nil {
		t.Fatal(err)
	}
	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(observed) != 2 {
		t.Fatalf("observations = %#v", observed)
	}
	if observed[0].Event.Seq != 1 || observed[0].Record.Status != StatusRunning ||
		observed[1].Event.Seq != 2 || observed[1].Record.Status != StatusSucceeded {
		t.Fatalf("ordered snapshots = %#v", observed)
	}
}

func TestRegistryObserverCanReenterWithoutReordering(t *testing.T) {
	registry := NewRegistry("")
	var observed []TaskEvent
	registry.SubscribeEvents(func(observation EventObservation) {
		observed = append(observed, observation.Event)
		if observation.Event.Type == EventTaskUpserted {
			if err := registry.Update(observation.Event.TaskID, func(record *Record) {
				record.Status = StatusSucceeded
			}); err != nil {
				t.Errorf("reentrant Update: %v", err)
			}
		}
	})
	if err := registry.Upsert(Record{
		TaskID: "reentrant", Task: "test", Status: StatusRunning,
	}); err != nil {
		t.Fatal(err)
	}
	if len(observed) != 2 || observed[0].Seq != 1 || observed[0].Type != EventTaskUpserted ||
		observed[1].Seq != 2 || observed[1].Type != EventTaskStatusChanged {
		t.Fatalf("reentrant observations = %#v", observed)
	}
}
