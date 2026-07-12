package tasks

import (
	"os"
	"path/filepath"
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
	badParent := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(badParent, []byte("blocked"), 0o600); err != nil {
		t.Fatal(err)
	}
	registry := NewRegistry(filepath.Join(badParent, "tasks.json"))
	called := false
	registry.SubscribeEvents(func(EventObservation) { called = true })
	if err := registry.Upsert(Record{TaskID: "task-1", Task: "test"}); err == nil {
		t.Fatal("expected persistence error")
	}
	if called {
		t.Fatal("observer called for failed append")
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
