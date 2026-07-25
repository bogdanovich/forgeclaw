package evalcapture

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/evaltrace"
)

func TestCoordinatorPersistsAndConfirmsExactRevision(t *testing.T) {
	source := newCoordinatorSource()
	source.set("task", 7)
	storage := &coordinatorStorage{}
	coordinator := newTestCoordinator(t, 4, source, storage)

	if err := coordinator.Request("tasks", "task"); err != nil {
		t.Fatal(err)
	}
	waitCoordinator(t, func() bool {
		return source.confirmedRevision("task") == 7 &&
			coordinator.Stats().Pending == 0
	})
	if got := storage.savedRevisions(); len(got) != 1 || got[0] != 7 {
		t.Fatalf("saved revisions = %v, want [7]", got)
	}
}

func TestCoordinatorRecoveryScanConfirmsAlreadyDurableRevision(t *testing.T) {
	source := newCoordinatorSource()
	source.set("task", 5)
	source.setAlreadyDurable("task")
	storage := &coordinatorStorage{}
	coordinator := newTestCoordinator(t, 4, source, storage)

	waitCoordinator(t, func() bool {
		return source.confirmedRevision("task") == 5 &&
			coordinator.Stats().Pending == 0
	})
	if got := storage.savedRevisions(); len(got) != 0 {
		t.Fatalf("already-durable revision was rewritten: %v", got)
	}
}

func TestCoordinatorRetriesSourceLoadFailure(t *testing.T) {
	source := newCoordinatorSource()
	source.set("task", 6)
	source.loadFailures["task"] = 1
	storage := &coordinatorStorage{}
	coordinator := newTestCoordinator(t, 4, source, storage)

	waitCoordinator(t, func() bool {
		return source.confirmedRevision("task") == 6 &&
			coordinator.Stats().Pending == 0
	})
	if stats := coordinator.Stats(); stats.LoadFailures != 1 {
		t.Fatalf("load failures = %d, want 1", stats.LoadFailures)
	}
}

func TestCoordinatorRebuildsAfterStaleReceipt(t *testing.T) {
	source := newCoordinatorSource()
	source.set("task", 1)
	storage := newBlockingCoordinatorStorage()
	coordinator := newTestCoordinator(t, 4, source, storage)

	if err := coordinator.Request("tasks", "task"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-storage.started:
	case <-time.After(time.Second):
		t.Fatal("first revision did not reach storage")
	}
	source.set("task", 2)
	if err := coordinator.Request("tasks", "task"); err != nil {
		t.Fatal(err)
	}
	close(storage.release)

	waitCoordinator(t, func() bool {
		return source.confirmedRevision("task") == 2 &&
			coordinator.Stats().Pending == 0
	})
	if got := storage.savedRevisions(); !equalUint64s(got, []uint64{1, 2}) {
		t.Fatalf("saved revisions = %v, want [1 2]", got)
	}
}

func TestCoordinatorRetriesConfirmationWithoutRewritingTrace(t *testing.T) {
	source := newCoordinatorSource()
	source.set("task", 3)
	source.confirmFailures["task"] = 1
	storage := &coordinatorStorage{}
	coordinator := newTestCoordinator(t, 4, source, storage)

	if err := coordinator.Request("tasks", "task"); err != nil {
		t.Fatal(err)
	}
	waitCoordinator(t, func() bool {
		return source.confirmedRevision("task") == 3 &&
			coordinator.Stats().Pending == 0
	})
	if got := storage.savedRevisions(); len(got) != 1 || got[0] != 3 {
		t.Fatalf("confirmation retry rewrote trace: %v", got)
	}
	if stats := coordinator.Stats(); stats.ConfirmFailures != 1 {
		t.Fatalf("confirm failures = %d, want 1", stats.ConfirmFailures)
	}
}

func TestCoordinatorRecoversCapacityDeferredSourceKey(t *testing.T) {
	source := newCoordinatorSource()
	source.set("first", 1)
	source.set("second", 2)
	storage := newBlockingCoordinatorStorage()
	coordinator := newTestCoordinator(t, 1, source, storage)

	if err := coordinator.Request("tasks", "first"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-storage.started:
	case <-time.After(time.Second):
		t.Fatal("first key did not reach storage")
	}
	if err := coordinator.Request("tasks", "second"); err == nil {
		t.Fatal("capacity-deferred request succeeded")
	}
	close(storage.release)

	waitCoordinator(t, func() bool {
		return source.confirmedRevision("first") == 1 &&
			source.confirmedRevision("second") == 2 &&
			coordinator.Stats().Pending == 0
	})
	if stats := coordinator.Stats(); stats.OverflowDeferrals == 0 {
		t.Fatal("capacity deferral was not observable")
	}
}

func TestCoordinatorRetriesPermanentWriterFailure(t *testing.T) {
	source := newCoordinatorSource()
	source.set("task", 9)
	storage := &coordinatorStorage{saveFailures: 1}
	coordinator := newTestCoordinator(t, 4, source, storage)

	if err := coordinator.Request("tasks", "task"); err != nil {
		t.Fatal(err)
	}
	waitCoordinator(t, func() bool {
		return source.confirmedRevision("task") == 9 &&
			coordinator.Stats().Pending == 0
	})
	if got := storage.savedRevisions(); len(got) != 1 || got[0] != 9 {
		t.Fatalf("saved revisions = %v, want [9]", got)
	}
	if storage.attemptCount() != 2 {
		t.Fatalf("save attempts = %d, want 2", storage.attemptCount())
	}
}

func TestCoordinatorCloseLeavesUnfinishedSourceRecoverable(t *testing.T) {
	source := newCoordinatorSource()
	source.set("task", 11)
	storage := newBlockingCoordinatorStorage()
	coordinator := newTestCoordinatorWithoutCleanup(t, 4, source, storage)

	if err := coordinator.Request("tasks", "task"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-storage.started:
	case <-time.After(time.Second):
		t.Fatal("task did not reach storage")
	}
	admissionCtx, cancelAdmission := context.WithTimeout(
		context.Background(),
		20*time.Millisecond,
	)
	defer cancelAdmission()
	drainCtx, cancelDrain := context.WithTimeout(
		context.Background(),
		40*time.Millisecond,
	)
	defer cancelDrain()
	if err := coordinator.Close(admissionCtx, drainCtx); err == nil {
		t.Fatal("Close succeeded while storage was blocked")
	}
	if keys, err := source.Pending(context.Background(), 1); err != nil ||
		len(keys) != 1 || keys[0] != "task" {
		t.Fatalf("recoverable keys = %v, error = %v", keys, err)
	}
	close(storage.release)
}

func newTestCoordinator(
	t *testing.T,
	capacity int,
	source *coordinatorSource,
	storage Storage,
) *Coordinator {
	t.Helper()
	coordinator := newTestCoordinatorWithoutCleanup(t, capacity, source, storage)
	t.Cleanup(func() {
		admissionCtx, cancelAdmission := context.WithTimeout(
			context.Background(),
			time.Second,
		)
		defer cancelAdmission()
		drainCtx, cancelDrain := context.WithTimeout(context.Background(), time.Second)
		defer cancelDrain()
		if err := coordinator.Close(admissionCtx, drainCtx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return coordinator
}

func newTestCoordinatorWithoutCleanup(
	t *testing.T,
	capacity int,
	source *coordinatorSource,
	storage Storage,
) *Coordinator {
	t.Helper()
	coordinator := NewCoordinator(CoordinatorOptions{
		PendingCapacity: capacity,
		RetryDelay:      time.Millisecond,
		Writer: Options{
			Capacity:    capacity,
			MaxAttempts: 1,
			RetryDelay:  -1,
			StorageFactory: func(Policy) Storage {
				return storage
			},
		},
	})
	if err := coordinator.RegisterSource("tasks", source); err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func waitCoordinator(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("coordinator did not reach expected state")
		}
		time.Sleep(time.Millisecond)
	}
}

type coordinatorSourceRecord struct {
	revision  uint64
	confirmed uint64
	persist   bool
}

type coordinatorSource struct {
	mu              sync.Mutex
	records         map[string]coordinatorSourceRecord
	confirmFailures map[string]int
	loadFailures    map[string]int
}

func newCoordinatorSource() *coordinatorSource {
	return &coordinatorSource{
		records:         make(map[string]coordinatorSourceRecord),
		confirmFailures: make(map[string]int),
		loadFailures:    make(map[string]int),
	}
}

func (s *coordinatorSource) set(key string, revision uint64) {
	s.mu.Lock()
	record := s.records[key]
	record.revision = revision
	record.persist = true
	s.records[key] = record
	s.mu.Unlock()
}

func (s *coordinatorSource) setAlreadyDurable(key string) {
	s.mu.Lock()
	record := s.records[key]
	record.persist = false
	s.records[key] = record
	s.mu.Unlock()
}

func (s *coordinatorSource) confirmedRevision(key string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.records[key].confirmed
}

func (s *coordinatorSource) Pending(_ context.Context, limit int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.records))
	for key, record := range s.records {
		if record.confirmed < record.revision {
			keys = append(keys, key)
		}
	}
	sortStrings(keys)
	if len(keys) > limit {
		keys = keys[:limit]
	}
	return keys, nil
}

func (s *coordinatorSource) LoadLatest(
	_ context.Context,
	key string,
) (DurableCandidate, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadFailures[key] > 0 {
		s.loadFailures[key]--
		return DurableCandidate{}, false, errors.New("injected load failure")
	}
	record, ok := s.records[key]
	if !ok || record.confirmed >= record.revision {
		return DurableCandidate{}, false, nil
	}
	trace := testTraceForRevision(key, record.revision)
	return DurableCandidate{
		Revision: record.revision,
		Policy:   testPolicy(),
		Trace:    trace,
		Persist:  record.persist,
	}, true, nil
}

func (s *coordinatorSource) Confirm(
	_ context.Context,
	key string,
	revision uint64,
) (Confirmation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.confirmFailures[key] > 0 {
		s.confirmFailures[key]--
		return "", errors.New("injected confirmation failure")
	}
	record, ok := s.records[key]
	if !ok {
		return ConfirmationGone, nil
	}
	if record.revision != revision {
		return ConfirmationStale, nil
	}
	record.confirmed = revision
	s.records[key] = record
	return ConfirmationCurrent, nil
}

type coordinatorStorage struct {
	mu           sync.Mutex
	saveFailures int
	attempts     int
	revisions    []uint64
}

func (s *coordinatorStorage) Save(trace evaltrace.Trace) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	if s.saveFailures > 0 {
		s.saveFailures--
		return "", errors.New("injected save failure")
	}
	revision := traceRevision(trace)
	s.revisions = append(s.revisions, revision)
	return trace.TraceID, nil
}

func (s *coordinatorStorage) Prune() (int, error) {
	return 0, nil
}

func (s *coordinatorStorage) savedRevisions() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.revisions...)
}

func (s *coordinatorStorage) attemptCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

type blockingCoordinatorStorage struct {
	coordinatorStorage
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func newBlockingCoordinatorStorage() *blockingCoordinatorStorage {
	return &blockingCoordinatorStorage{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *blockingCoordinatorStorage) Save(trace evaltrace.Trace) (string, error) {
	s.once.Do(func() {
		close(s.started)
		<-s.release
	})
	return s.coordinatorStorage.Save(trace)
}

func testTraceForRevision(key string, revision uint64) evaltrace.Trace {
	trace := evaltrace.Trace{
		SchemaVersion: evaltrace.SchemaVersionV1,
		TraceID:       fmt.Sprintf("%s-%d", key, revision),
		CreatedAt:     time.Unix(1, 0).UTC(),
		Policy:        evaltrace.CapturePolicy{ContentMode: "metadata_only"},
		Limits:        evaltrace.AppliedLimits{MaxRecords: 8, MaxTraceBytes: 4096},
		Metadata:      evaltrace.Metadata{RuntimeID: fmt.Sprintf("revision:%d", revision)},
		Records:       []evaltrace.Record{},
	}
	finalized, err := evaltrace.Finalize(trace)
	if err != nil {
		panic(err)
	}
	return finalized
}

func traceRevision(trace evaltrace.Trace) uint64 {
	var revision uint64
	_, _ = fmt.Sscanf(trace.Metadata.RuntimeID, "revision:%d", &revision)
	return revision
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}

func equalUint64s(left, right []uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
