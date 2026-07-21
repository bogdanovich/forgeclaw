package evalcapture

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/evaltrace"
)

func TestWriterPersistsAndPrunesByPolicy(t *testing.T) {
	root := t.TempDir()
	writer := NewWriter(Options{RetryDelay: -1})
	for i := 1; i <= 3; i++ {
		trace := testTrace(t, "trace-"+string(rune('0'+i)))
		err := writer.Submit(
			Policy{Root: root, MaxTraces: 2, Retention: time.Hour},
			trace,
			ClassOrdinary,
		)
		if err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("stored traces = %d, want 2", len(entries))
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("trace mode = %o, want 600", info.Mode().Perm())
		}
	}
	if got := writer.Stats(); got.Persisted != 3 || got.Pruned != 1 {
		t.Fatalf("stats = %+v", got)
	}
}

func TestWriterRetriesAndReportsPermanentFailure(t *testing.T) {
	store := &fakeStorage{saveFailures: 2}
	var events []Event
	writer := NewWriter(Options{
		MaxAttempts: 3, RetryDelay: -1,
		StorageFactory: func(Policy) Storage { return store },
		EventSink:      func(event Event) { events = append(events, event) },
	})
	if err := writer.Submit(testPolicy(), testTrace(t, "trace-retry"), ClassCritical); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := writer.Stats(); got.Retries != 2 || got.Persisted != 1 || got.PermanentFailures != 0 {
		t.Fatalf("stats = %+v", got)
	}
	if countEvents(events, EventRetrying) != 2 {
		t.Fatalf("events = %+v", events)
	}

	store = &fakeStorage{saveFailures: 4}
	writer = NewWriter(Options{
		MaxAttempts: 2, RetryDelay: -1,
		StorageFactory: func(Policy) Storage { return store },
		EventSink:      func(event Event) { events = append(events, event) },
	})
	if err := writer.Submit(testPolicy(), testTrace(t, "trace-fail"), ClassCritical); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := writer.Stats(); got.Retries != 1 || got.PermanentFailures != 1 || got.Persisted != 0 {
		t.Fatalf("failure stats = %+v", got)
	}
	if events[len(events)-1].Kind != EventPermanentlyFailed {
		t.Fatalf("last event = %+v", events[len(events)-1])
	}
}

func TestCriticalAdmissionEvictsOnlyQueuedOrdinaryTrace(t *testing.T) {
	store := newBlockingStorage()
	var events []Event
	var eventsMu sync.Mutex
	writer := NewWriter(Options{
		Capacity: 1, RetryDelay: -1,
		StorageFactory: func(Policy) Storage { return store },
		EventSink: func(event Event) {
			eventsMu.Lock()
			events = append(events, event)
			eventsMu.Unlock()
		},
	})
	if err := writer.Submit(testPolicy(), testTrace(t, "trace-active"), ClassOrdinary); err != nil {
		t.Fatal(err)
	}
	<-store.started
	if err := writer.Submit(testPolicy(), testTrace(t, "trace-evicted"), ClassOrdinary); err != nil {
		t.Fatal(err)
	}
	if err := writer.Submit(testPolicy(), testTrace(t, "trace-critical"), ClassCritical); err != nil {
		t.Fatal(err)
	}
	close(store.release)
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := store.savedIDs(); !equalStrings(got, []string{"trace-active", "trace-critical"}) {
		t.Fatalf("saved = %v", got)
	}
	if got := writer.Stats(); got.EvictedOrdinary != 1 || got.AcceptedCritical != 1 {
		t.Fatalf("stats = %+v", got)
	}
	eventsMu.Lock()
	defer eventsMu.Unlock()
	if len(events) != 1 || events[0].Kind != EventEvicted || events[0].TraceID != "trace-evicted" {
		t.Fatalf("events = %+v", events)
	}
}

func TestWriterRejectsWhenQueueContainsOnlyCriticalTraces(t *testing.T) {
	store := newBlockingStorage()
	writer := NewWriter(Options{Capacity: 1, RetryDelay: -1, StorageFactory: func(Policy) Storage { return store }})
	if err := writer.Submit(testPolicy(), testTrace(t, "trace-active"), ClassCritical); err != nil {
		t.Fatal(err)
	}
	<-store.started
	if err := writer.Submit(testPolicy(), testTrace(t, "trace-queued"), ClassCritical); err != nil {
		t.Fatal(err)
	}
	err := writer.Submit(testPolicy(), testTrace(t, "trace-rejected"), ClassCritical)
	var admission *AdmissionError
	if !errors.As(err, &admission) || admission.Reason != ReasonCapacity {
		t.Fatalf("Submit error = %v", err)
	}
	close(store.release)
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := writer.Stats(); got.RejectedCritical != 1 || got.Persisted != 2 {
		t.Fatalf("stats = %+v", got)
	}
}

func TestWriterSnapshotsTraceAndDrainsAfterClose(t *testing.T) {
	store := newBlockingStorage()
	writer := NewWriter(Options{Capacity: 2, RetryDelay: -1, StorageFactory: func(Policy) Storage { return store }})
	active := testTrace(t, "trace-active")
	if err := writer.Submit(testPolicy(), active, ClassCritical); err != nil {
		t.Fatal(err)
	}
	<-store.started
	queued := testTrace(t, "trace-snapshot")
	if err := writer.Submit(testPolicy(), queued, ClassCritical); err != nil {
		t.Fatal(err)
	}
	queued.Records[0].Data[0] = 'x'
	queued.Truncation.Reasons = append(queued.Truncation.Reasons, "mutated")

	closeDone := make(chan error, 1)
	go func() { closeDone <- writer.Close(context.Background()) }()
	select {
	case <-closeDone:
		t.Fatal("Close returned before admitted persistence drained")
	case <-time.After(20 * time.Millisecond):
	}
	close(store.release)
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	stored := store.savedTraces()
	if len(stored) != 2 || !json.Valid(stored[1].Records[0].Data) || len(stored[1].Truncation.Reasons) != 0 {
		t.Fatalf("stored trace was mutated: %+v", stored)
	}
	if err := writer.Submit(testPolicy(), testTrace(t, "trace-late"), ClassCritical); err == nil {
		t.Fatal("Submit after Close succeeded")
	}
}

func TestWriterRejectsInvalidInputsAndContainsSinkPanic(t *testing.T) {
	writer := NewWriter(Options{RetryDelay: -1, EventSink: func(Event) { panic("sink") }})
	invalid := testTrace(t, "trace-invalid")
	invalid.Records[0].Digest = "tampered"
	var admission *AdmissionError
	err := writer.Submit(testPolicy(), invalid, ClassOrdinary)
	if !errors.As(err, &admission) || admission.Reason != ReasonInvalidTrace {
		t.Fatalf("invalid trace error = %v", err)
	}
	err = writer.Submit(Policy{}, testTrace(t, "trace-policy"), ClassOrdinary)
	if !errors.As(err, &admission) || admission.Reason != ReasonInvalidPolicy {
		t.Fatalf("invalid policy error = %v", err)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWriterReportsPruneFailure(t *testing.T) {
	store := &fakeStorage{pruneErr: errors.New("prune failed")}
	var got Event
	writer := NewWriter(Options{
		RetryDelay: -1, StorageFactory: func(Policy) Storage { return store },
		EventSink: func(event Event) { got = event },
	})
	if err := writer.Submit(testPolicy(), testTrace(t, "trace-prune"), ClassOrdinary); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got.Kind != EventPruneFailed || got.Reason != ReasonRetentionFailed {
		t.Fatalf("event = %+v", got)
	}
}

func TestWriterReportsTruncatedSubmission(t *testing.T) {
	store := &fakeStorage{}
	var got Event
	writer := NewWriter(Options{
		RetryDelay: -1, StorageFactory: func(Policy) Storage { return store },
		EventSink: func(event Event) { got = event },
	})
	trace := testTrace(t, "trace-truncated")
	trace.Truncation = evaltrace.Truncation{
		Incomplete: true, DroppedRecords: 3, Reasons: []string{"record_count_limit"},
	}
	if err := writer.Submit(testPolicy(), trace, ClassCritical); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got.Kind != EventTruncated || got.Reason != ReasonTraceIncomplete || got.Dropped != 3 {
		t.Fatalf("event = %+v", got)
	}
	if writer.Stats().Truncated != 1 {
		t.Fatalf("stats = %+v", writer.Stats())
	}
}

func testTrace(t *testing.T, id string) evaltrace.Trace {
	t.Helper()
	trace, err := evaltrace.Finalize(evaltrace.Trace{
		TraceID: id, CreatedAt: time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC),
		Policy: evaltrace.CapturePolicy{ContentMode: evaltrace.ContentMetadataOnly},
		Limits: evaltrace.DefaultLimits(),
		Records: []evaltrace.Record{{
			Sequence: 1, Kind: evaltrace.RecordTurnStart,
			Origin: evaltrace.Origin{Kind: "test", ID: id + "-event"},
			Data:   json.RawMessage(`{"status":"started"}`),
		}},
	})
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	return trace
}

func testPolicy() Policy { return Policy{Root: filepath.Join(os.TempDir(), "evalcapture-test")} }

type fakeStorage struct {
	mu           sync.Mutex
	saveFailures int
	pruneErr     error
	saved        []evaltrace.Trace
}

func (s *fakeStorage) Save(trace evaltrace.Trace) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveFailures > 0 {
		s.saveFailures--
		return "", errors.New("save failed")
	}
	s.saved = append(s.saved, trace)
	return trace.TraceID, nil
}

func (s *fakeStorage) Prune() (int, error) { return 0, s.pruneErr }

type blockingStorage struct {
	fakeStorage
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingStorage() *blockingStorage {
	return &blockingStorage{started: make(chan struct{}), release: make(chan struct{})}
}

func (s *blockingStorage) Save(trace evaltrace.Trace) (string, error) {
	s.once.Do(func() {
		close(s.started)
		<-s.release
	})
	return s.fakeStorage.Save(trace)
}

func (s *blockingStorage) savedIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]string, len(s.saved))
	for i := range s.saved {
		result[i] = s.saved[i].TraceID
	}
	return result
}

func (s *blockingStorage) savedTraces() []evaltrace.Trace {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]evaltrace.Trace(nil), s.saved...)
}

func countEvents(events []Event, kind EventKind) int {
	count := 0
	for _, event := range events {
		if event.Kind == kind {
			count++
		}
	}
	return count
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
