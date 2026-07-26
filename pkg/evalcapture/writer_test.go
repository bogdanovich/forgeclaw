package evalcapture

import (
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
		if err := writer.Submit(
			Policy{Root: root, MaxTraces: 2, Retention: time.Hour},
			trace,
		); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	waitForWriter(t, writer, func(stats Stats) bool {
		return stats.Persisted == 3 && stats.Pruned == 1
	})
	writer.Close()

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
}

func TestWriterRetriesAndReportsPermanentFailure(t *testing.T) {
	store := &fakeStorage{saveFailures: 2}
	var events eventLog
	writer := NewWriter(Options{
		MaxAttempts: 3, RetryDelay: -1,
		StorageFactory: func(Policy) Storage { return store },
		EventSink:      events.append,
	})
	if err := writer.Submit(testPolicy(), testTrace(t, "trace-retry")); err != nil {
		t.Fatal(err)
	}
	waitForWriter(t, writer, func(stats Stats) bool { return stats.Persisted == 1 })
	if got := writer.Stats(); got.Retries != 2 || got.PermanentFailures != 0 {
		t.Fatalf("stats = %+v", got)
	}
	if events.count(EventRetrying) != 2 {
		t.Fatalf("events = %+v", events.snapshot())
	}
	writer.Close()

	store = &fakeStorage{saveFailures: 4}
	events = eventLog{}
	writer = NewWriter(Options{
		MaxAttempts: 2, RetryDelay: -1,
		StorageFactory: func(Policy) Storage { return store },
		EventSink:      events.append,
	})
	if err := writer.Submit(testPolicy(), testTrace(t, "trace-fail")); err != nil {
		t.Fatal(err)
	}
	waitForWriter(t, writer, func(stats Stats) bool {
		return stats.PermanentFailures == 1
	})
	if got := writer.Stats(); got.Retries != 1 || got.Persisted != 0 {
		t.Fatalf("failure stats = %+v", got)
	}
	if events.last().Kind != EventPermanentlyFailed {
		t.Fatalf("events = %+v", events.snapshot())
	}
	writer.Close()
}

func TestWriterDropsAtCapacityAndCloseNeverWaitsForStorage(t *testing.T) {
	store := newBlockingStorage()
	var events eventLog
	writer := NewWriter(Options{
		Capacity: 1, RetryDelay: -1,
		StorageFactory: func(Policy) Storage { return store },
		EventSink:      events.append,
	})
	if err := writer.Submit(testPolicy(), testTrace(t, "trace-active")); err != nil {
		t.Fatal(err)
	}
	<-store.started
	if err := writer.Submit(testPolicy(), testTrace(t, "trace-queued")); err != nil {
		t.Fatal(err)
	}
	err := writer.Submit(testPolicy(), testTrace(t, "trace-capacity"))
	var dropped *DropError
	if !errors.As(err, &dropped) || dropped.Reason != ReasonCapacity {
		t.Fatalf("Submit error = %v", err)
	}

	closed := make(chan struct{})
	go func() {
		writer.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Close waited for blocked storage")
	}
	if got := writer.Stats(); got.Dropped != 2 {
		t.Fatalf("stats after close = %+v", got)
	}
	if !events.contains(EventDropped, "trace-queued", ReasonShutdown) ||
		!events.contains(EventDropped, "trace-capacity", ReasonCapacity) {
		t.Fatalf("events = %+v", events.snapshot())
	}

	close(store.release)
	waitDone(t, writer)
	if got := store.savedIDs(); len(got) != 1 || got[0] != "trace-active" {
		t.Fatalf("saved = %v", got)
	}
}

func TestWriterCloseInterruptsRetryDelay(t *testing.T) {
	store := &fakeStorage{saveFailures: 10}
	retrying := make(chan struct{}, 1)
	writer := NewWriter(Options{
		MaxAttempts: 10, RetryDelay: time.Hour,
		StorageFactory: func(Policy) Storage { return store },
		EventSink: func(event Event) {
			if event.Kind == EventRetrying {
				select {
				case retrying <- struct{}{}:
				default:
				}
			}
		},
	})
	if err := writer.Submit(testPolicy(), testTrace(t, "trace-retry-stop")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-retrying:
	case <-time.After(time.Second):
		t.Fatal("writer did not enter retry delay")
	}
	writer.Close()
	waitDone(t, writer)
	if got := writer.Stats(); got.Dropped != 1 || got.Retries != 1 {
		t.Fatalf("stats = %+v", got)
	}
}

func TestWriterCloseStopsRetryBeforePublishingQueuedDrops(t *testing.T) {
	store := &fakeStorage{saveFailures: 10}
	retrying := make(chan struct{}, 1)
	dropping := make(chan struct{})
	releaseDrop := make(chan struct{})
	writer := NewWriter(Options{
		Capacity: 1, MaxAttempts: 10, RetryDelay: 20 * time.Millisecond,
		StorageFactory: func(Policy) Storage { return store },
		EventSink: func(event Event) {
			switch {
			case event.Kind == EventRetrying:
				select {
				case retrying <- struct{}{}:
				default:
				}
			case event.Kind == EventDropped && event.TraceID == "trace-queued":
				close(dropping)
				<-releaseDrop
			}
		},
	})
	if err := writer.Submit(testPolicy(), testTrace(t, "trace-retry")); err != nil {
		t.Fatal(err)
	}
	<-retrying
	if err := writer.Submit(testPolicy(), testTrace(t, "trace-queued")); err != nil {
		t.Fatal(err)
	}

	closed := make(chan struct{})
	go func() {
		writer.Close()
		close(closed)
	}()
	<-dropping
	time.Sleep(50 * time.Millisecond)
	store.mu.Lock()
	remainingFailures := store.saveFailures
	store.mu.Unlock()
	if remainingFailures != 9 {
		t.Fatalf("Save retried after shutdown began; remaining failures = %d", remainingFailures)
	}
	close(releaseDrop)
	<-closed
	waitDone(t, writer)
}

func TestWriterSnapshotsTraceWithoutHoldingRuntime(t *testing.T) {
	store := newBlockingStorage()
	writer := NewWriter(Options{
		Capacity: 1, RetryDelay: -1,
		StorageFactory: func(Policy) Storage { return store },
	})
	trace := testTrace(t, "trace-snapshot")
	if err := writer.Submit(testPolicy(), trace); err != nil {
		t.Fatal(err)
	}
	<-store.started
	trace.Records[0].Data[0] = 'x'
	trace.Truncation.Reasons = append(trace.Truncation.Reasons, "mutated")
	close(store.release)
	waitForWriter(t, writer, func(stats Stats) bool { return stats.Persisted == 1 })
	writer.Close()
	waitDone(t, writer)

	stored := store.savedTraces()
	if len(stored) != 1 || !json.Valid(stored[0].Records[0].Data) ||
		len(stored[0].Truncation.Reasons) != 0 {
		t.Fatalf("stored trace was mutated: %+v", stored)
	}
	err := writer.Submit(testPolicy(), testTrace(t, "trace-late"))
	var dropped *DropError
	if !errors.As(err, &dropped) || dropped.Reason != ReasonClosed {
		t.Fatalf("Submit after Close error = %v", err)
	}
}

func TestWriterDropsInvalidInputsAndContainsSinkPanic(t *testing.T) {
	writer := NewWriter(Options{RetryDelay: -1, EventSink: func(Event) { panic("sink") }})
	invalid := testTrace(t, "trace-invalid")
	invalid.Records[0].Digest = "tampered"
	var dropped *DropError
	err := writer.Submit(testPolicy(), invalid)
	if !errors.As(err, &dropped) || dropped.Reason != ReasonInvalidTrace {
		t.Fatalf("invalid trace error = %v", err)
	}
	err = writer.Submit(Policy{}, testTrace(t, "trace-policy"))
	if !errors.As(err, &dropped) || dropped.Reason != ReasonInvalidPolicy {
		t.Fatalf("invalid policy error = %v", err)
	}
	writer.Close()
	waitDone(t, writer)
}

func TestWriterReportsPruneFailure(t *testing.T) {
	store := &fakeStorage{pruneErr: errors.New("prune failed")}
	var events eventLog
	writer := NewWriter(Options{
		RetryDelay: -1, StorageFactory: func(Policy) Storage { return store },
		EventSink: events.append,
	})
	if err := writer.Submit(testPolicy(), testTrace(t, "trace-prune")); err != nil {
		t.Fatal(err)
	}
	waitForWriter(t, writer, func(stats Stats) bool {
		return stats.PruneFailures == 1
	})
	if !events.contains(EventPruneFailed, "trace-prune", ReasonRetentionFailed) {
		t.Fatalf("events = %+v", events.snapshot())
	}
	writer.Close()
}

func TestWriterReportsTruncatedSubmission(t *testing.T) {
	store := &fakeStorage{}
	var events eventLog
	writer := NewWriter(Options{
		RetryDelay: -1, StorageFactory: func(Policy) Storage { return store },
		EventSink: events.append,
	})
	trace := testTrace(t, "trace-truncated")
	trace.Truncation = evaltrace.Truncation{
		Incomplete: true, DroppedRecords: 3, Reasons: []string{"record_count_limit"},
	}
	if err := writer.Submit(testPolicy(), trace); err != nil {
		t.Fatal(err)
	}
	waitForWriter(t, writer, func(stats Stats) bool { return stats.Persisted == 1 })
	if !events.contains(EventTruncated, "trace-truncated", ReasonTraceIncomplete) {
		t.Fatalf("events = %+v", events.snapshot())
	}
	if writer.Stats().Truncated != 1 {
		t.Fatalf("stats = %+v", writer.Stats())
	}
	writer.Close()
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

func testPolicy() Policy {
	return Policy{Root: filepath.Join(os.TempDir(), "evalcapture-test")}
}

func waitForWriter(t *testing.T, writer *Writer, ready func(Stats) bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !ready(writer.Stats()) {
		if time.Now().After(deadline) {
			t.Fatalf("writer stats did not settle: %+v", writer.Stats())
		}
		time.Sleep(time.Millisecond)
	}
}

func waitDone(t *testing.T, writer *Writer) {
	t.Helper()
	select {
	case <-writer.done:
	case <-time.After(time.Second):
		t.Fatal("writer worker did not stop")
	}
}

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

type eventLog struct {
	mu     sync.Mutex
	events []Event
}

func (l *eventLog) append(event Event) {
	l.mu.Lock()
	l.events = append(l.events, event)
	l.mu.Unlock()
}

func (l *eventLog) snapshot() []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Event(nil), l.events...)
}

func (l *eventLog) count(kind EventKind) int {
	count := 0
	for _, event := range l.snapshot() {
		if event.Kind == kind {
			count++
		}
	}
	return count
}

func (l *eventLog) contains(kind EventKind, traceID string, reason Reason) bool {
	for _, event := range l.snapshot() {
		if event.Kind == kind && event.TraceID == traceID && event.Reason == reason {
			return true
		}
	}
	return false
}

func (l *eventLog) last() Event {
	events := l.snapshot()
	if len(events) == 0 {
		return Event{}
	}
	return events[len(events)-1]
}
