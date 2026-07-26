// Package evalcapture persists finalized evaluation traces independently of
// the runtime component that produced them.
package evalcapture

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sipeed/picoclaw/pkg/evaltrace"
)

const (
	defaultCapacity    = 128
	defaultMaxAttempts = 3
	defaultRetryDelay  = 100 * time.Millisecond
)

// Policy selects the isolated trace store and its pruning policy.
type Policy struct {
	Root      string
	Retention time.Duration
	MaxTraces int
}

// EventKind identifies an operational persistence outcome.
type EventKind string

const (
	EventDropped           EventKind = "dropped"
	EventRetrying          EventKind = "retrying"
	EventPersisted         EventKind = "persisted"
	EventPermanentlyFailed EventKind = "permanently_failed"
	EventPruneFailed       EventKind = "prune_failed"
	EventPruned            EventKind = "pruned"
	EventTruncated         EventKind = "truncated"
)

// Reason is a stable machine-readable drop or persistence reason.
type Reason string

const (
	ReasonClosed          Reason = "writer_closed"
	ReasonShutdown        Reason = "writer_shutdown"
	ReasonInvalidPolicy   Reason = "invalid_policy"
	ReasonInvalidTrace    Reason = "invalid_trace"
	ReasonCapacity        Reason = "capacity_exhausted"
	ReasonStorageFailure  Reason = "storage_failure"
	ReasonRetentionFailed Reason = "retention_failure"
	ReasonTraceIncomplete Reason = "trace_incomplete"
)

// Event reports an operational condition without exposing trace content.
// EventSink implementations must return promptly and must not call Close.
type Event struct {
	Kind    EventKind
	Reason  Reason
	TraceID string
	Attempt int
	Removed int
	Dropped int
	Err     error
}

type EventSink func(Event)

// DropError reports why a diagnostic trace was not accepted.
type DropError struct {
	Reason  Reason
	TraceID string
	Err     error
}

func (e *DropError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("evaluation trace %s dropped: %s: %v", e.TraceID, e.Reason, e.Err)
	}
	return fmt.Sprintf("evaluation trace %s dropped: %s", e.TraceID, e.Reason)
}

func (e *DropError) Unwrap() error { return e.Err }

// Storage is the persistence boundary used by Writer.
type Storage interface {
	Save(evaltrace.Trace) (string, error)
	Prune() (int, error)
}

type StorageFactory func(Policy) Storage

type Options struct {
	Capacity       int
	MaxAttempts    int
	RetryDelay     time.Duration
	EventSink      EventSink
	StorageFactory StorageFactory
}

type submission struct {
	policy Policy
	trace  evaltrace.Trace
}

// Writer accepts bounded best-effort diagnostics without waiting for
// filesystem I/O. Close stops admission and returns immediately.
type Writer struct {
	mu       sync.Mutex
	queue    []submission
	closed   bool
	capacity int
	wake     chan struct{}
	stop     chan struct{}
	stopOnce sync.Once
	done     chan struct{}

	maxAttempts int
	retryDelay  time.Duration
	eventSink   EventSink
	storage     StorageFactory
	stats       counters
}

type counters struct {
	accepted          atomic.Uint64
	dropped           atomic.Uint64
	retries           atomic.Uint64
	persisted         atomic.Uint64
	permanentFailures atomic.Uint64
	pruneFailures     atomic.Uint64
	pruned            atomic.Uint64
	truncated         atomic.Uint64
}

type Stats struct {
	Accepted          uint64
	Dropped           uint64
	Retries           uint64
	Persisted         uint64
	PermanentFailures uint64
	PruneFailures     uint64
	Pruned            uint64
	Truncated         uint64
}

func NewWriter(options Options) *Writer {
	capacity := options.Capacity
	if capacity <= 0 {
		capacity = defaultCapacity
	}
	maxAttempts := options.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxAttempts
	}
	retryDelay := options.RetryDelay
	if retryDelay < 0 {
		retryDelay = 0
	} else if retryDelay == 0 {
		retryDelay = defaultRetryDelay
	}
	storage := options.StorageFactory
	if storage == nil {
		storage = func(policy Policy) Storage {
			return evaltrace.Store{
				Root: policy.Root, Retention: policy.Retention, MaxTraces: policy.MaxTraces,
			}
		}
	}
	w := &Writer{
		capacity: capacity, maxAttempts: maxAttempts, retryDelay: retryDelay,
		eventSink: options.EventSink, storage: storage,
		wake:  make(chan struct{}, 1),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
		queue: make([]submission, 0, capacity),
	}
	go w.run()
	return w
}

// Submit snapshots a finalized trace and either admits or drops it immediately.
func (w *Writer) Submit(policy Policy, trace evaltrace.Trace) error {
	item, err := w.prepareSubmission(policy, trace)
	if err != nil {
		return err
	}
	if reason := w.tryAdmit(item); reason != "" {
		return w.drop(trace.TraceID, reason, nil)
	}
	w.stats.accepted.Add(1)
	if item.trace.Truncation.Incomplete || item.trace.Truncation.DroppedRecords > 0 {
		w.stats.truncated.Add(1)
		w.emit(Event{
			Kind: EventTruncated, Reason: ReasonTraceIncomplete,
			TraceID: item.trace.TraceID, Dropped: item.trace.Truncation.DroppedRecords,
		})
	}
	w.signal()
	return nil
}

func (w *Writer) prepareSubmission(
	policy Policy,
	trace evaltrace.Trace,
) (submission, error) {
	if w == nil {
		return submission{}, &DropError{Reason: ReasonClosed, TraceID: trace.TraceID}
	}
	if strings.TrimSpace(policy.Root) == "" {
		return submission{}, w.drop(
			trace.TraceID, ReasonInvalidPolicy, errors.New("store root is required"),
		)
	}
	if err := evaltrace.Validate(trace); err != nil {
		return submission{}, w.drop(trace.TraceID, ReasonInvalidTrace, err)
	}
	return submission{policy: policy, trace: cloneTrace(trace)}, nil
}

func (w *Writer) tryAdmit(item submission) Reason {
	if w == nil {
		return ReasonClosed
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ReasonClosed
	}
	if len(w.queue) >= w.capacity {
		return ReasonCapacity
	}
	w.queue = append(w.queue, item)
	return ""
}

func (w *Writer) drop(traceID string, reason Reason, err error) error {
	if w != nil {
		w.stats.dropped.Add(1)
		w.emit(Event{
			Kind: EventDropped, Reason: reason, TraceID: traceID, Err: err,
		})
	}
	return &DropError{Reason: reason, TraceID: traceID, Err: err}
}

// Close stops admission, reports queued traces as dropped, and returns without
// waiting for an in-flight filesystem operation.
func (w *Writer) Close() {
	if w == nil {
		return
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	dropped := w.queue
	w.queue = nil
	w.mu.Unlock()

	w.stopOnce.Do(func() { close(w.stop) })
	for _, item := range dropped {
		_ = w.drop(item.trace.TraceID, ReasonShutdown, nil)
	}
	w.signal()
}

func (w *Writer) Stats() Stats {
	if w == nil {
		return Stats{}
	}
	return Stats{
		Accepted:          w.stats.accepted.Load(),
		Dropped:           w.stats.dropped.Load(),
		Retries:           w.stats.retries.Load(),
		Persisted:         w.stats.persisted.Load(),
		PermanentFailures: w.stats.permanentFailures.Load(),
		PruneFailures:     w.stats.pruneFailures.Load(),
		Pruned:            w.stats.pruned.Load(),
		Truncated:         w.stats.truncated.Load(),
	}
}

func (w *Writer) signal() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *Writer) run() {
	defer close(w.done)
	for {
		item, ok := w.next()
		if !ok {
			return
		}
		w.persist(item)
	}
}

func (w *Writer) next() (submission, bool) {
	for {
		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			return submission{}, false
		}
		if len(w.queue) > 0 {
			item := w.queue[0]
			copy(w.queue, w.queue[1:])
			w.queue = w.queue[:len(w.queue)-1]
			w.mu.Unlock()
			return item, true
		}
		w.mu.Unlock()
		select {
		case <-w.stop:
			return submission{}, false
		case <-w.wake:
		}
	}
}

func (w *Writer) persist(item submission) {
	if w.stopped() {
		_ = w.drop(item.trace.TraceID, ReasonShutdown, nil)
		return
	}
	store := w.storage(item.policy)
	if store == nil {
		w.stats.permanentFailures.Add(1)
		w.emit(Event{
			Kind: EventPermanentlyFailed, Reason: ReasonStorageFailure,
			TraceID: item.trace.TraceID, Err: errors.New("storage factory returned nil"),
		})
		return
	}
	for attempt := 1; attempt <= w.maxAttempts; attempt++ {
		_, err := store.Save(item.trace)
		if err == nil {
			w.stats.persisted.Add(1)
			w.emit(Event{
				Kind: EventPersisted, TraceID: item.trace.TraceID, Attempt: attempt,
			})
			if !w.stopped() {
				w.prune(store, item)
			}
			return
		}
		if attempt == w.maxAttempts {
			w.stats.permanentFailures.Add(1)
			w.emit(Event{
				Kind: EventPermanentlyFailed, Reason: ReasonStorageFailure,
				TraceID: item.trace.TraceID, Attempt: attempt, Err: err,
			})
			return
		}
		w.stats.retries.Add(1)
		w.emit(Event{
			Kind: EventRetrying, Reason: ReasonStorageFailure,
			TraceID: item.trace.TraceID, Attempt: attempt, Err: err,
		})
		if !w.waitRetry() {
			_ = w.drop(item.trace.TraceID, ReasonShutdown, nil)
			return
		}
	}
}

func (w *Writer) waitRetry() bool {
	if w.retryDelay <= 0 {
		return !w.stopped()
	}
	timer := time.NewTimer(w.retryDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return !w.stopped()
	case <-w.stop:
		return false
	}
}

func (w *Writer) stopped() bool {
	if w == nil {
		return true
	}
	select {
	case <-w.stop:
		return true
	default:
		return false
	}
}

func (w *Writer) prune(store Storage, item submission) {
	removed, err := store.Prune()
	if err != nil {
		w.stats.pruneFailures.Add(1)
		w.emit(Event{
			Kind: EventPruneFailed, Reason: ReasonRetentionFailed,
			TraceID: item.trace.TraceID, Err: err,
		})
		return
	}
	if removed > 0 {
		w.stats.pruned.Add(uint64(removed))
		w.emit(Event{
			Kind: EventPruned, TraceID: item.trace.TraceID, Removed: removed,
		})
	}
}

func (w *Writer) emit(event Event) {
	if w != nil && w.eventSink != nil {
		func() {
			defer func() { _ = recover() }()
			w.eventSink(event)
		}()
	}
}

func cloneTrace(trace evaltrace.Trace) evaltrace.Trace {
	trace.Records = append([]evaltrace.Record(nil), trace.Records...)
	for i := range trace.Records {
		trace.Records[i].Data = append([]byte(nil), trace.Records[i].Data...)
	}
	trace.Corrections = append([]evaltrace.Correction(nil), trace.Corrections...)
	for i := range trace.Corrections {
		trace.Corrections[i].RecordRefs = append([]uint64(nil), trace.Corrections[i].RecordRefs...)
	}
	trace.Truncation.Reasons = append([]string(nil), trace.Truncation.Reasons...)
	if trace.Truncation.DroppedByKind != nil {
		trace.Truncation.DroppedByKind = make(
			map[evaltrace.RecordKind]int,
			len(trace.Truncation.DroppedByKind),
		)
		for kind, count := range trace.Truncation.DroppedByKind {
			trace.Truncation.DroppedByKind[kind] = count
		}
	}
	if trace.Outcome != nil {
		outcome := *trace.Outcome
		trace.Outcome = &outcome
	}
	return trace
}
