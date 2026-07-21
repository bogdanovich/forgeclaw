// Package evalcapture persists finalized evaluation traces independently of
// the runtime component that produced them.
package evalcapture

import (
	"context"
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

// Class controls admission when the bounded queue is full.
type Class string

const (
	ClassOrdinary Class = "ordinary"
	ClassCritical Class = "critical"
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
	EventRejected          EventKind = "rejected"
	EventEvicted           EventKind = "evicted"
	EventRetrying          EventKind = "retrying"
	EventPermanentlyFailed EventKind = "permanently_failed"
	EventPruneFailed       EventKind = "prune_failed"
	EventPruned            EventKind = "pruned"
	EventTruncated         EventKind = "truncated"
)

// Reason is a stable machine-readable admission or persistence reason.
type Reason string

const (
	ReasonClosed          Reason = "writer_closed"
	ReasonInvalidClass    Reason = "invalid_class"
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
	Class   Class
	Attempt int
	Removed int
	Dropped int
	Err     error
}

// EventSink receives typed operational events. Admission errors are also
// returned directly, so callers never depend on the sink for correctness.
type EventSink func(Event)

// AdmissionError reports why a trace was not accepted.
type AdmissionError struct {
	Reason  Reason
	TraceID string
	Class   Class
	Err     error
}

func (e *AdmissionError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("evaluation trace %s rejected: %s: %v", e.TraceID, e.Reason, e.Err)
	}
	return fmt.Sprintf("evaluation trace %s rejected: %s", e.TraceID, e.Reason)
}

func (e *AdmissionError) Unwrap() error { return e.Err }

// Storage is the persistence boundary used by Writer.
type Storage interface {
	Save(evaltrace.Trace) (string, error)
	Prune() (int, error)
}

// StorageFactory creates an isolated store for one submission policy.
type StorageFactory func(Policy) Storage

// Options configures bounded persistence behavior.
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
	class  Class
}

// Writer accepts finalized traces without waiting for filesystem I/O.
type Writer struct {
	mu       sync.Mutex
	queue    []submission
	closed   bool
	capacity int
	wake     chan struct{}
	done     chan struct{}

	maxAttempts int
	retryDelay  time.Duration
	eventSink   EventSink
	storage     StorageFactory
	stats       counters
}

type counters struct {
	acceptedOrdinary  atomic.Uint64
	acceptedCritical  atomic.Uint64
	rejectedOrdinary  atomic.Uint64
	rejectedCritical  atomic.Uint64
	evictedOrdinary   atomic.Uint64
	retries           atomic.Uint64
	persisted         atomic.Uint64
	permanentFailures atomic.Uint64
	pruneFailures     atomic.Uint64
	pruned            atomic.Uint64
	truncated         atomic.Uint64
}

// Stats is a consistent-enough monotonic operational snapshot.
type Stats struct {
	AcceptedOrdinary  uint64
	AcceptedCritical  uint64
	RejectedOrdinary  uint64
	RejectedCritical  uint64
	EvictedOrdinary   uint64
	Retries           uint64
	Persisted         uint64
	PermanentFailures uint64
	PruneFailures     uint64
	Pruned            uint64
	Truncated         uint64
}

// NewWriter starts a durable trace persistence worker.
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
		wake: make(chan struct{}, 1), done: make(chan struct{}),
		queue: make([]submission, 0, capacity),
	}
	go w.run()
	return w
}

// Submit snapshots and admits a finalized trace without waiting for persistence.
func (w *Writer) Submit(policy Policy, trace evaltrace.Trace, class Class) error {
	if w == nil {
		return &AdmissionError{Reason: ReasonClosed, TraceID: trace.TraceID, Class: class}
	}
	if class != ClassOrdinary && class != ClassCritical {
		return w.reject(trace.TraceID, class, ReasonInvalidClass, nil)
	}
	if strings.TrimSpace(policy.Root) == "" {
		return w.reject(trace.TraceID, class, ReasonInvalidPolicy, errors.New("store root is required"))
	}
	if err := evaltrace.Validate(trace); err != nil {
		return w.reject(trace.TraceID, class, ReasonInvalidTrace, err)
	}

	var evicted *submission
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return w.reject(trace.TraceID, class, ReasonClosed, nil)
	}
	if len(w.queue) >= w.capacity {
		if class == ClassCritical {
			for i := range w.queue {
				if w.queue[i].class == ClassOrdinary {
					item := w.queue[i]
					evicted = &item
					copy(w.queue[i:], w.queue[i+1:])
					w.queue = w.queue[:len(w.queue)-1]
					break
				}
			}
		}
		if len(w.queue) >= w.capacity {
			w.mu.Unlock()
			return w.reject(trace.TraceID, class, ReasonCapacity, nil)
		}
	}
	w.queue = append(w.queue, submission{policy: policy, trace: cloneTrace(trace), class: class})
	w.mu.Unlock()

	if class == ClassCritical {
		w.stats.acceptedCritical.Add(1)
	} else {
		w.stats.acceptedOrdinary.Add(1)
	}
	if evicted != nil {
		w.stats.evictedOrdinary.Add(1)
		w.emit(Event{Kind: EventEvicted, Reason: ReasonCapacity, TraceID: evicted.trace.TraceID, Class: evicted.class})
	}
	if trace.Truncation.Incomplete || trace.Truncation.DroppedRecords > 0 {
		w.stats.truncated.Add(1)
		w.emit(Event{
			Kind: EventTruncated, Reason: ReasonTraceIncomplete,
			TraceID: trace.TraceID, Class: class, Dropped: trace.Truncation.DroppedRecords,
		})
	}
	w.signal()
	return nil
}

func (w *Writer) reject(traceID string, class Class, reason Reason, err error) error {
	if class == ClassCritical {
		w.stats.rejectedCritical.Add(1)
	} else {
		w.stats.rejectedOrdinary.Add(1)
	}
	w.emit(Event{Kind: EventRejected, Reason: reason, TraceID: traceID, Class: class, Err: err})
	return &AdmissionError{Reason: reason, TraceID: traceID, Class: class, Err: err}
}

// Close stops admission and waits for every admitted trace to receive its
// bounded persistence attempts.
func (w *Writer) Close(ctx context.Context) error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.signal()
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stats returns monotonic admission and persistence counters.
func (w *Writer) Stats() Stats {
	if w == nil {
		return Stats{}
	}
	return Stats{
		AcceptedOrdinary: w.stats.acceptedOrdinary.Load(),
		AcceptedCritical: w.stats.acceptedCritical.Load(),
		RejectedOrdinary: w.stats.rejectedOrdinary.Load(),
		RejectedCritical: w.stats.rejectedCritical.Load(),
		EvictedOrdinary:  w.stats.evictedOrdinary.Load(),
		Retries:          w.stats.retries.Load(), Persisted: w.stats.persisted.Load(),
		PermanentFailures: w.stats.permanentFailures.Load(),
		PruneFailures:     w.stats.pruneFailures.Load(), Pruned: w.stats.pruned.Load(),
		Truncated: w.stats.truncated.Load(),
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
		if len(w.queue) > 0 {
			item := w.queue[0]
			copy(w.queue, w.queue[1:])
			w.queue = w.queue[:len(w.queue)-1]
			w.mu.Unlock()
			return item, true
		}
		closed := w.closed
		w.mu.Unlock()
		if closed {
			return submission{}, false
		}
		<-w.wake
	}
}

func (w *Writer) persist(item submission) {
	store := w.storage(item.policy)
	if store == nil {
		w.stats.permanentFailures.Add(1)
		w.emit(Event{
			Kind: EventPermanentlyFailed, Reason: ReasonStorageFailure,
			TraceID: item.trace.TraceID, Class: item.class,
			Err: errors.New("storage factory returned nil"),
		})
		return
	}
	for attempt := 1; attempt <= w.maxAttempts; attempt++ {
		_, err := store.Save(item.trace)
		if err == nil {
			w.stats.persisted.Add(1)
			w.prune(store, item)
			return
		}
		if attempt == w.maxAttempts {
			w.stats.permanentFailures.Add(1)
			w.emit(Event{
				Kind: EventPermanentlyFailed, Reason: ReasonStorageFailure,
				TraceID: item.trace.TraceID, Class: item.class, Attempt: attempt, Err: err,
			})
			return
		}
		w.stats.retries.Add(1)
		w.emit(Event{
			Kind: EventRetrying, Reason: ReasonStorageFailure,
			TraceID: item.trace.TraceID, Class: item.class, Attempt: attempt, Err: err,
		})
		if w.retryDelay > 0 {
			time.Sleep(w.retryDelay)
		}
	}
}

func (w *Writer) prune(store Storage, item submission) {
	removed, err := store.Prune()
	if err != nil {
		w.stats.pruneFailures.Add(1)
		w.emit(Event{
			Kind: EventPruneFailed, Reason: ReasonRetentionFailed,
			TraceID: item.trace.TraceID, Class: item.class, Err: err,
		})
		return
	}
	if removed > 0 {
		w.stats.pruned.Add(uint64(removed))
		w.emit(Event{Kind: EventPruned, TraceID: item.trace.TraceID, Class: item.class, Removed: removed})
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
		trace.Truncation.DroppedByKind = make(map[evaltrace.RecordKind]int, len(trace.Truncation.DroppedByKind))
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
