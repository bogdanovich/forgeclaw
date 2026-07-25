package evalcapture

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/evaltrace"
)

const (
	defaultProjectionCapacity   = 128
	defaultProjectionRetryDelay = 100 * time.Millisecond
	minimumProjectionRetryDelay = time.Millisecond
)

// Confirmation describes the result of atomically acknowledging one source
// revision after its trace has been persisted.
type Confirmation string

const (
	ConfirmationCurrent Confirmation = "current"
	ConfirmationStale   Confirmation = "stale"
	ConfirmationGone    Confirmation = "gone"
)

// DurableCandidate is an immutable projection of one exact source revision.
// LoadLatest may return Persist=false when the canonical trace already contains
// that revision; the coordinator still performs the source confirmation.
type DurableCandidate struct {
	Revision uint64
	Policy   Policy
	Trace    evaltrace.Trace
	Persist  bool
}

// DurableSource remains the authority for domain identity, revision, recovery,
// and compare-and-set confirmation. Implementations must return opaque keys
// from Pending and must not include secrets in errors.
type DurableSource interface {
	Pending(ctx context.Context, limit int) ([]string, error)
	LoadLatest(ctx context.Context, key string) (DurableCandidate, bool, error)
	Confirm(ctx context.Context, key string, revision uint64) (Confirmation, error)
}

// CoordinatorOptions configure the shared durable projection lifecycle.
type CoordinatorOptions struct {
	PendingCapacity int
	RetryDelay      time.Duration
	Writer          Options
}

// CoordinatorStats is a monotonic operational snapshot.
type CoordinatorStats struct {
	Pending           int
	Inflight          int
	OverflowDeferrals uint64
	LoadFailures      uint64
	ConfirmFailures   uint64
}

type projectionPhase uint8

const (
	projectionNeedsLoad projectionPhase = iota
	projectionAwaitingWrite
	projectionNeedsConfirm
)

type projectionID struct {
	source string
	key    string
}

type projectionState struct {
	phase      projectionPhase
	revision   uint64
	receipt    string
	retryAt    time.Time
	generation uint64
}

// Coordinator owns the writer and every persistence lifecycle concern shared
// by durable task and interaction projections. It stores no domain history.
type Coordinator struct {
	mu       sync.Mutex
	closed   bool
	stopping bool

	capacity   int
	retryDelay time.Duration
	sources    map[string]DurableSource
	recoverAt  map[string]time.Time
	states     map[projectionID]*projectionState
	receipts   map[string]projectionID
	next       uint64
	scans      int

	overflowDeferrals uint64
	loadFailures      uint64
	confirmFailures   uint64

	writer *Writer
	opCtx  context.Context
	cancel context.CancelFunc
	wake   chan struct{}
	done   chan struct{}
	idle   chan struct{}
}

// NewCoordinator starts one shared projection worker and its trace writer.
func NewCoordinator(options CoordinatorOptions) *Coordinator {
	capacity := options.PendingCapacity
	if capacity <= 0 {
		capacity = defaultProjectionCapacity
	}
	retryDelay := options.RetryDelay
	if retryDelay == 0 {
		retryDelay = defaultProjectionRetryDelay
	} else if retryDelay < minimumProjectionRetryDelay {
		retryDelay = minimumProjectionRetryDelay
	}
	opCtx, cancel := context.WithCancel(context.Background())
	c := &Coordinator{
		capacity: capacity, retryDelay: retryDelay,
		sources:   make(map[string]DurableSource),
		recoverAt: make(map[string]time.Time),
		states:    make(map[projectionID]*projectionState),
		receipts:  make(map[string]projectionID),
		opCtx:     opCtx,
		cancel:    cancel,
		wake:      make(chan struct{}, 1),
		done:      make(chan struct{}),
		idle:      make(chan struct{}, 1),
	}
	writerOptions := options.Writer
	externalSink := writerOptions.EventSink
	writerOptions.EventSink = func(event Event) {
		c.observeWriterEvent(event)
		if externalSink != nil {
			externalSink(event)
		}
	}
	c.writer = NewWriter(writerOptions)
	go c.run()
	return c
}

// RegisterSource installs an opaque durable source and schedules a recovery
// scan. Source IDs are process-local and are never persisted. Reconfiguration
// must explicitly unregister first so an in-flight receipt is never silently
// rebound to another authority.
func (c *Coordinator) RegisterSource(sourceID string, source DurableSource) error {
	if c == nil || source == nil {
		return errors.New("durable projection source is required")
	}
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return errors.New("durable projection source ID is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopping || c.closed {
		return errors.New("durable projection coordinator is closed")
	}
	if c.sources[sourceID] != nil {
		return fmt.Errorf("durable projection source %q is already registered", sourceID)
	}
	c.sources[sourceID] = source
	c.recoverAt[sourceID] = time.Time{}
	c.signalLocked()
	return nil
}

// UnregisterSource stops new loading from a source. Existing in-flight writer
// receipts remain harmless; their source marker is left for later recovery.
func (c *Coordinator) UnregisterSource(sourceID string) {
	if c == nil {
		return
	}
	sourceID = strings.TrimSpace(sourceID)
	c.mu.Lock()
	delete(c.sources, sourceID)
	delete(c.recoverAt, sourceID)
	c.removeSourceStatesLocked(sourceID)
	c.notifyIdleLocked()
	c.mu.Unlock()
}

// Request schedules the latest durable revision for one opaque source key.
// Capacity rejection is observable but safe: the source marker remains the
// recovery spool and Pending will be scanned again.
func (c *Coordinator) Request(sourceID, key string) error {
	if c == nil {
		return errors.New("durable projection coordinator is required")
	}
	sourceID, key = strings.TrimSpace(sourceID), strings.TrimSpace(key)
	if sourceID == "" || key == "" {
		return errors.New("durable projection source and key are required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopping || c.closed {
		return errors.New("durable projection coordinator is closed")
	}
	if c.sources[sourceID] == nil {
		return fmt.Errorf("durable projection source %q is not registered", sourceID)
	}
	id := projectionID{source: sourceID, key: key}
	if state := c.states[id]; state != nil {
		c.signalLocked()
		return nil
	}
	if len(c.states) >= c.capacity {
		c.overflowDeferrals++
		c.recoverAt[sourceID] = time.Time{}
		c.signalLocked()
		return &AdmissionError{
			Reason: ReasonCapacity, TraceID: "durable_projection",
			Class: ClassCritical,
		}
	}
	c.states[id] = &projectionState{phase: projectionNeedsLoad, generation: 1}
	c.signalLocked()
	return nil
}

// Submit admits a process-local trace through the coordinator-owned writer.
func (c *Coordinator) Submit(policy Policy, trace evaltrace.Trace, class Class) error {
	if c == nil {
		return &AdmissionError{Reason: ReasonClosed, TraceID: trace.TraceID, Class: class}
	}
	c.mu.Lock()
	writer, closed := c.writer, c.stopping || c.closed
	c.mu.Unlock()
	if closed || writer == nil {
		return &AdmissionError{Reason: ReasonClosed, TraceID: trace.TraceID, Class: class}
	}
	return writer.Submit(policy, trace, class)
}

// Stats returns bounded lifecycle and monotonic failure accounting.
func (c *Coordinator) Stats() CoordinatorStats {
	if c == nil {
		return CoordinatorStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	inflight := 0
	for _, state := range c.states {
		if state.phase == projectionAwaitingWrite {
			inflight++
		}
	}
	return CoordinatorStats{
		Pending: len(c.states), Inflight: inflight,
		OverflowDeferrals: c.overflowDeferrals,
		LoadFailures:      c.loadFailures,
		ConfirmFailures:   c.confirmFailures,
	}
}

// Close stops new work, gives durable sources a bounded opportunity to reach
// writer admission and confirmation, drains the shared writer with a separate
// deadline, then leaves any unfinished source markers for restart recovery.
func (c *Coordinator) Close(admissionCtx, drainCtx context.Context) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.stopping = true
	for sourceID := range c.sources {
		c.recoverAt[sourceID] = time.Time{}
	}
	c.signalLocked()
	c.mu.Unlock()

	cancelAtDrainDeadline := make(chan struct{})
	go func() {
		select {
		case <-drainCtx.Done():
			c.cancel()
		case <-cancelAtDrainDeadline:
		}
	}()
	admissionErr := c.waitIdle(admissionCtx)
	writerErr := c.writer.Close(drainCtx)
	c.signal()
	confirmErr := c.waitIdle(drainCtx)
	close(cancelAtDrainDeadline)
	c.cancel()

	c.mu.Lock()
	c.closed = true
	c.signalLocked()
	c.mu.Unlock()
	var workerErr error
	select {
	case <-c.done:
	case <-drainCtx.Done():
		workerErr = drainCtx.Err()
	}
	return errors.Join(admissionErr, writerErr, confirmErr, workerErr)
}

func (c *Coordinator) run() {
	defer close(c.done)
	for {
		wait := c.processAvailable()
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()
		if wait <= 0 {
			<-c.wake
			continue
		}
		timer := time.NewTimer(wait)
		select {
		case <-c.wake:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		}
	}
}

func (c *Coordinator) processAvailable() time.Duration {
	for {
		id, state, source, ok, next := c.nextWork()
		if !ok {
			return next
		}
		switch state.phase {
		case projectionNeedsLoad:
			c.processLoad(id, state, source)
		case projectionNeedsConfirm:
			c.processConfirm(id, state, source)
		}
	}
}

func (c *Coordinator) nextWork() (
	projectionID,
	projectionState,
	DurableSource,
	bool,
	time.Duration,
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	ids := make([]projectionID, 0, len(c.states))
	for id := range c.states {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		if ids[i].source != ids[j].source {
			return ids[i].source < ids[j].source
		}
		return ids[i].key < ids[j].key
	})
	var earliest time.Time
	for _, id := range ids {
		state := c.states[id]
		if state.phase == projectionAwaitingWrite {
			continue
		}
		if !state.retryAt.IsZero() && state.retryAt.After(now) {
			if earliest.IsZero() || state.retryAt.Before(earliest) {
				earliest = state.retryAt
			}
			continue
		}
		source := c.sources[id.source]
		if source == nil {
			delete(c.states, id)
			continue
		}
		return id, *state, source, true, 0
	}
	if len(c.states) < c.capacity && c.scans == 0 {
		sourceIDs := make([]string, 0, len(c.recoverAt))
		for sourceID, retryAt := range c.recoverAt {
			if c.sources[sourceID] == nil {
				continue
			}
			if retryAt.IsZero() || !retryAt.After(now) {
				sourceIDs = append(sourceIDs, sourceID)
			} else if earliest.IsZero() || retryAt.Before(earliest) {
				earliest = retryAt
			}
		}
		sort.Strings(sourceIDs)
		if len(sourceIDs) > 0 {
			sourceID := sourceIDs[0]
			delete(c.recoverAt, sourceID)
			c.scans++
			go c.scanSource(sourceID)
		}
	}
	c.notifyIdleLocked()
	if earliest.IsZero() {
		return projectionID{}, projectionState{}, nil, false, 0
	}
	return projectionID{}, projectionState{}, nil, false, time.Until(earliest)
}

func (c *Coordinator) scanSource(sourceID string) {
	defer func() {
		c.mu.Lock()
		c.scans--
		c.signalLocked()
		c.notifyIdleLocked()
		c.mu.Unlock()
	}()
	c.mu.Lock()
	source := c.sources[sourceID]
	limit := c.capacity - len(c.states)
	stopping := c.stopping
	c.mu.Unlock()
	if source == nil || limit <= 0 {
		return
	}
	keys, err := source.Pending(c.opCtx, limit)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.sources[sourceID] != source {
		return
	}
	if err != nil {
		c.loadFailures++
		c.recoverAt[sourceID] = time.Now().Add(c.retryDelay)
		c.signalLocked()
		return
	}
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		id := projectionID{source: sourceID, key: key}
		if c.states[id] != nil {
			continue
		}
		if len(c.states) >= c.capacity {
			c.overflowDeferrals++
			c.recoverAt[sourceID] = time.Time{}
			break
		}
		c.states[id] = &projectionState{phase: projectionNeedsLoad, generation: 1}
	}
	if len(keys) >= limit && !stopping {
		c.recoverAt[sourceID] = time.Time{}
	}
	c.signalLocked()
}

func (c *Coordinator) processLoad(
	id projectionID,
	snapshot projectionState,
	source DurableSource,
) {
	candidate, exists, err := source.LoadLatest(c.opCtx, id.key)
	c.mu.Lock()
	state := c.states[id]
	if state == nil || state.generation != snapshot.generation ||
		c.sources[id.source] != source {
		c.mu.Unlock()
		return
	}
	if err != nil {
		c.loadFailures++
		state.retryAt = time.Now().Add(c.retryDelay)
		c.mu.Unlock()
		return
	}
	if !exists {
		delete(c.states, id)
		c.recoverAt[id.source] = time.Time{}
		c.notifyIdleLocked()
		c.mu.Unlock()
		return
	}
	state.revision = candidate.Revision
	state.retryAt = time.Time{}
	if !candidate.Persist {
		state.phase = projectionNeedsConfirm
		state.generation++
		c.signalLocked()
		c.mu.Unlock()
		return
	}
	c.next++
	receipt := fmt.Sprintf("projection:%d", c.next)
	state.phase = projectionAwaitingWrite
	state.receipt = receipt
	state.generation++
	c.receipts[receipt] = id
	writer := c.writer
	c.mu.Unlock()

	err = writer.SubmitTracked(candidate.Policy, candidate.Trace, ClassCritical, receipt)
	if err == nil {
		return
	}
	c.mu.Lock()
	state = c.states[id]
	if state != nil && state.receipt == receipt {
		delete(c.receipts, receipt)
		state.receipt = ""
		state.phase = projectionNeedsLoad
		state.retryAt = time.Now().Add(c.retryDelay)
		state.generation++
	}
	c.mu.Unlock()
}

func (c *Coordinator) processConfirm(
	id projectionID,
	snapshot projectionState,
	source DurableSource,
) {
	result, err := source.Confirm(c.opCtx, id.key, snapshot.revision)
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.states[id]
	if state == nil || state.generation != snapshot.generation ||
		c.sources[id.source] != source {
		return
	}
	if err != nil {
		c.confirmFailures++
		state.retryAt = time.Now().Add(c.retryDelay)
		return
	}
	switch result {
	case ConfirmationCurrent, ConfirmationGone:
		delete(c.states, id)
		c.recoverAt[id.source] = time.Time{}
		c.notifyIdleLocked()
	case ConfirmationStale:
		state.phase = projectionNeedsLoad
		state.retryAt = time.Time{}
		state.generation++
	default:
		c.confirmFailures++
		state.retryAt = time.Now().Add(c.retryDelay)
	}
	c.signalLocked()
}

func (c *Coordinator) observeWriterEvent(event Event) {
	if c == nil || strings.TrimSpace(event.SubmissionID) == "" {
		return
	}
	if event.Kind != EventPersisted && event.Kind != EventPermanentlyFailed {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	id, ok := c.receipts[event.SubmissionID]
	if !ok {
		return
	}
	delete(c.receipts, event.SubmissionID)
	state := c.states[id]
	if state == nil || state.receipt != event.SubmissionID {
		return
	}
	state.receipt = ""
	state.generation++
	if event.Kind == EventPersisted {
		state.phase = projectionNeedsConfirm
		state.retryAt = time.Time{}
	} else {
		state.phase = projectionNeedsLoad
		state.retryAt = time.Now().Add(c.retryDelay)
	}
	c.signalLocked()
}

func (c *Coordinator) waitIdle(ctx context.Context) error {
	for {
		c.mu.Lock()
		idle := len(c.states) == 0 && c.scans == 0 && !c.recoveryPendingLocked()
		c.mu.Unlock()
		if idle {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.idle:
		}
	}
}

func (c *Coordinator) signal() {
	if c == nil {
		return
	}
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *Coordinator) signalLocked() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *Coordinator) notifyIdleLocked() {
	if len(c.states) != 0 || c.scans != 0 || c.recoveryPendingLocked() {
		return
	}
	select {
	case c.idle <- struct{}{}:
	default:
	}
}

func (c *Coordinator) recoveryPendingLocked() bool {
	for sourceID := range c.recoverAt {
		if c.sources[sourceID] != nil {
			return true
		}
	}
	return false
}

func (c *Coordinator) removeSourceStatesLocked(sourceID string) {
	for id, state := range c.states {
		if id.source != sourceID {
			continue
		}
		if state.receipt != "" {
			delete(c.receipts, state.receipt)
		}
		delete(c.states, id)
	}
}
