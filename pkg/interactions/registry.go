package interactions

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/fileutil"
	"github.com/sipeed/picoclaw/pkg/logger"
)

type Options struct {
	TerminalRetention time.Duration
	MaxRecords        int
	MaxEvents         int
	MaxSnapshotBytes  int
	Now               func() time.Time
}

type Snapshot struct {
	SchemaVersion string   `json:"schema_version"`
	Records       []Record `json:"records"`
	Events        []Event  `json:"events,omitempty"`
}

type Observer func(EventObservation)

type observerEntry struct {
	id       uint64
	observer Observer
}

type queuedObservation struct {
	observation EventObservation
	observers   []observerEntry
}

var observerSequence atomic.Uint64

type Registry struct {
	mu            sync.RWMutex
	storePath     string
	options       Options
	records       map[string]Record
	events        []Event
	observers     []observerEntry
	notifications []queuedObservation
	notifying     bool
	loadErr       error
}

var _ Store = (*Registry)(nil)

func NewRegistry(storePath string) *Registry {
	return NewRegistryWithOptions(storePath, Options{})
}

func NewRegistryWithOptions(storePath string, opts Options) *Registry {
	if opts.TerminalRetention <= 0 {
		opts.TerminalRetention = DefaultRetention
	}
	if opts.MaxRecords <= 0 {
		opts.MaxRecords = DefaultMaxRecords
	}
	if opts.MaxEvents <= 0 {
		opts.MaxEvents = DefaultMaxEvents
	}
	if opts.MaxSnapshotBytes <= 0 {
		opts.MaxSnapshotBytes = DefaultMaxBytes
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	r := &Registry{
		storePath: strings.TrimSpace(storePath),
		options:   opts,
		records:   make(map[string]Record),
	}
	if r.storePath != "" {
		r.mu.Lock()
		release, err := r.lockAndReloadLocked()
		if err != nil {
			r.loadErr = err
		} else {
			changed := r.extendTerminalDeadlinesLocked(opts.TerminalRetention)
			if r.pruneLocked(r.nowMillis()) {
				changed = true
			}
			if changed {
				r.loadErr = r.saveLocked()
			}
			release()
		}
		r.mu.Unlock()
	}
	return r
}

func WorkspaceStorePath(workspace string) string {
	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		return ""
	}
	return filepath.Join(workspace, "state", "interaction_registry.json")
}

func (r *Registry) LastLoadError() error {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.loadErr
}

// EnsureLimits monotonically raises terminal retention and record capacity.
// Retained terminal cleanup deadlines are extended and persisted.
func (r *Registry) EnsureLimits(retention time.Duration, maxRecords int) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	retention = max(retention, r.options.TerminalRetention)
	maxRecords = max(maxRecords, r.options.MaxRecords)
	if retention == r.options.TerminalRetention && maxRecords == r.options.MaxRecords {
		return nil
	}
	previousRetention := r.options.TerminalRetention
	previousMaxRecords := r.options.MaxRecords
	r.options.TerminalRetention = retention
	r.options.MaxRecords = maxRecords
	if err := r.availableLocked(); err != nil {
		r.options.TerminalRetention = previousRetention
		r.options.MaxRecords = previousMaxRecords
		return err
	}
	releaseStore, err := r.lockAndReloadLocked()
	if err != nil {
		r.options.TerminalRetention = previousRetention
		r.options.MaxRecords = previousMaxRecords
		return err
	}
	before := r.snapshotLocked()
	changed := r.extendTerminalDeadlinesLocked(retention)
	if changed {
		if err := r.saveLocked(); err != nil {
			r.restoreSnapshotLocked(before)
			r.options.TerminalRetention = previousRetention
			r.options.MaxRecords = previousMaxRecords
			releaseStore()
			return err
		}
	}
	releaseStore()
	return nil
}

func (r *Registry) extendTerminalDeadlinesLocked(retention time.Duration) bool {
	changed := false
	for id, record := range r.records {
		if !isTerminal(record.Status) || record.ResolvedAt <= 0 {
			continue
		}
		cleanupAfter := record.ResolvedAt + retention.Milliseconds()
		if record.CleanupAfter >= cleanupAfter {
			continue
		}
		record.CleanupAfter = cleanupAfter
		r.records[id] = record
		changed = true
	}
	return changed
}

func (r *Registry) Subscribe(observer Observer) func() {
	if r == nil || observer == nil {
		return func() {}
	}
	entry := observerEntry{id: observerSequence.Add(1), observer: observer}
	r.mu.Lock()
	r.observers = append(r.observers, entry)
	r.mu.Unlock()
	return r.unsubscribe(entry.id)
}

// SubscribeSnapshot atomically installs observer and returns the retained
// registry state at that subscription boundary.
func (r *Registry) SubscribeSnapshot(observer Observer) (ObservationSnapshot, func()) {
	if r == nil || observer == nil {
		return ObservationSnapshot{}, func() {}
	}
	entry := observerEntry{id: observerSequence.Add(1), observer: observer}
	r.mu.Lock()
	r.observers = append(r.observers, entry)
	snapshot := r.observationSnapshotLocked()
	r.mu.Unlock()
	return snapshot, r.unsubscribe(entry.id)
}

func (r *Registry) unsubscribe(id uint64) func() {
	return func() {
		r.mu.Lock()
		for i := range r.observers {
			if r.observers[i].id != id {
				continue
			}
			r.observers = append(r.observers[:i], r.observers[i+1:]...)
			break
		}
		r.mu.Unlock()
	}
}

func (r *Registry) observationSnapshotLocked() ObservationSnapshot {
	records := make([]Record, 0, len(r.records))
	for _, record := range r.records {
		records = append(records, cloneRecord(record))
	}
	sortRecords(records)
	return ObservationSnapshot{
		Records: records,
		Events:  append([]Event(nil), r.events...),
	}
}

func (r *Registry) Create(req CreateRequest) (Record, error) {
	if r == nil {
		return Record{}, ErrStoreUnavailable
	}
	now := r.nowMillis()
	rec, err := r.buildRecord(req, now)
	if err != nil {
		return Record{}, err
	}

	r.mu.Lock()
	if availableErr := r.availableLocked(); availableErr != nil {
		r.mu.Unlock()
		return Record{}, availableErr
	}
	releaseStore, err := r.lockAndReloadLocked()
	if err != nil {
		r.mu.Unlock()
		return Record{}, err
	}
	if len(r.records) >= r.options.MaxRecords {
		releaseStore()
		r.mu.Unlock()
		return Record{}, fmt.Errorf("%w: max records %d", ErrCapacityExceeded, r.options.MaxRecords)
	}
	eventsBefore := append([]Event(nil), r.events...)
	var supersededID string
	var supersededBefore Record
	for id, existing := range r.records {
		if !isTerminal(existing.Status) && existing.Route.SessionKey == rec.Route.SessionKey {
			if canChainInteraction(existing, rec) {
				supersededID = id
				supersededBefore = existing
				from := existing.Status
				existing.Status = StatusResolved
				existing.Revision++
				existing.UpdatedAt = now
				existing.ResolvedAt = now
				existing.CleanupAfter = now + r.options.TerminalRetention.Milliseconds()
				r.appendEventFromLocked(
					&existing, EventResolved, from, "continued_with_next_interaction", nil,
				)
				r.records[id] = existing
				continue
			}
			releaseStore()
			r.mu.Unlock()
			return Record{}, fmt.Errorf("%w: %s", ErrSessionHasActive, existing.ShortID)
		}
		if !isTerminal(existing.Status) && existing.ShortID == rec.ShortID {
			releaseStore()
			r.mu.Unlock()
			return Record{}, fmt.Errorf("%w: duplicate short id", ErrConflict)
		}
	}
	if _, exists := r.records[rec.ID]; exists {
		releaseStore()
		r.mu.Unlock()
		return Record{}, fmt.Errorf("%w: duplicate id", ErrConflict)
	}
	r.appendEventLocked(&rec, EventCreated, "", nil)
	r.records[rec.ID] = rec
	events := append([]Event(nil), r.events[len(eventsBefore):]...)
	r.trimEventsLocked()
	if err := r.saveLocked(); err != nil {
		delete(r.records, rec.ID)
		if supersededID != "" {
			r.records[supersededID] = supersededBefore
		}
		r.events = eventsBefore
		releaseStore()
		r.mu.Unlock()
		return Record{}, err
	}
	drainNotifications := r.queueNotificationsLocked(events)
	releaseStore()
	r.mu.Unlock()
	if drainNotifications {
		r.drainNotifications()
	}
	return cloneRecord(rec), nil
}

func canChainInteraction(existing, next Record) bool {
	return existing.Status == StatusResuming &&
		existing.Origin.TaskID == next.Origin.TaskID &&
		existing.Origin.ContinuationSessionKey != "" &&
		existing.Origin.ContinuationSessionKey == next.Origin.ContinuationSessionKey &&
		existing.Route == next.Route
}

func (r *Registry) MarkWaiting(id string, expectedRevision int64) (Record, error) {
	return r.update(
		id,
		expectedRevision,
		func(rec *Record, _ int64) (EventType, string, *bool, error) {
			if !validTransition(rec.Status, StatusWaiting) {
				return "", "", nil, fmt.Errorf(
					"%w: %s -> %s",
					ErrInvalidTransition,
					rec.Status,
					StatusWaiting,
				)
			}
			if rec.DeliveryTries == 0 || rec.DeliveryError != "" || !rec.PromptDelivered {
				return "", "", nil, fmt.Errorf(
					"%w: prompt delivery has not succeeded",
					ErrInvalidTransition,
				)
			}
			rec.Status = StatusWaiting
			return EventWaiting, "", nil, nil
		},
	)
}

func (r *Registry) RecordDeliveryAttempt(
	id string,
	expectedRevision int64,
	success bool,
	detail string,
) (Record, error) {
	return r.update(
		id,
		expectedRevision,
		func(rec *Record, now int64) (EventType, string, *bool, error) {
			if rec.Status != StatusCreated {
				return "", "", nil, fmt.Errorf(
					"%w: delivery from %s",
					ErrInvalidTransition,
					rec.Status,
				)
			}
			rec.DeliveryTries++
			rec.LastDeliveryAt = now
			if success {
				rec.PromptDelivered = true
				rec.PromptDeliveryState = DeliveryStateDelivered
				rec.DeliveryError = ""
			} else {
				rec.DeliveryError = bounded(detail, MaxSummaryLength)
			}
			return EventDeliveryAttempt, "", &success, nil
		},
	)
}

func (r *Registry) BeginPromptDelivery(id string, expectedRevision int64) (Record, error) {
	return r.update(
		id,
		expectedRevision,
		func(rec *Record, now int64) (EventType, string, *bool, error) {
			if rec.Status != StatusCreated ||
				(rec.PromptDeliveryState != "" && rec.PromptDeliveryState != DeliveryStateNotSent) ||
				rec.PromptDelivered {
				return "", "", nil, fmt.Errorf(
					"%w: begin prompt delivery from %s/%s",
					ErrInvalidTransition,
					rec.Status,
					rec.PromptDeliveryState,
				)
			}
			rec.DeliveryTries++
			rec.LastDeliveryAt = now
			rec.DeliveryError = ""
			rec.PromptDeliveryState = DeliveryStateSending
			return EventDeliveryAttempt, "delivery_started", nil, nil
		},
	)
}

func (r *Registry) CompletePromptDelivery(
	id string,
	expectedRevision int64,
	success bool,
	ambiguous bool,
	detail string,
) (Record, error) {
	return r.update(
		id,
		expectedRevision,
		func(rec *Record, now int64) (EventType, string, *bool, error) {
			if rec.Status != StatusCreated || rec.PromptDeliveryState != DeliveryStateSending {
				return "", "", nil, fmt.Errorf(
					"%w: complete prompt delivery from %s/%s",
					ErrInvalidTransition,
					rec.Status,
					rec.PromptDeliveryState,
				)
			}
			rec.LastDeliveryAt = now
			if success {
				rec.PromptDelivered = true
				rec.PromptDeliveryState = DeliveryStateDelivered
				rec.DeliveryError = ""
			} else if ambiguous {
				rec.PromptDeliveryState = DeliveryStateAmbiguous
				rec.DeliveryError = bounded(detail, MaxSummaryLength)
			} else {
				rec.PromptDeliveryState = DeliveryStateNotSent
				rec.DeliveryError = bounded(detail, MaxSummaryLength)
			}
			return EventDeliveryAttempt, "delivery_completed", &success, nil
		},
	)
}

func (r *Registry) RecordFinalDeliveryAttempt(
	id string,
	expectedRevision int64,
	success bool,
	detail string,
) (Record, error) {
	return r.update(
		id,
		expectedRevision,
		func(rec *Record, now int64) (EventType, string, *bool, error) {
			if rec.Status != StatusResuming {
				return "", "", nil, fmt.Errorf(
					"%w: final delivery from %s", ErrInvalidTransition, rec.Status,
				)
			}
			rec.FinalDeliveryTries++
			rec.LastFinalDeliveryAt = now
			if success {
				rec.FinalDelivered = true
				rec.FinalDeliveryState = DeliveryStateDelivered
				rec.FinalDeliveryError = ""
			} else {
				rec.FinalDeliveryError = bounded(detail, MaxSummaryLength)
			}
			return EventFinalDelivery, "", &success, nil
		},
	)
}

func (r *Registry) BeginFinalDelivery(id string, expectedRevision int64) (Record, error) {
	return r.update(
		id,
		expectedRevision,
		func(rec *Record, now int64) (EventType, string, *bool, error) {
			if rec.Status != StatusResuming ||
				(rec.FinalDeliveryState != "" && rec.FinalDeliveryState != DeliveryStateNotSent) ||
				rec.FinalDelivered {
				return "", "", nil, fmt.Errorf(
					"%w: begin final delivery from %s/%s",
					ErrInvalidTransition,
					rec.Status,
					rec.FinalDeliveryState,
				)
			}
			rec.FinalDeliveryTries++
			rec.LastFinalDeliveryAt = now
			rec.FinalDeliveryError = ""
			rec.FinalDeliveryState = DeliveryStateSending
			return EventFinalDelivery, "delivery_started", nil, nil
		},
	)
}

func (r *Registry) CompleteFinalDelivery(
	id string,
	expectedRevision int64,
	success bool,
	ambiguous bool,
	detail string,
) (Record, error) {
	return r.update(
		id,
		expectedRevision,
		func(rec *Record, now int64) (EventType, string, *bool, error) {
			if rec.Status != StatusResuming || rec.FinalDeliveryState != DeliveryStateSending {
				return "", "", nil, fmt.Errorf(
					"%w: complete final delivery from %s/%s",
					ErrInvalidTransition,
					rec.Status,
					rec.FinalDeliveryState,
				)
			}
			rec.LastFinalDeliveryAt = now
			if success {
				rec.FinalDelivered = true
				rec.FinalDeliveryState = DeliveryStateDelivered
				rec.FinalDeliveryError = ""
			} else if ambiguous {
				rec.FinalDeliveryState = DeliveryStateAmbiguous
				rec.FinalDeliveryError = bounded(detail, MaxSummaryLength)
			} else {
				rec.FinalDeliveryState = DeliveryStateNotSent
				rec.FinalDeliveryError = bounded(detail, MaxSummaryLength)
			}
			return EventFinalDelivery, "delivery_completed", &success, nil
		},
	)
}

func (r *Registry) ClaimDeliveryUnknown(id string, expectedRevision int64) (Record, error) {
	return r.update(
		id,
		expectedRevision,
		func(rec *Record, now int64) (EventType, string, *bool, error) {
			if rec.Status != StatusCreated ||
				(rec.PromptDeliveryState != DeliveryStateSending &&
					rec.PromptDeliveryState != DeliveryStateAmbiguous) {
				return "", "", nil, fmt.Errorf(
					"%w: claim unknown delivery from %s/%s",
					ErrInvalidTransition,
					rec.Status,
					rec.PromptDeliveryState,
				)
			}
			rec.Status = StatusClaimed
			rec.Outcome = OutcomeDeliveryUnknown
			rec.Answer = &Answer{ReceivedAt: now}
			return EventAnswerClaimed, "prompt_delivery_ambiguous", nil, nil
		},
	)
}

func (r *Registry) ClaimAnswer(
	id string,
	expectedRevision int64,
	answer Answer,
	outcome Outcome,
) (Record, error) {
	if outcome != OutcomeAnswered && outcome != OutcomeAllowed && outcome != OutcomeDenied {
		return Record{}, fmt.Errorf("%w: invalid answer outcome %q", ErrInvalidInteraction, outcome)
	}
	return r.claim(id, expectedRevision, answer, outcome)
}

func (r *Registry) ClaimOverdue(now time.Time) ([]Record, error) {
	if r == nil {
		return nil, ErrStoreUnavailable
	}
	nowMillis := now.UnixMilli()
	if now.IsZero() {
		nowMillis = r.nowMillis()
	}

	r.mu.Lock()
	if err := r.availableLocked(); err != nil {
		r.mu.Unlock()
		return nil, err
	}
	releaseStore, err := r.lockAndReloadLocked()
	if err != nil {
		r.mu.Unlock()
		return nil, err
	}
	eventsBefore := append([]Event(nil), r.events...)
	before := make(map[string]Record)
	claimed := make([]Record, 0)
	emitted := make([]Event, 0)
	for id, rec := range r.records {
		if (rec.Status != StatusCreated && rec.Status != StatusWaiting) ||
			rec.ExpiresAt <= 0 || rec.ExpiresAt > nowMillis {
			continue
		}
		before[id] = rec
		from := rec.Status
		rec.Status = StatusClaimed
		rec.Outcome = OutcomeTimedOut
		rec.Answer = &Answer{ReceivedAt: nowMillis}
		rec.Revision++
		rec.UpdatedAt = nowMillis
		r.appendEventFromLocked(&rec, EventAnswerClaimed, from, "timeout", nil)
		r.records[id] = rec
		emitted = append(emitted, r.events[len(r.events)-1])
		claimed = append(claimed, cloneRecord(rec))
	}
	if len(claimed) == 0 {
		releaseStore()
		r.mu.Unlock()
		return nil, nil
	}
	r.trimEventsLocked()
	if err := r.saveLocked(); err != nil {
		for id, rec := range before {
			r.records[id] = rec
		}
		r.events = eventsBefore
		releaseStore()
		r.mu.Unlock()
		return nil, err
	}
	drainNotifications := r.queueNotificationsLocked(emitted)
	releaseStore()
	r.mu.Unlock()
	if drainNotifications {
		r.drainNotifications()
	}
	sort.Slice(claimed, func(i, j int) bool { return claimed[i].ID < claimed[j].ID })
	return claimed, nil
}

func (r *Registry) MarkResuming(id string, expectedRevision int64) (Record, error) {
	return r.update(
		id,
		expectedRevision,
		func(rec *Record, now int64) (EventType, string, *bool, error) {
			if !validTransition(rec.Status, StatusResuming) {
				return "", "", nil, fmt.Errorf(
					"%w: %s -> %s",
					ErrInvalidTransition,
					rec.Status,
					StatusResuming,
				)
			}
			rec.Status = StatusResuming
			rec.ResumeTries++
			rec.LastResumeAt = now
			rec.ResumeError = ""
			return EventResumeStarted, "", nil, nil
		},
	)
}

func (r *Registry) RecordResumeFailure(
	id string,
	expectedRevision int64,
	detail string,
) (Record, error) {
	return r.update(
		id,
		expectedRevision,
		func(rec *Record, now int64) (EventType, string, *bool, error) {
			if rec.Status != StatusClaimed && rec.Status != StatusResuming {
				return "", "", nil, fmt.Errorf(
					"%w: resume failure from %s",
					ErrInvalidTransition,
					rec.Status,
				)
			}
			rec.ResumeError = bounded(detail, MaxSummaryLength)
			rec.LastResumeAt = now
			return EventRecoveryObserved, "resume_failed", nil, nil
		},
	)
}

func (r *Registry) Resolve(id string, expectedRevision int64) (Record, error) {
	return r.transition(id, expectedRevision, StatusResolved, EventResolved, "", nil)
}

// ConsumeApproval atomically spends an allow-once decision before the
// protected tool executes. A consumed approval is never executable again,
// including after a crash with an uncertain tool outcome.
func (r *Registry) ConsumeApproval(
	id string,
	expectedRevision int64,
	toolCallID string,
	toolName string,
	argumentHash string,
) (Record, error) {
	toolCallID = strings.TrimSpace(toolCallID)
	toolName = strings.TrimSpace(toolName)
	argumentHash = strings.TrimSpace(argumentHash)
	record, err := r.update(
		id,
		expectedRevision,
		func(rec *Record, now int64) (EventType, string, *bool, error) {
			if rec.Kind != KindApproval || rec.Status != StatusResuming ||
				rec.Outcome != OutcomeAllowed || rec.ApprovalConsumedAt != 0 ||
				rec.Origin.ToolCallID != toolCallID || rec.Origin.ToolName != toolName ||
				rec.Origin.ArgumentHash == "" || rec.Origin.ArgumentHash != argumentHash {
				return "", "", nil, fmt.Errorf("%w: approval does not match pending tool call", ErrInvalidTransition)
			}
			if rec.ExpiresAt > 0 && now >= rec.ExpiresAt {
				rec.Outcome = OutcomeTimedOut
				return EventApprovalExpired, "timeout_at_approval_consumption", nil, nil
			}
			rec.ApprovalConsumedAt = now
			return EventApprovalConsumed, "allow_once_consumed", nil, nil
		},
	)
	if err != nil {
		return Record{}, err
	}
	if record.Outcome == OutcomeTimedOut && record.ApprovalConsumedAt == 0 {
		return record, ErrApprovalExpired
	}
	return record, nil
}

func (r *Registry) BeginCancellation(
	id string,
	expectedRevision int64,
	code string,
) (Record, error) {
	return r.update(
		id,
		expectedRevision,
		func(rec *Record, _ int64) (EventType, string, *bool, error) {
			if !validTransition(rec.Status, StatusCanceling) {
				return "", "", nil, fmt.Errorf(
					"%w: %s -> %s",
					ErrInvalidTransition,
					rec.Status,
					StatusCanceling,
				)
			}
			rec.Status = StatusCanceling
			rec.FailureCode = bounded(code, 128)
			return EventCanceling, rec.FailureCode, nil, nil
		},
	)
}

func (r *Registry) CompleteCancellation(id string, expectedRevision int64) (Record, error) {
	return r.transition(
		id,
		expectedRevision,
		StatusCancelled,
		EventCancelled,
		"cancellation_completed",
		nil,
	)
}

func (r *Registry) Cancel(id string, expectedRevision int64, code string) (Record, error) {
	return r.transition(id, expectedRevision, StatusCancelled, EventCancelled, code, nil)
}

func (r *Registry) Fail(
	id string,
	expectedRevision int64,
	code string,
	detail string,
) (Record, error) {
	return r.update(
		id,
		expectedRevision,
		func(rec *Record, _ int64) (EventType, string, *bool, error) {
			if !validTransition(rec.Status, StatusFailed) {
				return "", "", nil, fmt.Errorf(
					"%w: %s -> %s",
					ErrInvalidTransition,
					rec.Status,
					StatusFailed,
				)
			}
			rec.Status = StatusFailed
			rec.FailureCode = bounded(code, 128)
			rec.FailureDetail = bounded(detail, MaxSummaryLength)
			return EventFailed, rec.FailureCode, nil, nil
		},
	)
}

func (r *Registry) Get(id string) (Record, bool) {
	if r == nil {
		return Record{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec, ok := r.records[strings.TrimSpace(id)]
	return cloneRecord(rec), ok
}

func (r *Registry) List() []Record {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Record, 0, len(r.records))
	for _, rec := range r.records {
		out = append(out, cloneRecord(rec))
	}
	sortRecords(out)
	return out
}

func (r *Registry) ListNonterminal() []Record {
	all := r.List()
	out := make([]Record, 0, len(all))
	for _, rec := range all {
		if !isTerminal(rec.Status) {
			out = append(out, rec)
		}
	}
	return out
}

func (r *Registry) FindWaitingBySession(sessionKey string) []Record {
	sessionKey = strings.TrimSpace(sessionKey)
	all := r.List()
	out := make([]Record, 0, 1)
	for _, rec := range all {
		if rec.Status == StatusWaiting && rec.Route.SessionKey == sessionKey {
			out = append(out, rec)
		}
	}
	return out
}

func (r *Registry) FindWaitingByRoute(route Route) []Record {
	route = normalizeRoute(route)
	all := r.List()
	out := make([]Record, 0, 1)
	for _, rec := range all {
		if rec.Status == StatusWaiting && routesEqual(rec.Route, route) {
			out = append(out, rec)
		}
	}
	return out
}

func (r *Registry) ListEvents(id string) []Event {
	if r == nil {
		return nil
	}
	id = strings.TrimSpace(id)
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Event, 0, len(r.events))
	for _, event := range r.events {
		if id == "" || event.InteractionID == id {
			out = append(out, event)
		}
	}
	return out
}

func (r *Registry) Prune(now time.Time) error {
	if r == nil {
		return ErrStoreUnavailable
	}
	nowMillis := now.UnixMilli()
	if now.IsZero() {
		nowMillis = r.nowMillis()
	}
	r.mu.Lock()
	if err := r.availableLocked(); err != nil {
		r.mu.Unlock()
		return err
	}
	releaseStore, err := r.lockAndReloadLocked()
	if err != nil {
		r.mu.Unlock()
		return err
	}
	before := r.snapshotLocked()
	if !r.pruneLocked(nowMillis) {
		releaseStore()
		r.mu.Unlock()
		return nil
	}
	if err := r.saveLocked(); err != nil {
		r.restoreSnapshotLocked(before)
		releaseStore()
		r.mu.Unlock()
		return err
	}
	releaseStore()
	r.mu.Unlock()
	return nil
}

func (r *Registry) Stats() Stats {
	if r == nil {
		return Stats{}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	stats := Stats{
		RecordCount:      len(r.records),
		EventCount:       len(r.events),
		SnapshotBytes:    r.snapshotSizeLocked(),
		Retention:        r.options.TerminalRetention,
		MaxRecords:       r.options.MaxRecords,
		MaxEvents:        r.options.MaxEvents,
		MaxSnapshotBytes: r.options.MaxSnapshotBytes,
	}
	for _, rec := range r.records {
		if !isTerminal(rec.Status) {
			stats.NonterminalCount++
		}
	}
	stats.OverBudget = stats.SnapshotBytes > stats.MaxSnapshotBytes
	return stats
}

func (r *Registry) claim(
	id string,
	expectedRevision int64,
	answer Answer,
	outcome Outcome,
) (Record, error) {
	if !validBoundedString(answer.Text, MaxAnswerLength) || len(answer.Values) > MaxQuestions {
		return Record{}, fmt.Errorf("%w: answer exceeds bounds", ErrInvalidInteraction)
	}
	answer.Text = strings.TrimSpace(answer.Text)
	answer.Values = cloneStringMap(answer.Values)
	for key, value := range answer.Values {
		if !questionIDPattern.MatchString(key) || !validBoundedString(value, MaxAnswerLength) {
			return Record{}, fmt.Errorf("%w: invalid answer value %q", ErrInvalidInteraction, key)
		}
		answer.Values[key] = strings.TrimSpace(value)
	}
	answer.MessageID = strings.TrimSpace(answer.MessageID)
	return r.update(
		id,
		expectedRevision,
		func(rec *Record, now int64) (EventType, string, *bool, error) {
			if rec.Status != StatusWaiting {
				return "", "", nil, fmt.Errorf("%w: status %s", ErrAnswerTooLate, rec.Status)
			}
			if rec.ExpiresAt > 0 && now >= rec.ExpiresAt {
				rec.Status = StatusClaimed
				rec.Outcome = OutcomeTimedOut
				rec.Answer = &Answer{MessageID: answer.MessageID, ReceivedAt: now}
				return EventAnswerClaimed, "timeout_at_answer_claim", nil, nil
			}
			if rec.Kind == KindQuestion && outcome != OutcomeAnswered {
				return "", "", nil, fmt.Errorf(
					"%w: question outcome %q",
					ErrInvalidInteraction,
					outcome,
				)
			}
			if rec.Kind == KindApproval && outcome != OutcomeAllowed && outcome != OutcomeDenied {
				return "", "", nil, fmt.Errorf(
					"%w: approval outcome %q",
					ErrInvalidInteraction,
					outcome,
				)
			}
			if rec.Kind == KindQuestion {
				known := make(map[string]struct{}, len(rec.Questions))
				for _, question := range rec.Questions {
					known[question.ID] = struct{}{}
				}
				for key := range answer.Values {
					if _, ok := known[key]; !ok {
						return "", "", nil, fmt.Errorf(
							"%w: unknown question id %q",
							ErrInvalidInteraction,
							key,
						)
					}
				}
			}
			if answer.MessageID != "" {
				identity := scopedAnswerMessageIdentity(rec.Route, answer.MessageID)
				for _, other := range r.records {
					if other.Answer != nil &&
						scopedAnswerMessageIdentity(other.Route, other.Answer.MessageID) == identity {
						return "", "", nil, fmt.Errorf(
							"%w: %s",
							ErrDuplicateAnswer,
							answer.MessageID,
						)
					}
				}
			}
			if answer.ReceivedAt == 0 {
				answer.ReceivedAt = now
			}
			rec.Status = StatusClaimed
			rec.Outcome = outcome
			rec.Answer = &answer
			return EventAnswerClaimed, "", nil, nil
		},
	)
}

func (r *Registry) transition(
	id string,
	expectedRevision int64,
	to Status,
	eventType EventType,
	code string,
	success *bool,
) (Record, error) {
	return r.update(
		id,
		expectedRevision,
		func(rec *Record, _ int64) (EventType, string, *bool, error) {
			if !validTransition(rec.Status, to) {
				return "", "", nil, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, rec.Status, to)
			}
			rec.Status = to
			return eventType, bounded(code, 128), success, nil
		},
	)
}

func (r *Registry) update(
	id string,
	expectedRevision int64,
	mutate func(*Record, int64) (EventType, string, *bool, error),
) (Record, error) {
	if r == nil {
		return Record{}, ErrStoreUnavailable
	}
	id = strings.TrimSpace(id)
	r.mu.Lock()
	if err := r.availableLocked(); err != nil {
		r.mu.Unlock()
		return Record{}, err
	}
	releaseStore, err := r.lockAndReloadLocked()
	if err != nil {
		r.mu.Unlock()
		return Record{}, err
	}
	rec, ok := r.records[id]
	if !ok {
		releaseStore()
		r.mu.Unlock()
		return Record{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if rec.Revision != expectedRevision {
		releaseStore()
		r.mu.Unlock()
		return Record{}, fmt.Errorf(
			"%w: have %d, want %d",
			ErrConflict,
			rec.Revision,
			expectedRevision,
		)
	}
	before := rec
	eventsBefore := append([]Event(nil), r.events...)
	now := r.nowMillis()
	eventType, code, success, err := mutate(&rec, now)
	if err != nil {
		releaseStore()
		r.mu.Unlock()
		return Record{}, err
	}
	from := before.Status
	rec.Revision++
	rec.UpdatedAt = now
	if isTerminal(rec.Status) {
		rec.ResolvedAt = now
		rec.CleanupAfter = now + r.options.TerminalRetention.Milliseconds()
	}
	r.appendEventFromLocked(&rec, eventType, from, code, success)
	r.records[id] = rec
	event := r.events[len(r.events)-1]
	r.trimEventsLocked()
	if err := r.saveLocked(); err != nil {
		r.records[id] = before
		r.events = eventsBefore
		releaseStore()
		r.mu.Unlock()
		return Record{}, err
	}
	drainNotifications := r.queueNotificationsLocked([]Event{event})
	releaseStore()
	r.mu.Unlock()
	if drainNotifications {
		r.drainNotifications()
	}
	return cloneRecord(rec), nil
}

func (r *Registry) buildRecord(req CreateRequest, now int64) (Record, error) {
	if req.Kind != KindQuestion && req.Kind != KindApproval {
		return Record{}, fmt.Errorf("%w: unsupported kind %q", ErrInvalidInteraction, req.Kind)
	}
	if err := req.Route.validate(); err != nil {
		return Record{}, err
	}
	if err := req.Origin.validate(); err != nil {
		return Record{}, err
	}
	if !validArgumentHashForKind(req.Kind, req.Origin.ArgumentHash) {
		return Record{}, fmt.Errorf("%w: approval requires a canonical argument hash", ErrInvalidInteraction)
	}
	if err := validateApprovalCreateMetadata(req.Kind, req.Origin.ExecutionContext, req.ApprovalAction); err != nil {
		return Record{}, err
	}
	if err := validateQuestions(req.Kind, req.Questions); err != nil {
		return Record{}, err
	}
	id := strings.TrimSpace(req.ID)
	if id == "" {
		var err error
		id, err = randomID()
		if err != nil {
			return Record{}, err
		}
	}
	if len(id) < 8 || len(id) > 128 || !regexpID.MatchString(id) {
		return Record{}, fmt.Errorf("%w: id must be 8 to 128 characters", ErrInvalidInteraction)
	}
	expiresAt := req.ExpiresAt.UnixMilli()
	if req.ExpiresAt.IsZero() || expiresAt <= now {
		return Record{}, fmt.Errorf("%w: expiry must be in the future", ErrInvalidInteraction)
	}
	return Record{
		ID:             id,
		ShortID:        shortID(id),
		Kind:           req.Kind,
		Status:         StatusCreated,
		Revision:       1,
		Route:          normalizeRoute(req.Route),
		Origin:         normalizeOrigin(req.Origin),
		Questions:      cloneQuestions(req.Questions),
		PromptSummary:  bounded(strings.TrimSpace(req.PromptSummary), MaxSummaryLength),
		ApprovalAction: bounded(strings.TrimSpace(req.ApprovalAction), MaxApprovalAction),
		CreatedAt:      now,
		UpdatedAt:      now,
		ExpiresAt:      expiresAt,
	}, nil
}

func (r *Registry) availableLocked() error {
	if r.loadErr != nil {
		return fmt.Errorf("%w: %v", ErrStoreUnavailable, r.loadErr)
	}
	return nil
}

func (r *Registry) lockAndReloadLocked() (func(), error) {
	if r.storePath == "" {
		return func() {}, nil
	}
	if err := os.MkdirAll(filepath.Dir(r.storePath), 0o700); err != nil {
		return nil, fmt.Errorf("create interaction store directory: %w", err)
	}
	release, err := acquireStoreFileLock(r.storePath + ".lock")
	if err != nil {
		return nil, err
	}
	before := r.snapshotLocked()
	r.records = make(map[string]Record)
	r.events = nil
	if err := r.load(); err != nil {
		r.restoreSnapshotLocked(before)
		release()
		return nil, fmt.Errorf("%w: reload under lock: %v", ErrStoreUnavailable, err)
	}
	return release, nil
}

func (r *Registry) load() error {
	data, err := os.ReadFile(r.storePath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}
	if snapshot.SchemaVersion != SnapshotSchemaVersion {
		return fmt.Errorf("unsupported interaction snapshot schema %q", snapshot.SchemaVersion)
	}
	activeSessions := make(map[string]string)
	activeShortIDs := make(map[string]string)
	answerMessages := make(map[answerMessageIdentity]string)
	for _, rec := range snapshot.Records {
		if err := validateStoredRecord(rec); err != nil {
			return err
		}
		if _, duplicate := r.records[rec.ID]; duplicate {
			return fmt.Errorf("duplicate interaction record %q", rec.ID)
		}
		if !isTerminal(rec.Status) {
			if existing := activeSessions[rec.Route.SessionKey]; existing != "" {
				return fmt.Errorf("active interactions %q and %q share session", existing, rec.ID)
			}
			if existing := activeShortIDs[rec.ShortID]; existing != "" {
				return fmt.Errorf("active interactions %q and %q share short id", existing, rec.ID)
			}
			activeSessions[rec.Route.SessionKey] = rec.ID
			activeShortIDs[rec.ShortID] = rec.ID
		}
		if rec.Answer != nil && rec.Answer.MessageID != "" {
			identity := scopedAnswerMessageIdentity(rec.Route, rec.Answer.MessageID)
			if existing := answerMessages[identity]; existing != "" {
				return fmt.Errorf("interactions %q and %q share answer message", existing, rec.ID)
			}
			answerMessages[identity] = rec.ID
		}
		r.records[rec.ID] = cloneRecord(rec)
	}
	eventSequences := make(map[string]int64)
	eventSeen := make(map[string]bool)
	for _, event := range snapshot.Events {
		if event.SchemaVersion != EventSchemaVersion || event.InteractionID == "" ||
			event.Type == "" {
			return fmt.Errorf("invalid interaction event %q", event.EventID)
		}
		rec, exists := r.records[event.InteractionID]
		if !exists || event.Sequence <= 0 || event.Sequence > rec.LastEventSeq ||
			event.Revision <= 0 || event.Revision > rec.Revision {
			return fmt.Errorf("invalid interaction event sequence %q", event.EventID)
		}
		if eventSeen[event.InteractionID] &&
			event.Sequence != eventSequences[event.InteractionID]+1 {
			return fmt.Errorf("invalid interaction event sequence %q", event.EventID)
		}
		eventSeen[event.InteractionID] = true
		eventSequences[event.InteractionID] = event.Sequence
		r.events = append(r.events, event)
	}
	return nil
}

type answerMessageIdentity struct {
	Channel   string
	AccountID string
	ChatID    string
	TopicID   string
	SpaceID   string
	MessageID string
}

func scopedAnswerMessageIdentity(route Route, messageID string) answerMessageIdentity {
	return answerMessageIdentity{
		Channel:   route.Channel,
		AccountID: route.AccountID,
		ChatID:    route.ChatID,
		TopicID:   route.TopicID,
		SpaceID:   route.SpaceID,
		MessageID: strings.TrimSpace(messageID),
	}
}

func validateStoredRecord(rec Record) error {
	if strings.TrimSpace(rec.ID) == "" || !regexpID.MatchString(rec.ID) ||
		rec.ShortID != shortID(rec.ID) || rec.Revision <= 0 || rec.LastEventSeq <= 0 ||
		rec.CreatedAt <= 0 || rec.UpdatedAt < rec.CreatedAt || rec.ExpiresAt <= rec.CreatedAt {
		return fmt.Errorf("invalid interaction record %q", rec.ID)
	}
	if rec.Kind != KindQuestion && rec.Kind != KindApproval {
		return fmt.Errorf("invalid interaction kind %q", rec.Kind)
	}
	switch rec.Status {
	case StatusCreated,
		StatusWaiting,
		StatusClaimed,
		StatusResuming,
		StatusCanceling,
		StatusResolved,
		StatusCancelled,
		StatusFailed:
	default:
		return fmt.Errorf("invalid interaction status %q", rec.Status)
	}
	if err := rec.Route.validate(); err != nil {
		return err
	}
	if err := rec.Origin.validate(); err != nil {
		return err
	}
	if !validStoredArgumentHashForKind(rec.Kind, rec.Origin.ArgumentHash) {
		return fmt.Errorf("invalid argument hash for interaction %q", rec.ID)
	}
	if err := validateStoredApprovalMetadata(
		rec.Kind, rec.Origin.ExecutionContext, rec.ApprovalAction,
	); err != nil {
		return fmt.Errorf("invalid approval metadata for interaction %q: %w", rec.ID, err)
	}
	if err := validateStoredQuestions(rec.Kind, rec.Questions); err != nil {
		return err
	}
	if !validDeliveryState(rec.PromptDeliveryState) || !validDeliveryState(rec.FinalDeliveryState) ||
		(rec.PromptDelivered && rec.PromptDeliveryState != "" &&
			rec.PromptDeliveryState != DeliveryStateDelivered) ||
		(rec.FinalDelivered && rec.FinalDeliveryState != "" &&
			rec.FinalDeliveryState != DeliveryStateDelivered) {
		return fmt.Errorf("invalid delivery state for interaction %q", rec.ID)
	}
	switch rec.Status {
	case StatusCreated:
		if rec.Answer != nil || rec.Outcome != "" || rec.DeliveryTries < 0 {
			return fmt.Errorf("invalid created interaction %q", rec.ID)
		}
	case StatusWaiting:
		if rec.Answer != nil || rec.Outcome != "" || rec.DeliveryTries == 0 ||
			!rec.PromptDelivered ||
			rec.DeliveryError != "" {
			return fmt.Errorf("invalid waiting interaction %q", rec.ID)
		}
	case StatusClaimed, StatusResuming, StatusResolved:
		if rec.Answer == nil || rec.Answer.ReceivedAt <= 0 ||
			!validStoredOutcome(rec.Kind, rec.Outcome) {
			return fmt.Errorf("invalid answered interaction %q", rec.ID)
		}
		if (rec.Status == StatusResuming || rec.Status == StatusResolved) && rec.ResumeTries == 0 {
			return fmt.Errorf("invalid resuming interaction %q", rec.ID)
		}
	}
	if isTerminal(rec.Status) && (rec.ResolvedAt <= 0 || rec.CleanupAfter <= rec.ResolvedAt) {
		return fmt.Errorf("invalid terminal interaction %q", rec.ID)
	}
	if rec.ApprovalConsumedAt != 0 && (rec.Kind != KindApproval ||
		rec.Outcome != OutcomeAllowed || rec.Status == StatusCreated ||
		rec.Status == StatusWaiting || rec.Status == StatusClaimed) {
		return fmt.Errorf("invalid consumed approval %q", rec.ID)
	}
	return nil
}

func validStoredOutcome(kind Kind, outcome Outcome) bool {
	if outcome == OutcomeTimedOut || outcome == OutcomeDeliveryUnknown {
		return true
	}
	if kind == KindQuestion {
		return outcome == OutcomeAnswered
	}
	return outcome == OutcomeAllowed || outcome == OutcomeDenied
}

func validDeliveryState(state DeliveryState) bool {
	switch state {
	case "", DeliveryStateNotSent, DeliveryStateSending, DeliveryStateDelivered, DeliveryStateAmbiguous:
		return true
	default:
		return false
	}
}

func validArgumentHashForKind(kind Kind, value string) bool {
	value = strings.TrimSpace(value)
	if kind != KindApproval {
		return value == ""
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func validStoredArgumentHashForKind(kind Kind, value string) bool {
	if kind == KindApproval && strings.TrimSpace(value) == "" {
		// Obsolete approval records are inert: recovery cannot consume them
		// without an exact argument hash and immutable execution context.
		return true
	}
	return validArgumentHashForKind(kind, value)
}

func (r *Registry) saveLocked() error {
	if r.storePath == "" {
		return nil
	}
	data, err := json.MarshalIndent(r.snapshotLocked(), "", "  ")
	if err != nil {
		return err
	}
	if len(data) > r.options.MaxSnapshotBytes {
		return fmt.Errorf(
			"%w: %d > %d",
			ErrSnapshotOverBudget,
			len(data),
			r.options.MaxSnapshotBytes,
		)
	}
	return fileutil.WriteFileAtomic(r.storePath, data, 0o600)
}

func (r *Registry) snapshotLocked() Snapshot {
	records := make([]Record, 0, len(r.records))
	for _, rec := range r.records {
		records = append(records, cloneRecord(rec))
	}
	sortRecords(records)
	return Snapshot{
		SchemaVersion: SnapshotSchemaVersion,
		Records:       records,
		Events:        append([]Event(nil), r.events...),
	}
}

func (r *Registry) restoreSnapshotLocked(snapshot Snapshot) {
	r.records = make(map[string]Record, len(snapshot.Records))
	for _, rec := range snapshot.Records {
		r.records[rec.ID] = cloneRecord(rec)
	}
	r.events = append([]Event(nil), snapshot.Events...)
}

func (r *Registry) snapshotSizeLocked() int {
	data, _ := json.MarshalIndent(r.snapshotLocked(), "", "  ")
	return len(data)
}

func (r *Registry) pruneLocked(now int64) bool {
	changed := false
	for id, rec := range r.records {
		if isTerminal(rec.Status) && rec.CleanupAfter > 0 && now >= rec.CleanupAfter {
			delete(r.records, id)
			changed = true
		}
	}
	if len(r.records) > r.options.MaxRecords {
		terminal := make([]Record, 0)
		for _, rec := range r.records {
			if isTerminal(rec.Status) {
				terminal = append(terminal, rec)
			}
		}
		sort.Slice(
			terminal,
			func(i, j int) bool { return terminal[i].ResolvedAt < terminal[j].ResolvedAt },
		)
		for len(r.records) > r.options.MaxRecords && len(terminal) > 0 {
			delete(r.records, terminal[0].ID)
			terminal = terminal[1:]
			changed = true
		}
	}
	if r.trimEventsLocked() {
		changed = true
	}
	return changed
}

func (r *Registry) trimEventsLocked() bool {
	changed := false
	kept := r.events[:0]
	for _, event := range r.events {
		if _, exists := r.records[event.InteractionID]; exists {
			kept = append(kept, event)
		} else {
			changed = true
		}
	}
	r.events = kept
	if len(r.events) > r.options.MaxEvents {
		r.events = append([]Event(nil), r.events[len(r.events)-r.options.MaxEvents:]...)
		changed = true
	}
	return changed
}

func (r *Registry) appendEventLocked(rec *Record, eventType EventType, code string, success *bool) {
	r.appendEventFromLocked(rec, eventType, "", code, success)
}

func (r *Registry) appendEventFromLocked(
	rec *Record,
	eventType EventType,
	from Status,
	code string,
	success *bool,
) {
	rec.LastEventSeq++
	sequence := rec.LastEventSeq
	r.events = append(r.events, Event{
		SchemaVersion: EventSchemaVersion,
		EventID:       fmt.Sprintf("%s:%06d:%s", rec.ID, sequence, eventType),
		InteractionID: rec.ID,
		Type:          eventType,
		From:          from,
		To:            rec.Status,
		Outcome:       rec.Outcome,
		Revision:      rec.Revision,
		Sequence:      sequence,
		EmittedAt:     rec.UpdatedAt,
		Code:          strings.TrimSpace(code),
		Success:       success,
	})
}

func (r *Registry) queueNotificationsLocked(events []Event) bool {
	if len(events) == 0 {
		return false
	}
	observers := append([]observerEntry(nil), r.observers...)
	for _, event := range events {
		r.notifications = append(r.notifications, queuedObservation{
			observation: EventObservation{
				Event:  event,
				Record: cloneRecord(r.records[event.InteractionID]),
			},
			observers: observers,
		})
	}
	if r.notifying {
		return false
	}
	r.notifying = true
	return true
}

func (r *Registry) drainNotifications() {
	for {
		r.mu.Lock()
		if len(r.notifications) == 0 {
			r.notifying = false
			r.mu.Unlock()
			return
		}
		queued := r.notifications[0]
		r.notifications[0] = queuedObservation{}
		r.notifications = r.notifications[1:]
		r.mu.Unlock()
		for _, entry := range queued.observers {
			notifyObserver(entry.observer, queued.observation)
		}
	}
}

func notifyObserver(observer Observer, observation EventObservation) {
	defer func() {
		if recovered := recover(); recovered != nil {
			logger.WarnCF(
				"interactions",
				"Recovered interaction event observer panic",
				map[string]any{
					"event_id": observation.Event.EventID,
				},
			)
		}
	}()
	observer(observation)
}

func (r *Registry) nowMillis() int64 {
	return r.options.Now().UnixMilli()
}

func randomID() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("create interaction id: %w", err)
	}
	return "interaction_" + hex.EncodeToString(raw), nil
}

func shortID(id string) string {
	id = strings.TrimPrefix(id, "interaction_")
	if len(id) > 8 {
		id = id[:8]
	}
	return id
}

func bounded(value string, maxLength int) string {
	if utf8.RuneCountInString(value) <= maxLength {
		return value
	}
	return string([]rune(value)[:maxLength])
}

var regexpID = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func normalizeRoute(route Route) Route {
	route.AgentID = strings.TrimSpace(route.AgentID)
	route.SessionKey = strings.TrimSpace(route.SessionKey)
	route.RouteSessionKey = strings.TrimSpace(route.RouteSessionKey)
	route.Channel = strings.TrimSpace(route.Channel)
	route.AccountID = strings.TrimSpace(route.AccountID)
	route.ChatID = strings.TrimSpace(route.ChatID)
	route.TopicID = strings.TrimSpace(route.TopicID)
	route.SenderID = strings.TrimSpace(route.SenderID)
	return route
}

func normalizeOrigin(origin Origin) Origin {
	origin.TurnID = strings.TrimSpace(origin.TurnID)
	origin.ToolCallID = strings.TrimSpace(origin.ToolCallID)
	origin.ToolName = strings.TrimSpace(origin.ToolName)
	origin.TaskID = strings.TrimSpace(origin.TaskID)
	origin.ContinuationSessionKey = strings.TrimSpace(origin.ContinuationSessionKey)
	origin.ArgumentHash = strings.TrimSpace(origin.ArgumentHash)
	origin.ExecutionContext = cloneExecutionContext(origin.ExecutionContext)
	return origin
}

func routesEqual(left, right Route) bool {
	return left.AgentID == right.AgentID &&
		left.SessionKey == right.SessionKey &&
		left.RouteSessionKey == right.RouteSessionKey &&
		left.Channel == right.Channel &&
		left.AccountID == right.AccountID &&
		left.ChatID == right.ChatID &&
		left.TopicID == right.TopicID &&
		left.SenderID == right.SenderID
}

func cloneRecord(rec Record) Record {
	rec.Origin.ExecutionContext = cloneExecutionContext(rec.Origin.ExecutionContext)
	rec.Questions = cloneQuestions(rec.Questions)
	if rec.Answer != nil {
		answer := *rec.Answer
		answer.Values = cloneStringMap(rec.Answer.Values)
		rec.Answer = &answer
	}
	return rec
}

func cloneExecutionContext(src *bus.InboundContext) *bus.InboundContext {
	if src == nil {
		return nil
	}
	cloned := *src
	cloned.ReplyHandles = cloneStringMap(src.ReplyHandles)
	cloned.Raw = cloneStringMap(src.Raw)
	return &cloned
}

func validateApprovalCreateMetadata(
	kind Kind,
	executionContext *bus.InboundContext,
	action string,
) error {
	if kind != KindApproval {
		if executionContext != nil || strings.TrimSpace(action) != "" {
			return fmt.Errorf("%w: question interactions cannot carry approval metadata", ErrInvalidInteraction)
		}
		return nil
	}
	if executionContext == nil {
		return fmt.Errorf("%w: approval requires the original execution context", ErrInvalidInteraction)
	}
	if strings.TrimSpace(action) == "" || !validBoundedString(action, MaxApprovalAction) {
		return fmt.Errorf("%w: approval requires a bounded action description", ErrInvalidInteraction)
	}
	return validateExecutionContext(executionContext)
}

func validateStoredApprovalMetadata(
	kind Kind,
	executionContext *bus.InboundContext,
	action string,
) error {
	if kind != KindApproval {
		if executionContext != nil || strings.TrimSpace(action) != "" {
			return fmt.Errorf("question interaction carries approval metadata")
		}
		return nil
	}
	// Obsolete snapshots can remain readable, but the agent recovery path
	// refuses to execute approvals without current authority metadata.
	if executionContext != nil {
		if err := validateExecutionContext(executionContext); err != nil {
			return err
		}
	}
	if !validBoundedString(action, MaxApprovalAction) {
		return fmt.Errorf("approval action exceeds bounds")
	}
	return nil
}

func validateExecutionContext(ctx *bus.InboundContext) error {
	data, err := json.Marshal(ctx)
	if err != nil {
		return fmt.Errorf("%w: encode execution context: %v", ErrInvalidInteraction, err)
	}
	if len(data) > MaxExecutionContext {
		return fmt.Errorf("%w: execution context exceeds %d bytes", ErrInvalidInteraction, MaxExecutionContext)
	}
	return nil
}

func cloneQuestions(questions []Question) []Question {
	if len(questions) == 0 {
		return nil
	}
	out := make([]Question, len(questions))
	for i, question := range questions {
		out[i] = question
		out[i].Options = append([]Option(nil), question.Options...)
	}
	return out
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func sortRecords(records []Record) {
	sort.Slice(records, func(i, j int) bool {
		if records[i].CreatedAt != records[j].CreatedAt {
			return records[i].CreatedAt < records[j].CreatedAt
		}
		return records[i].ID < records[j].ID
	})
}
