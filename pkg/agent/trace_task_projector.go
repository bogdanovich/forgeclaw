package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/evalcapture"
	"github.com/sipeed/picoclaw/pkg/evaltrace"
	"github.com/sipeed/picoclaw/pkg/logger"
	taskregistry "github.com/sipeed/picoclaw/pkg/tasks"
)

const (
	taskTraceAdmissionRetryDelay  = 100 * time.Millisecond
	maxCompletedTaskTraces        = 4096
	maxPendingTaskTraceAdmissions = 128
)

var errTaskTraceAlreadyDurable = errors.New("task trace is already durable")

type taskTraceKey struct {
	workspace    string
	taskID       string
	generationID string
}

type taskTraceState struct {
	trace      *activeTraceCapture
	settings   traceCaptureSettings
	lastSeq    int64
	lastOffset int64
	terminal   bool
	retryable  bool
	admitted   bool
	dirty      bool
	receipt    string
}

type taskRegistrySubscription struct {
	registry    *taskregistry.Registry
	unsubscribe func()
}

type taskTraceProjector struct {
	mu       sync.Mutex
	closed   bool
	settings traceCaptureSettings

	registries       map[string]*taskregistry.Registry
	subs             map[string]taskRegistrySubscription
	traces           map[taskTraceKey]*taskTraceState
	completed        map[taskTraceKey]int64
	order            []taskTraceKey
	retryTimer       *time.Timer
	submit           func(traceCaptureSettings, *activeTraceCapture) error
	submitWait       func(context.Context, traceCaptureSettings, *activeTraceCapture) error
	awaitPersistence bool
	inflight         map[string]taskTraceKey
	nextSubmission   uint64

	pendingAdmissions int
	overflowDeferrals uint64
	permanentDrops    uint64
	deferred          bool
}

func newTaskTraceProjector(
	settings traceCaptureSettings,
	submit func(traceCaptureSettings, *activeTraceCapture) error,
	waiters ...func(context.Context, traceCaptureSettings, *activeTraceCapture) error,
) *taskTraceProjector {
	var submitWait func(context.Context, traceCaptureSettings, *activeTraceCapture) error
	if len(waiters) > 0 {
		submitWait = waiters[0]
	}
	return &taskTraceProjector{
		settings:   settings,
		registries: make(map[string]*taskregistry.Registry),
		subs:       make(map[string]taskRegistrySubscription),
		traces:     make(map[taskTraceKey]*taskTraceState),
		completed:  make(map[taskTraceKey]int64),
		inflight:   make(map[string]taskTraceKey),
		submit:     submit,
		submitWait: submitWait,
	}
}

func (m *traceCaptureManager) attachTaskRegistry(
	workspace string,
	registry *taskregistry.Registry,
) {
	if m != nil && m.tasks != nil {
		m.tasks.attach(workspace, registry)
	}
}

func (p *taskTraceProjector) attach(workspace string, registry *taskregistry.Registry) {
	if p == nil || registry == nil {
		return
	}
	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		return
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	if _, exists := p.registries[workspace]; exists {
		p.mu.Unlock()
		return
	}
	p.registries[workspace] = registry
	enabled := p.settings.enabled
	p.mu.Unlock()
	if enabled {
		p.subscribe(workspace, registry)
	}
}

func (p *taskTraceProjector) updateSettings(settings traceCaptureSettings) {
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	wasEnabled := p.settings.enabled
	p.settings = settings
	if wasEnabled && !settings.enabled {
		subs := p.takeSubscriptionsLocked()
		registries := cloneTaskRegistries(p.registries)
		p.clearCaptureStateLocked()
		p.mu.Unlock()
		unsubscribeTaskRegistries(subs)
		if p.awaitPersistence {
			setTaskRegistryTraceProtection(registries, false)
		}
		return
	}
	if wasEnabled || !settings.enabled {
		p.mu.Unlock()
		return
	}
	registries := cloneTaskRegistries(p.registries)
	p.mu.Unlock()
	for workspace, registry := range registries {
		p.subscribe(workspace, registry)
	}
}

func (p *taskTraceProjector) subscribe(
	workspace string,
	registry *taskregistry.Registry,
) {
	if p.awaitPersistence {
		if err := registry.SetTraceCaptureProtection(true); err != nil {
			logger.WarnCF("evaltrace", "Failed to protect task trace snapshot", map[string]any{
				"workspace": workspace,
				"error":     err.Error(),
			})
			return
		}
	}
	snapshot, activate, unsubscribe := registry.SubscribeSnapshot(
		func(observation taskregistry.EventObservation) {
			p.observe(workspace, observation)
		},
	)

	p.mu.Lock()
	if p.closed || !p.settings.enabled || p.registries[workspace] != registry {
		p.mu.Unlock()
		unsubscribe()
		return
	}
	if _, exists := p.subs[workspace]; exists {
		p.mu.Unlock()
		unsubscribe()
		return
	}
	p.subs[workspace] = taskRegistrySubscription{
		registry: registry, unsubscribe: unsubscribe,
	}
	p.applySnapshotLocked(workspace, snapshot)
	p.mu.Unlock()
	activate()
}

func (p *taskTraceProjector) applySnapshotLocked(
	workspace string,
	snapshot taskregistry.ObservationSnapshot,
) {
	p.clearWorkspaceLocked(workspace)
	records := append([]taskregistry.Record(nil), snapshot.Records...)
	sort.Slice(records, func(i, j int) bool {
		if records[i].TaskID != records[j].TaskID {
			return records[i].TaskID < records[j].TaskID
		}
		return records[i].GenerationID < records[j].GenerationID
	})
	events := make(map[taskTraceKey][]taskregistry.TaskEvent, len(records))
	for _, event := range snapshot.Events {
		key := newTaskTraceKey(workspace, event.TaskID, event.GenerationID)
		events[key] = append(events[key], event)
	}
	for _, record := range records {
		key := newTaskTraceKey(workspace, record.TaskID, record.GenerationID)
		state := p.restoreStateLocked(workspace, record, events[key])
		p.traces[key] = state
		if taskRecordIsCaptureTerminal(record) {
			p.terminalizeLocked(key, state, record)
		}
	}
}

func (p *taskTraceProjector) restoreStateLocked(
	workspace string,
	record taskregistry.Record,
	history []taskregistry.TaskEvent,
) *taskTraceState {
	history = append([]taskregistry.TaskEvent(nil), history...)
	sort.Slice(history, func(i, j int) bool {
		if history[i].Seq != history[j].Seq {
			return history[i].Seq < history[j].Seq
		}
		return history[i].EventID < history[j].EventID
	})
	state := newTaskTraceState(p.settings, workspace, record, firstTaskEvent(history))
	if len(history) == 0 {
		state.trace.builder.MarkIncomplete(
			"task_history_missing_at_startup",
			int(max(1, record.LastEventSeq)),
		)
		state.lastSeq = record.LastEventSeq
		return state
	}
	for _, event := range history {
		if event.GenerationID == record.GenerationID {
			p.appendEventLocked(state, event, record)
		}
	}
	if state.lastSeq < record.LastEventSeq {
		state.trace.builder.MarkIncomplete(
			"task_history_missing_at_startup",
			int(record.LastEventSeq-state.lastSeq),
		)
		state.lastSeq = record.LastEventSeq
	}
	return state
}

func (p *taskTraceProjector) observe(
	workspace string,
	observation taskregistry.EventObservation,
) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || !p.settings.enabled {
		return
	}
	event, record := observation.Event, observation.Record
	key := newTaskTraceKey(workspace, event.TaskID, event.GenerationID)
	if key.taskID == "" || key.generationID == "" ||
		record.TaskID != event.TaskID || record.GenerationID != event.GenerationID {
		return
	}
	if completedSeq, done := p.completed[key]; done {
		if event.Seq <= completedSeq {
			return
		}
		p.removeCompletedLocked(key)
		if registry := p.registries[workspace]; registry != nil {
			state := p.restoreStateLocked(
				workspace,
				record,
				registry.ListEvents(record.TaskID),
			)
			p.traces[key] = state
			if observation.FinalForTask && taskRecordIsCaptureTerminal(record) {
				p.terminalizeLocked(key, state, record)
			}
			return
		}
	}
	state := p.traces[key]
	if state == nil {
		state = newTaskTraceState(p.settings, workspace, record, event)
		p.traces[key] = state
	}
	if event.Seq <= state.lastSeq {
		if state.terminal && state.retryable {
			p.trySubmitLocked(key, state)
		}
		return
	}
	state.terminal = false
	if state.retryable {
		p.pendingAdmissions--
		state.retryable = false
	}
	if state.admitted {
		state.dirty = true
	}
	p.appendEventLocked(state, event, record)
	if observation.FinalForTask && taskRecordIsCaptureTerminal(record) {
		p.terminalizeLocked(key, state, record)
	}
}

func (p *taskTraceProjector) appendEventLocked(
	state *taskTraceState,
	event taskregistry.TaskEvent,
	record taskregistry.Record,
) {
	if state == nil || state.trace == nil {
		return
	}
	if event.Seq <= state.lastSeq {
		return
	}
	if event.Seq > state.lastSeq+1 {
		state.trace.builder.MarkIncomplete(
			"task_event_sequence_gap",
			int(event.Seq-state.lastSeq-1),
		)
	}
	taskRecord, critical := normalizedTaskEventRecord(
		state.settings,
		state.trace,
		event,
		record,
	)
	if taskRecord.OffsetNanos < state.lastOffset {
		taskRecord.OffsetNanos = state.lastOffset
	}
	appendCaptureRecord(state.trace, taskRecord, critical)
	state.lastSeq = event.Seq
	state.lastOffset = taskRecord.OffsetNanos
}

func (p *taskTraceProjector) terminalizeLocked(
	key taskTraceKey,
	state *taskTraceState,
	record taskregistry.Record,
) {
	state.trace.builder.SetOutcome(evaltrace.Outcome{
		Status: string(record.Status), ErrorCode: taskErrorCode(record),
	})
	state.terminal = true
	p.trySubmitLocked(key, state)
}

func (p *taskTraceProjector) trySubmitLocked(
	key taskTraceKey,
	state *taskTraceState,
) {
	if p.submit == nil || state == nil || state.trace == nil {
		return
	}
	if state.admitted {
		return
	}
	traceID := state.trace.builder.TraceID()
	submissionID := ""
	if p.awaitPersistence {
		if err := p.setTracePendingLocked(key, true); err != nil {
			logger.WarnCF("evaltrace", "Failed to protect task trace retry marker", map[string]any{
				"workspace":     key.workspace,
				"task_id":       key.taskID,
				"generation_id": key.generationID,
				"error":         err.Error(),
			})
			if !state.retryable {
				p.retryOrDeferLocked(key, state)
			}
			return
		}
		submissionID = p.nextSubmissionIDLocked(traceID)
		state.trace.submissionID = submissionID
		p.inflight[submissionID] = key
	}
	err := p.submit(state.settings, state.trace)
	if errors.Is(err, errTaskTraceAlreadyDurable) {
		delete(p.inflight, submissionID)
		confirmed, markerErr := p.confirmTracePersistedLocked(key, state)
		if markerErr != nil {
			if !state.retryable {
				p.retryOrDeferLocked(key, state)
			}
			return
		}
		if !confirmed {
			return
		}
		if state.retryable {
			p.pendingAdmissions--
		}
		delete(p.traces, key)
		p.recordCompletedLocked(key, state.lastSeq)
		return
	}
	if err == nil {
		if state.retryable {
			p.pendingAdmissions--
			state.retryable = false
		}
		if p.awaitPersistence {
			state.admitted = true
			state.receipt = submissionID
			return
		}
		delete(p.traces, key)
		p.recordCompletedLocked(key, state.lastSeq)
		return
	}
	delete(p.inflight, submissionID)
	if taskTraceAdmissionCanRetry(err) {
		if !state.retryable {
			p.retryOrDeferLocked(key, state)
		}
		return
	}
	if state.retryable {
		p.pendingAdmissions--
	}
	delete(p.traces, key)
	p.recordCompletedLocked(key, state.lastSeq)
	p.permanentDrops++
	logger.WarnCF("evaltrace", "Dropped task trace after permanent admission failure", map[string]any{
		"workspace":       key.workspace,
		"task_id":         key.taskID,
		"generation_id":   key.generationID,
		"permanent_drops": p.permanentDrops,
		"error":           err.Error(),
	})
}

func (p *taskTraceProjector) retryOrDeferLocked(
	key taskTraceKey,
	state *taskTraceState,
) {
	if p.pendingAdmissions < maxPendingTaskTraceAdmissions {
		state.retryable = true
		p.pendingAdmissions++
		p.scheduleRetryLocked()
		return
	}
	delete(p.traces, key)
	p.deferred = true
	p.overflowDeferrals++
	logger.WarnCF(
		"evaltrace",
		"Deferred task trace to durable registry after admission spool saturation",
		map[string]any{
			"workspace":          key.workspace,
			"task_id":            key.taskID,
			"generation_id":      key.generationID,
			"pending":            p.pendingAdmissions,
			"pending_limit":      maxPendingTaskTraceAdmissions,
			"overflow_deferrals": p.overflowDeferrals,
		},
	)
	p.scheduleRetryLocked()
}

func (p *taskTraceProjector) observeWriterEvent(event evalcapture.Event) {
	if p == nil ||
		(event.Kind != evalcapture.EventPersisted &&
			event.Kind != evalcapture.EventPermanentlyFailed) {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.observeWriterEventLocked(event)
}

func (p *taskTraceProjector) observeWriterEventLocked(event evalcapture.Event) {
	if strings.TrimSpace(event.SubmissionID) == "" {
		return
	}
	key, tracked := p.inflight[event.SubmissionID]
	if !tracked {
		return
	}
	delete(p.inflight, event.SubmissionID)
	state := p.traces[key]
	if state == nil || state.receipt != event.SubmissionID {
		return
	}
	state.admitted = false
	state.receipt = ""
	switch event.Kind {
	case evalcapture.EventPersisted:
		if state.dirty {
			state.dirty = false
			if state.terminal {
				p.trySubmitLocked(key, state)
			}
			return
		}
		confirmed, err := p.confirmTracePersistedLocked(key, state)
		if err != nil {
			p.retryOrDeferLocked(key, state)
			return
		}
		if !confirmed {
			return
		}
		delete(p.traces, key)
		p.recordCompletedLocked(key, state.lastSeq)
	case evalcapture.EventPermanentlyFailed:
		state.dirty = false
		p.retryOrDeferLocked(key, state)
	}
}

func (p *taskTraceProjector) confirmTracePersistedLocked(
	key taskTraceKey,
	state *taskTraceState,
) (bool, error) {
	registry := p.registries[key.workspace]
	if registry == nil {
		return true, nil
	}
	record, confirmed, err := registry.ConfirmTraceCapturePersisted(
		key.taskID,
		key.generationID,
		state.lastSeq,
	)
	if err != nil {
		logger.WarnCF("evaltrace", "Failed to confirm task trace persistence", map[string]any{
			"workspace":      key.workspace,
			"task_id":        key.taskID,
			"generation_id":  key.generationID,
			"last_event_seq": state.lastSeq,
			"error":          err.Error(),
		})
		return false, err
	}
	if confirmed {
		return true, nil
	}
	if record.LastEventSeq <= state.lastSeq {
		return false, fmt.Errorf(
			"task %q trace revision moved backward from %d to %d",
			key.taskID,
			state.lastSeq,
			record.LastEventSeq,
		)
	}
	if state.retryable {
		p.pendingAdmissions--
	}
	rebuilt := p.restoreStateLocked(
		key.workspace,
		record,
		registry.ListEvents(record.TaskID),
	)
	p.traces[key] = rebuilt
	if taskRecordIsCaptureTerminal(record) {
		p.terminalizeLocked(key, rebuilt, record)
	}
	return false, nil
}

func (p *taskTraceProjector) setTracePendingLocked(
	key taskTraceKey,
	pending bool,
) error {
	registry := p.registries[key.workspace]
	if registry == nil {
		return nil
	}
	return registry.SetTraceCapturePending(
		key.taskID,
		key.generationID,
		pending,
	)
}

func taskTraceAdmissionCanRetry(err error) bool {
	var admission *evalcapture.AdmissionError
	return errors.As(err, &admission) && admission.Reason == evalcapture.ReasonCapacity
}

func (p *taskTraceProjector) scheduleRetryLocked() {
	if p.retryTimer != nil || p.closed || !p.settings.enabled {
		return
	}
	p.retryTimer = time.AfterFunc(taskTraceAdmissionRetryDelay, p.retryPending)
}

func (p *taskTraceProjector) retryPending() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.retryTimer = nil
	if p.closed || !p.settings.enabled {
		return
	}
	keys := make([]taskTraceKey, 0, len(p.traces))
	for key, state := range p.traces {
		if state.terminal && state.retryable {
			keys = append(keys, key)
		}
	}
	sortTaskTraceKeys(keys)
	for _, key := range keys {
		if state := p.traces[key]; state != nil {
			p.trySubmitLocked(key, state)
		}
	}
	p.recoverDeferredLocked()
	if p.pendingAdmissions > 0 || p.deferred {
		p.scheduleRetryLocked()
	}
}

func (p *taskTraceProjector) recoverDeferredLocked() {
	if !p.deferred || p.submit == nil {
		return
	}
	p.deferred = false
	workspaces := make([]string, 0, len(p.registries))
	for workspace := range p.registries {
		workspaces = append(workspaces, workspace)
	}
	sort.Strings(workspaces)
	for _, workspace := range workspaces {
		registry := p.registries[workspace]
		records := registry.List()
		sort.Slice(records, func(i, j int) bool {
			if records[i].TaskID != records[j].TaskID {
				return records[i].TaskID < records[j].TaskID
			}
			return records[i].GenerationID < records[j].GenerationID
		})
		for _, record := range records {
			if !taskRecordIsCaptureTerminal(record) {
				continue
			}
			key := newTaskTraceKey(workspace, record.TaskID, record.GenerationID)
			completedSeq, done := p.completed[key]
			if (done && record.LastEventSeq <= completedSeq) ||
				p.traces[key] != nil {
				continue
			}
			if done {
				p.removeCompletedLocked(key)
			}
			if p.pendingAdmissions >= maxPendingTaskTraceAdmissions {
				p.deferred = true
				return
			}
			state := p.restoreStateLocked(
				workspace,
				record,
				registry.ListEvents(record.TaskID),
			)
			p.traces[key] = state
			p.terminalizeLocked(key, state, record)
		}
	}
}

func (p *taskTraceProjector) close() {
	ctx, cancel := context.WithTimeout(context.Background(), traceShutdownAdmissionTimeout)
	defer cancel()
	_ = p.closeWithContext(ctx)
	p.finishClose()
}

func (p *taskTraceProjector) closeWithContext(ctx context.Context) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	if p.retryTimer != nil {
		p.retryTimer.Stop()
		p.retryTimer = nil
	}
	subs := p.takeSubscriptionsLocked()
	keys := make([]taskTraceKey, 0, len(p.traces))
	for key := range p.traces {
		keys = append(keys, key)
	}
	sortTaskTraceKeys(keys)
	type pendingClose struct {
		key   taskTraceKey
		state *taskTraceState
	}
	states := make([]pendingClose, 0, len(keys))
	for _, key := range keys {
		state := p.traces[key]
		if state.admitted && !state.dirty {
			continue
		}
		if !state.terminal {
			state.trace.builder.MarkIncomplete(
				"runtime_closed_before_terminal_task_delivery",
				0,
			)
		}
		states = append(states, pendingClose{key: key, state: state})
	}
	p.mu.Unlock()

	unsubscribeTaskRegistries(subs)
	var firstErr error
	for _, pending := range states {
		state := pending.state
		traceID := state.trace.builder.TraceID()
		submissionID := ""
		if p.awaitPersistence {
			p.mu.Lock()
			markerErr := p.setTracePendingLocked(pending.key, true)
			if markerErr == nil {
				submissionID = p.nextSubmissionIDLocked(traceID)
				state.trace.submissionID = submissionID
				p.inflight[submissionID] = pending.key
				state.receipt = submissionID
				state.admitted = true
			}
			p.mu.Unlock()
			if markerErr != nil {
				if firstErr == nil {
					firstErr = markerErr
				}
				continue
			}
		}
		var err error
		if p.submitWait != nil {
			err = p.submitWait(ctx, state.settings, state.trace)
		} else if p.submit != nil {
			err = p.submit(state.settings, state.trace)
		}
		p.mu.Lock()
		if p.traces[pending.key] != state {
			p.mu.Unlock()
			continue
		}
		switch {
		case errors.Is(err, errTaskTraceAlreadyDurable):
			p.clearInflightForKeyLocked(pending.key)
			confirmed, markerErr := p.confirmTracePersistedLocked(
				pending.key,
				state,
			)
			if markerErr != nil {
				if firstErr == nil {
					firstErr = markerErr
				}
			} else if confirmed {
				delete(p.traces, pending.key)
				p.recordCompletedLocked(pending.key, state.lastSeq)
			}
		case err != nil:
			delete(p.inflight, submissionID)
			if state.receipt == submissionID {
				state.admitted = false
				state.receipt = ""
			}
			if firstErr == nil {
				firstErr = err
			}
		default:
			state.admitted = p.awaitPersistence
			state.dirty = false
		}
		p.mu.Unlock()
	}
	return firstErr
}

func (p *taskTraceProjector) finishClose() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.traces = nil
	p.completed = nil
	p.inflight = nil
	p.order = nil
	p.registries = nil
}

type taskTraceProjectorStats struct {
	PendingAdmissions int
	OverflowDeferrals uint64
	PermanentDrops    uint64
}

func (p *taskTraceProjector) stats() taskTraceProjectorStats {
	if p == nil {
		return taskTraceProjectorStats{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return taskTraceProjectorStats{
		PendingAdmissions: p.pendingAdmissions,
		OverflowDeferrals: p.overflowDeferrals,
		PermanentDrops:    p.permanentDrops,
	}
}

func (p *taskTraceProjector) recordCompletedLocked(
	key taskTraceKey,
	lastSeq int64,
) {
	if _, exists := p.completed[key]; exists {
		p.completed[key] = max(p.completed[key], lastSeq)
		return
	}
	p.completed[key] = lastSeq
	p.order = append(p.order, key)
	if len(p.order) <= maxCompletedTaskTraces {
		return
	}
	oldest := p.order[0]
	p.order[0] = taskTraceKey{}
	p.order = p.order[1:]
	delete(p.completed, oldest)
}

func (p *taskTraceProjector) removeCompletedLocked(key taskTraceKey) {
	delete(p.completed, key)
	for i, ordered := range p.order {
		if ordered != key {
			continue
		}
		copy(p.order[i:], p.order[i+1:])
		p.order[len(p.order)-1] = taskTraceKey{}
		p.order = p.order[:len(p.order)-1]
		return
	}
}

func (p *taskTraceProjector) nextSubmissionIDLocked(traceID string) string {
	p.nextSubmission++
	return traceID + ":" + strconv.FormatUint(p.nextSubmission, 10)
}

func (p *taskTraceProjector) clearInflightForKeyLocked(key taskTraceKey) {
	for submissionID, inflightKey := range p.inflight {
		if inflightKey == key {
			delete(p.inflight, submissionID)
		}
	}
}

func (p *taskTraceProjector) clearWorkspaceLocked(workspace string) {
	for traceID, key := range p.inflight {
		if key.workspace == workspace {
			delete(p.inflight, traceID)
		}
	}
	for key := range p.traces {
		if key.workspace == workspace {
			if p.traces[key].retryable {
				p.pendingAdmissions--
			}
			delete(p.traces, key)
		}
	}
	if len(p.completed) == 0 {
		return
	}
	order := p.order[:0]
	for _, key := range p.order {
		if key.workspace == workspace {
			delete(p.completed, key)
			continue
		}
		order = append(order, key)
	}
	p.order = order
}

func (p *taskTraceProjector) clearCaptureStateLocked() {
	if p.retryTimer != nil {
		p.retryTimer.Stop()
		p.retryTimer = nil
	}
	p.traces = make(map[taskTraceKey]*taskTraceState)
	p.completed = make(map[taskTraceKey]int64)
	p.inflight = make(map[string]taskTraceKey)
	p.order = nil
	p.pendingAdmissions = 0
	p.deferred = false
}

func (p *taskTraceProjector) takeSubscriptionsLocked() []taskRegistrySubscription {
	subs := make([]taskRegistrySubscription, 0, len(p.subs))
	for _, sub := range p.subs {
		subs = append(subs, sub)
	}
	p.subs = make(map[string]taskRegistrySubscription)
	return subs
}

func unsubscribeTaskRegistries(subs []taskRegistrySubscription) {
	for _, sub := range subs {
		if sub.unsubscribe != nil {
			sub.unsubscribe()
		}
	}
}

func setTaskRegistryTraceProtection(
	registries map[string]*taskregistry.Registry,
	enabled bool,
) {
	for workspace, registry := range registries {
		if err := registry.SetTraceCaptureProtection(enabled); err != nil {
			logger.WarnCF("evaltrace", "Failed to update task trace retention protection", map[string]any{
				"workspace": workspace,
				"enabled":   enabled,
				"error":     err.Error(),
			})
		}
	}
}

func cloneTaskRegistries(
	registries map[string]*taskregistry.Registry,
) map[string]*taskregistry.Registry {
	cloned := make(map[string]*taskregistry.Registry, len(registries))
	for workspace, registry := range registries {
		cloned[workspace] = registry
	}
	return cloned
}

func newTaskTraceKey(workspace, taskID, generationID string) taskTraceKey {
	return taskTraceKey{
		workspace:    strings.TrimSpace(workspace),
		taskID:       strings.TrimSpace(taskID),
		generationID: strings.TrimSpace(generationID),
	}
}

func newTaskTraceState(
	settings traceCaptureSettings,
	workspace string,
	record taskregistry.Record,
	firstEvent taskregistry.TaskEvent,
) *taskTraceState {
	startedAt := time.UnixMilli(record.CreatedAt)
	if record.CreatedAt <= 0 {
		startedAt = time.UnixMilli(firstEvent.EmittedAt)
	}
	if startedAt.IsZero() || startedAt.UnixMilli() <= 0 {
		startedAt = time.UnixMilli(1)
	}
	trace := &activeTraceCapture{
		workspace: workspace,
		startedAt: startedAt,
		builder: evalcapture.NewTraceBuilder(evaltrace.Trace{
			SchemaVersion: evaltrace.SchemaVersionV1,
			TraceID: opaqueTraceID(
				"task",
				workspace+"\x00"+record.TaskID+"\x00"+record.GenerationID,
				startedAt,
			),
			CreatedAt: startedAt.UTC(),
			Policy: evaltrace.CapturePolicy{
				ContentMode: settings.contentMode,
				Redactor:    captureRedactorVersion(settings.contentMode),
			},
			Limits: settings.limits,
			Metadata: evaltrace.Metadata{
				SessionHash: safeHash(settings, record.RequesterSessionKey),
				AgentID:     record.AgentID,
			},
			Records: make([]evaltrace.Record, 0, 16),
		}),
	}
	return &taskTraceState{trace: trace, settings: settings}
}

func firstTaskEvent(events []taskregistry.TaskEvent) taskregistry.TaskEvent {
	if len(events) == 0 {
		return taskregistry.TaskEvent{}
	}
	return events[0]
}

func normalizedTaskEventRecord(
	settings traceCaptureSettings,
	trace *activeTraceCapture,
	event taskregistry.TaskEvent,
	state taskregistry.Record,
) (evaltrace.Record, bool) {
	kind := evaltrace.RecordTaskTransition
	critical := false
	if event.Type == taskregistry.EventTaskDeliveryDecision {
		kind = evaltrace.RecordDeliveryDecision
	} else if event.Type == taskregistry.EventTaskDeliveryChanged {
		kind, critical = evaltrace.RecordDeliveryOutcome, true
	}
	var payload any
	if kind == evaltrace.RecordTaskTransition {
		payload = evaltrace.TaskPayload{
			EventType: string(event.Type), Runtime: string(event.Runtime),
			Status: string(event.Status), DeliveryStatus: string(event.DeliveryStatus),
			GenerationID: event.GenerationID, Sequence: event.Seq,
			Fingerprint: event.Fingerprint, Producer: event.Producer,
		}
	} else {
		payload = evaltrace.DeliveryPayload{
			Mode: event.Payload["mode"], Status: string(event.DeliveryStatus),
			WillUser:   parseTaskBool(event.Payload["will_user"]),
			WillParent: parseTaskBool(event.Payload["will_parent"]),
			ContentLen: parseTaskInt(event.Payload["content_len"]),
			ErrorCode: taskErrorCode(taskregistry.Record{
				Status: event.Status, DeliveryStatus: event.DeliveryStatus,
			}),
		}
	}
	data, _ := json.Marshal(payload)
	return evaltrace.Record{
		OffsetNanos: max(0, time.UnixMilli(event.EmittedAt).Sub(trace.startedAt).Nanoseconds()),
		Kind:        kind,
		Origin:      evaltrace.Origin{Kind: "task_event", ID: event.EventID},
		Scope: evaltrace.Scope{
			AgentID: state.AgentID, SessionHash: safeHash(settings, state.RequesterSessionKey),
			TaskID: event.TaskID, Channel: state.Channel,
			TargetHash: safeHash(settings, targetKey(state.Channel, state.ChatID)),
		},
		Correlation: evaltrace.Correlation{
			CompletionID: event.Payload["completion_id"],
			EventID:      event.EventID,
		},
		Data: data,
	}, critical
}

func taskRecordIsCaptureTerminal(record taskregistry.Record) bool {
	statusTerminal := record.Status == taskregistry.StatusSucceeded ||
		record.Status == taskregistry.StatusFailed ||
		record.Status == taskregistry.StatusTimedOut ||
		record.Status == taskregistry.StatusCancelled ||
		record.Status == taskregistry.StatusLost &&
			strings.TrimSpace(record.InteractionID) == ""
	deliveryTerminal := record.DeliveryStatus == taskregistry.DeliveryDelivered ||
		record.DeliveryStatus == taskregistry.DeliverySessionQueued ||
		record.DeliveryStatus == taskregistry.DeliveryParentMissing ||
		record.DeliveryStatus == taskregistry.DeliveryNotApplicable
	if record.DeliveryStatus == taskregistry.DeliveryFailed {
		deliveryTerminal = strings.TrimSpace(record.InteractionID) == ""
	}
	return statusTerminal && deliveryTerminal
}

func reconcileTaskTraceCandidate(
	existing, candidate evaltrace.Trace,
) (evaltrace.Trace, bool) {
	if existing.TraceID != candidate.TraceID {
		return candidate, true
	}
	if !existing.Truncation.Incomplete && candidate.Truncation.Incomplete {
		if merged, ok := mergeCompleteTaskTrace(existing, candidate); ok {
			return merged, true
		}
		return existing, false
	}
	if traceRecordsExtend(existing.Records, candidate.Records) {
		improves := len(candidate.Records) > len(existing.Records) ||
			existing.Truncation.Incomplete != candidate.Truncation.Incomplete ||
			existing.Truncation.DroppedRecords != candidate.Truncation.DroppedRecords
		return candidate, improves
	}
	if existing.Truncation.Incomplete && !candidate.Truncation.Incomplete {
		return candidate, true
	}
	return existing, false
}

func traceRecordsExtend(existing, candidate []evaltrace.Record) bool {
	if len(candidate) < len(existing) {
		return false
	}
	for i := range existing {
		if existing[i].Origin != candidate[i].Origin ||
			existing[i].Digest != candidate[i].Digest {
			return false
		}
	}
	return true
}

func mergeCompleteTaskTrace(
	existing, candidate evaltrace.Trace,
) (evaltrace.Trace, bool) {
	if len(existing.Records) >= candidate.Limits.MaxRecords {
		return evaltrace.Trace{}, false
	}
	records := append([]evaltrace.Record(nil), existing.Records...)
	seen := make(map[string]struct{}, len(records))
	for _, record := range records {
		seen[record.Origin.Kind+"\x00"+record.Origin.ID] = struct{}{}
	}
	for _, record := range candidate.Records {
		key := record.Origin.Kind + "\x00" + record.Origin.ID
		if _, exists := seen[key]; exists {
			continue
		}
		records = append(records, record)
		seen[key] = struct{}{}
	}
	if len(records) <= len(existing.Records) ||
		len(records) > candidate.Limits.MaxRecords {
		return evaltrace.Trace{}, false
	}
	sort.Slice(records, func(i, j int) bool {
		left, leftOK := taskTraceEventSequence(records[i])
		right, rightOK := taskTraceEventSequence(records[j])
		if leftOK && rightOK && left != right {
			return left < right
		}
		return records[i].Origin.ID < records[j].Origin.ID
	})
	for i := range records {
		sequence, ok := taskTraceEventSequence(records[i])
		if !ok || sequence != int64(i+1) {
			return evaltrace.Trace{}, false
		}
		records[i].Sequence = uint64(i + 1)
		if i > 0 && records[i].OffsetNanos < records[i-1].OffsetNanos {
			records[i].OffsetNanos = records[i-1].OffsetNanos
		}
	}
	candidate.Records = records
	candidate.Truncation = repairTaskHistoryTruncation(candidate.Truncation)
	merged, err := evaltrace.Finalize(candidate)
	if err != nil {
		return evaltrace.Trace{}, false
	}
	return merged, true
}

func repairTaskHistoryTruncation(
	truncation evaltrace.Truncation,
) evaltrace.Truncation {
	reasons := truncation.Reasons[:0]
	for _, reason := range truncation.Reasons {
		if reason == "task_history_missing_at_startup" ||
			reason == "task_event_sequence_gap" {
			continue
		}
		reasons = append(reasons, reason)
	}
	truncation.Reasons = reasons
	if len(reasons) == 0 {
		truncation.DroppedRecords = 0
		for _, dropped := range truncation.DroppedByKind {
			truncation.DroppedRecords += dropped
		}
	}
	truncation.Incomplete = len(truncation.Reasons) > 0 ||
		truncation.DroppedRecords > 0 ||
		len(truncation.DroppedByKind) > 0
	if !truncation.Incomplete {
		return evaltrace.Truncation{}
	}
	return truncation
}

func taskTraceEventSequence(record evaltrace.Record) (int64, bool) {
	if record.Origin.Kind != "task_event" {
		return 0, false
	}
	eventID := record.Origin.ID
	eventType := strings.LastIndexByte(eventID, ':')
	if eventType <= 0 {
		return 0, false
	}
	sequence := strings.LastIndexByte(eventID[:eventType], ':')
	if sequence < 0 {
		return 0, false
	}
	value, err := strconv.ParseInt(eventID[sequence+1:eventType], 10, 64)
	return value, err == nil && value > 0
}

func taskErrorCode(record taskregistry.Record) string {
	if record.DeliveryStatus == taskregistry.DeliveryFailed {
		return "delivery_failed"
	}
	if record.Status == taskregistry.StatusLost {
		return "task_lost"
	}
	if record.Status == taskregistry.StatusFailed {
		return "task_failed"
	}
	return ""
}

func parseTaskBool(value string) bool {
	parsed, _ := strconv.ParseBool(value)
	return parsed
}

func parseTaskInt(value string) int {
	parsed, _ := strconv.Atoi(value)
	return parsed
}

func sortTaskTraceKeys(keys []taskTraceKey) {
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].workspace != keys[j].workspace {
			return keys[i].workspace < keys[j].workspace
		}
		if keys[i].taskID != keys[j].taskID {
			return keys[i].taskID < keys[j].taskID
		}
		return keys[i].generationID < keys[j].generationID
	})
}
