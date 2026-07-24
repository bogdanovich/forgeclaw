package agent

import (
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/evalcapture"
	"github.com/sipeed/picoclaw/pkg/evaltrace"
	taskregistry "github.com/sipeed/picoclaw/pkg/tasks"
)

const (
	taskTraceAdmissionRetryDelay = 100 * time.Millisecond
	maxCompletedTaskTraces       = 4096
)

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
}

type taskRegistrySubscription struct {
	registry    *taskregistry.Registry
	unsubscribe func()
}

type taskTraceProjector struct {
	mu       sync.Mutex
	closed   bool
	settings traceCaptureSettings

	registries map[string]*taskregistry.Registry
	subs       map[string]taskRegistrySubscription
	traces     map[taskTraceKey]*taskTraceState
	completed  map[taskTraceKey]struct{}
	order      []taskTraceKey
	retryTimer *time.Timer
	submit     func(traceCaptureSettings, *activeTraceCapture) error
}

func newTaskTraceProjector(
	settings traceCaptureSettings,
	submit func(traceCaptureSettings, *activeTraceCapture) error,
) *taskTraceProjector {
	return &taskTraceProjector{
		settings:   settings,
		registries: make(map[string]*taskregistry.Registry),
		subs:       make(map[string]taskRegistrySubscription),
		traces:     make(map[taskTraceKey]*taskTraceState),
		completed:  make(map[taskTraceKey]struct{}),
		submit:     submit,
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
		p.clearCaptureStateLocked()
		p.mu.Unlock()
		unsubscribeTaskRegistries(subs)
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
		history := events[key]
		sort.Slice(history, func(i, j int) bool {
			if history[i].Seq != history[j].Seq {
				return history[i].Seq < history[j].Seq
			}
			return history[i].EventID < history[j].EventID
		})
		state := newTaskTraceState(p.settings, workspace, record, firstTaskEvent(history))
		p.traces[key] = state
		if len(history) == 0 {
			state.trace.builder.MarkIncomplete(
				"task_history_missing_at_startup",
				int(max(1, record.LastEventSeq)),
			)
			state.lastSeq = record.LastEventSeq
		} else {
			for _, event := range history {
				p.appendEventLocked(state, event, record)
			}
			if state.lastSeq < record.LastEventSeq {
				state.trace.builder.MarkIncomplete(
					"task_history_missing_at_startup",
					int(record.LastEventSeq-state.lastSeq),
				)
				state.lastSeq = record.LastEventSeq
			}
		}
		if taskRecordIsCaptureTerminal(record) {
			p.terminalizeLocked(key, state, record)
		}
	}
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
	if _, done := p.completed[key]; done {
		return
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
	state.retryable = false
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
	err := p.submit(state.settings, state.trace)
	if err == nil {
		delete(p.traces, key)
		p.recordCompletedLocked(key)
		return
	}
	state.retryable = taskTraceAdmissionCanRetry(err)
	if state.retryable {
		p.scheduleRetryLocked()
	}
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
}

func (p *taskTraceProjector) close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
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
	states := make([]*taskTraceState, 0, len(keys))
	for _, key := range keys {
		state := p.traces[key]
		if !state.terminal {
			state.trace.builder.MarkIncomplete(
				"runtime_closed_before_terminal_task_delivery",
				0,
			)
		}
		states = append(states, state)
	}
	p.traces = nil
	p.completed = nil
	p.order = nil
	p.registries = nil
	p.mu.Unlock()

	unsubscribeTaskRegistries(subs)
	for _, state := range states {
		if p.submit != nil {
			_ = p.submit(state.settings, state.trace)
		}
	}
}

func (p *taskTraceProjector) recordCompletedLocked(key taskTraceKey) {
	if _, exists := p.completed[key]; exists {
		return
	}
	p.completed[key] = struct{}{}
	p.order = append(p.order, key)
	if len(p.order) <= maxCompletedTaskTraces {
		return
	}
	oldest := p.order[0]
	p.order[0] = taskTraceKey{}
	p.order = p.order[1:]
	delete(p.completed, oldest)
}

func (p *taskTraceProjector) clearWorkspaceLocked(workspace string) {
	for key := range p.traces {
		if key.workspace == workspace {
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
	p.completed = make(map[taskTraceKey]struct{})
	p.order = nil
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
				TraceKind:   evaltrace.TraceKindTask,
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
			CompletionID: firstNonEmpty(event.Payload["completion_id"], state.LastCompletionID),
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
		record.DeliveryStatus == taskregistry.DeliveryFailed ||
		record.DeliveryStatus == taskregistry.DeliveryParentMissing ||
		record.DeliveryStatus == taskregistry.DeliveryNotApplicable
	return statusTerminal && deliveryTerminal
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
