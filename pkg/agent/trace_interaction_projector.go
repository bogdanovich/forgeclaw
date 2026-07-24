package agent

import (
	"bytes"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/evalcapture"
	"github.com/sipeed/picoclaw/pkg/evaltrace"
	"github.com/sipeed/picoclaw/pkg/interactions"
)

const (
	interactionTraceAdmissionRetryDelay = 100 * time.Millisecond
	maxCompletedInteractionTraces       = 4096
)

type interactionTraceKey struct {
	workspace     string
	interactionID string
}

type interactionTraceState struct {
	record    interactions.Record
	events    []interactions.Event
	settings  traceCaptureSettings
	terminal  bool
	retryable bool
}

type interactionRegistrySubscription struct {
	registry    *interactions.Registry
	unsubscribe func()
}

type interactionTraceEvidence struct {
	Complete      bool
	FirstSequence int64
	LastSequence  int64
	LastRevision  int64
}

type interactionTraceProjector struct {
	mu       sync.Mutex
	closed   bool
	settings traceCaptureSettings

	registries map[string]*interactions.Registry
	subs       map[string]interactionRegistrySubscription
	traces     map[interactionTraceKey]*interactionTraceState
	completed  map[interactionTraceKey]struct{}
	order      []interactionTraceKey
	retryTimer *time.Timer
	submit     func(traceCaptureSettings, *activeTraceCapture) error
	current    func(traceCaptureSettings, string, evaltrace.Trace) bool
}

func newInteractionTraceProjector(
	settings traceCaptureSettings,
	submit func(traceCaptureSettings, *activeTraceCapture) error,
) *interactionTraceProjector {
	return &interactionTraceProjector{
		settings:   settings,
		registries: make(map[string]*interactions.Registry),
		subs:       make(map[string]interactionRegistrySubscription),
		traces:     make(map[interactionTraceKey]*interactionTraceState),
		completed:  make(map[interactionTraceKey]struct{}),
		submit:     submit,
		current:    currentInteractionTrace,
	}
}

func (m *traceCaptureManager) attachInteractionRegistry(
	workspace string,
	registry *interactions.Registry,
) {
	if m != nil && m.interactions != nil {
		m.interactions.attach(workspace, registry)
	}
}

func (p *interactionTraceProjector) attach(
	workspace string,
	registry *interactions.Registry,
) {
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

func (p *interactionTraceProjector) updateSettings(settings traceCaptureSettings) {
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
		unsubscribeInteractionRegistries(subs)
		return
	}
	if wasEnabled || !settings.enabled {
		p.mu.Unlock()
		return
	}
	registries := cloneInteractionRegistries(p.registries)
	p.mu.Unlock()
	for workspace, registry := range registries {
		p.subscribe(workspace, registry)
	}
}

func (p *interactionTraceProjector) subscribe(
	workspace string,
	registry *interactions.Registry,
) {
	snapshot, activate, unsubscribe := registry.SubscribeSnapshot(
		func(observation interactions.EventObservation) {
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
	p.subs[workspace] = interactionRegistrySubscription{
		registry: registry, unsubscribe: unsubscribe,
	}
	terminal := p.applySnapshotLocked(workspace, snapshot)
	p.mu.Unlock()

	for _, key := range terminal {
		p.reconcileTerminal(key)
	}
	activate()
}

func (p *interactionTraceProjector) applySnapshotLocked(
	workspace string,
	snapshot interactions.ObservationSnapshot,
) []interactionTraceKey {
	p.clearWorkspaceLocked(workspace)
	events := make(map[string][]interactions.Event, len(snapshot.Records))
	for _, event := range snapshot.Events {
		events[event.InteractionID] = append(events[event.InteractionID], event)
	}
	records := append([]interactions.Record(nil), snapshot.Records...)
	sort.Slice(records, func(i, j int) bool { return records[i].ID < records[j].ID })
	terminal := make([]interactionTraceKey, 0)
	for _, record := range records {
		key := newInteractionTraceKey(workspace, record.ID)
		if key.interactionID == "" {
			continue
		}
		history := append([]interactions.Event(nil), events[record.ID]...)
		sortInteractionEvents(history)
		state := &interactionTraceState{
			record: record, events: history, settings: p.settings,
			terminal: interactions.IsTerminalStatus(record.Status),
		}
		p.traces[key] = state
		if state.terminal {
			terminal = append(terminal, key)
		}
	}
	sortInteractionTraceKeys(terminal)
	return terminal
}

func (p *interactionTraceProjector) reconcileTerminal(key interactionTraceKey) {
	p.mu.Lock()
	state := p.traces[key]
	if p.closed || state == nil || !state.terminal {
		p.mu.Unlock()
		return
	}
	active, _ := buildInteractionTrace(
		state.settings, key.workspace, state.record, state.events,
	)
	current := p.current
	p.mu.Unlock()

	finalized, err := active.builder.Finalize()
	if err == nil && current != nil && current(state.settings, key.workspace, finalized) {
		p.mu.Lock()
		if p.traces[key] == state {
			delete(p.traces, key)
			p.recordCompletedLocked(key)
		}
		p.mu.Unlock()
		return
	}

	p.mu.Lock()
	if p.closed || p.traces[key] != state {
		p.mu.Unlock()
		return
	}
	p.trySubmitLocked(key, state, active)
	p.mu.Unlock()
}

func (p *interactionTraceProjector) observe(
	workspace string,
	observation interactions.EventObservation,
) {
	if p == nil {
		return
	}
	event, record := observation.Event, observation.Record
	key := newInteractionTraceKey(workspace, event.InteractionID)
	if key.interactionID == "" || record.ID != event.InteractionID {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || !p.settings.enabled {
		return
	}
	if _, done := p.completed[key]; done {
		return
	}
	state := p.traces[key]
	if state == nil {
		state = &interactionTraceState{settings: p.settings}
		p.traces[key] = state
	}
	state.record = record
	state.events = appendInteractionEvent(state.events, event)
	state.terminal = interactions.IsTerminalStatus(record.Status)
	state.retryable = false
	if state.terminal {
		active, _ := buildInteractionTrace(
			state.settings, key.workspace, state.record, state.events,
		)
		p.trySubmitLocked(key, state, active)
	}
}

func (p *interactionTraceProjector) trySubmitLocked(
	key interactionTraceKey,
	state *interactionTraceState,
	active *activeTraceCapture,
) {
	if p.submit == nil || state == nil || active == nil {
		return
	}
	err := p.submit(state.settings, active)
	if err == nil {
		delete(p.traces, key)
		p.recordCompletedLocked(key)
		return
	}
	state.retryable = interactionTraceAdmissionCanRetry(err)
	if state.retryable {
		p.scheduleRetryLocked()
	}
}

func interactionTraceAdmissionCanRetry(err error) bool {
	var admission *evalcapture.AdmissionError
	return errors.As(err, &admission) && admission.Reason == evalcapture.ReasonCapacity
}

func (p *interactionTraceProjector) scheduleRetryLocked() {
	if p.retryTimer != nil || p.closed || !p.settings.enabled {
		return
	}
	p.retryTimer = time.AfterFunc(interactionTraceAdmissionRetryDelay, p.retryPending)
}

func (p *interactionTraceProjector) retryPending() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.retryTimer = nil
	if p.closed || !p.settings.enabled {
		return
	}
	keys := make([]interactionTraceKey, 0, len(p.traces))
	for key, state := range p.traces {
		if state.terminal && state.retryable {
			keys = append(keys, key)
		}
	}
	sortInteractionTraceKeys(keys)
	for _, key := range keys {
		state := p.traces[key]
		active, _ := buildInteractionTrace(
			state.settings, key.workspace, state.record, state.events,
		)
		p.trySubmitLocked(key, state, active)
	}
}

func (p *interactionTraceProjector) close() {
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
	states := make([]struct {
		key   interactionTraceKey
		state *interactionTraceState
	}, 0, len(p.traces))
	for key, state := range p.traces {
		if state.terminal {
			states = append(states, struct {
				key   interactionTraceKey
				state *interactionTraceState
			}{key: key, state: state})
		}
	}
	sort.Slice(states, func(i, j int) bool {
		return interactionTraceKeyLess(states[i].key, states[j].key)
	})
	p.traces = nil
	p.completed = nil
	p.order = nil
	p.registries = nil
	p.mu.Unlock()

	unsubscribeInteractionRegistries(subs)
	for _, item := range states {
		active, _ := buildInteractionTrace(
			item.state.settings,
			item.key.workspace,
			item.state.record,
			item.state.events,
		)
		if p.submit != nil {
			_ = p.submit(item.state.settings, active)
		}
	}
}

func buildInteractionTrace(
	settings traceCaptureSettings,
	workspace string,
	record interactions.Record,
	events []interactions.Event,
) (*activeTraceCapture, interactionTraceEvidence) {
	startedAt := time.UnixMilli(record.CreatedAt)
	if record.CreatedAt <= 0 {
		startedAt = time.UnixMilli(1)
	}
	metadataSettings := settings
	metadataSettings.contentMode = evaltrace.ContentMetadataOnly
	active := &activeTraceCapture{
		workspace: workspace,
		turnID:    record.Origin.TurnID,
		startedAt: startedAt,
		builder: evalcapture.NewTraceBuilder(evaltrace.Trace{
			SchemaVersion: evaltrace.SchemaVersionV1,
			TraceID: opaqueTraceID(
				"interaction",
				strings.TrimSpace(workspace)+"\x00"+record.ID,
				startedAt,
			),
			CreatedAt: startedAt.UTC(),
			Policy: evaltrace.CapturePolicy{
				ContentMode: evaltrace.ContentMetadataOnly,
			},
			Limits: settings.limits,
			Metadata: evaltrace.Metadata{
				TraceKind:   evaltrace.TraceKindInteraction,
				RootTurnID:  record.Origin.TurnID,
				SessionHash: safeHash(metadataSettings, record.Route.SessionKey),
				AgentID:     record.Route.AgentID,
			},
			Records: make([]evaltrace.Record, 0, len(events)),
		}),
	}
	history := append([]interactions.Event(nil), events...)
	sortInteractionEvents(history)
	evidence := interactionTraceEvidence{}
	historyComplete := true
	lastOffset := int64(0)
	for _, event := range history {
		if event.InteractionID != record.ID || event.Sequence <= evidence.LastSequence {
			continue
		}
		if evidence.FirstSequence == 0 {
			evidence.FirstSequence = event.Sequence
			if event.Sequence > 1 {
				historyComplete = false
				active.builder.MarkIncomplete(
					"interaction_history_prefix_missing",
					int(event.Sequence-1),
				)
			}
		} else if event.Sequence > evidence.LastSequence+1 {
			historyComplete = false
			active.builder.MarkIncomplete(
				"interaction_event_sequence_gap",
				int(event.Sequence-evidence.LastSequence-1),
			)
		}
		item := normalizedInteractionEventRecord(
			metadataSettings, active, record, event,
		)
		if item.OffsetNanos < lastOffset {
			item.OffsetNanos = lastOffset
		}
		appendCaptureRecord(active, item, true)
		lastOffset = item.OffsetNanos
		evidence.LastSequence = event.Sequence
		evidence.LastRevision = event.Revision
	}
	if evidence.LastSequence < record.LastEventSeq {
		historyComplete = false
		active.builder.MarkIncomplete(
			"interaction_history_suffix_missing",
			int(record.LastEventSeq-evidence.LastSequence),
		)
	}
	if len(history) == 0 && record.LastEventSeq <= 0 {
		historyComplete = false
		active.builder.MarkIncomplete("interaction_history_missing", 1)
	}
	evidence.Complete = historyComplete && evidence.FirstSequence == 1 &&
		evidence.LastSequence == record.LastEventSeq &&
		evidence.LastRevision == record.Revision
	if !evidence.Complete && evidence.LastSequence == record.LastEventSeq &&
		evidence.LastRevision != record.Revision {
		active.builder.MarkIncomplete("interaction_revision_evidence_missing", 0)
	}
	if interactions.IsTerminalStatus(record.Status) {
		active.builder.SetOutcome(evaltrace.Outcome{
			Status: string(record.Status), ErrorCode: interactionErrorCode(record),
		})
	}
	return active, evidence
}

func normalizedInteractionEventRecord(
	settings traceCaptureSettings,
	active *activeTraceCapture,
	record interactions.Record,
	event interactions.Event,
) evaltrace.Record {
	payload := evaltrace.InteractionPayload{
		EventType:      string(event.Type),
		Kind:           string(record.Kind),
		From:           string(event.From),
		Status:         string(event.To),
		Outcome:        string(event.Outcome),
		Revision:       event.Revision,
		Sequence:       event.Sequence,
		CommitSequence: event.CommitSequence,
		CodeHash:       safeHash(settings, event.Code),
		Success:        event.Success,
	}
	data, _ := json.Marshal(payload)
	return evaltrace.Record{
		OffsetNanos: max(
			0,
			time.UnixMilli(event.EmittedAt).Sub(active.startedAt).Nanoseconds(),
		),
		Kind:   evaltrace.RecordInteractionTransition,
		Origin: evaltrace.Origin{Kind: "interaction_event", ID: event.EventID},
		Scope: evaltrace.Scope{
			AgentID:     record.Route.AgentID,
			SessionHash: safeHash(settings, record.Route.SessionKey),
			TurnID:      record.Origin.TurnID,
			TaskID:      record.Origin.TaskID,
			Channel:     record.Route.Channel,
			TargetHash: safeHash(
				settings,
				targetKey(record.Route.Channel, record.Route.ChatID),
			),
		},
		Correlation: evaltrace.Correlation{
			InteractionID: record.ID,
			ToolCallID:    record.Origin.ToolCallID,
			EventID:       event.EventID,
		},
		Data: data,
	}
}

func currentInteractionTrace(
	settings traceCaptureSettings,
	workspace string,
	candidate evaltrace.Trace,
) bool {
	existing, err := (evaltrace.Store{
		Root: traceStoreRoot(settings, workspace),
	}).Load(candidate.TraceID)
	return err == nil && interactionTracesEqual(existing, candidate)
}

func interactionTracesEqual(left, right evaltrace.Trace) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}

func interactionErrorCode(record interactions.Record) string {
	if record.Status == interactions.StatusFailed {
		return "interaction_failed"
	}
	if record.Outcome == interactions.OutcomeDeliveryUnknown {
		return "delivery_unknown"
	}
	return ""
}

func appendInteractionEvent(
	events []interactions.Event,
	event interactions.Event,
) []interactions.Event {
	for i := range events {
		if events[i].EventID == event.EventID {
			events[i] = event
			sortInteractionEvents(events)
			return events
		}
	}
	events = append(events, event)
	sortInteractionEvents(events)
	return events
}

func sortInteractionEvents(events []interactions.Event) {
	sort.Slice(events, func(i, j int) bool {
		if events[i].Sequence != events[j].Sequence {
			return events[i].Sequence < events[j].Sequence
		}
		return events[i].EventID < events[j].EventID
	})
}

func newInteractionTraceKey(workspace, interactionID string) interactionTraceKey {
	return interactionTraceKey{
		workspace: strings.TrimSpace(workspace), interactionID: strings.TrimSpace(interactionID),
	}
}

func sortInteractionTraceKeys(keys []interactionTraceKey) {
	sort.Slice(keys, func(i, j int) bool {
		return interactionTraceKeyLess(keys[i], keys[j])
	})
}

func interactionTraceKeyLess(left, right interactionTraceKey) bool {
	if left.workspace != right.workspace {
		return left.workspace < right.workspace
	}
	return left.interactionID < right.interactionID
}

func (p *interactionTraceProjector) recordCompletedLocked(key interactionTraceKey) {
	if _, exists := p.completed[key]; exists {
		return
	}
	p.completed[key] = struct{}{}
	p.order = append(p.order, key)
	if len(p.order) <= maxCompletedInteractionTraces {
		return
	}
	oldest := p.order[0]
	p.order[0] = interactionTraceKey{}
	p.order = p.order[1:]
	delete(p.completed, oldest)
}

func (p *interactionTraceProjector) clearWorkspaceLocked(workspace string) {
	for key := range p.traces {
		if key.workspace == workspace {
			delete(p.traces, key)
		}
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

func (p *interactionTraceProjector) clearCaptureStateLocked() {
	if p.retryTimer != nil {
		p.retryTimer.Stop()
		p.retryTimer = nil
	}
	p.traces = make(map[interactionTraceKey]*interactionTraceState)
	p.completed = make(map[interactionTraceKey]struct{})
	p.order = nil
}

func (p *interactionTraceProjector) takeSubscriptionsLocked() []interactionRegistrySubscription {
	subs := make([]interactionRegistrySubscription, 0, len(p.subs))
	for _, sub := range p.subs {
		subs = append(subs, sub)
	}
	p.subs = make(map[string]interactionRegistrySubscription)
	return subs
}

func unsubscribeInteractionRegistries(subs []interactionRegistrySubscription) {
	for _, sub := range subs {
		if sub.unsubscribe != nil {
			sub.unsubscribe()
		}
	}
}

func cloneInteractionRegistries(
	registries map[string]*interactions.Registry,
) map[string]*interactions.Registry {
	cloned := make(map[string]*interactions.Registry, len(registries))
	for workspace, registry := range registries {
		cloned[workspace] = registry
	}
	return cloned
}
