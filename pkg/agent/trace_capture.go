package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/evaltrace"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/interactions"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
	taskregistry "github.com/sipeed/picoclaw/pkg/tasks"
)

const (
	traceCaptureBuffer             = 512
	tracePersistBuffer             = 128
	traceDeliverySettlementTimeout = 30 * time.Second
)

type traceCaptureSettings struct {
	enabled     bool
	contentMode evaltrace.ContentMode
	stateDir    string
	limits      evaltrace.AppliedLimits
	retention   time.Duration
	maxTraces   int
	filter      func(string) string
}

type activeTraceCapture struct {
	trace           evaltrace.Trace
	turnID          string
	workspace       string
	startedAt       time.Time
	retainedAt      time.Time
	critical        map[uint64]bool
	origins         map[string]struct{}
	settlementTimer *time.Timer
}

type traceCaptureManager struct {
	mu      sync.Mutex
	closed  bool
	startMu sync.Mutex

	settings           traceCaptureSettings
	turns              map[string]*activeTraceCapture
	tasks              map[scopedTraceKey]*activeTraceCapture
	interactions       map[scopedTraceKey]*activeTraceCapture
	sessions           map[scopedTraceKey]string
	taskSubs           map[string]func()
	interactionSubs    map[string]func()
	interactionRegs    map[string]*interactions.Registry
	interactionGens    map[string]uint64
	nextInteractionGen uint64
	sub                runtimeevents.Subscription
	eventBus           runtimeevents.Bus

	lastDropped     uint64
	persistCh       chan tracePersistRequest
	persistWG       sync.WaitGroup
	persistMu       sync.RWMutex
	persistClosed   bool
	criticalQueue   map[string]tracePersistRequest
	persistWake     chan struct{}
	droppedPersists atomic.Uint64
	persistAttempt  func(traceCaptureSettings, *activeTraceCapture) (bool, bool)
	retryInterval   time.Duration
}

type tracePersistRequest struct {
	settings traceCaptureSettings
	trace    *activeTraceCapture
	critical bool
}

type scopedTraceKey struct {
	workspace string
	id        string
}

func newTraceCaptureManager(cfg *config.Config, eventBus runtimeevents.Bus) *traceCaptureManager {
	m := &traceCaptureManager{
		settings:        traceCaptureSettingsFromConfig(cfg),
		turns:           make(map[string]*activeTraceCapture),
		tasks:           make(map[scopedTraceKey]*activeTraceCapture),
		interactions:    make(map[scopedTraceKey]*activeTraceCapture),
		sessions:        make(map[scopedTraceKey]string),
		taskSubs:        make(map[string]func()),
		interactionSubs: make(map[string]func()),
		interactionRegs: make(map[string]*interactions.Registry),
		interactionGens: make(map[string]uint64),
		eventBus:        eventBus,
		retryInterval:   5 * time.Second,
	}
	if !m.settings.enabled {
		return m
	}
	m.start()
	return m
}

func (m *traceCaptureManager) start() {
	if m == nil {
		return
	}
	m.startMu.Lock()
	defer m.startMu.Unlock()
	m.mu.Lock()
	closed := m.closed
	m.mu.Unlock()
	if closed {
		return
	}
	m.persistMu.Lock()
	if m.persistCh == nil && !m.persistClosed {
		m.persistCh = make(chan tracePersistRequest, tracePersistBuffer)
		m.persistWake = make(chan struct{}, 1)
		m.criticalQueue = make(map[string]tracePersistRequest)
		m.persistWG.Add(1)
		go m.runPersistWorker(m.persistCh)
	}
	m.persistMu.Unlock()
	if m.sub != nil || m.eventBus == nil {
		return
	}
	sub, err := m.eventBus.Channel().Subscribe(context.Background(), runtimeevents.SubscribeOptions{
		Name:         "evaluation-trace-capture",
		Buffer:       traceCaptureBuffer,
		Concurrency:  runtimeevents.Locked,
		Backpressure: runtimeevents.DropNewest,
		PanicPolicy:  runtimeevents.RecoverAndLog,
	}, func(_ context.Context, event runtimeevents.Event) error {
		m.observeRuntimeEvent(event)
		return nil
	})
	if err != nil {
		logger.WarnCF(
			"evaltrace",
			"Failed to subscribe trace capture",
			map[string]any{"error": err.Error()},
		)
		return
	}
	m.sub = sub
}

func traceCaptureSettingsFromConfig(cfg *config.Config) traceCaptureSettings {
	if cfg == nil {
		return traceCaptureSettings{}
	}
	capture := cfg.Evaluation.TraceCapture
	return traceCaptureSettings{
		enabled:     capture.Enabled,
		contentMode: evaltrace.ContentMode(capture.EffectiveContentMode()),
		stateDir:    strings.TrimSpace(capture.StateDir),
		limits: evaltrace.NormalizeLimits(evaltrace.AppliedLimits{
			MaxTraceBytes: capture.MaxTraceBytes, MaxRecords: capture.MaxRecords,
			MaxRecordBytes: capture.MaxRecordBytes, MaxCorrections: capture.MaxCorrections,
		}),
		retention: time.Duration(capture.RetentionHours) * time.Hour,
		maxTraces: capture.MaxTraces,
		filter:    cfg.FilterSensitiveData,
	}
}

func (m *traceCaptureManager) updateConfig(cfg *config.Config) {
	if m == nil {
		return
	}
	m.mu.Lock()
	updated := traceCaptureSettingsFromConfig(cfg)
	wasEnabled := m.settings.enabled
	if wasEnabled && !updated.enabled {
		for _, trace := range m.turns {
			if trace.settlementTimer != nil {
				trace.settlementTimer.Stop()
			}
		}
		m.turns = make(map[string]*activeTraceCapture)
		m.tasks = make(map[scopedTraceKey]*activeTraceCapture)
		m.interactions = make(map[scopedTraceKey]*activeTraceCapture)
		m.sessions = make(map[scopedTraceKey]string)
	}
	type registryBootstrap struct {
		workspace         string
		snapshot          interactions.ObservationSnapshot
		reconciledThrough map[string]int64
		ready             chan struct{}
		oldUnsubscribe    func()
	}
	bootstraps := make([]registryBootstrap, 0)
	if !wasEnabled && updated.enabled {
		bootstraps = make([]registryBootstrap, 0, len(m.interactionRegs))
		for workspace, registry := range m.interactionRegs {
			generation := m.nextInteractionGenerationLocked(workspace)
			reconciledThrough := make(map[string]int64)
			ready := make(chan struct{})
			snapshot, unsubscribe := registry.SubscribeSnapshot(
				func(observation interactions.EventObservation) {
					<-ready
					if observation.Event.Sequence <=
						reconciledThrough[observation.Event.InteractionID] {
						return
					}
					m.observeInteractionRegistryEventGeneration(workspace, generation, observation)
				},
			)
			bootstraps = append(bootstraps, registryBootstrap{
				workspace: workspace, snapshot: snapshot,
				reconciledThrough: reconciledThrough, ready: ready,
				oldUnsubscribe: m.interactionSubs[workspace],
			})
			m.interactionSubs[workspace] = unsubscribe
		}
	}
	m.settings = updated
	m.mu.Unlock()
	if updated.enabled {
		m.start()
	}
	for _, bootstrap := range bootstraps {
		if bootstrap.oldUnsubscribe != nil {
			bootstrap.oldUnsubscribe()
		}
		m.reconcileInteractionObservationSnapshot(
			bootstrap.workspace,
			updated,
			bootstrap.snapshot,
			bootstrap.reconciledThrough,
		)
		close(bootstrap.ready)
	}
}

func (m *traceCaptureManager) attachInteractionRegistry(
	workspace string,
	registry *interactions.Registry,
) {
	if m == nil || registry == nil {
		return
	}
	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		return
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	if _, exists := m.interactionSubs[workspace]; exists {
		m.mu.Unlock()
		return
	}
	ready := make(chan struct{})
	generation := m.nextInteractionGenerationLocked(workspace)
	reconciledThrough := make(map[string]int64)
	snapshot, unsubscribe := registry.SubscribeSnapshot(func(observation interactions.EventObservation) {
		<-ready
		if observation.Event.Sequence <= reconciledThrough[observation.Event.InteractionID] {
			return
		}
		m.observeInteractionRegistryEventGeneration(workspace, generation, observation)
	})
	m.interactionSubs[workspace] = unsubscribe
	m.interactionRegs[workspace] = registry
	settings := m.settings
	m.mu.Unlock()
	defer close(ready)
	if !settings.enabled {
		return
	}
	m.reconcileInteractionObservationSnapshot(
		workspace,
		settings,
		snapshot,
		reconciledThrough,
	)
}

func (m *traceCaptureManager) reconcileInteractionObservationSnapshot(
	workspace string,
	settings traceCaptureSettings,
	snapshot interactions.ObservationSnapshot,
	reconciledThrough map[string]int64,
) {
	store := evaltrace.Store{
		Root: traceStoreRoot(settings, workspace), Retention: settings.retention,
		MaxTraces: settings.maxTraces,
	}
	if _, err := store.Prune(); err != nil {
		logger.WarnCF("evaltrace", "Failed to prune evaluation traces before reconciliation", map[string]any{
			"error": err.Error(),
		})
	}
	eventsByInteraction := make(map[string][]interactions.Event)
	for _, event := range snapshot.Events {
		eventsByInteraction[event.InteractionID] = append(
			eventsByInteraction[event.InteractionID],
			event,
		)
		if reconciledThrough != nil {
			reconciledThrough[event.InteractionID] = max(
				reconciledThrough[event.InteractionID],
				event.Sequence,
			)
		}
	}
	for _, state := range snapshot.Records {
		history := eventsByInteraction[state.ID]
		if interactionRecordIsTerminal(state) {
			startedAt := interactionTraceStartedAt(state, history)
			traceID := opaqueTraceID("interaction", state.ID, startedAt)
			if existing, err := store.Load(traceID); err == nil &&
				completeTraceMatchesTerminalInteraction(existing, state) {
				continue
			}
			retainedAt := interactionTraceRetainedAt(state, history)
			allowed, err := store.AllowsBackfill(traceID, retainedAt)
			if err != nil {
				logger.WarnCF("evaltrace", "Failed to evaluate interaction trace retention", map[string]any{
					"trace_id": traceID, "error": err.Error(),
				})
				continue
			}
			if !allowed {
				continue
			}
		}
		m.reconcileInteractionSnapshot(workspace, state, history)
	}
}

func (m *traceCaptureManager) nextInteractionGenerationLocked(workspace string) uint64 {
	m.nextInteractionGen++
	m.interactionGens[workspace] = m.nextInteractionGen
	return m.nextInteractionGen
}

func (m *traceCaptureManager) enabled() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return !m.closed && m.settings.enabled
}

func (m *traceCaptureManager) attachTaskRegistry(
	workspace string,
	registry *taskregistry.Registry,
) {
	if m == nil || registry == nil {
		return
	}
	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		return
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	if _, exists := m.taskSubs[workspace]; exists {
		m.mu.Unlock()
		return
	}
	unsubscribe := registry.SubscribeEvents(func(observation taskregistry.EventObservation) {
		m.observeTaskEvent(workspace, registry, observation)
	})
	m.taskSubs[workspace] = unsubscribe
	m.mu.Unlock()
}

func (m *traceCaptureManager) close() {
	if m == nil {
		return
	}
	m.startMu.Lock()
	defer m.startMu.Unlock()
	if m.sub != nil {
		_ = m.sub.Close()
		<-m.sub.Done()
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	m.markRuntimeDropsLocked()
	for _, unsubscribe := range m.taskSubs {
		unsubscribe()
	}
	for _, unsubscribe := range m.interactionSubs {
		unsubscribe()
	}
	m.taskSubs = nil
	m.interactionSubs = nil
	m.interactionRegs = nil
	m.interactionGens = nil
	settings := m.settings
	traces := make([]*activeTraceCapture, 0, len(m.turns)+len(m.tasks)+len(m.interactions))
	for _, trace := range m.turns {
		if trace.settlementTimer != nil {
			trace.settlementTimer.Stop()
			trace.settlementTimer = nil
		}
		trace.trace.Truncation.Incomplete = true
		trace.trace.Truncation.Reasons = append(
			trace.trace.Truncation.Reasons,
			"runtime_closed_before_terminal_outcome",
		)
		traces = append(traces, trace)
	}
	for _, trace := range m.tasks {
		trace.trace.Truncation.Incomplete = true
		trace.trace.Truncation.Reasons = append(
			trace.trace.Truncation.Reasons,
			"runtime_closed_before_terminal_task_delivery",
		)
		traces = append(traces, trace)
	}
	for _, trace := range m.interactions {
		trace.trace.Truncation.Incomplete = true
		trace.trace.Truncation.Reasons = append(
			trace.trace.Truncation.Reasons,
			"runtime_closed_before_terminal_interaction",
		)
		traces = append(traces, trace)
	}
	m.turns = nil
	m.tasks = nil
	m.interactions = nil
	m.mu.Unlock()
	for _, trace := range traces {
		m.enqueuePersist(settings, trace)
	}
	m.persistMu.Lock()
	m.persistClosed = true
	if m.persistCh != nil {
		close(m.persistCh)
	}
	m.persistMu.Unlock()
	m.persistWG.Wait()
}

func (m *traceCaptureManager) observeRuntimeEvent(event runtimeevents.Event) {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	settings := m.settings
	m.markRuntimeDropsLocked()
	if !settings.enabled {
		m.mu.Unlock()
		return
	}
	if event.Time.IsZero() {
		event.Time = time.Now()
	}
	turnID := strings.TrimSpace(event.Scope.TurnID)
	if event.Kind == runtimeevents.KindAgentTurnStart && turnID != "" {
		m.startTurnLocked(settings, event)
	}
	turnIDs := runtimeEventTurnIDs(event)
	settled := make([]*activeTraceCapture, 0, len(turnIDs))
	for _, correlatedTurnID := range turnIDs {
		trace := m.turns[correlatedTurnID]
		if trace == nil {
			continue
		}
		correlatedEvent := event
		correlatedEvent.Scope.TurnID = correlatedTurnID
		if record, critical, ok := runtimeEventRecord(settings, trace, correlatedEvent); ok {
			appendCaptureRecord(trace, record, critical)
		}
		if isTerminalChannelDeliveryEvent(event.Kind) && trace.settlementTimer != nil {
			m.removeTurnLocked(trace.turnID, trace)
			trace.settlementTimer.Stop()
			trace.settlementTimer = nil
			settled = append(settled, trace)
		}
	}
	if len(settled) > 0 {
		m.mu.Unlock()
		for _, trace := range settled {
			m.enqueuePersist(settings, trace)
		}
		return
	}
	trace := m.turns[turnID]
	if event.Kind != runtimeevents.KindAgentTurnEnd || trace == nil {
		m.mu.Unlock()
		return
	}
	deliveryExpected := false
	if payload, ok := event.Payload.(TurnEndPayload); ok {
		trace.workspace = strings.TrimSpace(payload.Workspace)
		deliveryExpected = payload.DeliveryExpected
		trace.trace.Outcome = &evaltrace.Outcome{
			Status: string(payload.Status), ContentHash: safeHash(settings, payload.FinalContent),
			ContentLen: payload.FinalContentLen,
		}
	}
	if deliveryExpected {
		settlementTurnID := turnID
		trace.settlementTimer = time.AfterFunc(traceDeliverySettlementTimeout, func() {
			m.expireTurnSettlement(settlementTurnID, trace)
		})
		m.mu.Unlock()
		return
	}
	m.removeTurnLocked(turnID, trace)
	m.mu.Unlock()
	m.enqueuePersist(settings, trace)
}

func (m *traceCaptureManager) expireTurnSettlement(turnID string, trace *activeTraceCapture) {
	m.mu.Lock()
	if m.closed || m.turns[turnID] != trace || trace.settlementTimer == nil {
		m.mu.Unlock()
		return
	}
	settings := m.settings
	trace.settlementTimer = nil
	trace.trace.Truncation.Incomplete = true
	trace.trace.Truncation.Reasons = appendUnique(
		trace.trace.Truncation.Reasons,
		"delivery_settlement_timeout",
	)
	m.removeTurnLocked(turnID, trace)
	m.mu.Unlock()
	m.enqueuePersist(settings, trace)
}

func isTerminalChannelDeliveryEvent(kind runtimeevents.Kind) bool {
	return kind == runtimeevents.KindChannelMessageOutboundSent ||
		kind == runtimeevents.KindChannelMessageOutboundFailed
}

func runtimeEventTurnIDs(event runtimeevents.Event) []string {
	turnIDs := appendUniqueString(nil, event.Scope.TurnID)
	if payload, ok := event.Payload.(channels.ChannelOutboundPayload); ok {
		for _, turnID := range payload.TurnIDs {
			turnIDs = appendUniqueString(turnIDs, turnID)
		}
	}
	return turnIDs
}

func (m *traceCaptureManager) startTurnLocked(
	settings traceCaptureSettings,
	event runtimeevents.Event,
) {
	turnID := strings.TrimSpace(event.Scope.TurnID)
	if _, exists := m.turns[turnID]; exists {
		return
	}
	workspace := ""
	if payload, ok := event.Payload.(TurnStartPayload); ok {
		workspace = strings.TrimSpace(payload.Workspace)
	}
	trace := &activeTraceCapture{
		turnID:    turnID,
		workspace: workspace,
		startedAt: event.Time,
		critical:  make(map[uint64]bool),
		origins:   make(map[string]struct{}),
		trace: evaltrace.Trace{
			SchemaVersion: evaltrace.SchemaVersionV1,
			TraceID:       opaqueTraceID("turn", turnID, event.Time),
			CreatedAt:     event.Time.UTC(),
			Policy: evaltrace.CapturePolicy{
				ContentMode: settings.contentMode,
				Redactor:    captureRedactorVersion(settings.contentMode),
			},
			Limits: settings.limits,
			Metadata: evaltrace.Metadata{
				TraceKind:  evaltrace.TraceKindTurn,
				RootTurnID: turnID, SessionHash: safeHash(settings, event.Scope.SessionKey),
				AgentID: event.Scope.AgentID, RuntimeID: event.Scope.RuntimeID,
			},
			Records: make([]evaltrace.Record, 0, 32),
		},
	}
	m.turns[turnID] = trace
	if event.Scope.SessionKey != "" {
		m.sessions[traceScopeKey(workspace, event.Scope.SessionKey)] = turnID
	}
}

func (m *traceCaptureManager) observeTaskEvent(
	workspace string,
	registry *taskregistry.Registry,
	observation taskregistry.EventObservation,
) {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	settings := m.settings
	if !settings.enabled {
		m.mu.Unlock()
		return
	}
	event := observation.Event
	record := observation.Record
	taskKey := traceScopeKey(workspace, event.TaskID)
	trace := m.tasks[taskKey]
	createdTrace := false
	if trace == nil {
		emittedAt := time.UnixMilli(event.EmittedAt)
		trace = &activeTraceCapture{
			workspace: workspace, startedAt: emittedAt,
			critical: make(map[uint64]bool), origins: make(map[string]struct{}),
			trace: evaltrace.Trace{
				SchemaVersion: evaltrace.SchemaVersionV1,
				TraceID: opaqueTraceID(
					"task",
					event.TaskID,
					emittedAt,
				), CreatedAt: emittedAt.UTC(),
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
			},
		}
		m.tasks[taskKey] = trace
		createdTrace = true
	}
	observations := []taskregistry.EventObservation{observation}
	if createdTrace && registry != nil {
		history := registry.ListEvents(event.TaskID)
		observations = make([]taskregistry.EventObservation, 0, len(history))
		for i, historical := range history {
			observations = append(observations, taskregistry.EventObservation{
				Event: historical, Record: record, FinalForTask: i == len(history)-1,
			})
		}
	}
	for _, item := range observations {
		taskRecord, critical := normalizedTaskEventRecord(settings, trace, item)
		appendCaptureRecord(trace, taskRecord, critical)
		turn := m.turns[m.sessions[traceScopeKey(workspace, record.RequesterSessionKey)]]
		if turn != nil && turn.workspace == workspace {
			appendCaptureRecord(turn, taskRecord, critical)
		}
	}
	if !observation.FinalForTask || !taskRecordIsTerminal(record) {
		m.mu.Unlock()
		return
	}
	delete(m.tasks, taskKey)
	trace.trace.Outcome = &evaltrace.Outcome{
		Status:    string(record.Status),
		ErrorCode: taskErrorCode(record),
	}
	m.mu.Unlock()
	m.enqueuePersist(settings, trace)
}

func (m *traceCaptureManager) observeInteractionRegistryEventGeneration(
	workspace string,
	generation uint64,
	observation interactions.EventObservation,
) {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	if generation != 0 && m.interactionGens[workspace] != generation {
		m.mu.Unlock()
		return
	}
	settings := m.settings
	if !settings.enabled {
		m.mu.Unlock()
		return
	}
	event, state := observation.Event, observation.Record
	interactionKey := traceScopeKey(workspace, event.InteractionID)
	trace := m.interactions[interactionKey]
	if trace == nil {
		trace = buildInteractionTrace(settings, workspace, state, []interactions.Event{event})
		m.interactions[interactionKey] = trace
	} else {
		interactionRecord, critical := normalizedInteractionEventRecord(settings, trace, observation)
		appendCaptureRecord(trace, interactionRecord, critical)
	}
	turn := m.turns[state.Origin.TurnID]
	if turn != nil && turn.workspace != workspace {
		turn = nil
	}
	if turn == nil {
		turn = m.turns[m.sessions[traceScopeKey(workspace, state.Route.SessionKey)]]
	}
	if turn != nil && turn.workspace != workspace {
		turn = nil
	}
	if turn != nil {
		turnRecord, critical := normalizedInteractionEventRecord(settings, turn, observation)
		appendCaptureRecord(turn, turnRecord, critical)
	}
	if !interactionRecordIsTerminal(state) {
		m.mu.Unlock()
		return
	}
	delete(m.interactions, interactionKey)
	trace.retainedAt = interactionTraceRetainedAt(state, []interactions.Event{event})
	trace.trace.Outcome = &evaltrace.Outcome{
		Status:    string(state.Status),
		ErrorCode: interactionErrorCode(state),
	}
	m.mu.Unlock()
	m.enqueuePersist(settings, trace)
}

func (m *traceCaptureManager) reconcileInteractionSnapshot(
	workspace string,
	state interactions.Record,
	history []interactions.Event,
) {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.closed || !m.settings.enabled {
		m.mu.Unlock()
		return
	}
	settings := m.settings
	interactionKey := traceScopeKey(workspace, state.ID)
	if _, exists := m.interactions[interactionKey]; exists {
		m.mu.Unlock()
		return
	}
	trace := buildInteractionTrace(settings, workspace, state, history)
	if !interactionRecordIsTerminal(state) {
		m.interactions[interactionKey] = trace
		m.mu.Unlock()
		return
	}
	trace.trace.Outcome = &evaltrace.Outcome{
		Status: string(state.Status), ErrorCode: interactionErrorCode(state),
	}
	m.mu.Unlock()
	m.enqueuePersist(settings, trace)
}

func buildInteractionTrace(
	settings traceCaptureSettings,
	workspace string,
	state interactions.Record,
	history []interactions.Event,
) *activeTraceCapture {
	startedAt := interactionTraceStartedAt(state, history)
	trace := &activeTraceCapture{
		workspace: workspace, startedAt: startedAt,
		critical: make(map[uint64]bool), origins: make(map[string]struct{}),
		trace: evaltrace.Trace{
			SchemaVersion: evaltrace.SchemaVersionV1,
			TraceID:       opaqueTraceID("interaction", state.ID, startedAt),
			CreatedAt:     startedAt.UTC(),
			Policy: evaltrace.CapturePolicy{
				ContentMode: settings.contentMode,
				Redactor:    captureRedactorVersion(settings.contentMode),
			},
			Limits: settings.limits,
			Metadata: evaltrace.Metadata{
				TraceKind:   evaltrace.TraceKindInteraction,
				RootTurnID:  state.Origin.TurnID,
				SessionHash: safeHash(settings, state.Route.SessionKey),
			},
			Records: make([]evaltrace.Record, 0, min(16, len(history))),
		},
	}
	if interactionRecordIsTerminal(state) {
		trace.retainedAt = interactionTraceRetainedAt(state, history)
	}
	if len(history) == 0 {
		trace.trace.Truncation.Incomplete = true
		trace.trace.Truncation.Reasons = appendUnique(
			trace.trace.Truncation.Reasons,
			"interaction_event_history_missing",
		)
	} else if history[0].Sequence > 1 {
		trace.trace.Truncation.Incomplete = true
		trace.trace.Truncation.Reasons = appendUnique(
			trace.trace.Truncation.Reasons,
			"interaction_event_history_incomplete",
		)
	}
	for _, event := range history {
		observation := interactions.EventObservation{Event: event, Record: state}
		record, critical := normalizedInteractionEventRecord(settings, trace, observation)
		appendCaptureRecord(trace, record, critical)
	}
	return trace
}

func interactionTraceStartedAt(state interactions.Record, history []interactions.Event) time.Time {
	if state.CreatedAt > 0 {
		return time.UnixMilli(state.CreatedAt)
	}
	if len(history) > 0 {
		return time.UnixMilli(history[0].EmittedAt)
	}
	return time.UnixMilli(0)
}

func interactionTraceRetainedAt(state interactions.Record, history []interactions.Event) time.Time {
	if state.ResolvedAt > 0 {
		return time.UnixMilli(state.ResolvedAt)
	}
	if len(history) > 0 {
		return time.UnixMilli(history[len(history)-1].EmittedAt)
	}
	return interactionTraceStartedAt(state, history)
}

func completeTraceMatchesTerminalInteraction(
	trace evaltrace.Trace,
	state interactions.Record,
) bool {
	if trace.Metadata.TraceKind != evaltrace.TraceKindInteraction ||
		trace.Truncation.Incomplete || trace.Outcome == nil ||
		trace.Outcome.Status != string(state.Status) ||
		trace.Outcome.ErrorCode != interactionErrorCode(state) {
		return false
	}
	for i := len(trace.Records) - 1; i >= 0; i-- {
		if trace.Records[i].Kind != evaltrace.RecordInteractionTransition {
			continue
		}
		var payload evaltrace.InteractionPayload
		if err := json.Unmarshal(trace.Records[i].Data, &payload); err != nil {
			return false
		}
		return payload.Sequence == state.LastEventSeq &&
			payload.Revision == state.Revision &&
			payload.Status == string(state.Status)
	}
	return false
}

func (m *traceCaptureManager) enqueuePersist(
	settings traceCaptureSettings,
	trace *activeTraceCapture,
) {
	if m == nil || trace == nil {
		return
	}
	request := tracePersistRequest{
		settings: settings,
		trace:    trace,
		critical: trace.trace.Metadata.TraceKind == evaltrace.TraceKindInteraction && trace.trace.Outcome != nil,
	}
	if request.critical {
		m.persistMu.Lock()
		defer m.persistMu.Unlock()
		if m.persistClosed {
			return
		}
		if m.criticalQueue == nil {
			m.criticalQueue = make(map[string]tracePersistRequest)
		}
		m.criticalQueue[persistRequestKey(request)] = request
		select {
		case m.persistWake <- struct{}{}:
		default:
		}
		return
	}
	m.persistMu.RLock()
	defer m.persistMu.RUnlock()
	if m.persistClosed {
		return
	}
	select {
	case m.persistCh <- request:
	default:
		m.droppedPersists.Add(1)
		logger.WarnCF(
			"evaltrace",
			"Dropped finalized evaluation trace due to persistence backpressure",
			map[string]any{
				"trace_id": trace.trace.TraceID,
			},
		)
	}
}

func (m *traceCaptureManager) runPersistWorker(ch <-chan tracePersistRequest) {
	defer m.persistWG.Done()
	retryInterval := m.retryInterval
	if retryInterval <= 0 {
		retryInterval = 5 * time.Second
	}
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()
	pending := make(map[string]tracePersistRequest)
	attempt := func(request tracePersistRequest) {
		stored, retryable := m.attemptPersist(request.settings, request.trace)
		key := persistRequestKey(request)
		if stored || !request.critical || !retryable {
			delete(pending, key)
			return
		}
		pending[key] = request
	}
	for {
		select {
		case request, ok := <-ch:
			if !ok {
				for _, critical := range m.takeCriticalQueue() {
					attempt(critical)
				}
				for _, critical := range pending {
					stored, _ := m.attemptPersist(critical.settings, critical.trace)
					if !stored {
						logger.ErrorCF(
							"evaltrace",
							"Critical evaluation trace remained unpersisted at shutdown",
							map[string]any{"trace_id": critical.trace.trace.TraceID},
						)
					}
				}
				return
			}
			attempt(request)
		case <-m.persistWake:
			for _, critical := range m.takeCriticalQueue() {
				attempt(critical)
			}
		case <-ticker.C:
			for _, request := range pending {
				attempt(request)
			}
		}
	}
}

func (m *traceCaptureManager) takeCriticalQueue() []tracePersistRequest {
	m.persistMu.Lock()
	defer m.persistMu.Unlock()
	queued := make([]tracePersistRequest, 0, len(m.criticalQueue))
	for _, request := range m.criticalQueue {
		queued = append(queued, request)
	}
	clear(m.criticalQueue)
	return queued
}

func persistRequestKey(request tracePersistRequest) string {
	if request.trace == nil {
		return ""
	}
	return request.trace.workspace + "\x00" + request.trace.trace.TraceID
}

func (m *traceCaptureManager) attemptPersist(
	settings traceCaptureSettings,
	trace *activeTraceCapture,
) (bool, bool) {
	if m.persistAttempt != nil {
		return m.persistAttempt(settings, trace)
	}
	return m.persistNow(settings, trace)
}

func (m *traceCaptureManager) persistNow(
	settings traceCaptureSettings,
	trace *activeTraceCapture,
) (bool, bool) {
	if trace == nil || strings.TrimSpace(trace.workspace) == "" {
		return false, false
	}
	finalized, err := finalizeCaptureTrace(trace)
	if err != nil {
		logger.WarnCF("evaltrace", "Failed to finalize evaluation trace", map[string]any{
			"trace_id": trace.trace.TraceID, "error": err.Error(),
		})
		return false, false
	}
	store := evaltrace.Store{
		Root: traceStoreRoot(settings, trace.workspace), Retention: settings.retention,
		MaxTraces: settings.maxTraces,
	}
	if _, err := store.SaveAt(finalized, trace.retainedAt); err != nil {
		logger.WarnCF("evaltrace", "Failed to store evaluation trace", map[string]any{
			"trace_id": trace.trace.TraceID, "error": err.Error(),
		})
		return false, true
	}
	if _, err := store.Prune(); err != nil {
		logger.WarnCF(
			"evaltrace",
			"Failed to prune evaluation traces",
			map[string]any{"error": err.Error()},
		)
	}
	return true, false
}

func (m *traceCaptureManager) markRuntimeDropsLocked() {
	if m == nil || m.sub == nil {
		return
	}
	dropped := m.sub.Stats().Dropped
	if dropped <= m.lastDropped {
		return
	}
	delta := int(dropped - m.lastDropped)
	m.lastDropped = dropped
	for _, trace := range m.turns {
		trace.trace.Truncation.Incomplete = true
		trace.trace.Truncation.DroppedRecords += delta
		trace.trace.Truncation.Reasons = appendUnique(
			trace.trace.Truncation.Reasons,
			"runtime_event_backpressure",
		)
	}
}

func runtimeEventRecord(
	settings traceCaptureSettings,
	trace *activeTraceCapture,
	event runtimeevents.Event,
) (evaltrace.Record, bool, bool) {
	var kind evaltrace.RecordKind
	var payload any
	critical := false
	toolCallID := ""
	switch event.Kind {
	case runtimeevents.KindAgentTurnStart:
		value, ok := event.Payload.(TurnStartPayload)
		if !ok {
			return evaltrace.Record{}, false, false
		}
		kind = evaltrace.RecordTurnStart
		payload = evaltrace.TurnPayload{
			InputHash: safeHash(settings, value.UserMessage),
			InputLen:  len(value.UserMessage),
		}
		critical = true
	case runtimeevents.KindAgentTurnEnd:
		value, ok := event.Payload.(TurnEndPayload)
		if !ok {
			return evaltrace.Record{}, false, false
		}
		kind = evaltrace.RecordTurnEnd
		payload = evaltrace.TurnPayload{
			Status:     string(value.Status),
			FinalHash:  safeHash(settings, value.FinalContent),
			FinalLen:   value.FinalContentLen,
			Iterations: value.Iterations,
		}
		critical = true
	case runtimeevents.KindAgentLLMRequest:
		value, ok := event.Payload.(LLMRequestPayload)
		if !ok {
			return evaltrace.Record{}, false, false
		}
		kind = evaltrace.RecordModelRequest
		payload = evaltrace.ModelPayload{
			Provider:   value.Provider,
			Model:      value.Model,
			PromptHash: value.PromptHash,
			Messages:   value.MessagesCount,
			Tools:      value.ToolsCount,
		}
	case runtimeevents.KindAgentLLMResponse:
		value, ok := event.Payload.(LLMResponsePayload)
		if !ok {
			return evaltrace.Record{}, false, false
		}
		kind = evaltrace.RecordModelResponse
		payload = evaltrace.ModelPayload{
			Status:         "success",
			ResponseHash:   value.ResponseHash,
			PromptTokens:   value.PromptTokens,
			ResponseTokens: value.CompletionTokens,
		}
	case runtimeevents.KindAgentLLMRetry:
		value, ok := event.Payload.(LLMRetryPayload)
		if !ok {
			return evaltrace.Record{}, false, false
		}
		kind = evaltrace.RecordModelRetry
		payload = evaltrace.ModelPayload{
			Attempt:   value.Attempt,
			Status:    "retry",
			Reason:    value.Reason,
			ErrorCode: value.Reason,
		}
	case runtimeevents.KindAgentLLMFallbackAttempt:
		value, ok := event.Payload.(LLMFallbackAttemptPayload)
		if !ok {
			return evaltrace.Record{}, false, false
		}
		kind = evaltrace.RecordModelFallbackAttempt
		payload = evaltrace.ModelPayload{
			Provider:    value.Provider,
			Model:       value.Model,
			IdentityKey: value.IdentityKey,
			Attempt:     value.Attempt,
			Status:      value.Status,
			Reason:      value.Reason,
			Skipped:     value.Skipped,
			ErrorCode:   value.ErrorCode,
		}
	case runtimeevents.KindAgentToolExecStart:
		value, ok := event.Payload.(ToolExecStartPayload)
		if !ok {
			return evaltrace.Record{}, false, false
		}
		kind = evaltrace.RecordToolCall
		payload = evaltrace.ToolPayload{
			Tool:     value.Tool,
			ArgsHash: safeJSONHash(settings, value.Arguments),
			Status:   "started",
			Executed: true,
		}
		toolCallID = value.ToolCallID
	case runtimeevents.KindAgentToolExecEnd:
		value, ok := event.Payload.(ToolExecEndPayload)
		if !ok {
			return evaltrace.Record{}, false, false
		}
		kind = evaltrace.RecordToolResult
		payload = evaltrace.ToolPayload{
			Tool:       value.Tool,
			ResultHash: value.ResultHash,
			Status:     "completed",
			Executed:   true,
			IsError:    value.IsError,
		}
		toolCallID = value.ToolCallID
	case runtimeevents.KindAgentToolExecSkipped:
		value, ok := event.Payload.(ToolExecSkippedPayload)
		if !ok {
			return evaltrace.Record{}, false, false
		}
		kind = evaltrace.RecordToolSkipped
		payload = evaltrace.ToolPayload{
			Tool:         value.Tool,
			Status:       "skipped",
			Executed:     false,
			DecisionCode: safeCode(value.Reason),
		}
		toolCallID = value.ToolCallID
	case runtimeevents.KindAgentToolLoopDecision:
		value, ok := event.Payload.(ToolLoopDecisionPayload)
		if !ok {
			return evaltrace.Record{}, false, false
		}
		kind = evaltrace.RecordToolLoopDecision
		payload = evaltrace.ToolPayload{
			Tool:         value.Tool,
			ArgsHash:     value.ArgsHash,
			Action:       value.Action,
			DecisionCode: value.Code,
			Count:        value.Count,
			Threshold:    value.Threshold,
		}
	case runtimeevents.KindAgentToolSteeringDecision:
		value, ok := event.Payload.(ToolSteeringDecisionPayload)
		if !ok {
			return evaltrace.Record{}, false, false
		}
		kind = evaltrace.RecordToolSteeringDecision
		payload = evaltrace.ToolPayload{
			Tool: value.Tool, Action: value.Decision, Classification: value.Classification, Cause: value.Cause,
		}
		toolCallID = value.ToolCallID
	case runtimeevents.KindAgentSteeringInjected:
		value, ok := event.Payload.(SteeringInjectedPayload)
		if !ok {
			return evaltrace.Record{}, false, false
		}
		kind = evaltrace.RecordSteeringInjected
		payload = evaltrace.SteeringPayload{
			Status:     "injected",
			Count:      value.Count,
			ContentLen: value.TotalContentLen,
		}
	case runtimeevents.KindAgentInterruptReceived:
		value, ok := event.Payload.(InterruptReceivedPayload)
		if !ok {
			return evaltrace.Record{}, false, false
		}
		kind = evaltrace.RecordInterrupt
		payload = evaltrace.SteeringPayload{
			Status:      string(value.Kind),
			Role:        value.Role,
			MessageHash: value.MessageHash,
			ContentLen:  value.ContentLen,
			QueueDepth:  value.QueueDepth,
		}
	case runtimeevents.KindAgentContextCompress, runtimeevents.KindAgentSessionSummarize:
		kind = evaltrace.RecordContextCompaction
		switch value := event.Payload.(type) {
		case ContextCompressPayload:
			payload = evaltrace.ContextPayload{
				Reason:         string(value.Reason),
				BeforeMessages: value.DroppedMessages + value.RemainingMessages,
				AfterMessages:  value.RemainingMessages,
			}
		case SessionSummarizePayload:
			payload = evaltrace.ContextPayload{
				Reason:         "summarize",
				BeforeMessages: value.SummarizedMessages + value.KeptMessages,
				AfterMessages:  value.KeptMessages,
			}
		default:
			return evaltrace.Record{}, false, false
		}
	case runtimeevents.KindAgentContextSnapshot:
		value, ok := event.Payload.(ContextSnapshotPayload)
		if !ok {
			return evaltrace.Record{}, false, false
		}
		kind, critical = evaltrace.RecordContextSnapshot, true
		protected := []string{"tool_pairing_valid:" + strconv.FormatBool(value.ToolPairingValid)}
		if value.GoalHash != "" {
			protected = append(protected, "goal:"+value.GoalHash)
		}
		if value.SteeringCount > 0 {
			protected = append(protected, "steering_count:"+strconv.Itoa(value.SteeringCount))
		}
		payload = evaltrace.ContextPayload{
			AfterMessages: value.MessageCount, SnapshotHash: value.SnapshotHash,
			ProtectedFactRefs: protected,
		}
	case runtimeevents.KindChannelMessageOutboundQueued,
		runtimeevents.KindChannelMessageOutboundSent,
		runtimeevents.KindChannelMessageOutboundFailed:
		value, ok := event.Payload.(channels.ChannelOutboundPayload)
		if !ok {
			return evaltrace.Record{}, false, false
		}
		kind = evaltrace.RecordDeliveryAttempt
		status := "queued"
		if event.Kind == runtimeevents.KindChannelMessageOutboundSent {
			kind, status, critical = evaltrace.RecordDeliveryOutcome, "sent", true
		} else if event.Kind == runtimeevents.KindChannelMessageOutboundFailed {
			kind, status, critical = evaltrace.RecordDeliveryOutcome, "failed", true
		}
		payload = evaltrace.DeliveryPayload{
			Status:     status,
			TargetHash: safeHash(settings, targetKey(event.Scope.Channel, event.Scope.ChatID)),
			ContentLen: value.ContentLen,
			Attempt:    value.Retries,
			ErrorCode:  deliveryErrorCode(value.Error),
		}
	case runtimeevents.KindAgentAsyncCompletion:
		value, ok := event.Payload.(AsyncCompletionPayload)
		if !ok {
			return evaltrace.Record{}, false, false
		}
		kind = evaltrace.RecordDeliveryDecision
		payload = evaltrace.DeliveryPayload{
			Mode:       value.DeliveryMode,
			Status:     "decided",
			WillUser:   value.WillUser,
			WillParent: value.WillParent,
			ContentLen: value.ContentLen,
		}
	default:
		return evaltrace.Record{}, false, false
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return evaltrace.Record{}, false, false
	}
	return evaltrace.Record{
		OffsetNanos: max(0, event.Time.Sub(trace.startedAt).Nanoseconds()), Kind: kind,
		Origin: evaltrace.Origin{Kind: "runtime_event", ID: event.ID},
		Scope: evaltrace.Scope{
			AgentID:     event.Scope.AgentID,
			SessionHash: safeHash(settings, event.Scope.SessionKey),
			TurnID:      event.Scope.TurnID,
			Channel:     event.Scope.Channel,
			TargetHash:  safeHash(settings, targetKey(event.Scope.Channel, event.Scope.ChatID)),
		},
		Correlation: evaltrace.Correlation{
			ParentTurnID: event.Correlation.ParentTurnID,
			RequestID:    event.Correlation.RequestID,
			ToolCallID:   toolCallID,
			EventID:      event.ID,
		},
		Data: data,
	}, critical, true
}

func normalizedTaskEventRecord(
	settings traceCaptureSettings,
	trace *activeTraceCapture,
	observation taskregistry.EventObservation,
) (evaltrace.Record, bool) {
	event, state := observation.Event, observation.Record
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
			EventType:      string(event.Type),
			Runtime:        string(event.Runtime),
			Status:         string(event.Status),
			DeliveryStatus: string(event.DeliveryStatus),
			Sequence:       event.Seq,
			Fingerprint:    event.Fingerprint,
			Producer:       event.Producer,
		}
	} else {
		payload = evaltrace.DeliveryPayload{
			Mode: event.Payload["mode"], Status: string(event.DeliveryStatus),
			WillUser: parseBool(event.Payload["will_user"]), WillParent: parseBool(event.Payload["will_parent"]),
			ContentLen: parseInt(event.Payload["content_len"]), ErrorCode: taskErrorCode(state),
		}
	}
	data, _ := json.Marshal(payload)
	return evaltrace.Record{
		OffsetNanos: max(0, time.UnixMilli(event.EmittedAt).Sub(trace.startedAt).Nanoseconds()),
		Kind:        kind, Origin: evaltrace.Origin{Kind: "task_event", ID: event.EventID},
		Scope: evaltrace.Scope{
			AgentID:     state.AgentID,
			SessionHash: safeHash(settings, state.RequesterSessionKey),
			TaskID:      event.TaskID,
			Channel:     state.Channel,
			TargetHash:  safeHash(settings, targetKey(state.Channel, state.ChatID)),
		},
		Correlation: evaltrace.Correlation{
			CompletionID: firstNonEmpty(event.Payload["completion_id"], state.LastCompletionID),
			EventID:      event.EventID,
		},
		Data: data,
	}, critical
}

func normalizedInteractionEventRecord(
	settings traceCaptureSettings,
	trace *activeTraceCapture,
	observation interactions.EventObservation,
) (evaltrace.Record, bool) {
	event, state := observation.Event, observation.Record
	payload := evaltrace.InteractionPayload{
		EventType: string(event.Type),
		Kind:      string(state.Kind),
		From:      string(event.From),
		Status:    string(event.To),
		Outcome:   string(event.Outcome),
		Revision:  event.Revision,
		Sequence:  event.Sequence,
		Code:      safeCode(event.Code),
		Success:   event.Success,
	}
	data, _ := json.Marshal(payload)
	return evaltrace.Record{
		OffsetNanos: max(0, time.UnixMilli(event.EmittedAt).Sub(trace.startedAt).Nanoseconds()),
		Kind:        evaltrace.RecordInteractionTransition,
		Origin:      evaltrace.Origin{Kind: "interaction_event", ID: event.EventID},
		Scope: evaltrace.Scope{
			SessionHash: safeHash(settings, state.Route.SessionKey),
			TurnID:      state.Origin.TurnID,
			TaskID:      state.Origin.TaskID,
			TargetHash: safeHash(
				settings,
				targetKey(state.Route.Channel, state.Route.ChatID),
			),
		},
		Correlation: evaltrace.Correlation{
			InteractionID: state.ID,
			ToolCallID:    state.Origin.ToolCallID,
			EventID:       event.EventID,
		},
		Data: data,
	}, interactionEventIsCritical(event.Type)
}

func appendCaptureRecord(trace *activeTraceCapture, record evaltrace.Record, critical bool) {
	if trace == nil {
		return
	}
	if trace.origins == nil {
		trace.origins = make(map[string]struct{})
	}
	originKey := record.Origin.Kind + "\x00" + record.Origin.ID
	if record.Origin.ID != "" {
		if _, exists := trace.origins[originKey]; exists {
			return
		}
	}
	limits := trace.trace.Limits
	if len(record.Data) > limits.MaxRecordBytes {
		trace.trace.Truncation.Incomplete = true
		trace.trace.Truncation.DroppedRecords++
		trace.trace.Truncation.Reasons = appendUnique(
			trace.trace.Truncation.Reasons,
			"record_size_limit",
		)
		trace.trace.Truncation.DroppedByKind = incrementDropped(
			trace.trace.Truncation.DroppedByKind,
			record.Kind,
		)
		return
	}
	if len(trace.trace.Records) >= limits.MaxRecords {
		if !critical {
			trace.trace.Truncation.Incomplete = true
			trace.trace.Truncation.DroppedRecords++
			trace.trace.Truncation.Reasons = appendUnique(
				trace.trace.Truncation.Reasons,
				"record_count_limit",
			)
			trace.trace.Truncation.DroppedByKind = incrementDropped(
				trace.trace.Truncation.DroppedByKind,
				record.Kind,
			)
			return
		}
		for i := len(trace.trace.Records) - 1; i >= 0; i-- {
			if trace.critical[trace.trace.Records[i].Sequence] {
				continue
			}
			dropped := trace.trace.Records[i]
			trace.trace.Records = append(trace.trace.Records[:i], trace.trace.Records[i+1:]...)
			delete(trace.critical, dropped.Sequence)
			trace.trace.Truncation.Incomplete = true
			trace.trace.Truncation.DroppedRecords++
			trace.trace.Truncation.Reasons = appendUnique(
				trace.trace.Truncation.Reasons,
				"record_count_limit",
			)
			trace.trace.Truncation.DroppedByKind = incrementDropped(
				trace.trace.Truncation.DroppedByKind,
				dropped.Kind,
			)
			break
		}
		if len(trace.trace.Records) >= limits.MaxRecords {
			trace.trace.Truncation.Incomplete = true
			trace.trace.Truncation.DroppedRecords++
			trace.trace.Truncation.Reasons = appendUnique(
				trace.trace.Truncation.Reasons,
				"record_count_limit",
			)
			trace.trace.Truncation.DroppedByKind = incrementDropped(
				trace.trace.Truncation.DroppedByKind,
				record.Kind,
			)
			return
		}
	}
	record.Sequence = nextCaptureSequence(trace.trace.Records)
	trace.trace.Records = append(trace.trace.Records, record)
	trace.critical[record.Sequence] = critical
	if record.Origin.ID != "" {
		trace.origins[originKey] = struct{}{}
	}
}

func finalizeCaptureTrace(trace *activeTraceCapture) (evaltrace.Trace, error) {
	for {
		finalized, err := evaltrace.Finalize(trace.trace)
		if err == nil {
			return finalized, nil
		}
		if !strings.Contains(err.Error(), "trace exceeds byte limit") ||
			len(trace.trace.Records) == 0 {
			return evaltrace.Trace{}, err
		}
		dropped := trace.trace.Records[len(trace.trace.Records)-1]
		trace.trace.Records = trace.trace.Records[:len(trace.trace.Records)-1]
		trace.trace.Truncation.Incomplete = true
		trace.trace.Truncation.DroppedRecords++
		trace.trace.Truncation.Reasons = appendUnique(
			trace.trace.Truncation.Reasons,
			"trace_size_limit",
		)
		trace.trace.Truncation.DroppedByKind = incrementDropped(
			trace.trace.Truncation.DroppedByKind,
			dropped.Kind,
		)
	}
}

func traceScopeKey(workspace, id string) scopedTraceKey {
	return scopedTraceKey{workspace: strings.TrimSpace(workspace), id: strings.TrimSpace(id)}
}

func (m *traceCaptureManager) removeTurnLocked(turnID string, trace *activeTraceCapture) {
	delete(m.turns, turnID)
	for session, id := range m.sessions {
		if id == turnID {
			delete(m.sessions, session)
		}
	}
}

func traceStoreRoot(settings traceCaptureSettings, workspace string) string {
	if settings.stateDir == "" {
		return filepath.Join(workspace, "state", "evaluation", "traces")
	}
	if filepath.IsAbs(settings.stateDir) {
		return filepath.Join(settings.stateDir, "traces")
	}
	clean := filepath.Clean(settings.stateDir)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return filepath.Join(workspace, "state", "evaluation", "traces")
	}
	return filepath.Join(workspace, clean, "traces")
}

func taskRecordIsTerminal(record taskregistry.Record) bool {
	statusTerminal := record.Status == taskregistry.StatusSucceeded ||
		record.Status == taskregistry.StatusFailed ||
		record.Status == taskregistry.StatusTimedOut ||
		record.Status == taskregistry.StatusCancelled ||
		record.Status == taskregistry.StatusLost
	deliveryTerminal := record.DeliveryStatus == taskregistry.DeliveryDelivered ||
		record.DeliveryStatus == taskregistry.DeliverySessionQueued ||
		record.DeliveryStatus == taskregistry.DeliveryFailed ||
		record.DeliveryStatus == taskregistry.DeliveryParentMissing ||
		record.DeliveryStatus == taskregistry.DeliveryNotApplicable
	return statusTerminal && deliveryTerminal
}

func interactionRecordIsTerminal(record interactions.Record) bool {
	return record.Status == interactions.StatusResolved ||
		record.Status == interactions.StatusCancelled ||
		record.Status == interactions.StatusFailed
}

func interactionEventIsCritical(event interactions.EventType) bool {
	switch event {
	case interactions.EventCreated,
		interactions.EventAnswerClaimed,
		interactions.EventApprovalConsumed,
		interactions.EventApprovalExpired,
		interactions.EventResolved,
		interactions.EventCancelled,
		interactions.EventFailed:
		return true
	default:
		return false
	}
}

func interactionErrorCode(record interactions.Record) string {
	if record.Status == interactions.StatusFailed {
		return safeCode(record.FailureCode)
	}
	return ""
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

func deliveryErrorCode(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	return "channel_delivery_failed"
}

func safeCode(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' || r == '-' {
			return r
		}
		return '_'
	}, value)
	if len(value) > 64 {
		value = value[:64]
	}
	return value
}

func safeHash(settings traceCaptureSettings, value string) string {
	if value == "" {
		return ""
	}
	if settings.filter != nil {
		value = settings.filter(value)
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func evaluationSafeHash(cfg *config.Config, value string) string {
	return safeHash(traceCaptureSettingsFromConfig(cfg), value)
}

func buildContextSnapshotPayload(cfg *config.Config, ts *turnState) ContextSnapshotPayload {
	if ts == nil {
		return ContextSnapshotPayload{}
	}
	messages := ts.persistedMessagesSnapshot()
	canonical := make([]map[string]any, 0, len(messages))
	toolCalls := make(map[string]struct{})
	toolResults := make(map[string]struct{})
	for _, message := range messages {
		item := map[string]any{"role": message.Role, "content": message.Content}
		if len(message.ToolCalls) > 0 {
			ids := make([]string, 0, len(message.ToolCalls))
			for _, call := range message.ToolCalls {
				ids = append(ids, call.ID)
				toolCalls[call.ID] = struct{}{}
			}
			item["tool_call_ids"] = ids
		}
		if message.ToolCallID != "" {
			item["tool_call_id"] = message.ToolCallID
			toolResults[message.ToolCallID] = struct{}{}
		}
		canonical = append(canonical, item)
	}
	pairingValid := len(toolCalls) == len(toolResults)
	if pairingValid {
		for id := range toolCalls {
			if _, ok := toolResults[id]; !ok {
				pairingValid = false
				break
			}
		}
	}
	return ContextSnapshotPayload{
		MessageCount:     len(messages),
		SnapshotHash:     safeJSONHash(traceCaptureSettingsFromConfig(cfg), canonical),
		GoalHash:         evaluationSafeHash(cfg, ts.opts.ActiveGoal),
		SteeringCount:    len(ts.acceptedSteeringSnapshot()),
		ToolPairingValid: pairingValid,
	}
}

func safeJSONHash(settings traceCaptureSettings, value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return safeHash(settings, string(data))
}

func opaqueTraceID(kind, id string, created time.Time) string {
	sum := sha256.Sum256(
		[]byte(kind + "\x00" + id + "\x00" + created.UTC().Format(time.RFC3339Nano)),
	)
	return "trace-" + kind + "-" + hex.EncodeToString(sum[:12])
}

func targetKey(channel, chatID string) string {
	channel, chatID = strings.TrimSpace(channel), strings.TrimSpace(chatID)
	if channel == "" || chatID == "" {
		return ""
	}
	return channel + "\x00" + chatID
}

func captureRedactorVersion(mode evaltrace.ContentMode) string {
	if mode == evaltrace.ContentRedacted {
		return "forgeclaw.config_filter.v1"
	}
	return ""
}

func nextCaptureSequence(records []evaltrace.Record) uint64 {
	if len(records) == 0 {
		return 1
	}
	return records[len(records)-1].Sequence + 1
}

func incrementDropped(
	values map[evaltrace.RecordKind]int,
	kind evaltrace.RecordKind,
) map[evaltrace.RecordKind]int {
	if values == nil {
		values = make(map[evaltrace.RecordKind]int)
	}
	values[kind]++
	return values
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func parseBool(value string) bool {
	parsed, _ := strconv.ParseBool(value)
	return parsed
}

func parseInt(value string) int {
	parsed, _ := strconv.Atoi(value)
	return parsed
}

func primaryCandidateProvider(candidates []providers.FallbackCandidate) string {
	if len(candidates) == 0 {
		return ""
	}
	return candidates[0].Provider
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
