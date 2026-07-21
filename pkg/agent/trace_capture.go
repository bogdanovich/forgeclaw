package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/evalcapture"
	"github.com/sipeed/picoclaw/pkg/evaltrace"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
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
	builder         *evalcapture.TraceBuilder
	turnID          string
	workspace       string
	startedAt       time.Time
	deliverySettled bool
	settlementTimer *time.Timer
}

type taskTraceKey struct {
	workspace string
	taskID    string
}

type traceCaptureManager struct {
	mu      sync.Mutex
	closed  bool
	startMu sync.Mutex

	settings traceCaptureSettings
	turns    map[runtimeevents.TraceScope]*activeTraceCapture
	tasks    map[taskTraceKey]*activeTraceCapture
	taskSubs map[string]func()
	sub      runtimeevents.Subscription
	eventBus runtimeevents.Bus

	lastDropped uint64
	writer      *evalcapture.Writer
}

func newTraceCaptureManager(cfg *config.Config, eventBus runtimeevents.Bus) *traceCaptureManager {
	m := &traceCaptureManager{
		settings: traceCaptureSettingsFromConfig(cfg),
		turns:    make(map[runtimeevents.TraceScope]*activeTraceCapture),
		tasks:    make(map[taskTraceKey]*activeTraceCapture),
		taskSubs: make(map[string]func()),
		eventBus: eventBus,
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
	m.mu.Lock()
	if m.writer == nil {
		m.writer = evalcapture.NewWriter(evalcapture.Options{
			Capacity:  tracePersistBuffer,
			EventSink: logTraceWriterEvent,
		})
	}
	m.mu.Unlock()
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
	if m.settings.enabled && !updated.enabled {
		for _, trace := range m.turns {
			if trace.settlementTimer != nil {
				trace.settlementTimer.Stop()
			}
		}
		m.turns = make(map[runtimeevents.TraceScope]*activeTraceCapture)
		m.tasks = make(map[taskTraceKey]*activeTraceCapture)
	}
	m.settings = updated
	m.mu.Unlock()
	if updated.enabled {
		m.start()
	}
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
	m.taskSubs = nil
	settings := m.settings
	traces := make([]*activeTraceCapture, 0, len(m.turns)+len(m.tasks))
	for _, trace := range m.turns {
		if trace.settlementTimer != nil {
			trace.settlementTimer.Stop()
			trace.settlementTimer = nil
		}
		trace.builder.MarkIncomplete("runtime_closed_before_terminal_outcome", 0)
		traces = append(traces, trace)
	}
	for _, trace := range m.tasks {
		trace.builder.MarkIncomplete("runtime_closed_before_terminal_task_delivery", 0)
		traces = append(traces, trace)
	}
	m.turns = nil
	m.tasks = nil
	m.mu.Unlock()
	for _, trace := range traces {
		m.enqueuePersist(settings, trace)
	}
	m.mu.Lock()
	writer := m.writer
	m.writer = nil
	m.mu.Unlock()
	if writer != nil {
		_ = writer.Close(context.Background())
	}
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
	traceScopes, traceSettlement := runtimeEventTraceScopes(event)
	if event.Kind == runtimeevents.KindAgentTurnStart && len(traceScopes) == 1 {
		m.startTurnLocked(settings, traceScopes[0], event)
	}
	for _, traceScope := range traceScopes {
		trace := m.turns[traceScope]
		if trace == nil {
			continue
		}
		if record, critical, ok := runtimeEventRecord(settings, trace, event); ok {
			appendCaptureRecord(trace, record, critical)
		}
	}
	if isTerminalChannelDeliveryEvent(event.Kind) && traceSettlement {
		settled := make([]*activeTraceCapture, 0, len(traceScopes))
		for _, traceScope := range traceScopes {
			trace := m.turns[traceScope]
			if trace == nil {
				continue
			}
			trace.deliverySettled = true
			if trace.settlementTimer == nil {
				continue
			}
			m.removeTurnLocked(traceScope, trace)
			trace.settlementTimer.Stop()
			trace.settlementTimer = nil
			settled = append(settled, trace)
		}
		m.mu.Unlock()
		for _, trace := range settled {
			m.enqueuePersist(settings, trace)
		}
		return
	}
	if len(traceScopes) != 1 {
		m.mu.Unlock()
		return
	}
	traceScope := traceScopes[0]
	trace := m.turns[traceScope]
	if event.Kind != runtimeevents.KindAgentTurnEnd || trace == nil {
		m.mu.Unlock()
		return
	}
	deliveryExpected := false
	if payload, ok := event.Payload.(TurnEndPayload); ok {
		deliveryExpected = payload.DeliveryExpected
		trace.builder.SetOutcome(evaltrace.Outcome{
			Status: string(payload.Status), ContentHash: safeHash(settings, payload.FinalContent),
			ContentLen: payload.FinalContentLen,
		})
	}
	if deliveryExpected {
		if trace.deliverySettled {
			m.removeTurnLocked(traceScope, trace)
			m.mu.Unlock()
			m.enqueuePersist(settings, trace)
			return
		}
		settlementScope := traceScope
		trace.settlementTimer = time.AfterFunc(traceDeliverySettlementTimeout, func() {
			m.expireTurnSettlement(settlementScope, trace)
		})
		m.mu.Unlock()
		return
	}
	m.removeTurnLocked(traceScope, trace)
	m.mu.Unlock()
	m.enqueuePersist(settings, trace)
}

func (m *traceCaptureManager) expireTurnSettlement(
	traceScope runtimeevents.TraceScope,
	trace *activeTraceCapture,
) {
	m.mu.Lock()
	if m.closed || m.turns[traceScope] != trace || trace.settlementTimer == nil {
		m.mu.Unlock()
		return
	}
	settings := m.settings
	trace.settlementTimer = nil
	trace.builder.MarkIncomplete("delivery_settlement_timeout", 0)
	m.removeTurnLocked(traceScope, trace)
	m.mu.Unlock()
	m.enqueuePersist(settings, trace)
}

func isTerminalChannelDeliveryEvent(kind runtimeevents.Kind) bool {
	return kind == runtimeevents.KindChannelMessageOutboundSent ||
		kind == runtimeevents.KindChannelMessageOutboundFailed
}

func (m *traceCaptureManager) startTurnLocked(
	settings traceCaptureSettings,
	traceScope runtimeevents.TraceScope,
	event runtimeevents.Event,
) {
	if !traceScope.Complete() {
		return
	}
	if _, exists := m.turns[traceScope]; exists {
		return
	}
	trace := &activeTraceCapture{
		turnID:    traceScope.TurnID,
		workspace: traceScope.Workspace,
		startedAt: event.Time,
		builder: evalcapture.NewTraceBuilder(evaltrace.Trace{
			SchemaVersion: evaltrace.SchemaVersionV1,
			TraceID: opaqueTraceID(
				"turn", traceScope.Workspace+"\x00"+traceScope.TurnID, event.Time,
			),
			CreatedAt: event.Time.UTC(),
			Policy: evaltrace.CapturePolicy{
				ContentMode: settings.contentMode,
				Redactor:    captureRedactorVersion(settings.contentMode),
			},
			Limits: settings.limits,
			Metadata: evaltrace.Metadata{
				RootTurnID: traceScope.TurnID, SessionHash: safeHash(settings, event.Scope.SessionKey),
				AgentID: event.Scope.AgentID, RuntimeID: event.Scope.RuntimeID,
			},
			Records: make([]evaltrace.Record, 0, 32),
		}),
	}
	m.turns[traceScope] = trace
}

func runtimeEventTraceScopes(event runtimeevents.Event) ([]runtimeevents.TraceScope, bool) {
	if event.Kind == runtimeevents.KindChannelMessageOutboundQueued ||
		isTerminalChannelDeliveryEvent(event.Kind) {
		payload, ok := event.Payload.(channels.ChannelOutboundPayload)
		if !ok {
			return nil, false
		}
		return normalizedRuntimeTraceScopes(payload.TraceScopes), payload.TraceSettlement
	}
	traceScope := event.Scope.TurnTraceScope()
	if !traceScope.Complete() {
		return nil, false
	}
	return []runtimeevents.TraceScope{traceScope}, false
}

func normalizedRuntimeTraceScopes(scopes []runtimeevents.TraceScope) []runtimeevents.TraceScope {
	normalized := make([]runtimeevents.TraceScope, 0, len(scopes))
	workspace := ""
	for _, scope := range scopes {
		scope = runtimeevents.NewTraceScope(scope.Workspace, scope.TurnID)
		if !scope.Complete() {
			continue
		}
		if workspace == "" {
			workspace = scope.Workspace
		} else if scope.Workspace != workspace {
			return nil
		}
		if !slices.Contains(normalized, scope) {
			normalized = append(normalized, scope)
		}
	}
	return normalized
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
	key := taskTraceKey{workspace: workspace, taskID: strings.TrimSpace(event.TaskID)}
	trace := m.tasks[key]
	createdTrace := false
	if trace == nil {
		emittedAt := time.UnixMilli(event.EmittedAt)
		trace = &activeTraceCapture{
			workspace: workspace, startedAt: emittedAt,
			builder: evalcapture.NewTraceBuilder(evaltrace.Trace{
				SchemaVersion: evaltrace.SchemaVersionV1,
				TraceID: opaqueTraceID(
					"task",
					workspace+"\x00"+event.TaskID,
					emittedAt,
				), CreatedAt: emittedAt.UTC(),
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
		m.tasks[key] = trace
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
	}
	if !observation.FinalForTask || !taskRecordIsTerminal(record) {
		m.mu.Unlock()
		return
	}
	delete(m.tasks, key)
	trace.builder.SetOutcome(evaltrace.Outcome{
		Status:    string(record.Status),
		ErrorCode: taskErrorCode(record),
	})
	m.mu.Unlock()
	m.enqueuePersist(settings, trace)
}

func (m *traceCaptureManager) enqueuePersist(
	settings traceCaptureSettings,
	trace *activeTraceCapture,
) {
	if m == nil || trace == nil || strings.TrimSpace(trace.workspace) == "" {
		return
	}
	finalized, err := trace.builder.Finalize()
	if err != nil {
		logger.WarnCF("evaltrace", "Failed to finalize evaluation trace", map[string]any{
			"trace_id": trace.builder.TraceID(), "error": err.Error(),
		})
		return
	}
	m.mu.Lock()
	writer := m.writer
	m.mu.Unlock()
	if writer == nil {
		return
	}
	err = writer.Submit(evalcapture.Policy{
		Root:      traceStoreRoot(settings, trace.workspace),
		Retention: settings.retention,
		MaxTraces: settings.maxTraces,
	}, finalized, evalcapture.ClassCritical)
	if err != nil {
		logger.WarnCF("evaltrace", "Failed to admit finalized evaluation trace", map[string]any{
			"trace_id": trace.builder.TraceID(), "error": err.Error(),
		})
	}
}

func logTraceWriterEvent(event evalcapture.Event) {
	fields := map[string]any{
		"event": string(event.Kind), "reason": string(event.Reason),
		"trace_id": event.TraceID, "class": string(event.Class),
	}
	if event.Attempt > 0 {
		fields["attempt"] = event.Attempt
	}
	if event.Removed > 0 {
		fields["removed"] = event.Removed
	}
	if event.Dropped > 0 {
		fields["dropped"] = event.Dropped
	}
	if event.Err != nil {
		fields["error"] = event.Err.Error()
	}
	logger.WarnCF("evaltrace", "Evaluation trace writer event", fields)
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
		trace.builder.MarkIncomplete("runtime_event_backpressure", delta)
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
			TurnID:      firstNonEmptyString(event.Scope.TurnID, trace.turnID),
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

func appendCaptureRecord(trace *activeTraceCapture, record evaltrace.Record, critical bool) {
	if trace == nil || trace.builder == nil {
		return
	}
	class := evalcapture.RecordOrdinary
	if critical {
		class = evalcapture.RecordCritical
	}
	trace.builder.Append(record, class)
}

func (m *traceCaptureManager) removeTurnLocked(
	traceScope runtimeevents.TraceScope,
	trace *activeTraceCapture,
) {
	if m.turns[traceScope] == trace {
		delete(m.turns, traceScope)
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
