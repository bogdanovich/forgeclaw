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
)

const (
	traceCaptureBuffer             = 512
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

type turnTraceProjector struct {
	mu      sync.Mutex
	closed  bool
	startMu sync.Mutex

	settings traceCaptureSettings
	turns    map[runtimeevents.TraceScope]*activeTraceCapture
	sub      runtimeevents.Subscription
	eventBus runtimeevents.Bus

	lastDropped uint64
	submit      func(traceCaptureSettings, *activeTraceCapture) error
}

func newTurnTraceProjector(
	settings traceCaptureSettings,
	eventBus runtimeevents.Bus,
	submit func(traceCaptureSettings, *activeTraceCapture) error,
) *turnTraceProjector {
	return &turnTraceProjector{
		settings: settings,
		turns:    make(map[runtimeevents.TraceScope]*activeTraceCapture),
		eventBus: eventBus,
		submit:   submit,
	}
}

func (p *turnTraceProjector) start() {
	if p == nil {
		return
	}
	p.startMu.Lock()
	defer p.startMu.Unlock()
	p.mu.Lock()
	closed := p.closed
	enabled := p.settings.enabled
	p.mu.Unlock()
	if closed || !enabled {
		return
	}
	if p.sub != nil || p.eventBus == nil {
		return
	}
	sub, err := p.eventBus.Channel().Subscribe(context.Background(), runtimeevents.SubscribeOptions{
		Name:         "evaluation-trace-capture",
		Buffer:       traceCaptureBuffer,
		Concurrency:  runtimeevents.Locked,
		Backpressure: runtimeevents.DropNewest,
		PanicPolicy:  runtimeevents.RecoverAndLog,
	}, func(_ context.Context, event runtimeevents.Event) error {
		p.observeRuntimeEvent(event)
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
	p.sub = sub
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

func (p *turnTraceProjector) updateSettings(settings traceCaptureSettings) {
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	wasEnabled := p.settings.enabled
	if wasEnabled && !settings.enabled {
		for _, trace := range p.turns {
			if trace.settlementTimer != nil {
				trace.settlementTimer.Stop()
			}
		}
		p.turns = make(map[runtimeevents.TraceScope]*activeTraceCapture)
	}
	p.settings = settings
	p.mu.Unlock()
	if !wasEnabled && settings.enabled {
		p.start()
	}
}

func (p *turnTraceProjector) close() {
	if p == nil {
		return
	}
	p.startMu.Lock()
	defer p.startMu.Unlock()
	if p.sub != nil {
		_ = p.sub.Close()
		<-p.sub.Done()
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.markRuntimeDropsLocked()
	settings := p.settings
	traces := make([]*activeTraceCapture, 0, len(p.turns))
	for _, trace := range p.turns {
		if trace.settlementTimer != nil {
			trace.settlementTimer.Stop()
			trace.settlementTimer = nil
		}
		trace.builder.MarkIncomplete("runtime_closed_before_terminal_outcome", 0)
		traces = append(traces, trace)
	}
	p.turns = nil
	p.mu.Unlock()
	for _, trace := range traces {
		if p.submit != nil {
			_ = p.submit(settings, trace)
		}
	}
}

func (p *turnTraceProjector) observeRuntimeEvent(event runtimeevents.Event) {
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	settings := p.settings
	p.markRuntimeDropsLocked()
	if !settings.enabled {
		p.mu.Unlock()
		return
	}
	if event.Time.IsZero() {
		event.Time = time.Now()
	}
	traceScopes, traceSettlement := runtimeEventTraceScopes(event)
	if event.Kind == runtimeevents.KindAgentTurnStart && len(traceScopes) == 1 {
		p.startTurnLocked(settings, traceScopes[0], event)
	}
	for _, traceScope := range traceScopes {
		trace := p.turns[traceScope]
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
			trace := p.turns[traceScope]
			if trace == nil {
				continue
			}
			trace.deliverySettled = true
			if trace.settlementTimer == nil {
				continue
			}
			p.removeTurnLocked(traceScope, trace)
			trace.settlementTimer.Stop()
			trace.settlementTimer = nil
			settled = append(settled, trace)
		}
		p.mu.Unlock()
		for _, trace := range settled {
			if p.submit != nil {
				_ = p.submit(settings, trace)
			}
		}
		return
	}
	if len(traceScopes) != 1 {
		p.mu.Unlock()
		return
	}
	traceScope := traceScopes[0]
	trace := p.turns[traceScope]
	if event.Kind != runtimeevents.KindAgentTurnEnd || trace == nil {
		p.mu.Unlock()
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
			p.removeTurnLocked(traceScope, trace)
			p.mu.Unlock()
			if p.submit != nil {
				_ = p.submit(settings, trace)
			}
			return
		}
		settlementScope := traceScope
		trace.settlementTimer = time.AfterFunc(traceDeliverySettlementTimeout, func() {
			p.expireTurnSettlement(settlementScope, trace)
		})
		p.mu.Unlock()
		return
	}
	p.removeTurnLocked(traceScope, trace)
	p.mu.Unlock()
	if p.submit != nil {
		_ = p.submit(settings, trace)
	}
}

func (p *turnTraceProjector) expireTurnSettlement(
	traceScope runtimeevents.TraceScope,
	trace *activeTraceCapture,
) {
	p.mu.Lock()
	if p.closed || p.turns[traceScope] != trace || trace.settlementTimer == nil {
		p.mu.Unlock()
		return
	}
	settings := p.settings
	trace.settlementTimer = nil
	trace.builder.MarkIncomplete("delivery_settlement_timeout", 0)
	p.removeTurnLocked(traceScope, trace)
	p.mu.Unlock()
	if p.submit != nil {
		_ = p.submit(settings, trace)
	}
}

func isTerminalChannelDeliveryEvent(kind runtimeevents.Kind) bool {
	return kind == runtimeevents.KindChannelMessageOutboundSent ||
		kind == runtimeevents.KindChannelMessageOutboundFailed
}

func (p *turnTraceProjector) startTurnLocked(
	settings traceCaptureSettings,
	traceScope runtimeevents.TraceScope,
	event runtimeevents.Event,
) {
	if !traceScope.Complete() {
		return
	}
	if _, exists := p.turns[traceScope]; exists {
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
	p.turns[traceScope] = trace
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

func (p *turnTraceProjector) markRuntimeDropsLocked() {
	if p == nil || p.sub == nil {
		return
	}
	dropped := p.sub.Stats().Dropped
	if dropped <= p.lastDropped {
		return
	}
	delta := int(dropped - p.lastDropped)
	p.lastDropped = dropped
	for _, trace := range p.turns {
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

func (p *turnTraceProjector) removeTurnLocked(
	traceScope runtimeevents.TraceScope,
	trace *activeTraceCapture,
) {
	if p.turns[traceScope] == trace {
		delete(p.turns, traceScope)
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
