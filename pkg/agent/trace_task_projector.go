package agent

import (
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/evalcapture"
	"github.com/sipeed/picoclaw/pkg/evaltrace"
	taskregistry "github.com/sipeed/picoclaw/pkg/tasks"
)

type taskTraceKey struct {
	workspace string
	taskID    string
}

type taskTraceProjector struct {
	mu       sync.Mutex
	closed   bool
	settings traceCaptureSettings
	traces   map[taskTraceKey]*activeTraceCapture
	subs     map[string]func()
	submit   func(traceCaptureSettings, *activeTraceCapture)
}

func newTaskTraceProjector(
	settings traceCaptureSettings,
	submit func(traceCaptureSettings, *activeTraceCapture),
) *taskTraceProjector {
	return &taskTraceProjector{
		settings: settings,
		traces:   make(map[taskTraceKey]*activeTraceCapture),
		subs:     make(map[string]func()),
		submit:   submit,
	}
}

func (m *traceCaptureManager) attachTaskRegistry(
	workspace string,
	registry *taskregistry.Registry,
) {
	if m != nil {
		m.tasks.attach(workspace, registry)
	}
}

func (p *taskTraceProjector) updateSettings(settings traceCaptureSettings) {
	if p == nil {
		return
	}
	p.mu.Lock()
	if !p.closed && p.settings.enabled && !settings.enabled {
		p.traces = make(map[taskTraceKey]*activeTraceCapture)
	}
	p.settings = settings
	p.mu.Unlock()
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
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	if _, exists := p.subs[workspace]; exists {
		return
	}
	p.subs[workspace] = registry.SubscribeEvents(func(observation taskregistry.EventObservation) {
		p.observe(workspace, registry, observation)
	})
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
	settings := p.settings
	for _, trace := range p.traces {
		trace.builder.MarkIncomplete("runtime_closed_before_terminal_task_delivery", 0)
		p.submitTraceLocked(settings, trace)
	}
	p.traces = nil
	subs := p.subs
	p.subs = nil
	p.mu.Unlock()
	for _, unsubscribe := range subs {
		unsubscribe()
	}
}

func (p *taskTraceProjector) observe(
	workspace string,
	registry *taskregistry.Registry,
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
	settings := p.settings
	event, record := observation.Event, observation.Record
	key := taskTraceKey{workspace: workspace, taskID: strings.TrimSpace(event.TaskID)}
	trace := p.traces[key]
	createdTrace := false
	if trace == nil {
		trace = newTaskTrace(settings, workspace, event, record)
		p.traces[key] = trace
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
		return
	}
	delete(p.traces, key)
	trace.builder.SetOutcome(evaltrace.Outcome{
		Status: string(record.Status), ErrorCode: taskErrorCode(record),
	})
	p.submitTraceLocked(settings, trace)
}

func (p *taskTraceProjector) submitTraceLocked(
	settings traceCaptureSettings,
	trace *activeTraceCapture,
) {
	if p.submit != nil {
		p.submit(settings, trace)
	}
}

func newTaskTrace(
	settings traceCaptureSettings,
	workspace string,
	event taskregistry.TaskEvent,
	record taskregistry.Record,
) *activeTraceCapture {
	emittedAt := time.UnixMilli(event.EmittedAt)
	return &activeTraceCapture{
		workspace: workspace,
		startedAt: emittedAt,
		builder: evalcapture.NewTraceBuilder(evaltrace.Trace{
			SchemaVersion: evaltrace.SchemaVersionV1,
			TraceID:       opaqueTraceID("task", workspace+"\x00"+event.TaskID, emittedAt),
			CreatedAt:     emittedAt.UTC(),
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
			EventType: string(event.Type), Runtime: string(event.Runtime),
			Status: string(event.Status), DeliveryStatus: string(event.DeliveryStatus),
			Sequence: event.Seq, Fingerprint: event.Fingerprint, Producer: event.Producer,
		}
	} else {
		payload = evaltrace.DeliveryPayload{
			Mode: event.Payload["mode"], Status: string(event.DeliveryStatus),
			WillUser:   parseTaskBool(event.Payload["will_user"]),
			WillParent: parseTaskBool(event.Payload["will_parent"]),
			ContentLen: parseTaskInt(event.Payload["content_len"]),
			ErrorCode:  taskErrorCode(state),
		}
	}
	data, _ := json.Marshal(payload)
	return evaltrace.Record{
		OffsetNanos: max(0, time.UnixMilli(event.EmittedAt).Sub(trace.startedAt).Nanoseconds()),
		Kind:        kind, Origin: evaltrace.Origin{Kind: "task_event", ID: event.EventID},
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

func parseTaskBool(value string) bool {
	parsed, _ := strconv.ParseBool(value)
	return parsed
}

func parseTaskInt(value string) int {
	parsed, _ := strconv.Atoi(value)
	return parsed
}
