package agent

import (
	"encoding/json"
	"sort"
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
	segment   string
}

type taskTraceIdentity struct {
	workspace string
	taskID    string
}

type taskTraceSegment struct {
	key      taskTraceKey
	startSeq int64
}

type completedTaskTrace struct {
	terminalSeq int64
}

type taskTraceProjector struct {
	mu        sync.Mutex
	closed    bool
	settings  traceCaptureSettings
	traces    map[taskTraceKey]*activeTraceCapture
	segments  map[taskTraceIdentity]taskTraceSegment
	completed map[taskTraceKey]completedTaskTrace
	subs      map[string]func()
	submit    func(traceCaptureSettings, *activeTraceCapture) bool
}

func newTaskTraceProjector(
	settings traceCaptureSettings,
	submit func(traceCaptureSettings, *activeTraceCapture) bool,
) *taskTraceProjector {
	return &taskTraceProjector{
		settings:  settings,
		traces:    make(map[taskTraceKey]*activeTraceCapture),
		segments:  make(map[taskTraceIdentity]taskTraceSegment),
		completed: make(map[taskTraceKey]completedTaskTrace),
		subs:      make(map[string]func()),
		submit:    submit,
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
		p.segments = make(map[taskTraceIdentity]taskTraceSegment)
		p.completed = make(map[taskTraceKey]completedTaskTrace)
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
	p.segments = nil
	p.completed = nil
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
	observations := []taskregistry.EventObservation{observation}
	if registry != nil {
		observations = taskHistoryThrough(registry.ListEvents(event.TaskID), observation)
	}
	identity := taskTraceIdentity{
		workspace: workspace,
		taskID:    strings.TrimSpace(event.TaskID),
	}
	boundary := observations[0].Event
	candidate := taskTraceSegment{key: taskTraceKey{
		workspace: workspace,
		taskID:    identity.taskID,
		segment:   boundary.EventID,
	}, startSeq: boundary.Seq}
	segment, known := p.segments[identity]
	if !known || taskEventStartsSegment(boundary) &&
		boundary.EventID != segment.key.segment && boundary.Seq > segment.startSeq {
		if known {
			delete(p.completed, segment.key)
		}
		segment = candidate
	}
	key := segment.key
	observations = taskObservationsFrom(observations, segment.startSeq)
	if len(observations) == 0 {
		return
	}
	if terminal, completed := p.completed[key]; completed {
		if event.Seq > terminal.terminalSeq {
			terminal.terminalSeq = event.Seq
			p.completed[key] = terminal
		}
		return
	}
	p.segments[identity] = segment
	trace := p.traces[key]
	if trace == nil {
		trace = newTaskTrace(settings, workspace, observations[0].Event, record)
		p.traces[key] = trace
	}
	for _, item := range observations {
		taskRecord, critical := normalizedTaskEventRecord(settings, trace, item)
		appendCaptureRecord(trace, taskRecord, critical)
	}
	if !observation.FinalForTask || !taskEventIsTerminal(event) {
		return
	}
	trace.builder.SetOutcome(evaltrace.Outcome{
		Status: string(event.Status), ErrorCode: taskEventErrorCode(event),
	})
	if !p.submitTraceLocked(settings, trace) {
		return
	}
	delete(p.traces, key)
	p.completed[key] = completedTaskTrace{terminalSeq: event.Seq}
	p.pruneTaskStateLocked(workspace, registry)
}

func taskHistoryThrough(
	history []taskregistry.TaskEvent,
	trigger taskregistry.EventObservation,
) []taskregistry.EventObservation {
	bounded := make([]taskregistry.TaskEvent, 0, len(history)+1)
	foundTrigger := false
	for _, event := range history {
		if event.Seq > trigger.Event.Seq {
			continue
		}
		bounded = append(bounded, event)
		foundTrigger = foundTrigger || event.EventID == trigger.Event.EventID
	}
	if !foundTrigger {
		bounded = append(bounded, trigger.Event)
	}
	sort.Slice(bounded, func(i, j int) bool {
		if bounded[i].Seq != bounded[j].Seq {
			return bounded[i].Seq < bounded[j].Seq
		}
		return bounded[i].EventID < bounded[j].EventID
	})
	segmentStart := 0
	for i, event := range bounded {
		if taskEventStartsSegment(event) {
			segmentStart = i
		}
	}
	bounded = bounded[segmentStart:]
	observations := make([]taskregistry.EventObservation, 0, len(bounded))
	for i, event := range bounded {
		observations = append(observations, taskregistry.EventObservation{
			Event: event, Record: trigger.Record, FinalForTask: i == len(bounded)-1,
		})
	}
	return observations
}

func taskEventStartsSegment(event taskregistry.TaskEvent) bool {
	return event.Type == taskregistry.EventTaskUpserted ||
		event.Type == taskregistry.EventTaskStatusChanged &&
			event.Payload["from"] == string(taskregistry.StatusLost) &&
			event.Status != taskregistry.StatusLost
}

func taskObservationsFrom(
	observations []taskregistry.EventObservation,
	startSeq int64,
) []taskregistry.EventObservation {
	start := sort.Search(len(observations), func(i int) bool {
		return observations[i].Event.Seq >= startSeq
	})
	return observations[start:]
}

func (p *taskTraceProjector) pruneTaskStateLocked(
	workspace string,
	registry *taskregistry.Registry,
) {
	if registry == nil {
		return
	}
	for identity, segment := range p.segments {
		if identity.workspace != workspace {
			continue
		}
		if _, exists := registry.Get(identity.taskID); !exists {
			delete(p.segments, identity)
			delete(p.completed, segment.key)
		}
	}
}

func (p *taskTraceProjector) submitTraceLocked(
	settings traceCaptureSettings,
	trace *activeTraceCapture,
) bool {
	if p.submit != nil {
		return p.submit(settings, trace)
	}
	return false
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
			TraceID: opaqueTraceID(
				"task", workspace+"\x00"+event.TaskID+"\x00"+event.EventID, emittedAt,
			),
			CreatedAt: emittedAt.UTC(),
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

func taskEventIsTerminal(event taskregistry.TaskEvent) bool {
	statusTerminal := event.Status == taskregistry.StatusSucceeded ||
		event.Status == taskregistry.StatusFailed ||
		event.Status == taskregistry.StatusTimedOut ||
		event.Status == taskregistry.StatusCancelled ||
		event.Status == taskregistry.StatusLost
	deliveryTerminal := event.DeliveryStatus == taskregistry.DeliveryDelivered ||
		event.DeliveryStatus == taskregistry.DeliverySessionQueued ||
		event.DeliveryStatus == taskregistry.DeliveryFailed ||
		event.DeliveryStatus == taskregistry.DeliveryParentMissing ||
		event.DeliveryStatus == taskregistry.DeliveryNotApplicable
	return statusTerminal && deliveryTerminal
}

func taskEventErrorCode(event taskregistry.TaskEvent) string {
	return taskErrorCode(taskregistry.Record{
		Status: event.Status, DeliveryStatus: event.DeliveryStatus,
	})
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
