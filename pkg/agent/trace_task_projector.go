package agent

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
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

const taskTraceSubscriptionRetryDelay = time.Second

type taskTraceStorageError struct {
	err error
}

func (e *taskTraceStorageError) Error() string {
	return "task trace storage: " + e.err.Error()
}

func (e *taskTraceStorageError) Unwrap() error {
	return e.err
}

type taskTraceState struct {
	trace      *activeTraceCapture
	settings   traceCaptureSettings
	lastSeq    int64
	lastOffset int64
}

type taskRegistrySubscription struct {
	registry    *taskregistry.Registry
	unsubscribe func()
}

// taskTraceProjector owns task-domain observation and source registration.
// Persistence scheduling, writer receipts, retry, capacity, and drain are
// exclusively owned by evalcapture.Coordinator.
type taskTraceProjector struct {
	mu          sync.Mutex
	closed      bool
	settings    traceCaptureSettings
	coordinator *evalcapture.Coordinator

	registries map[string]*taskregistry.Registry
	sources    map[string]*taskTraceSource
	subs       map[string]taskRegistrySubscription
	retryTimer *time.Timer
}

type taskTraceSource struct {
	mu        sync.RWMutex
	workspace string
	registry  *taskregistry.Registry
	settings  traceCaptureSettings
}

func newTaskTraceProjector(
	settings traceCaptureSettings,
	coordinator *evalcapture.Coordinator,
) *taskTraceProjector {
	return &taskTraceProjector{
		settings:    settings,
		coordinator: coordinator,
		registries:  make(map[string]*taskregistry.Registry),
		sources:     make(map[string]*taskTraceSource),
		subs:        make(map[string]taskRegistrySubscription),
	}
}

func (p *taskTraceProjector) setCoordinator(
	coordinator *evalcapture.Coordinator,
) {
	if p == nil || coordinator == nil {
		return
	}
	p.mu.Lock()
	if p.coordinator == nil {
		p.coordinator = coordinator
	}
	p.mu.Unlock()
}

func (m *traceCaptureManager) attachTaskRegistry(
	workspace string,
	registry *taskregistry.Registry,
) {
	if m != nil && m.tasks != nil {
		m.tasks.attach(workspace, registry)
	}
}

func (p *taskTraceProjector) attach(
	workspace string,
	registry *taskregistry.Registry,
) {
	if p == nil || registry == nil {
		return
	}
	workspace = normalizeRuntimeWorkspace(workspace)
	if workspace == "" {
		return
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	if existing := p.registries[workspace]; existing != nil {
		retry := existing == registry && p.settings.enabled &&
			p.sources[workspace] == nil
		p.mu.Unlock()
		if retry {
			p.install(workspace, registry)
		}
		return
	}
	p.registries[workspace] = registry
	enabled := p.settings.enabled
	p.mu.Unlock()
	if enabled {
		p.install(workspace, registry)
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
		subs, sources := p.detachLocked()
		registries := cloneTaskRegistries(p.registries)
		p.mu.Unlock()
		unsubscribeTaskRegistries(subs)
		p.unregisterSources(sources)
		setTaskRegistryTraceProtection(registries, false, 0)
		return
	}
	if wasEnabled {
		for _, source := range p.sources {
			source.updateSettings(settings)
		}
		registries := cloneTaskRegistries(p.registries)
		p.mu.Unlock()
		setTaskRegistryTraceProtection(
			registries,
			true,
			settings.limits.MaxRecords,
		)
		return
	}
	if !settings.enabled {
		p.mu.Unlock()
		return
	}
	registries := cloneTaskRegistries(p.registries)
	p.mu.Unlock()
	for workspace, registry := range registries {
		p.install(workspace, registry)
	}
}

//nolint:dupl // Typed registry hooks mirror interaction installation; lifecycle is shared.
func (p *taskTraceProjector) install(
	workspace string,
	registry *taskregistry.Registry,
) {
	p.mu.Lock()
	source := &taskTraceSource{
		workspace: workspace,
		registry:  registry,
		settings:  p.settings,
	}
	sourceID := taskTraceSourceID(workspace)
	activate := bindDurableProjectionSource(
		!p.closed && p.settings.enabled &&
			p.registries[workspace] == registry &&
			p.sources[workspace] == nil &&
			p.coordinator != nil,
		"task",
		workspace,
		p.coordinator,
		sourceID,
		source,
		p.settings.limits.MaxRecords,
		func(maxEvents int) error {
			return registry.SetTraceCaptureProtection(true, maxEvents)
		},
		func() (taskregistry.ObservationSnapshot, func(), func()) {
			return registry.SubscribeSnapshot(
				func(observation taskregistry.EventObservation) {
					p.observe(workspace, sourceID, observation)
				},
			)
		},
		func(source *taskTraceSource, unsubscribe func()) {
			p.sources[workspace] = source
			p.subs[workspace] = taskRegistrySubscription{
				registry: registry, unsubscribe: unsubscribe,
			}
		},
		p.requestSnapshotLocked,
		p.scheduleRetryLocked,
	)
	p.mu.Unlock()
	if activate != nil {
		activate()
	}
}

func (p *taskTraceProjector) requestSnapshotLocked(
	sourceID string,
	snapshot taskregistry.ObservationSnapshot,
) {
	records := append([]taskregistry.Record(nil), snapshot.Records...)
	sort.Slice(records, func(i, j int) bool {
		if records[i].TaskID != records[j].TaskID {
			return records[i].TaskID < records[j].TaskID
		}
		return records[i].GenerationID < records[j].GenerationID
	})
	for _, record := range records {
		if record.TraceCapturePending &&
			taskregistry.IsTraceCaptureTerminal(record) {
			p.request(sourceID, record)
		}
	}
}

func (p *taskTraceProjector) observe(
	workspace, sourceID string,
	observation taskregistry.EventObservation,
) {
	if p == nil || !observation.FinalForTask ||
		!taskregistry.IsTraceCaptureTerminal(observation.Record) {
		return
	}
	p.mu.Lock()
	active := !p.closed && p.settings.enabled &&
		p.sources[workspace] != nil
	p.mu.Unlock()
	if active {
		p.request(sourceID, observation.Record)
	}
}

func (p *taskTraceProjector) request(
	sourceID string,
	record taskregistry.Record,
) {
	if p == nil || p.coordinator == nil {
		return
	}
	key := encodeTaskTraceProjectionKey(record.TaskID, record.GenerationID)
	err := p.coordinator.Request(sourceID, key)
	var admission *evalcapture.AdmissionError
	if err != nil && !errors.As(err, &admission) {
		logger.WarnCF("evaltrace", "Failed to request task trace projection", map[string]any{
			"source": sourceID,
			"error":  err.Error(),
		})
	}
}

func (p *taskTraceProjector) scheduleRetryLocked() {
	if p.retryTimer != nil || p.closed || !p.settings.enabled {
		return
	}
	p.retryTimer = time.AfterFunc(
		taskTraceSubscriptionRetryDelay,
		p.retryInstallations,
	)
}

func (p *taskTraceProjector) retryInstallations() {
	p.mu.Lock()
	p.retryTimer = nil
	if p.closed || !p.settings.enabled {
		p.mu.Unlock()
		return
	}
	registries := make(map[string]*taskregistry.Registry)
	for workspace, registry := range p.registries {
		if p.sources[workspace] == nil {
			registries[workspace] = registry
		}
	}
	p.mu.Unlock()
	for workspace, registry := range registries {
		p.install(workspace, registry)
	}
}

// stop detaches live observers but intentionally leaves sources and durable
// markers installed so Coordinator.Close can perform its final recovery scan.
func (p *taskTraceProjector) stop() {
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
	subs := make([]taskRegistrySubscription, 0, len(p.subs))
	for workspace, sub := range p.subs {
		subs = append(subs, sub)
		delete(p.subs, workspace)
	}
	p.mu.Unlock()
	unsubscribeTaskRegistries(subs)
}

func (p *taskTraceProjector) finish() {
	if p == nil {
		return
	}
	p.mu.Lock()
	sources := make(map[string]*taskTraceSource, len(p.sources))
	for workspace, source := range p.sources {
		sources[workspace] = source
	}
	registries := cloneTaskRegistries(p.registries)
	p.sources = nil
	p.registries = nil
	p.subs = nil
	p.mu.Unlock()
	p.unregisterSources(sources)
	setTaskRegistryTraceProtection(registries, false, 0)
}

func (p *taskTraceProjector) detachLocked() (
	[]taskRegistrySubscription,
	map[string]*taskTraceSource,
) {
	if p.retryTimer != nil {
		p.retryTimer.Stop()
		p.retryTimer = nil
	}
	subs := make([]taskRegistrySubscription, 0, len(p.subs))
	for _, sub := range p.subs {
		subs = append(subs, sub)
	}
	sources := make(map[string]*taskTraceSource, len(p.sources))
	for workspace, source := range p.sources {
		sources[workspace] = source
	}
	p.subs = make(map[string]taskRegistrySubscription)
	p.sources = make(map[string]*taskTraceSource)
	return subs, sources
}

func (p *taskTraceProjector) unregisterSources(
	sources map[string]*taskTraceSource,
) {
	if p == nil || p.coordinator == nil {
		return
	}
	for workspace := range sources {
		p.coordinator.UnregisterSource(taskTraceSourceID(workspace))
	}
}

func (s *taskTraceSource) updateSettings(settings traceCaptureSettings) {
	s.mu.Lock()
	s.settings = settings
	s.mu.Unlock()
}

func (s *taskTraceSource) currentSettings() traceCaptureSettings {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.settings
}

func (s *taskTraceSource) Pending(
	ctx context.Context,
	limit int,
) ([]string, error) {
	if s == nil || s.registry == nil || limit <= 0 {
		return nil, nil
	}
	records := s.registry.List()
	sort.Slice(records, func(i, j int) bool {
		if records[i].TaskID != records[j].TaskID {
			return records[i].TaskID < records[j].TaskID
		}
		return records[i].GenerationID < records[j].GenerationID
	})
	keys := make([]string, 0, min(limit, len(records)))
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !record.TraceCapturePending ||
			!taskregistry.IsTraceCaptureTerminal(record) {
			continue
		}
		keys = append(keys, encodeTaskTraceProjectionKey(
			record.TaskID,
			record.GenerationID,
		))
		if len(keys) == limit {
			break
		}
	}
	return keys, nil
}

func (s *taskTraceSource) LoadLatest(
	ctx context.Context,
	key string,
) (evalcapture.DurableCandidate, bool, error) {
	if err := ctx.Err(); err != nil {
		return evalcapture.DurableCandidate{}, false, err
	}
	taskID, generationID, err := decodeTaskTraceProjectionKey(key)
	if err != nil {
		return evalcapture.DurableCandidate{}, false, err
	}
	record, events, exists := s.registry.GetGeneration(taskID, generationID)
	if !exists || !record.TraceCapturePending ||
		!taskregistry.IsTraceCaptureTerminal(record) {
		return evalcapture.DurableCandidate{}, false, nil
	}
	if record.LastEventSeq <= 0 {
		return evalcapture.DurableCandidate{}, false,
			errors.New("terminal task trace has no revision")
	}
	settings := s.currentSettings()
	trace := buildTaskTrace(
		settings,
		s.workspace,
		record,
		events,
	)
	finalized, policy, err := prepareTrace(settings, trace)
	if err != nil {
		return evalcapture.DurableCandidate{}, false, err
	}
	finalized, _, err = reconcileStoredTaskTrace(policy, finalized)
	if err != nil {
		return evalcapture.DurableCandidate{}, false, err
	}
	// TraceCapturePending is an unconfirmed durability claim. A visible file
	// may be the result of a failed post-rename directory sync, so this source
	// always requires a successful writer receipt before clearing the marker.
	return evalcapture.DurableCandidate{
		Revision: uint64(record.LastEventSeq),
		Policy:   policy,
		Trace:    finalized,
		Persist:  true,
	}, true, nil
}

func (s *taskTraceSource) Confirm(
	ctx context.Context,
	key string,
	revision uint64,
) (evalcapture.Confirmation, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if revision > math.MaxInt64 {
		return "", fmt.Errorf("task trace revision exceeds int64")
	}
	taskID, generationID, err := decodeTaskTraceProjectionKey(key)
	if err != nil {
		return "", err
	}
	current, exists := s.registry.Get(taskID)
	if !exists || current.GenerationID != generationID {
		return evalcapture.ConfirmationGone, nil
	}
	record, confirmed, err := s.registry.ConfirmTraceCapturePersisted(
		taskID,
		generationID,
		int64(revision),
	)
	if err != nil {
		current, exists = s.registry.Get(taskID)
		if !exists || current.GenerationID != generationID {
			return evalcapture.ConfirmationGone, nil
		}
		return "", err
	}
	if confirmed {
		return evalcapture.ConfirmationCurrent, nil
	}
	if record.LastEventSeq != int64(revision) {
		return evalcapture.ConfirmationStale, nil
	}
	return "", fmt.Errorf("task trace revision %d was not confirmed", revision)
}

func taskTraceSourceID(workspace string) string {
	workspace = normalizeRuntimeWorkspace(workspace)
	sum := sha256.Sum256([]byte(workspace))
	return fmt.Sprintf("task:%x", sum[:12])
}

func encodeTaskTraceProjectionKey(taskID, generationID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(taskID)) + "." +
		base64.RawURLEncoding.EncodeToString([]byte(generationID))
}

func decodeTaskTraceProjectionKey(key string) (string, string, error) {
	parts := strings.Split(key, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", errors.New("invalid task trace projection key")
	}
	taskID, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", "", fmt.Errorf("decode task ID: %w", err)
	}
	generationID, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", fmt.Errorf("decode generation ID: %w", err)
	}
	if len(taskID) == 0 || len(generationID) == 0 {
		return "", "", errors.New("invalid empty task trace projection identity")
	}
	return string(taskID), string(generationID), nil
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
	maxEvents int,
) {
	for workspace, registry := range registries {
		if err := registry.SetTraceCaptureProtection(
			enabled,
			maxEvents,
		); err != nil {
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

func buildTaskTrace(
	settings traceCaptureSettings,
	workspace string,
	record taskregistry.Record,
	history []taskregistry.TaskEvent,
) *activeTraceCapture {
	workspace = normalizeRuntimeWorkspace(workspace)
	history = append([]taskregistry.TaskEvent(nil), history...)
	sort.Slice(history, func(i, j int) bool {
		if history[i].Seq != history[j].Seq {
			return history[i].Seq < history[j].Seq
		}
		return history[i].EventID < history[j].EventID
	})
	state := newTaskTraceState(
		settings,
		workspace,
		record,
		firstTaskEvent(history),
	)
	if record.TraceCaptureDropped > 0 {
		state.trace.builder.MarkIncomplete(
			"task_capture_journal_truncated",
			record.TraceCaptureDropped,
		)
		state.lastSeq = int64(record.TraceCaptureDropped)
	}
	if len(history) == 0 {
		if state.lastSeq < record.LastEventSeq {
			state.trace.builder.MarkIncomplete(
				"task_history_missing_at_startup",
				int(record.LastEventSeq-state.lastSeq),
			)
		}
		state.lastSeq = record.LastEventSeq
	} else {
		for _, event := range history {
			if event.GenerationID == record.GenerationID {
				appendTaskEvent(state, event, record)
			}
		}
		if state.lastSeq < record.LastEventSeq {
			state.trace.builder.MarkIncomplete(
				"task_history_missing_at_startup",
				int(record.LastEventSeq-state.lastSeq),
			)
			state.lastSeq = record.LastEventSeq
		}
	}
	state.trace.builder.SetOutcome(evaltrace.Outcome{
		Status:    string(record.Status),
		ErrorCode: taskErrorCode(record),
	})
	return state.trace
}

func appendTaskEvent(
	state *taskTraceState,
	event taskregistry.TaskEvent,
	record taskregistry.Record,
) {
	if state == nil || state.trace == nil || event.Seq <= state.lastSeq {
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
				TraceKind:          evaltrace.TraceKindTask,
				SessionHash:        safeHash(settings, record.RequesterSessionKey),
				AgentID:            record.AgentID,
				ProjectionRevision: uint64(max(0, record.LastEventSeq)),
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

func reconcileStoredTaskTrace(
	policy evalcapture.Policy,
	candidate evaltrace.Trace,
) (evaltrace.Trace, bool, error) {
	existing, err := (evaltrace.Store{Root: policy.Root}).Load(candidate.TraceID)
	if errors.Is(err, os.ErrNotExist) {
		return candidate, true, nil
	}
	var corrupt *evaltrace.CorruptTraceError
	if errors.As(err, &corrupt) {
		logger.WarnCF("evaltrace", "Replacing corrupt stored task trace", map[string]any{
			"trace_id": candidate.TraceID,
			"error":    corrupt.Error(),
		})
		return candidate, true, nil
	}
	if err != nil {
		return evaltrace.Trace{}, false, &taskTraceStorageError{err: err}
	}
	selected, persist := reconcileTaskTraceCandidate(existing, candidate)
	return selected, persist, nil
}

func reconcileTaskTraceCandidate(
	existing, candidate evaltrace.Trace,
) (evaltrace.Trace, bool) {
	if existing.TraceID != candidate.TraceID {
		return candidate, true
	}
	if taskTraceRevision(candidate) > taskTraceRevision(existing) {
		if merged, ok := mergeCompleteTaskTrace(existing, candidate); ok {
			return merged, true
		}
	}
	if !existing.Truncation.Incomplete && candidate.Truncation.Incomplete {
		if merged, ok := mergeCompleteTaskTrace(existing, candidate); ok {
			return merged, true
		}
		if taskTraceRevision(candidate) > taskTraceRevision(existing) {
			return candidate, true
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
	if taskTraceRevision(candidate) > taskTraceRevision(existing) {
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
			reason == "task_event_sequence_gap" ||
			reason == "task_capture_journal_truncated" {
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

func maxTaskTraceEventSequence(records []evaltrace.Record) int64 {
	var highest int64
	for _, record := range records {
		if sequence, ok := taskTraceEventSequence(record); ok && sequence > highest {
			highest = sequence
		}
	}
	return highest
}

func taskTraceRevision(trace evaltrace.Trace) uint64 {
	if trace.Metadata.ProjectionRevision > 0 {
		return trace.Metadata.ProjectionRevision
	}
	return uint64(max(0, maxTaskTraceEventSequence(trace.Records)))
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
