package tasks

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/sipeed/picoclaw/pkg/fileutil"
)

type Runtime string

const (
	RuntimeSubagent Runtime = "subagent"
	RuntimeDelegate Runtime = "delegate"
	RuntimeTool     Runtime = "tool"
	RuntimeCron     Runtime = "cron"
)

var (
	ErrTaskAlreadyExists   = errors.New("task already exists")
	ErrTraceCapturePending = errors.New("task trace capture is pending")
)

type Status string

const (
	StatusQueued          Status = "queued"
	StatusRunning         Status = "running"
	StatusWaitingForInput Status = "waiting_for_input"
	StatusSucceeded       Status = "succeeded"
	StatusFailed          Status = "failed"
	StatusTimedOut        Status = "timed_out"
	//nolint:misspell // External task status value intentionally uses British spelling for compatibility.
	StatusCancelled Status = "cancelled"
	StatusLost      Status = "lost"
)

type DeliveryStatus string

const (
	DeliveryPending       DeliveryStatus = "pending"
	DeliveryDelivered     DeliveryStatus = "delivered"
	DeliverySessionQueued DeliveryStatus = "session_queued"
	DeliveryFailed        DeliveryStatus = "failed"
	DeliveryParentMissing DeliveryStatus = "parent_missing"
	DeliveryNotApplicable DeliveryStatus = "not_applicable"
)

type NotifyPolicy string

const (
	NotifyDoneOnly     NotifyPolicy = "done_only"
	NotifyStateChanges NotifyPolicy = "state_changes"
	NotifySilent       NotifyPolicy = "silent"
)

const (
	DefaultTerminalRetention = 7 * 24 * time.Hour
	DefaultMaxRecords        = 1000
	DefaultMaxEvents         = 5000
	DefaultMaxSnapshotBytes  = 2 * 1024 * 1024
	TaskEventSchemaVersion   = "task_event.v2"
	DeliverableReportV1      = "deliverable_report.v1"
)

type EventType string

const (
	EventTaskUpserted         EventType = "task.upserted"
	EventTaskStatusChanged    EventType = "task.status_changed"
	EventTaskDeliveryChanged  EventType = "task.delivery_changed"
	EventTaskDeliveryDecision EventType = "task.delivery_decision"
	EventTaskProgress         EventType = "task.progress"
	EventTaskUpdated          EventType = "task.updated"
	EventTaskReconciled       EventType = "task.reconciled"
)

type CompletionPayload struct {
	Text  string            `json:"text,omitempty"`
	Media []CompletionMedia `json:"media,omitempty"`
}

type CompletionMedia struct {
	Ref         string `json:"ref"`
	Type        string `json:"type,omitempty"`
	Filename    string `json:"filename,omitempty"`
	ContentType string `json:"content_type,omitempty"`
}

type DeliverablePayload struct {
	Text      string             `json:"text,omitempty"`
	Artifacts []DeliverableItem  `json:"artifacts,omitempty"`
	Metadata  map[string]string  `json:"metadata,omitempty"`
	Report    *DeliverableReport `json:"report,omitempty"`
}

type DeliverableItem struct {
	Ref         string `json:"ref"`
	Kind        string `json:"kind,omitempty"`
	Filename    string `json:"filename,omitempty"`
	ContentType string `json:"content_type,omitempty"`
	Delivered   bool   `json:"delivered,omitempty"`
}

// DeliverableReport is a versioned canonical report for durable outputs. The
// surrounding DeliverablePayload remains the compatibility projection for older
// tools; Report is the schemaed contract new consumers should prefer.
type DeliverableReport struct {
	SchemaVersion string             `json:"schema_version"`
	ReportID      string             `json:"report_id"`
	ContentHash   string             `json:"content_hash"`
	GeneratedAt   int64              `json:"generated_at"`
	Summary       string             `json:"summary,omitempty"`
	Claims        []ReportClaim      `json:"claims,omitempty"`
	FieldDeltas   []ReportFieldDelta `json:"field_deltas,omitempty"`
	Provenance    map[string]string  `json:"provenance,omitempty"`
	Metadata      map[string]string  `json:"metadata,omitempty"`
	Extra         map[string]any     `json:"extra,omitempty"`
}

type ReportClaim struct {
	Kind       string            `json:"kind"`
	Text       string            `json:"text"`
	Confidence string            `json:"confidence,omitempty"`
	SourceRefs []string          `json:"source_refs,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

type ReportFieldDelta struct {
	Field string `json:"field"`
	From  string `json:"from,omitempty"`
	To    string `json:"to,omitempty"`
}

// TaskEvent is the append-only canonical event stream for task state. Records
// remain the current-state projection; chat, terminal, and status tools should
// render from records or reports, not treat prose output as source of truth.
type TaskEvent struct {
	SchemaVersion  string            `json:"schema_version"`
	EventID        string            `json:"event_id"`
	TaskID         string            `json:"task_id"`
	GenerationID   string            `json:"generation_id"`
	Runtime        Runtime           `json:"runtime,omitempty"`
	ParentTaskID   string            `json:"parent_task_id,omitempty"`
	Type           EventType         `json:"type"`
	Status         Status            `json:"status,omitempty"`
	DeliveryStatus DeliveryStatus    `json:"delivery_status,omitempty"`
	Seq            int64             `json:"seq"`
	EmittedAt      int64             `json:"emitted_at"`
	Source         string            `json:"source,omitempty"`
	Producer       string            `json:"producer,omitempty"`
	Fingerprint    string            `json:"fingerprint,omitempty"`
	Payload        map[string]string `json:"payload,omitempty"`
}

type Record struct {
	TaskID              string              `json:"task_id"`
	GenerationID        string              `json:"generation_id"`
	LastEventSeq        int64               `json:"last_event_sequence"`
	Runtime             Runtime             `json:"runtime"`
	TaskKind            string              `json:"task_kind,omitempty"`
	ParentTaskID        string              `json:"parent_task_id,omitempty"`
	RequesterSessionKey string              `json:"requester_session_key,omitempty"`
	OwnerKey            string              `json:"owner_key,omitempty"`
	ScopeKind           string              `json:"scope_kind,omitempty"`
	Channel             string              `json:"channel,omitempty"`
	ChatID              string              `json:"chat_id,omitempty"`
	TopicID             string              `json:"topic_id,omitempty"`
	AgentID             string              `json:"agent_id,omitempty"`
	Label               string              `json:"label,omitempty"`
	Task                string              `json:"task"`
	Status              Status              `json:"status"`
	DeliveryStatus      DeliveryStatus      `json:"delivery_status"`
	NotifyPolicy        NotifyPolicy        `json:"notify_policy"`
	DeliveryMode        string              `json:"delivery_mode,omitempty"`
	TimeoutSeconds      int                 `json:"timeout_seconds,omitempty"`
	LastCompletionID    string              `json:"last_completion_id,omitempty"`
	DeliveredAt         int64               `json:"delivered_at,omitempty"`
	DeliveryError       string              `json:"delivery_error,omitempty"`
	TraceCapturePending bool                `json:"trace_capture_pending,omitempty"`
	TraceCaptureEvents  []TaskEvent         `json:"trace_capture_events,omitempty"`
	TraceCaptureDropped int                 `json:"trace_capture_dropped,omitempty"`
	CreatedAt           int64               `json:"created_at"`
	StartedAt           int64               `json:"started_at,omitempty"`
	EndedAt             int64               `json:"ended_at,omitempty"`
	LastEventAt         int64               `json:"last_event_at,omitempty"`
	CleanupAfter        int64               `json:"cleanup_after,omitempty"`
	Error               string              `json:"error,omitempty"`
	ProgressSummary     string              `json:"progress_summary,omitempty"`
	TerminalSummary     string              `json:"terminal_summary,omitempty"`
	InteractionID       string              `json:"interaction_id,omitempty"`
	InteractionShortID  string              `json:"interaction_short_id,omitempty"`
	InteractionSummary  string              `json:"interaction_summary,omitempty"`
	Completion          *CompletionPayload  `json:"completion,omitempty"`
	Deliverable         *DeliverablePayload `json:"deliverable,omitempty"`
}

type Options struct {
	TerminalRetention time.Duration
	MaxRecords        int
	MaxEvents         int
	MaxSnapshotBytes  int
}

type Registry struct {
	mu          sync.RWMutex
	store       string
	options     Options
	records     map[string]Record
	events      []TaskEvent
	observers   []observerEntry
	lastLoad    error
	writeAtomic func(string, []byte, os.FileMode) error

	traceCaptureProtection        bool
	traceCaptureProtectionPending bool
	traceCaptureMaxEvents         int
	unsyncedWrite                 bool
}

type Snapshot struct {
	Tasks  []Record    `json:"tasks"`
	Events []TaskEvent `json:"events,omitempty"`
}

type registryState struct {
	records map[string]Record
	events  []TaskEvent
}

// Stats describes the current durable registry state and the retention limits
// that apply to it. Protected records are active, non-terminal, or have not
// reached a final delivery state, so retention never removes them.
type Stats struct {
	TaskCount          int
	EventCount         int
	ProtectedTaskCount int
	SnapshotBytes      int
	TerminalRetention  time.Duration
	MaxRecords         int
	MaxEvents          int
	MaxSnapshotBytes   int
	OverSnapshotBudget bool
}

func NewRegistry(storePath string) *Registry {
	return NewRegistryWithOptions(storePath, Options{})
}

func NewRegistryWithOptions(storePath string, opts Options) *Registry {
	if opts.TerminalRetention <= 0 {
		opts.TerminalRetention = DefaultTerminalRetention
	}
	if opts.MaxRecords <= 0 {
		opts.MaxRecords = DefaultMaxRecords
	}
	if opts.MaxEvents <= 0 {
		opts.MaxEvents = DefaultMaxEvents
	}
	if opts.MaxSnapshotBytes <= 0 {
		opts.MaxSnapshotBytes = DefaultMaxSnapshotBytes
	}
	r := &Registry{
		store:                 strings.TrimSpace(storePath),
		options:               opts,
		records:               make(map[string]Record),
		events:                make([]TaskEvent, 0),
		traceCaptureMaxEvents: opts.MaxEvents,
		writeAtomic:           fileutil.WriteFileAtomic,
	}
	if r.store != "" {
		r.lastLoad = r.load()
		if r.lastLoad == nil {
			r.pruneLoadedState(time.Now().UnixMilli())
		}
	}
	return r
}

func WorkspaceStorePath(workspace string) string {
	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		return ""
	}
	return filepath.Join(workspace, "state", "task_registry.json")
}

func (r *Registry) LastLoadError() error {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastLoad
}

// Stats returns an exact serialized snapshot size and retention state.
func (r *Registry) Stats() Stats {
	if r == nil {
		return Stats{}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	stats := Stats{
		TaskCount:         len(r.records),
		EventCount:        len(r.events),
		SnapshotBytes:     r.snapshotSizeLocked(),
		TerminalRetention: r.options.TerminalRetention,
		MaxRecords:        r.options.MaxRecords,
		MaxEvents:         r.options.MaxEvents,
		MaxSnapshotBytes:  r.options.MaxSnapshotBytes,
	}
	for _, rec := range r.records {
		if !canPruneRecord(rec) {
			stats.ProtectedTaskCount++
		}
	}
	stats.OverSnapshotBudget = stats.MaxSnapshotBytes > 0 && stats.SnapshotBytes > stats.MaxSnapshotBytes
	return stats
}

// Create persists a new task generation without replacing an existing task.
func (r *Registry) Create(rec Record) error {
	err := r.storeNewGeneration(rec, true)
	if fileutil.IsCommittedWriteError(err) {
		return nil
	}
	return err
}

func (r *Registry) Upsert(rec Record) error {
	return r.storeNewGeneration(rec, false)
}

func (r *Registry) storeNewGeneration(rec Record, rejectExisting bool) error {
	if r == nil || strings.TrimSpace(rec.TaskID) == "" {
		return nil
	}
	rec = cloneTaskRecord(rec)
	if err := canonicalizeRecordExtra(&rec); err != nil {
		return fmt.Errorf("canonicalize task %q report extra: %w", rec.TaskID, err)
	}
	now := time.Now().UnixMilli()
	if rec.CreatedAt == 0 {
		rec.CreatedAt = now
	}
	if rec.LastEventAt == 0 {
		rec.LastEventAt = now
	}
	if rec.Status == "" {
		rec.Status = StatusQueued
	}
	if rec.DeliveryStatus == "" {
		rec.DeliveryStatus = DeliveryPending
	}
	if rec.NotifyPolicy == "" {
		rec.NotifyPolicy = NotifyDoneOnly
	}
	if rec.Runtime == "" {
		rec.Runtime = RuntimeTool
	}
	rec = r.normalizeRecord(rec, now)
	rec.GenerationID = uuid.NewString()
	rec.LastEventSeq = 0

	r.mu.Lock()
	if err := r.writableErrorLocked(); err != nil {
		r.mu.Unlock()
		return err
	}
	if existing, ok := r.records[rec.TaskID]; ok {
		if rejectExisting {
			r.mu.Unlock()
			return fmt.Errorf("task %q: %w", rec.TaskID, ErrTaskAlreadyExists)
		}
		if existing.TraceCapturePending {
			r.mu.Unlock()
			return fmt.Errorf(
				"task %q generation %q: %w",
				rec.TaskID,
				existing.GenerationID,
				ErrTraceCapturePending,
			)
		}
	}
	rollbackState := r.captureStateLocked()
	eventStart := len(r.events)
	r.records[rec.TaskID] = rec
	r.appendEventLocked(rec, EventTaskUpserted, now, map[string]string{
		"task_kind": rec.TaskKind,
		"label":     rec.Label,
	})
	newEvents := r.eventsSinceLocked(eventStart)
	r.pruneMutationLocked(now, newEvents)
	err := r.saveLocked()
	_, deliveries := r.completeMutationLocked(err, rollbackState, newEvents)
	r.mu.Unlock()
	drainEventObservers(deliveries)
	return err
}

func (r *Registry) Update(taskID string, mutate func(*Record)) error {
	if r == nil || strings.TrimSpace(taskID) == "" || mutate == nil {
		return nil
	}
	r.mu.Lock()
	if err := r.writableErrorLocked(); err != nil {
		r.mu.Unlock()
		return err
	}
	rollbackState := r.captureStateLocked()
	eventStart := len(r.events)
	rec, ok := r.records[taskID]
	if !ok {
		r.mu.Unlock()
		return fmt.Errorf("task %q not found", taskID)
	}
	before := cloneTaskRecord(rec)
	rec = cloneTaskRecord(rec)
	mutate(&rec)
	if err := canonicalizeRecordExtra(&rec); err != nil {
		r.mu.Unlock()
		return fmt.Errorf("canonicalize task %q report extra: %w", taskID, err)
	}
	rec.GenerationID = before.GenerationID
	rec.LastEventSeq = before.LastEventSeq
	now := time.Now().UnixMilli()
	if rec.LastEventAt == 0 || recordChanged(before, rec) {
		rec.LastEventAt = now
	}
	rec = r.normalizeRecord(rec, rec.LastEventAt)
	r.records[taskID] = rec
	r.appendUpdateEventsLocked(before, rec, rec.LastEventAt)
	newEvents := r.eventsSinceLocked(eventStart)
	r.pruneMutationLocked(rec.LastEventAt, newEvents)
	err := r.saveLocked()
	_, deliveries := r.completeMutationLocked(err, rollbackState, newEvents)
	r.mu.Unlock()
	drainEventObservers(deliveries)
	return err
}

func (r *Registry) AppendEvent(taskID string, eventType EventType, payload map[string]string) error {
	if r == nil || strings.TrimSpace(taskID) == "" || eventType == "" {
		return nil
	}
	r.mu.Lock()
	if err := r.writableErrorLocked(); err != nil {
		r.mu.Unlock()
		return err
	}
	rollbackState := r.captureStateLocked()
	eventStart := len(r.events)
	rec, ok := r.records[taskID]
	if !ok {
		r.mu.Unlock()
		return fmt.Errorf("task %q not found", taskID)
	}
	now := time.Now().UnixMilli()
	r.appendEventLocked(rec, eventType, now, payload)
	newEvents := r.eventsSinceLocked(eventStart)
	r.pruneMutationLocked(now, newEvents)
	err := r.saveLocked()
	_, deliveries := r.completeMutationLocked(err, rollbackState, newEvents)
	r.mu.Unlock()
	drainEventObservers(deliveries)
	return err
}

// SetTraceCapturePending durably protects a terminal task while its canonical
// evaluation trace is awaiting persistence. This projection is operational
// metadata and intentionally does not append a task lifecycle event.
func (r *Registry) SetTraceCapturePending(
	taskID, generationID string,
	pending bool,
) error {
	if r == nil || strings.TrimSpace(taskID) == "" ||
		strings.TrimSpace(generationID) == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.writableErrorLocked(); err != nil {
		return err
	}
	record, ok := r.records[taskID]
	if !ok || record.GenerationID != generationID {
		return fmt.Errorf("task %q generation %q not found", taskID, generationID)
	}
	if record.TraceCapturePending == pending {
		if !r.unsyncedWrite {
			return nil
		}
		return r.saveLocked()
	}
	rollback := r.captureStateLocked()
	record.TraceCapturePending = pending
	if pending {
		record.TraceCaptureEvents, record.TraceCaptureDropped = r.traceCaptureJournalLocked(record)
	} else {
		record.TraceCaptureEvents = nil
		record.TraceCaptureDropped = 0
	}
	r.records[taskID] = record
	if err := r.saveLocked(); err != nil {
		if !fileutil.IsCommittedWriteError(err) {
			r.restoreStateLocked(rollback)
		}
		return err
	}
	return nil
}

// ConfirmTraceCapturePersisted atomically clears trace protection only when the
// acknowledged generation and event sequence are still current.
func (r *Registry) ConfirmTraceCapturePersisted(
	taskID, generationID string,
	expectedLastEventSeq int64,
) (Record, bool, error) {
	if r == nil || strings.TrimSpace(taskID) == "" ||
		strings.TrimSpace(generationID) == "" {
		return Record{}, false, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.writableErrorLocked(); err != nil {
		return Record{}, false, err
	}
	record, ok := r.records[taskID]
	if !ok || record.GenerationID != generationID {
		return Record{}, false, fmt.Errorf(
			"task %q generation %q not found",
			taskID,
			generationID,
		)
	}
	if record.LastEventSeq != expectedLastEventSeq {
		return cloneTaskRecord(record), false, nil
	}
	if !record.TraceCapturePending {
		if r.unsyncedWrite {
			if err := r.saveLocked(); err != nil {
				return cloneTaskRecord(record), false, err
			}
		}
		return cloneTaskRecord(record), true, nil
	}
	rollback := r.captureStateLocked()
	record.TraceCapturePending = false
	record.TraceCaptureEvents = nil
	record.TraceCaptureDropped = 0
	r.records[taskID] = record
	if err := r.saveLocked(); err != nil {
		// A visible post-rename snapshot is not a durability acknowledgement.
		// Keep the in-memory marker until a later save confirms directory sync,
		// so task ID reuse cannot replace the generation being acknowledged.
		r.restoreStateLocked(rollback)
		return cloneTaskRecord(r.records[taskID]), false, err
	}
	return cloneTaskRecord(record), true, nil
}

// SetTraceCaptureProtection controls durable lifecycle journaling for task
// transitions. Disabling preserves terminal pending markers for restart
// recovery; enabling journals retained records before the caller takes its
// startup snapshot.
func (r *Registry) SetTraceCaptureProtection(
	enabled bool,
	maxEvents int,
) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.writableErrorLocked(); err != nil {
		return err
	}
	rollback := r.captureStateLocked()
	previous := r.traceCaptureProtection
	r.traceCaptureProtection = enabled
	if !enabled {
		r.traceCaptureProtectionPending = false
		return nil
	}
	if maxEvents <= 0 {
		maxEvents = DefaultMaxEvents
	}
	r.traceCaptureMaxEvents = maxEvents
	changed := r.journalRetainedTraceRecordsLocked()
	if !changed && previous == enabled &&
		!r.traceCaptureProtectionPending && !r.unsyncedWrite {
		return nil
	}
	if err := r.saveLocked(); err != nil {
		if !fileutil.IsCommittedWriteError(err) {
			r.restoreStateLocked(rollback)
		}
		// Keep protection requested in memory so pruning remains fail-closed
		// until the caller retries and a complete protected snapshot is durable.
		r.traceCaptureProtection = true
		r.traceCaptureProtectionPending = true
		return err
	}
	r.traceCaptureProtectionPending = false
	return nil
}

func (r *Registry) Heartbeat(taskID, progress string) error {
	now := time.Now().UnixMilli()
	return r.Update(taskID, func(rec *Record) {
		if rec.Status != StatusQueued && rec.Status != StatusRunning {
			return
		}
		rec.LastEventAt = now
		if progress = strings.TrimSpace(progress); progress != "" {
			rec.ProgressSummary = progress
		}
	})
}

// MarkWaitingForInput projects a durable human interaction onto a running task.
// Only bounded, user-safe interaction metadata belongs in the task registry.
func (r *Registry) MarkWaitingForInput(
	taskID, interactionID, shortID, summary string,
) error {
	interactionID = strings.TrimSpace(interactionID)
	if interactionID == "" {
		return fmt.Errorf("interaction ID is required")
	}
	return r.updateInteractionProjection(taskID, func(rec *Record) (bool, error) {
		if rec.Status == StatusWaitingForInput && rec.InteractionID != interactionID {
			return false, fmt.Errorf(
				"task %q already waits for interaction %q",
				taskID,
				rec.InteractionID,
			)
		}
		if rec.Status != StatusQueued && rec.Status != StatusRunning &&
			rec.Status != StatusWaitingForInput {
			return false, fmt.Errorf(
				"task %q cannot wait for input from status %q", taskID, rec.Status,
			)
		}
		rec.Status = StatusWaitingForInput
		rec.InteractionID = interactionID
		rec.InteractionShortID = truncateInteractionField(shortID, 64)
		rec.InteractionSummary = truncateInteractionField(summary, 500)
		rec.ProgressSummary = "waiting for human input"
		return true, nil
	})
}

// MarkInteractionRunning returns a matching waiting task to running before its
// suspended continuation starts. The interaction ID is retained for audit and
// correlation while display-only waiting metadata is cleared.
func (r *Registry) MarkInteractionRunning(taskID, interactionID string) error {
	interactionID = strings.TrimSpace(interactionID)
	if interactionID == "" {
		return fmt.Errorf("interaction ID is required")
	}
	return r.updateInteractionProjection(taskID, func(rec *Record) (bool, error) {
		if rec.InteractionID != interactionID {
			return false, fmt.Errorf(
				"task %q interaction mismatch: have %q, got %q",
				taskID,
				rec.InteractionID,
				interactionID,
			)
		}
		if rec.Status != StatusWaitingForInput && rec.Status != StatusRunning &&
			rec.Status != StatusLost {
			return false, fmt.Errorf(
				"task %q cannot resume from status %q", taskID, rec.Status,
			)
		}
		if rec.Status == StatusLost {
			rec.EndedAt = 0
			rec.CleanupAfter = 0
			rec.Error = ""
			if rec.DeliveryStatus == DeliveryNotApplicable {
				rec.DeliveryStatus = DeliveryPending
			}
		}
		rec.Status = StatusRunning
		rec.InteractionShortID = ""
		rec.InteractionSummary = ""
		rec.ProgressSummary = "resuming after human input"
		return true, nil
	})
}

// FinishInteraction projects a terminal interaction failure onto its owning
// task. Successful answers resume the task and are completed by the task owner.
func (r *Registry) FinishInteraction(
	taskID, interactionID string,
	status Status,
	summary string,
) error {
	switch status {
	case StatusFailed, StatusTimedOut, StatusCancelled:
	default:
		return fmt.Errorf("invalid terminal interaction task status %q", status)
	}
	interactionID = strings.TrimSpace(interactionID)
	if interactionID == "" {
		return fmt.Errorf("interaction ID is required")
	}
	return r.updateInteractionProjection(taskID, func(rec *Record) (bool, error) {
		if rec.InteractionID != interactionID {
			return false, fmt.Errorf(
				"task %q interaction mismatch: have %q, got %q",
				taskID,
				rec.InteractionID,
				interactionID,
			)
		}
		if isTerminalStatus(rec.Status) && rec.Status != StatusLost {
			return false, nil
		}
		if rec.Status == StatusLost {
			rec.EndedAt = 0
			rec.CleanupAfter = 0
		}
		rec.Status = status
		rec.InteractionShortID = ""
		rec.InteractionSummary = ""
		rec.ProgressSummary = ""
		rec.Error = truncateInteractionField(summary, 1000)
		return true, nil
	})
}

// CompleteInteractionTask terminalizes a task only after its suspended
// continuation has produced and delivered the final result.
func (r *Registry) CompleteInteractionTask(
	taskID, interactionID, content string,
	delivery DeliveryStatus,
) error {
	interactionID = strings.TrimSpace(interactionID)
	if interactionID == "" {
		return fmt.Errorf("interaction ID is required")
	}
	if delivery == "" {
		delivery = DeliveryNotApplicable
	}
	return r.updateInteractionProjection(taskID, func(rec *Record) (bool, error) {
		if rec.InteractionID != interactionID {
			return false, fmt.Errorf(
				"task %q interaction mismatch: have %q, got %q",
				taskID, rec.InteractionID, interactionID,
			)
		}
		if isTerminalStatus(rec.Status) && rec.Status != StatusLost {
			return false, nil
		}
		if rec.Status == StatusLost {
			rec.EndedAt = 0
			rec.CleanupAfter = 0
		}
		summary := truncateInteractionField(content, 1000)
		rec.Status = StatusSucceeded
		rec.DeliveryStatus = delivery
		rec.InteractionShortID = ""
		rec.InteractionSummary = ""
		rec.ProgressSummary = ""
		rec.TerminalSummary = summary
		rec.Error = ""
		if strings.TrimSpace(content) != "" {
			rec.Completion = &CompletionPayload{Text: content}
		}
		if delivery == DeliveryDelivered || delivery == DeliveryNotApplicable {
			rec.DeliveredAt = time.Now().UnixMilli()
		}
		return true, nil
	})
}

func (r *Registry) updateInteractionProjection(
	taskID string,
	mutate func(*Record) (bool, error),
) error {
	if r == nil || strings.TrimSpace(taskID) == "" || mutate == nil {
		return nil
	}
	r.mu.Lock()
	if err := r.writableErrorLocked(); err != nil {
		r.mu.Unlock()
		return err
	}
	rollbackState := r.captureStateLocked()
	eventStart := len(r.events)
	rec, ok := r.records[taskID]
	if !ok {
		r.mu.Unlock()
		return fmt.Errorf("task %q not found", taskID)
	}
	before := rec
	changed, err := mutate(&rec)
	if err != nil || !changed {
		r.mu.Unlock()
		return err
	}
	rec.GenerationID = before.GenerationID
	rec.LastEventSeq = before.LastEventSeq
	now := time.Now().UnixMilli()
	rec.LastEventAt = now
	rec = r.normalizeRecord(rec, now)
	r.records[taskID] = rec
	r.appendUpdateEventsLocked(before, rec, now)
	newEvents := r.eventsSinceLocked(eventStart)
	r.pruneMutationLocked(now, newEvents)
	err = r.saveLocked()
	_, deliveries := r.completeMutationLocked(err, rollbackState, newEvents)
	r.mu.Unlock()
	drainEventObservers(deliveries)
	return err
}

func (r *Registry) Get(taskID string) (Record, bool) {
	if r == nil {
		return Record{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec, ok := r.records[taskID]
	return cloneTaskRecord(rec), ok
}

// GetGeneration returns one exact task generation and its retained event
// stream from the same registry revision.
func (r *Registry) GetGeneration(
	taskID, generationID string,
) (Record, []TaskEvent, bool) {
	if r == nil {
		return Record{}, nil, false
	}
	taskID = strings.TrimSpace(taskID)
	generationID = strings.TrimSpace(generationID)
	if taskID == "" || generationID == "" {
		return Record{}, nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	record, ok := r.records[taskID]
	if !ok || record.GenerationID != generationID {
		return Record{}, nil, false
	}
	if record.TraceCapturePending {
		events := make(
			[]TaskEvent,
			len(record.TraceCaptureEvents),
		)
		for i := range record.TraceCaptureEvents {
			events[i] = cloneTaskEvent(record.TraceCaptureEvents[i])
		}
		return cloneTaskRecord(record), events, true
	}
	events := make([]TaskEvent, 0, len(r.events))
	for _, event := range r.events {
		if event.TaskID == taskID && event.GenerationID == generationID {
			events = append(events, cloneTaskEvent(event))
		}
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].Seq != events[j].Seq {
			return events[i].Seq < events[j].Seq
		}
		return events[i].EventID < events[j].EventID
	})
	return cloneTaskRecord(record), events, true
}

func (r *Registry) List() []Record {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Record, 0, len(r.records))
	for _, rec := range r.records {
		out = append(out, cloneTaskRecord(rec))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt != out[j].CreatedAt {
			return out[i].CreatedAt < out[j].CreatedAt
		}
		return out[i].TaskID < out[j].TaskID
	})
	return out
}

func (r *Registry) ListEvents(taskID string) []TaskEvent {
	if r == nil {
		return nil
	}
	taskID = strings.TrimSpace(taskID)
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]TaskEvent, 0, len(r.events))
	for _, evt := range r.events {
		if taskID == "" || evt.TaskID == taskID {
			out = append(out, cloneTaskEvent(evt))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].EmittedAt != out[j].EmittedAt {
			return out[i].EmittedAt < out[j].EmittedAt
		}
		if out[i].Seq != out[j].Seq {
			return out[i].Seq < out[j].Seq
		}
		return out[i].EventID < out[j].EventID
	})
	return out
}

func (r *Registry) ListActive() []Record {
	records := r.List()
	out := make([]Record, 0)
	for _, rec := range records {
		if rec.Status == StatusQueued || rec.Status == StatusRunning ||
			rec.Status == StatusWaitingForInput {
			out = append(out, rec)
		}
	}
	return out
}

func (r *Registry) MarkStaleActiveLost(maxAge time.Duration, reason string) (int, error) {
	if r == nil || maxAge <= 0 {
		return 0, nil
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "active task did not report progress before stale timeout"
	}
	now := time.Now().UnixMilli()
	staleBefore := now - int64(maxAge/time.Millisecond)
	changed := 0

	r.mu.Lock()
	if err := r.writableErrorLocked(); err != nil {
		r.mu.Unlock()
		return 0, err
	}
	rollbackState := r.captureStateLocked()
	eventStart := len(r.events)
	for _, id := range r.sortedRecordIDsLocked() {
		rec := r.records[id]
		if rec.Status != StatusQueued && rec.Status != StatusRunning {
			continue
		}
		if rec.Status == StatusRunning && strings.TrimSpace(rec.InteractionID) != "" {
			continue
		}
		before := rec
		ref := rec.LastEventAt
		if ref == 0 {
			ref = rec.StartedAt
		}
		if ref == 0 {
			ref = rec.CreatedAt
		}
		if ref > 0 && ref > staleBefore {
			continue
		}
		rec.Status = StatusLost
		if !isFinalDeliveryStatus(rec.DeliveryStatus) {
			rec.DeliveryStatus = DeliveryNotApplicable
		}
		rec.LastEventAt = now
		rec.EndedAt = now
		if strings.TrimSpace(rec.Error) == "" {
			rec.Error = reason
		}
		rec = r.normalizeRecord(rec, now)
		r.records[id] = rec
		r.appendUpdateEventsLocked(before, rec, now)
		r.appendEventLocked(rec, EventTaskReconciled, now, map[string]string{"reason": reason})
		changed++
	}
	err := error(nil)
	newEvents := r.eventsSinceLocked(eventStart)
	var deliveries []*eventObserverDelivery
	if changed > 0 {
		r.pruneMutationLocked(now, newEvents)
		err = r.saveLocked()
		committed, queued := r.completeMutationLocked(err, rollbackState, newEvents)
		deliveries = queued
		if !committed {
			changed = 0
		}
	}
	r.mu.Unlock()
	drainEventObservers(deliveries)
	return changed, err
}

func (r *Registry) MarkActiveLost(reason string) (int, error) {
	if r == nil {
		return 0, nil
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "active task owner is no longer alive"
	}
	now := time.Now().UnixMilli()
	changed := 0

	r.mu.Lock()
	if err := r.writableErrorLocked(); err != nil {
		r.mu.Unlock()
		return 0, err
	}
	rollbackState := r.captureStateLocked()
	eventStart := len(r.events)
	for _, id := range r.sortedRecordIDsLocked() {
		rec := r.records[id]
		if rec.Status != StatusQueued && rec.Status != StatusRunning {
			continue
		}
		if rec.Status == StatusRunning && strings.TrimSpace(rec.InteractionID) != "" {
			continue
		}
		before := rec
		rec.Status = StatusLost
		if !isFinalDeliveryStatus(rec.DeliveryStatus) {
			rec.DeliveryStatus = DeliveryNotApplicable
		}
		rec.LastEventAt = now
		rec.EndedAt = now
		if strings.TrimSpace(rec.Error) == "" {
			rec.Error = reason
		}
		rec = r.normalizeRecord(rec, now)
		r.records[id] = rec
		r.appendUpdateEventsLocked(before, rec, now)
		r.appendEventLocked(rec, EventTaskReconciled, now, map[string]string{"reason": reason})
		changed++
	}
	err := error(nil)
	newEvents := r.eventsSinceLocked(eventStart)
	var deliveries []*eventObserverDelivery
	if changed > 0 {
		r.pruneMutationLocked(now, newEvents)
		err = r.saveLocked()
		committed, queued := r.completeMutationLocked(err, rollbackState, newEvents)
		deliveries = queued
		if !committed {
			changed = 0
		}
	}
	r.mu.Unlock()
	drainEventObservers(deliveries)
	return changed, err
}

func (r *Registry) ListPendingTerminalDelivery() []Record {
	if r == nil {
		return nil
	}
	records := r.List()
	out := make([]Record, 0)
	for _, rec := range records {
		if rec.DeliveryStatus == DeliveryPending && isTerminalStatus(rec.Status) {
			out = append(out, rec)
		}
	}
	return out
}

func (r *Registry) normalizeRecord(rec Record, now int64) Record {
	if r == nil {
		return rec
	}
	if now == 0 {
		now = time.Now().UnixMilli()
	}
	if isTerminalStatus(rec.Status) && rec.EndedAt == 0 {
		rec.EndedAt = rec.LastEventAt
		if rec.EndedAt == 0 {
			rec.EndedAt = now
		}
	}
	if isTerminalStatus(rec.Status) && rec.CleanupAfter == 0 {
		base := recordReferenceAt(rec)
		if base == 0 {
			base = now
		}
		rec.CleanupAfter = base + int64(r.options.TerminalRetention/time.Millisecond)
	}
	if rec.Deliverable != nil {
		rec.Deliverable = normalizeDeliverablePayload(rec.Deliverable, now)
	}
	rec.InteractionID = strings.TrimSpace(rec.InteractionID)
	rec.InteractionShortID = truncateInteractionField(rec.InteractionShortID, 64)
	rec.InteractionSummary = truncateInteractionField(rec.InteractionSummary, 500)
	return rec
}

func truncateInteractionField(value string, maxRunes int) string {
	value = strings.TrimSpace(value)
	runes := []rune(value)
	if maxRunes > 0 && len(runes) > maxRunes {
		return string(runes[:maxRunes])
	}
	return value
}

func (r *Registry) pruneLocked(now int64) bool {
	if r == nil {
		return false
	}
	if r.traceCaptureProtectionPending {
		return false
	}
	changed := false
	for id, rec := range r.records {
		if shouldPruneExpired(rec, now) {
			delete(r.records, id)
			changed = true
		}
	}
	if r.options.MaxRecords > 0 && len(r.records) > r.options.MaxRecords {
		terminal := make([]Record, 0, len(r.records))
		for _, rec := range r.records {
			if canPruneRecord(rec) {
				terminal = append(terminal, rec)
			}
		}
		sort.Slice(terminal, func(i, j int) bool {
			return recordReferenceAt(terminal[i]) < recordReferenceAt(terminal[j])
		})
		for len(r.records) > r.options.MaxRecords && len(terminal) > 0 {
			victim := terminal[0]
			terminal = terminal[1:]
			delete(r.records, victim.TaskID)
			changed = true
		}
	}
	if r.pruneEventsLocked() {
		changed = true
	}
	if r.pruneSnapshotBytesLocked() {
		changed = true
	}
	return changed
}

func (r *Registry) pruneLoadedState(now int64) {
	rollback := r.captureStateLocked()
	if !r.pruneLocked(now) {
		return
	}
	if err := r.saveLocked(); err != nil && !fileutil.IsCommittedWriteError(err) {
		r.restoreStateLocked(rollback)
		r.lastLoad = fmt.Errorf("persist pruned task registry: %w", err)
	}
}

func (r *Registry) pruneMutationLocked(now int64, candidates []TaskEvent) {
	if r.traceCaptureProtectionPending {
		r.journalRetainedTraceRecordsLocked()
	}
	r.pruneLocked(now)
	if len(candidates) == 0 {
		return
	}
	type streamKey struct {
		taskID       string
		generationID string
	}
	retainedStreams := make(map[streamKey]struct{}, len(candidates))
	candidateIDs := make(map[string]struct{}, len(candidates))
	for _, event := range candidates {
		candidateIDs[event.EventID] = struct{}{}
	}
	retainedCandidates := make(map[string]struct{}, len(candidates))
	nonCandidates := make([]TaskEvent, 0, len(r.events))
	for _, event := range r.events {
		if _, candidate := candidateIDs[event.EventID]; candidate {
			retainedCandidates[event.EventID] = struct{}{}
			retainedStreams[streamKey{event.TaskID, event.GenerationID}] = struct{}{}
		} else {
			nonCandidates = append(nonCandidates, event)
		}
	}
	floorByStream := make(map[streamKey]TaskEvent)
	for _, event := range candidates {
		key := streamKey{event.TaskID, event.GenerationID}
		if _, retained := retainedStreams[key]; retained {
			continue
		}
		record, exists := r.records[event.TaskID]
		if exists && record.GenerationID == event.GenerationID {
			floorByStream[key] = event
		}
	}
	mutationEvents := make([]TaskEvent, 0, len(candidates))
	for _, event := range candidates {
		key := streamKey{event.TaskID, event.GenerationID}
		floor, isFloor := floorByStream[key]
		_, retained := retainedCandidates[event.EventID]
		if retained || isFloor && floor.EventID == event.EventID {
			mutationEvents = append(mutationEvents, event)
		}
	}
	r.events = append(nonCandidates, mutationEvents...)
}

func (r *Registry) pruneSnapshotBytesLocked() bool {
	if r == nil || r.options.MaxSnapshotBytes <= 0 || r.snapshotSizeLocked() <= r.options.MaxSnapshotBytes {
		return false
	}
	changed := false
	if r.trimEventsForSnapshotBudgetLocked() {
		changed = true
	}
	if r.snapshotSizeLocked() <= r.options.MaxSnapshotBytes {
		return changed
	}
	candidates := make([]Record, 0, len(r.records))
	for _, rec := range r.records {
		if canPruneRecord(rec) {
			candidates = append(candidates, rec)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		return recordReferenceAt(candidates[i]) < recordReferenceAt(candidates[j])
	})
	for _, rec := range candidates {
		if r.snapshotSizeLocked() <= r.options.MaxSnapshotBytes {
			break
		}
		delete(r.records, rec.TaskID)
		r.pruneEventsLocked()
		changed = true
	}
	return changed
}

func (r *Registry) trimEventsForSnapshotBudgetLocked() bool {
	if r == nil || len(r.events) == 0 || r.snapshotSizeLocked() <= r.options.MaxSnapshotBytes {
		return false
	}
	original := r.events
	low, high := 0, len(original)
	for low < high {
		mid := low + (high-low)/2
		r.events = original[mid:]
		if r.snapshotSizeLocked() <= r.options.MaxSnapshotBytes {
			high = mid
		} else {
			low = mid + 1
		}
	}
	r.events = original[low:]
	return low > 0
}

func (r *Registry) pruneEventsLocked() bool {
	if r == nil || len(r.events) == 0 {
		return false
	}
	changed := false
	kept := r.events[:0]
	for _, evt := range r.events {
		if _, ok := r.records[evt.TaskID]; ok {
			kept = append(kept, evt)
		} else {
			changed = true
		}
	}
	r.events = kept
	if r.options.MaxEvents > 0 && len(r.events) > r.options.MaxEvents {
		r.events = append([]TaskEvent(nil), r.events[len(r.events)-r.options.MaxEvents:]...)
		changed = true
	}
	return changed
}

func shouldPruneExpired(rec Record, now int64) bool {
	return canPruneRecord(rec) && rec.CleanupAfter > 0 && now >= rec.CleanupAfter
}

func canPruneRecord(rec Record) bool {
	return !rec.TraceCapturePending &&
		taskRecordIsRetentionTerminal(rec)
}

func taskRecordIsRetentionTerminal(rec Record) bool {
	return IsTraceCaptureTerminal(rec)
}

// IsTraceCaptureTerminal reports whether a task generation has reached a
// stable terminal state that can be projected and released from retention.
func IsTraceCaptureTerminal(rec Record) bool {
	if strings.TrimSpace(rec.InteractionID) != "" &&
		(rec.Status == StatusLost ||
			rec.DeliveryStatus == DeliveryFailed) {
		return false
	}
	return isTerminalStatus(rec.Status) &&
		isFinalDeliveryStatus(rec.DeliveryStatus)
}

func isTerminalStatus(status Status) bool {
	switch status {
	case StatusSucceeded, StatusFailed, StatusTimedOut, StatusCancelled, StatusLost:
		return true
	default:
		return false
	}
}

func isFinalDeliveryStatus(status DeliveryStatus) bool {
	switch status {
	case DeliveryDelivered, DeliverySessionQueued, DeliveryFailed, DeliveryParentMissing, DeliveryNotApplicable:
		return true
	default:
		return false
	}
}

func recordReferenceAt(rec Record) int64 {
	for _, value := range []int64{rec.EndedAt, rec.LastEventAt, rec.StartedAt, rec.CreatedAt} {
		if value > 0 {
			return value
		}
	}
	return 0
}

func (r *Registry) load() error {
	data, err := os.ReadFile(r.store)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var snap Snapshot
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&snap); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("task registry contains a trailing JSON value")
		}
		return fmt.Errorf("task registry contains trailing data: %w", err)
	}
	now := time.Now().UnixMilli()
	records := make(map[string]Record, len(snap.Tasks))
	events := make([]TaskEvent, 0, len(snap.Events))
	for _, rec := range snap.Tasks {
		if strings.TrimSpace(rec.TaskID) == "" {
			continue
		}
		if strings.TrimSpace(rec.GenerationID) == "" {
			return fmt.Errorf("task %q is missing generation_id", rec.TaskID)
		}
		if rec.LastEventSeq <= 0 {
			return fmt.Errorf("task %q has invalid last_event_sequence", rec.TaskID)
		}
		if err := validateTraceCaptureJournal(rec); err != nil {
			return fmt.Errorf("task %q trace capture journal: %w", rec.TaskID, err)
		}
		records[rec.TaskID] = r.normalizeRecord(rec, now)
	}
	for _, evt := range snap.Events {
		if strings.TrimSpace(evt.TaskID) == "" || evt.Type == "" {
			continue
		}
		if strings.TrimSpace(evt.GenerationID) == "" {
			return fmt.Errorf("task event %q is missing generation_id", evt.EventID)
		}
		if evt.SchemaVersion != TaskEventSchemaVersion {
			return fmt.Errorf(
				"task event %q has schema %q, want %q",
				evt.EventID, evt.SchemaVersion, TaskEventSchemaVersion,
			)
		}
		if rec, ok := records[evt.TaskID]; ok && rec.GenerationID == evt.GenerationID &&
			(evt.Seq <= 0 || evt.Seq > rec.LastEventSeq) {
			return fmt.Errorf("task event %q has invalid generation sequence", evt.EventID)
		}
		events = append(events, evt)
	}
	r.records = records
	r.events = events
	return nil
}

func validateTraceCaptureJournal(record Record) error {
	if record.TraceCaptureDropped < 0 {
		return errors.New("negative dropped event count")
	}
	if len(record.TraceCaptureEvents) == 0 {
		if record.TraceCaptureDropped != 0 {
			return errors.New("dropped event count without retained events")
		}
		return nil
	}
	var previousSeq int64
	for i, event := range record.TraceCaptureEvents {
		if event.TaskID != record.TaskID ||
			event.GenerationID != record.GenerationID {
			return fmt.Errorf("event %d belongs to another task generation", i)
		}
		if event.SchemaVersion != TaskEventSchemaVersion {
			return fmt.Errorf(
				"event %q has schema %q, want %q",
				event.EventID,
				event.SchemaVersion,
				TaskEventSchemaVersion,
			)
		}
		if event.Type == "" {
			return fmt.Errorf("event %d is missing type", i)
		}
		if event.Seq <= previousSeq || event.Seq > record.LastEventSeq {
			return fmt.Errorf("event %q has invalid generation sequence", event.EventID)
		}
		if i > 0 && event.Seq != previousSeq+1 {
			return fmt.Errorf("event %q is not contiguous", event.EventID)
		}
		expectedID := fmt.Sprintf(
			"%s:%s:%06d:%s",
			event.TaskID,
			event.GenerationID,
			event.Seq,
			event.Type,
		)
		if event.EventID != expectedID {
			return fmt.Errorf("event %q has invalid identity", event.EventID)
		}
		if event.Fingerprint != taskEventFingerprint(event) {
			return fmt.Errorf("event %q has invalid fingerprint", event.EventID)
		}
		previousSeq = event.Seq
	}
	firstSeq := record.TraceCaptureEvents[0].Seq
	if record.TraceCaptureDropped != int(firstSeq-1) {
		return fmt.Errorf(
			"dropped event count %d does not match first retained sequence %d",
			record.TraceCaptureDropped,
			firstSeq,
		)
	}
	if previousSeq != record.LastEventSeq {
		return fmt.Errorf(
			"journal ends at sequence %d, want %d",
			previousSeq,
			record.LastEventSeq,
		)
	}
	return nil
}

func (r *Registry) saveLocked() error {
	if err := r.writableErrorLocked(); err != nil {
		return err
	}
	if r.store == "" {
		return nil
	}
	data, err := json.MarshalIndent(r.snapshotLocked(), "", "  ")
	if err != nil {
		return err
	}
	writeAtomic := r.writeAtomic
	if writeAtomic == nil {
		writeAtomic = fileutil.WriteFileAtomic
	}
	err = writeAtomic(r.store, data, 0o600)
	if err == nil {
		r.unsyncedWrite = false
	} else if fileutil.IsCommittedWriteError(err) {
		r.unsyncedWrite = true
	}
	return err
}

func (r *Registry) writableErrorLocked() error {
	if r.lastLoad == nil {
		return nil
	}
	return fmt.Errorf("task registry is read-only after load failure: %w", r.lastLoad)
}

func (r *Registry) snapshotSizeLocked() int {
	data, err := json.MarshalIndent(r.snapshotLocked(), "", "  ")
	if err != nil {
		return 0
	}
	return len(data)
}

func (r *Registry) snapshotLocked() Snapshot {
	tasks := make([]Record, 0, len(r.records))
	for _, rec := range r.records {
		tasks = append(tasks, rec)
	}
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].CreatedAt != tasks[j].CreatedAt {
			return tasks[i].CreatedAt < tasks[j].CreatedAt
		}
		return tasks[i].TaskID < tasks[j].TaskID
	})
	events := append([]TaskEvent(nil), r.events...)
	return Snapshot{Tasks: tasks, Events: events}
}

func (r *Registry) captureStateLocked() registryState {
	records := make(map[string]Record, len(r.records))
	for id, record := range r.records {
		records[id] = cloneTaskRecord(record)
	}
	events := make([]TaskEvent, len(r.events))
	for i := range r.events {
		events[i] = cloneTaskEvent(r.events[i])
	}
	return registryState{records: records, events: events}
}

func (r *Registry) restoreStateLocked(state registryState) {
	r.records = state.records
	r.events = state.events
}

func (r *Registry) completeMutationLocked(
	saveErr error,
	rollback registryState,
	events []TaskEvent,
) (bool, []*eventObserverDelivery) {
	committed := saveErr == nil || fileutil.IsCommittedWriteError(saveErr)
	if !committed {
		r.restoreStateLocked(rollback)
	} else {
		events = r.retainedEventsLocked(events)
	}
	if saveErr == nil && r.traceCaptureProtectionPending {
		r.traceCaptureProtectionPending = false
	}
	return committed, r.queueCommittedNotificationsLocked(committed, events)
}

func (r *Registry) journalRetainedTraceRecordsLocked() bool {
	changed := false
	for taskID, record := range r.records {
		events, dropped := r.traceCaptureJournalLocked(record)
		pending := taskRecordIsRetentionTerminal(record)
		if record.TraceCapturePending == pending &&
			record.TraceCaptureDropped == dropped &&
			taskEventsEqual(record.TraceCaptureEvents, events) {
			continue
		}
		record.TraceCapturePending = pending
		record.TraceCaptureEvents = events
		record.TraceCaptureDropped = dropped
		r.records[taskID] = record
		changed = true
	}
	return changed
}

func (r *Registry) traceCaptureJournalLocked(
	record Record,
) ([]TaskEvent, int) {
	events := make([]TaskEvent, 0, len(record.TraceCaptureEvents))
	seen := make(map[string]struct{}, len(record.TraceCaptureEvents))
	for _, event := range record.TraceCaptureEvents {
		if !taskEventFingerprintValid(event) {
			continue
		}
		events = append(events, cloneTaskEvent(event))
		seen[event.EventID] = struct{}{}
	}
	for _, event := range r.events {
		if event.TaskID != record.TaskID ||
			event.GenerationID != record.GenerationID ||
			!taskEventFingerprintValid(event) {
			continue
		}
		if _, exists := seen[event.EventID]; exists {
			continue
		}
		events = append(events, cloneTaskEvent(event))
		seen[event.EventID] = struct{}{}
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].Seq != events[j].Seq {
			return events[i].Seq < events[j].Seq
		}
		return events[i].EventID < events[j].EventID
	})
	start := len(events)
	if start > 0 {
		start--
		for start > 0 &&
			events[start-1].Seq+1 == events[start].Seq {
			start--
		}
		events = append([]TaskEvent(nil), events[start:]...)
	}
	if limit := r.traceCaptureMaxEvents; limit > 0 &&
		len(events) > limit {
		events = append(
			[]TaskEvent(nil),
			events[len(events)-limit:]...,
		)
	}
	if len(events) == 0 {
		return nil, 0
	}
	return events, max(0, int(events[0].Seq-1))
}

func taskEventsEqual(left, right []TaskEvent) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].EventID != right[i].EventID ||
			left[i].Fingerprint != right[i].Fingerprint {
			return false
		}
	}
	return true
}

func (r *Registry) retainedEventsLocked(candidates []TaskEvent) []TaskEvent {
	if len(candidates) == 0 || len(r.events) == 0 {
		return nil
	}
	retainedIDs := make(map[string]struct{}, len(r.events))
	for _, event := range r.events {
		retainedIDs[event.EventID] = struct{}{}
	}
	retained := make([]TaskEvent, 0, len(candidates))
	for _, event := range candidates {
		if _, ok := retainedIDs[event.EventID]; ok {
			retained = append(retained, event)
		}
	}
	return retained
}

func (r *Registry) sortedRecordIDsLocked() []string {
	ids := make([]string, 0, len(r.records))
	for id := range r.records {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (r *Registry) appendUpdateEventsLocked(before, after Record, emittedAt int64) {
	if before.Status != after.Status {
		r.appendEventLocked(after, EventTaskStatusChanged, emittedAt, map[string]string{
			"from": string(before.Status),
			"to":   string(after.Status),
		})
	}
	if before.DeliveryStatus != after.DeliveryStatus {
		payload := map[string]string{
			"from": string(before.DeliveryStatus),
			"to":   string(after.DeliveryStatus),
		}
		if completionID := strings.TrimSpace(after.LastCompletionID); completionID != "" {
			payload["completion_id"] = completionID
		}
		r.appendEventLocked(after, EventTaskDeliveryChanged, emittedAt, payload)
	}
	if before.ProgressSummary != after.ProgressSummary && strings.TrimSpace(after.ProgressSummary) != "" {
		r.appendEventLocked(after, EventTaskProgress, emittedAt, map[string]string{
			"summary": after.ProgressSummary,
		})
	}
	if before.Status == after.Status &&
		before.DeliveryStatus == after.DeliveryStatus &&
		before.ProgressSummary == after.ProgressSummary &&
		recordChanged(before, after) {
		r.appendEventLocked(after, EventTaskUpdated, emittedAt, nil)
	}
}

func (r *Registry) appendEventLocked(rec Record, eventType EventType, emittedAt int64, payload map[string]string) {
	if r == nil || strings.TrimSpace(rec.TaskID) == "" || eventType == "" {
		return
	}
	if emittedAt == 0 {
		emittedAt = time.Now().UnixMilli()
	}
	stored, ok := r.records[rec.TaskID]
	if !ok || stored.GenerationID != rec.GenerationID {
		return
	}
	stored.LastEventSeq++
	r.records[rec.TaskID] = stored
	seq := stored.LastEventSeq
	evt := TaskEvent{
		SchemaVersion:  TaskEventSchemaVersion,
		TaskID:         rec.TaskID,
		GenerationID:   rec.GenerationID,
		Runtime:        rec.Runtime,
		ParentTaskID:   rec.ParentTaskID,
		Type:           eventType,
		Status:         rec.Status,
		DeliveryStatus: rec.DeliveryStatus,
		Seq:            seq,
		EmittedAt:      emittedAt,
		Source:         "task_registry",
		Producer:       firstNonEmpty(rec.AgentID, string(rec.Runtime)),
		Payload:        cleanPayload(payload),
	}
	evt.EventID = fmt.Sprintf("%s:%s:%06d:%s", rec.TaskID, rec.GenerationID, seq, eventType)
	evt.Fingerprint = taskEventFingerprint(evt)
	r.events = append(r.events, evt)
	if r.traceCaptureProtection || stored.TraceCapturePending {
		if r.traceCaptureProtection {
			stored.TraceCapturePending = taskRecordIsRetentionTerminal(stored)
		}
		count := len(stored.TraceCaptureEvents)
		if count == 0 ||
			stored.TraceCaptureEvents[count-1].Seq+1 != evt.Seq {
			stored.TraceCaptureEvents, stored.TraceCaptureDropped = r.traceCaptureJournalLocked(stored)
		} else {
			stored.TraceCaptureEvents = append(
				stored.TraceCaptureEvents,
				cloneTaskEvent(evt),
			)
			if limit := r.traceCaptureMaxEvents; limit > 0 &&
				len(stored.TraceCaptureEvents) > limit {
				dropped := len(stored.TraceCaptureEvents) - limit
				stored.TraceCaptureEvents = append(
					[]TaskEvent(nil),
					stored.TraceCaptureEvents[dropped:]...,
				)
			}
			stored.TraceCaptureDropped = max(
				0,
				int(stored.TraceCaptureEvents[0].Seq-1),
			)
		}
		r.records[rec.TaskID] = stored
	} else if len(stored.TraceCaptureEvents) > 0 ||
		stored.TraceCaptureDropped > 0 {
		stored.TraceCaptureEvents = nil
		stored.TraceCaptureDropped = 0
		r.records[rec.TaskID] = stored
	}
}

func taskEventFingerprint(evt TaskEvent) string {
	type immutableEvent struct {
		SchemaVersion  string            `json:"schema_version"`
		EventID        string            `json:"event_id"`
		TaskID         string            `json:"task_id"`
		GenerationID   string            `json:"generation_id"`
		Runtime        Runtime           `json:"runtime"`
		ParentTaskID   string            `json:"parent_task_id"`
		Type           EventType         `json:"type"`
		Status         Status            `json:"status"`
		DeliveryStatus DeliveryStatus    `json:"delivery_status"`
		Seq            int64             `json:"seq"`
		EmittedAt      int64             `json:"emitted_at"`
		Source         string            `json:"source"`
		Producer       string            `json:"producer"`
		Payload        map[string]string `json:"payload"`
	}
	payload, _ := json.Marshal(immutableEvent{
		SchemaVersion: evt.SchemaVersion, EventID: evt.EventID,
		TaskID: evt.TaskID, GenerationID: evt.GenerationID,
		Runtime: evt.Runtime, ParentTaskID: evt.ParentTaskID,
		Type: evt.Type, Status: evt.Status,
		DeliveryStatus: evt.DeliveryStatus, Seq: evt.Seq,
		EmittedAt: evt.EmittedAt, Source: evt.Source,
		Producer: evt.Producer, Payload: evt.Payload,
	})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func taskEventFingerprintValid(event TaskEvent) bool {
	return event.Fingerprint != "" &&
		event.Fingerprint == taskEventFingerprint(event)
}

func normalizeDeliverablePayload(payload *DeliverablePayload, generatedAt int64) *DeliverablePayload {
	if payload == nil {
		return nil
	}
	out := *payload
	out.Artifacts = append([]DeliverableItem(nil), payload.Artifacts...)
	out.Metadata = copyStringMap(payload.Metadata)
	if payload.Report != nil {
		report := cloneDeliverableReport(payload.Report)
		if report.SchemaVersion == "" {
			report.SchemaVersion = DeliverableReportV1
		}
		if report.GeneratedAt == 0 {
			report.GeneratedAt = generatedAt
		}
		if report.ContentHash == "" {
			report.ContentHash = deliverableContentHash(&out)
		}
		if report.ReportID == "" {
			report.ReportID = "deliverable:" + report.ContentHash
		}
		out.Report = report
		return &out
	}
	if strings.TrimSpace(out.Text) == "" && len(out.Artifacts) == 0 && len(out.Metadata) == 0 {
		return &out
	}
	contentHash := deliverableContentHash(&out)
	report := &DeliverableReport{
		SchemaVersion: DeliverableReportV1,
		ReportID:      "deliverable:" + contentHash,
		ContentHash:   contentHash,
		GeneratedAt:   generatedAt,
		Summary:       strings.TrimSpace(out.Text),
		Metadata:      copyStringMap(out.Metadata),
		Provenance: map[string]string{
			"source":     "task_registry_projection",
			"projection": "deliverable_payload",
		},
	}
	if summary := strings.TrimSpace(out.Text); summary != "" {
		report.Claims = append(report.Claims, ReportClaim{
			Kind:       "fact",
			Text:       summary,
			Confidence: "producer_reported",
		})
	}
	out.Report = report
	return &out
}

func deliverableContentHash(payload *DeliverablePayload) string {
	if payload == nil {
		return ""
	}
	type hashPayload struct {
		Text      string            `json:"text,omitempty"`
		Artifacts []DeliverableItem `json:"artifacts,omitempty"`
		Metadata  map[string]string `json:"metadata,omitempty"`
	}
	data, _ := json.Marshal(hashPayload{
		Text:      strings.TrimSpace(payload.Text),
		Artifacts: append([]DeliverableItem(nil), payload.Artifacts...),
		Metadata:  copyStringMap(payload.Metadata),
	})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func cloneDeliverableReport(report *DeliverableReport) *DeliverableReport {
	if report == nil {
		return nil
	}
	cloned := &DeliverableReport{
		SchemaVersion: report.SchemaVersion,
		ReportID:      report.ReportID,
		ContentHash:   report.ContentHash,
		GeneratedAt:   report.GeneratedAt,
		Summary:       report.Summary,
		Provenance:    copyStringMap(report.Provenance),
		Metadata:      copyStringMap(report.Metadata),
		Extra:         copyAnyMap(report.Extra),
	}
	for _, claim := range report.Claims {
		cloned.Claims = append(cloned.Claims, ReportClaim{
			Kind:       claim.Kind,
			Text:       claim.Text,
			Confidence: claim.Confidence,
			SourceRefs: append([]string(nil), claim.SourceRefs...),
			Metadata:   copyStringMap(claim.Metadata),
		})
	}
	cloned.FieldDeltas = append([]ReportFieldDelta(nil), report.FieldDeltas...)
	return cloned
}

func recordChanged(before, after Record) bool {
	b, _ := json.Marshal(before)
	a, _ := json.Marshal(after)
	return string(b) != string(a)
}

func cleanPayload(payload map[string]string) map[string]string {
	if len(payload) == 0 {
		return nil
	}
	out := make(map[string]string, len(payload))
	for key, value := range payload {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key != "" && value != "" {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func copyStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func copyAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out, err := canonicalAnyMap(in)
	if err == nil {
		return out
	}
	// Public mutations reject invalid Extra values before accepting state.
	// Keep the outer map detached while the invalid candidate is validated.
	out = make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func canonicalizeRecordExtra(record *Record) error {
	if record == nil || record.Deliverable == nil || record.Deliverable.Report == nil {
		return nil
	}
	extra, err := canonicalAnyMap(record.Deliverable.Report.Extra)
	if err != nil {
		return err
	}
	record.Deliverable.Report.Extra = extra
	return nil
}

func canonicalAnyMap(in map[string]any) (map[string]any, error) {
	if len(in) == 0 {
		return nil, nil
	}
	data, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}
