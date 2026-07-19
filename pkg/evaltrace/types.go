// Package evaltrace defines the safe, versioned evidence contract used by
// ForgeClaw replay and evaluation.
package evaltrace

import (
	"encoding/json"
	"time"
)

const SchemaVersionV1 = "forgeclaw.eval_trace.v1"

type ContentMode string

const (
	ContentMetadataOnly ContentMode = "metadata_only"
	ContentRedacted     ContentMode = "redacted_content"
	ContentFixture      ContentMode = "fixture"
)

type RecordKind string

const (
	RecordTurnStart              RecordKind = "turn.start"
	RecordTurnEnd                RecordKind = "turn.end"
	RecordFinalOutcome           RecordKind = "outcome.final"
	RecordModelRequest           RecordKind = "model.request"
	RecordModelResponse          RecordKind = "model.response"
	RecordModelRetry             RecordKind = "model.retry"
	RecordModelFallbackAttempt   RecordKind = "model.fallback_attempt"
	RecordToolCall               RecordKind = "tool.call"
	RecordToolResult             RecordKind = "tool.result"
	RecordToolSkipped            RecordKind = "tool.skipped"
	RecordToolLoopDecision       RecordKind = "tool.loop_decision"
	RecordToolSteeringDecision   RecordKind = "tool.steering_decision"
	RecordSteeringEnqueued       RecordKind = "steering.enqueued"
	RecordSteeringInjected       RecordKind = "steering.injected"
	RecordInterrupt              RecordKind = "steering.interrupt"
	RecordTaskTransition         RecordKind = "task.transition"
	RecordInteractionTransition  RecordKind = "interaction.transition"
	RecordDeliveryDecision       RecordKind = "delivery.decision"
	RecordDeliveryAttempt        RecordKind = "delivery.attempt"
	RecordDeliveryOutcome        RecordKind = "delivery.outcome"
	RecordContextCompaction      RecordKind = "context.compaction"
	RecordContextReconciliation  RecordKind = "context.reconciliation"
	RecordContextSnapshot        RecordKind = "context.snapshot"
	RecordRestartBoundary        RecordKind = "runtime.restart"
	RecordInboundSpoolTransition RecordKind = "inbound_spool.transition"
	RecordUserCorrection         RecordKind = "user.correction"
)

type Trace struct {
	SchemaVersion string        `json:"schema_version"`
	TraceID       string        `json:"trace_id"`
	CreatedAt     time.Time     `json:"created_at"`
	Source        Source        `json:"source"`
	Policy        CapturePolicy `json:"policy"`
	Limits        AppliedLimits `json:"limits"`
	Metadata      Metadata      `json:"metadata,omitempty"`
	Records       []Record      `json:"records"`
	Outcome       *Outcome      `json:"outcome,omitempty"`
	Corrections   []Correction  `json:"corrections,omitempty"`
	Truncation    Truncation    `json:"truncation,omitempty"`
}

type Source struct {
	ForgeClawVersion string `json:"forgeclaw_version,omitempty"`
	Commit           string `json:"commit,omitempty"`
	FixtureID        string `json:"fixture_id,omitempty"`
	FixtureSource    string `json:"fixture_source,omitempty"`
}

type CapturePolicy struct {
	ContentMode ContentMode `json:"content_mode"`
	Redactor    string      `json:"redactor,omitempty"`
}

type AppliedLimits struct {
	MaxTraceBytes  int `json:"max_trace_bytes"`
	MaxRecords     int `json:"max_records"`
	MaxRecordBytes int `json:"max_record_bytes"`
	MaxCorrections int `json:"max_corrections"`
}

type Metadata struct {
	RootTurnID  string `json:"root_turn_id,omitempty"`
	SessionHash string `json:"session_hash,omitempty"`
	AgentID     string `json:"agent_id,omitempty"`
	RuntimeID   string `json:"runtime_id,omitempty"`
}

type Record struct {
	Sequence    uint64          `json:"sequence"`
	OffsetNanos int64           `json:"offset_nanos"`
	Kind        RecordKind      `json:"kind"`
	Origin      Origin          `json:"origin"`
	Scope       Scope           `json:"scope,omitempty"`
	Correlation Correlation     `json:"correlation,omitempty"`
	Digest      string          `json:"digest"`
	Data        json.RawMessage `json:"data,omitempty"`
}

type Origin struct {
	Kind string `json:"kind"`
	ID   string `json:"id,omitempty"`
}

type Scope struct {
	AgentID     string `json:"agent_id,omitempty"`
	SessionHash string `json:"session_hash,omitempty"`
	TurnID      string `json:"turn_id,omitempty"`
	TaskID      string `json:"task_id,omitempty"`
	Channel     string `json:"channel,omitempty"`
	TargetHash  string `json:"target_hash,omitempty"`
}

type Correlation struct {
	ParentTurnID  string `json:"parent_turn_id,omitempty"`
	RequestID     string `json:"request_id,omitempty"`
	InteractionID string `json:"interaction_id,omitempty"`
	ToolCallID    string `json:"tool_call_id,omitempty"`
	CompletionID  string `json:"completion_id,omitempty"`
	EventID       string `json:"event_id,omitempty"`
}

type Outcome struct {
	Status      string `json:"status"`
	ContentHash string `json:"content_hash,omitempty"`
	ContentLen  int    `json:"content_len,omitempty"`
	ErrorCode   string `json:"error_code,omitempty"`
}

type Correction struct {
	CorrectionID string   `json:"correction_id"`
	RecordRefs   []uint64 `json:"record_refs,omitempty"`
	Category     string   `json:"category,omitempty"`
	Note         string   `json:"note,omitempty"`
}

type Truncation struct {
	Incomplete     bool               `json:"incomplete,omitempty"`
	DroppedRecords int                `json:"dropped_records,omitempty"`
	Reasons        []string           `json:"reasons,omitempty"`
	DroppedByKind  map[RecordKind]int `json:"dropped_by_kind,omitempty"`
}
