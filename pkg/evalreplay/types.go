// Package evalreplay deterministically projects normalized evaluation traces.
// It never constructs production providers, tools, channels, or state stores.
package evalreplay

import (
	"encoding/json"

	"github.com/sipeed/picoclaw/pkg/evaltrace"
)

const ReducerVersionV1 = "forgeclaw.eval_replay.reducer.v1"

type Severity string

const (
	SeverityInfo  Severity = "info"
	SeverityWarn  Severity = "warn"
	SeverityError Severity = "error"
)

type Diagnostic struct {
	Code       string   `json:"code"`
	Severity   Severity `json:"severity"`
	Sequence   uint64   `json:"sequence,omitempty"`
	RecordKind string   `json:"record_kind,omitempty"`
	Message    string   `json:"message"`
}

type Projection struct {
	ReducerVersion string                        `json:"reducer_version"`
	TraceID        string                        `json:"trace_id"`
	Processed      int                           `json:"processed"`
	Terminal       bool                          `json:"terminal"`
	Tasks          map[string]TaskProjection     `json:"tasks,omitempty"`
	Deliveries     map[string]DeliveryProjection `json:"deliveries,omitempty"`
	Steering       SteeringProjection            `json:"steering,omitempty"`
	Context        ContextProjection             `json:"context,omitempty"`
	Providers      ProviderProjection            `json:"providers,omitempty"`
	Tools          map[string]ToolProjection     `json:"tools,omitempty"`
	ToolLoop       ToolLoopProjection            `json:"tool_loop,omitempty"`
	Evolution      EvolutionProjection           `json:"evolution,omitempty"`
	Restarts       []RestartProjection           `json:"restarts,omitempty"`
	Corrections    []CorrectionProjection        `json:"corrections,omitempty"`
	Diagnostics    []Diagnostic                  `json:"diagnostics,omitempty"`
}

type TaskProjection struct {
	TaskID         string `json:"task_id"`
	Status         string `json:"status,omitempty"`
	DeliveryStatus string `json:"delivery_status,omitempty"`
	LastSequence   int64  `json:"last_sequence,omitempty"`
	Terminal       bool   `json:"terminal"`
}

type DeliveryProjection struct {
	Key          string `json:"key"`
	Mode         string `json:"mode,omitempty"`
	TargetHash   string `json:"target_hash,omitempty"`
	CompletionID string `json:"completion_id,omitempty"`
	Attempts     int    `json:"attempts,omitempty"`
	Terminal     string `json:"terminal,omitempty"`
	WillUser     bool   `json:"will_user,omitempty"`
	WillParent   bool   `json:"will_parent,omitempty"`
}

type SteeringProjection struct {
	Enqueued   int            `json:"enqueued,omitempty"`
	Injected   int            `json:"injected,omitempty"`
	Pending    int            `json:"pending,omitempty"`
	Interrupts int            `json:"interrupts,omitempty"`
	Messages   map[string]int `json:"messages,omitempty"`
}

type ToolLoopProjection struct {
	Decisions []ToolLoopDecision `json:"decisions,omitempty"`
}

type ToolLoopDecision struct {
	Sequence  uint64 `json:"sequence"`
	Tool      string `json:"tool,omitempty"`
	Action    string `json:"action,omitempty"`
	Code      string `json:"code,omitempty"`
	Count     int    `json:"count,omitempty"`
	Threshold int    `json:"threshold,omitempty"`
}

type ContextProjection struct {
	Compactions       int      `json:"compactions,omitempty"`
	Reconciliations   int      `json:"reconciliations,omitempty"`
	LastSnapshotHash  string   `json:"last_snapshot_hash,omitempty"`
	AfterMessages     int      `json:"after_messages,omitempty"`
	ProtectedFactRefs []string `json:"protected_fact_refs,omitempty"`
}

type ProviderProjection struct {
	Requests         int      `json:"requests,omitempty"`
	Responses        int      `json:"responses,omitempty"`
	Retries          int      `json:"retries,omitempty"`
	FallbackAttempts int      `json:"fallback_attempts,omitempty"`
	SelectedIdentity string   `json:"selected_identity,omitempty"`
	Attempted        []string `json:"attempted,omitempty"`
}

type ToolProjection struct {
	ToolCallID   string `json:"tool_call_id"`
	Tool         string `json:"tool,omitempty"`
	Called       bool   `json:"called,omitempty"`
	Executed     bool   `json:"executed,omitempty"`
	Result       bool   `json:"result,omitempty"`
	Skipped      bool   `json:"skipped,omitempty"`
	DecisionCode string `json:"decision_code,omitempty"`
}

type EvolutionProjection struct {
	Records   int      `json:"records,omitempty"`
	Drafts    int      `json:"drafts,omitempty"`
	Reviews   int      `json:"reviews,omitempty"`
	Applies   int      `json:"applies,omitempty"`
	Rollbacks int      `json:"rollbacks,omitempty"`
	Profiles  int      `json:"profiles,omitempty"`
	DraftIDs  []string `json:"draft_ids,omitempty"`
}

type RestartProjection struct {
	Sequence  uint64 `json:"sequence"`
	Phase     string `json:"phase"`
	Status    string `json:"status,omitempty"`
	StateHash string `json:"state_hash,omitempty"`
}

type CorrectionProjection struct {
	CorrectionID string   `json:"correction_id"`
	Sequence     uint64   `json:"sequence,omitempty"`
	RecordRefs   []uint64 `json:"record_refs,omitempty"`
	Category     string   `json:"category,omitempty"`
}

type Result struct {
	Projection Projection      `json:"projection"`
	Canonical  json.RawMessage `json:"canonical"`
}

type reducer struct {
	projection   Projection
	origins      map[string]string
	turnStarted  bool
	turnEnded    bool
	lastSequence uint64
	lastOffset   int64
	corrections  map[string]struct{}
}

func newReducer(trace evaltrace.Trace) *reducer {
	return &reducer{
		projection: Projection{
			ReducerVersion: ReducerVersionV1,
			TraceID:        trace.TraceID,
			Tasks:          make(map[string]TaskProjection),
			Deliveries:     make(map[string]DeliveryProjection),
			Steering:       SteeringProjection{Messages: make(map[string]int)},
			Tools:          make(map[string]ToolProjection),
		},
		origins:     make(map[string]string),
		corrections: make(map[string]struct{}),
	}
}
