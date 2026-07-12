package agent

import "time"

// TurnEndStatus describes the terminal state of a turn.
type TurnEndStatus string

const (
	// TurnEndStatusCompleted indicates the turn finished normally.
	TurnEndStatusCompleted TurnEndStatus = "completed"
	// TurnEndStatusError indicates the turn ended because of an error.
	TurnEndStatusError TurnEndStatus = "error"
	// TurnEndStatusAborted indicates the turn was hard-aborted and rolled back.
	TurnEndStatusAborted TurnEndStatus = "aborted"
)

// TurnStartPayload describes the start of a turn.
type TurnStartPayload struct {
	UserMessage string
	MediaCount  int
	Workspace   string
}

type LLMFallbackAttemptPayload struct {
	Provider    string
	Model       string
	IdentityKey string
	Attempt     int
	Status      string
	Reason      string
	ErrorCode   string
	Skipped     bool
}

type EvolutionTransitionPayload struct {
	Workspace     string
	RecordID      string
	DraftID       string
	SkillName     string
	Action        string
	Status        string
	Success       *bool
	ProvenanceIDs []string
	PolicyCodes   []string
}

const (
	skillContextTriggerInitialBuild        = "initial_build"
	skillContextTriggerContextRetryRebuild = "context_retry_rebuild"
)

type SkillContextSnapshot struct {
	Sequence   int      `json:"sequence"`
	Trigger    string   `json:"trigger"`
	SkillNames []string `json:"skill_names,omitempty"`
}

type ToolExecutionRecord struct {
	Name         string   `json:"name"`
	Success      bool     `json:"success"`
	ErrorSummary string   `json:"error_summary,omitempty"`
	SkillNames   []string `json:"skill_names,omitempty"`
}

// TurnEndPayload describes the completion of a turn.
type TurnEndPayload struct {
	Status                TurnEndStatus
	Workspace             string
	DeliveryExpected      bool
	Iterations            int
	Duration              time.Duration
	LLMCalls              int
	PromptTokens          int
	CompletionTokens      int
	TotalTokens           int
	FinalContentLen       int
	UserMessage           string
	FinalContent          string
	ActiveSkills          []string
	AttemptedSkills       []string
	FinalSuccessfulPath   []string
	SkillContextSnapshots []SkillContextSnapshot
	ToolKinds             []string
	ToolExecutions        []ToolExecutionRecord
}

// LLMRequestPayload describes an outbound LLM request.
type LLMRequestPayload struct {
	Provider      string
	Model         string
	PromptHash    string
	MessagesCount int
	ToolsCount    int
	MaxTokens     int
	Temperature   float64
}

// LLMResponsePayload describes an inbound LLM response.
type LLMResponsePayload struct {
	ResponseHash     string
	ContentLen       int
	ToolCalls        int
	HasReasoning     bool
	PromptTokens     int
	CompletionTokens int
	TotalTokens      int
	HasProviderUsage bool
}

// LLMDeltaPayload describes a streamed LLM delta.
type LLMDeltaPayload struct {
	ContentDeltaLen   int
	ReasoningDeltaLen int
}

// LLMRetryPayload describes a retry of an LLM request.
type LLMRetryPayload struct {
	Attempt    int
	MaxRetries int
	Reason     string
	Error      string
	Backoff    time.Duration
}

// ContextCompressReason identifies why emergency compression ran.
type ContextCompressReason string

const (
	// ContextCompressReasonProactive indicates compression before the first LLM call.
	ContextCompressReasonProactive ContextCompressReason = "proactive_budget"
	// ContextCompressReasonRetry indicates compression during context-error retry handling.
	ContextCompressReasonRetry ContextCompressReason = "llm_retry"
	// ContextCompressReasonSummarize indicates post-turn async summarization.
	ContextCompressReasonSummarize ContextCompressReason = "summarize"
)

// ContextCompressPayload describes a forced history compression.
type ContextCompressPayload struct {
	Reason            ContextCompressReason
	DroppedMessages   int
	RemainingMessages int
}

type ContextSnapshotPayload struct {
	MessageCount     int
	SnapshotHash     string
	GoalHash         string
	SteeringCount    int
	ToolPairingValid bool
}

// SessionSummarizePayload describes a completed async session summarization.
type SessionSummarizePayload struct {
	SummarizedMessages int
	KeptMessages       int
	SummaryLen         int
	OmittedOversized   bool
}

// ToolExecStartPayload describes a tool execution request.
type ToolExecStartPayload struct {
	ToolCallID string
	Tool       string
	Arguments  map[string]any
}

// ToolExecEndPayload describes the outcome of a tool execution.
type ToolExecEndPayload struct {
	ToolCallID string
	Tool       string
	Duration   time.Duration
	ForLLMLen  int
	ForUserLen int
	IsError    bool
	Async      bool
	ResultHash string
}

// ToolExecSkippedPayload describes a skipped tool call.
type ToolExecSkippedPayload struct {
	ToolCallID string
	Tool       string
	Reason     string
}

// ToolLoopDecisionPayload contains only hash-safe loop protection metadata.
type ToolLoopDecisionPayload struct {
	Tool      string
	ArgsHash  string
	Action    string
	Code      string
	Count     int
	Threshold int
}

// SteeringInjectedPayload describes steering messages appended before the next LLM call.
type SteeringInjectedPayload struct {
	Count           int
	TotalContentLen int
}

// FollowUpQueuedPayload describes an async follow-up queued back into the inbound bus.
type FollowUpQueuedPayload struct {
	SourceTool string
	ContentLen int
}

// AsyncCompletionPayload describes a typed async tool completion event before
// the runtime applies user/parent delivery policy.
type AsyncCompletionPayload struct {
	SourceTool   string
	CompletionID string
	TaskID       string
	DeliveryMode string
	ContentLen   int
	ForUserLen   int
	MediaCount   int
	IsError      bool
	WillUser     bool
	WillParent   bool
}

type InterruptKind string

const (
	InterruptKindSteering InterruptKind = "steering"
	InterruptKindGraceful InterruptKind = "graceful"
	InterruptKindHard     InterruptKind = "hard_abort"
)

// InterruptReceivedPayload describes accepted turn-control input.
type InterruptReceivedPayload struct {
	Kind        InterruptKind
	Role        string
	ContentLen  int
	QueueDepth  int
	HintLen     int
	MessageHash string
}

// SubTurnSpawnPayload describes the creation of a child turn.
type SubTurnSpawnPayload struct {
	AgentID      string
	Label        string
	ParentTurnID string
}

// SubTurnEndPayload describes the completion of a child turn.
type SubTurnEndPayload struct {
	AgentID string
	Status  string
}

// SubTurnResultDeliveredPayload describes delivery of a sub-turn result.
type SubTurnResultDeliveredPayload struct {
	TargetChannel string
	TargetChatID  string
	ContentLen    int
}

// SubTurnOrphanPayload describes a sub-turn result that could not be delivered.
type SubTurnOrphanPayload struct {
	ParentTurnID string
	ChildTurnID  string
	Reason       string
}

// ErrorPayload describes an execution error inside the agent loop.
type ErrorPayload struct {
	Stage   string
	Message string
}
