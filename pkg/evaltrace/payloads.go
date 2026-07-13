package evaltrace

// Payload structs intentionally contain only normalized evidence. Capture
// adapters must project source-specific values into these fields instead of
// storing arbitrary runtime payloads.

type TurnPayload struct {
	Status     string `json:"status,omitempty"`
	InputHash  string `json:"input_hash,omitempty"`
	InputLen   int    `json:"input_len,omitempty"`
	FinalHash  string `json:"final_hash,omitempty"`
	FinalLen   int    `json:"final_len,omitempty"`
	Iterations int    `json:"iterations,omitempty"`
	ErrorCode  string `json:"error_code,omitempty"`
}

type ModelPayload struct {
	Provider       string `json:"provider,omitempty"`
	Model          string `json:"model,omitempty"`
	IdentityKey    string `json:"identity_key,omitempty"`
	Attempt        int    `json:"attempt,omitempty"`
	Status         string `json:"status,omitempty"`
	Reason         string `json:"reason,omitempty"`
	Messages       int    `json:"messages,omitempty"`
	Tools          int    `json:"tools,omitempty"`
	PromptHash     string `json:"prompt_hash,omitempty"`
	ResponseHash   string `json:"response_hash,omitempty"`
	PromptTokens   int    `json:"prompt_tokens,omitempty"`
	ResponseTokens int    `json:"response_tokens,omitempty"`
	Skipped        bool   `json:"skipped,omitempty"`
	ErrorCode      string `json:"error_code,omitempty"`
}

type ToolPayload struct {
	Tool           string `json:"tool"`
	ArgsHash       string `json:"args_hash,omitempty"`
	ResultHash     string `json:"result_hash,omitempty"`
	Status         string `json:"status,omitempty"`
	Executed       bool   `json:"executed,omitempty"`
	IsError        bool   `json:"is_error,omitempty"`
	Action         string `json:"action,omitempty"`
	DecisionCode   string `json:"decision_code,omitempty"`
	Count          int    `json:"count,omitempty"`
	Threshold      int    `json:"threshold,omitempty"`
	Classification string `json:"classification,omitempty"`
	Cause          string `json:"cause,omitempty"`
}

type SteeringPayload struct {
	Status      string `json:"status,omitempty"`
	Role        string `json:"role,omitempty"`
	MessageHash string `json:"message_hash,omitempty"`
	ContentLen  int    `json:"content_len,omitempty"`
	Count       int    `json:"count,omitempty"`
	QueueDepth  int    `json:"queue_depth,omitempty"`
	ScopeHash   string `json:"scope_hash,omitempty"`
}

type TaskPayload struct {
	EventType      string `json:"event_type"`
	Runtime        string `json:"runtime,omitempty"`
	Status         string `json:"status,omitempty"`
	DeliveryStatus string `json:"delivery_status,omitempty"`
	Sequence       int64  `json:"sequence,omitempty"`
	Fingerprint    string `json:"fingerprint,omitempty"`
	Producer       string `json:"producer,omitempty"`
}

type DeliveryPayload struct {
	Mode        string `json:"mode,omitempty"`
	Status      string `json:"status,omitempty"`
	TargetHash  string `json:"target_hash,omitempty"`
	Attempt     int    `json:"attempt,omitempty"`
	WillUser    bool   `json:"will_user,omitempty"`
	WillParent  bool   `json:"will_parent,omitempty"`
	ErrorCode   string `json:"error_code,omitempty"`
	ContentHash string `json:"content_hash,omitempty"`
	ContentLen  int    `json:"content_len,omitempty"`
}

type ContextPayload struct {
	Reason            string   `json:"reason,omitempty"`
	BeforeMessages    int      `json:"before_messages,omitempty"`
	AfterMessages     int      `json:"after_messages,omitempty"`
	BeforeTokens      int      `json:"before_tokens,omitempty"`
	AfterTokens       int      `json:"after_tokens,omitempty"`
	Revision          int64    `json:"revision,omitempty"`
	SnapshotHash      string   `json:"snapshot_hash,omitempty"`
	ProtectedFactRefs []string `json:"protected_fact_refs,omitempty"`
}

type RestartPayload struct {
	Phase     string `json:"phase"`
	SpoolID   string `json:"spool_id,omitempty"`
	Status    string `json:"status,omitempty"`
	Reason    string `json:"reason,omitempty"`
	StateHash string `json:"state_hash,omitempty"`
}

type EvolutionPayload struct {
	RecordID      string   `json:"record_id,omitempty"`
	DraftID       string   `json:"draft_id,omitempty"`
	SkillName     string   `json:"skill_name,omitempty"`
	Action        string   `json:"action,omitempty"`
	Status        string   `json:"status,omitempty"`
	Eligible      bool     `json:"eligible,omitempty"`
	Success       *bool    `json:"success,omitempty"`
	ProvenanceIDs []string `json:"provenance_ids,omitempty"`
	PolicyCodes   []string `json:"policy_codes,omitempty"`
}
