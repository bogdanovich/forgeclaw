package diagnostictrace

// Payload structs intentionally contain only normalized evidence. Capture
// adapters must project source-specific values into these fields instead of
// storing arbitrary runtime payloads.

type TurnPayload struct {
	Status       string `json:"status,omitempty"`
	InputHash    string `json:"input_hash,omitempty"`
	InputLen     int    `json:"input_len,omitempty"`
	FinalHash    string `json:"final_hash,omitempty"`
	FinalLen     int    `json:"final_len,omitempty"`
	Iterations   int    `json:"iterations,omitempty"`
	InputPreview string `json:"input_preview,omitempty"`
	FinalPreview string `json:"final_preview,omitempty"`
}

type ModelPayload struct {
	Provider         string `json:"provider,omitempty"`
	Model            string `json:"model,omitempty"`
	IdentityKey      string `json:"identity_key,omitempty"`
	Attempt          int    `json:"attempt,omitempty"`
	Status           string `json:"status,omitempty"`
	Reason           string `json:"reason,omitempty"`
	Messages         int    `json:"messages,omitempty"`
	Tools            int    `json:"tools,omitempty"`
	PromptHash       string `json:"prompt_hash,omitempty"`
	ResponseHash     string `json:"response_hash,omitempty"`
	PromptTokens     int    `json:"prompt_tokens,omitempty"`
	ResponseTokens   int    `json:"response_tokens,omitempty"`
	Skipped          bool   `json:"skipped,omitempty"`
	ErrorCode        string `json:"error_code,omitempty"`
	MessagesPreview  string `json:"messages_preview,omitempty"`
	ResponsePreview  string `json:"response_preview,omitempty"`
	ReasoningPreview string `json:"reasoning_preview,omitempty"`
	ToolCallsPreview string `json:"tool_calls_preview,omitempty"`
	ErrorPreview     string `json:"error_preview,omitempty"`
}

type ToolPayload struct {
	Tool             string `json:"tool"`
	ArgsHash         string `json:"args_hash,omitempty"`
	ResultHash       string `json:"result_hash,omitempty"`
	Status           string `json:"status,omitempty"`
	Executed         bool   `json:"executed,omitempty"`
	IsError          bool   `json:"is_error,omitempty"`
	Action           string `json:"action,omitempty"`
	DecisionCode     string `json:"decision_code,omitempty"`
	Count            int    `json:"count,omitempty"`
	Threshold        int    `json:"threshold,omitempty"`
	Classification   string `json:"classification,omitempty"`
	Cause            string `json:"cause,omitempty"`
	ArgumentsPreview string `json:"arguments_preview,omitempty"`
	ResultPreview    string `json:"result_preview,omitempty"`
}

type SteeringPayload struct {
	Status         string `json:"status,omitempty"`
	Role           string `json:"role,omitempty"`
	MessageHash    string `json:"message_hash,omitempty"`
	ContentLen     int    `json:"content_len,omitempty"`
	Count          int    `json:"count,omitempty"`
	QueueDepth     int    `json:"queue_depth,omitempty"`
	ContentPreview string `json:"content_preview,omitempty"`
}

type DeliveryPayload struct {
	Mode         string `json:"mode,omitempty"`
	Status       string `json:"status,omitempty"`
	TargetHash   string `json:"target_hash,omitempty"`
	Attempt      int    `json:"attempt,omitempty"`
	WillUser     bool   `json:"will_user,omitempty"`
	WillParent   bool   `json:"will_parent,omitempty"`
	ErrorCode    string `json:"error_code,omitempty"`
	ContentLen   int    `json:"content_len,omitempty"`
	ErrorPreview string `json:"error_preview,omitempty"`
}

type ContextPayload struct {
	Reason            string   `json:"reason,omitempty"`
	BeforeMessages    int      `json:"before_messages,omitempty"`
	AfterMessages     int      `json:"after_messages,omitempty"`
	TokensSaved       int      `json:"tokens_saved,omitempty"`
	SnapshotHash      string   `json:"snapshot_hash,omitempty"`
	ProtectedFactRefs []string `json:"protected_fact_refs,omitempty"`
}

type RuntimeErrorPayload struct {
	Stage          string `json:"stage,omitempty"`
	MessagePreview string `json:"message_preview,omitempty"`
}
