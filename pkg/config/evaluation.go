package config

const (
	DefaultEvaluationTraceMaxBytes       = 2 * 1024 * 1024
	DefaultEvaluationTraceMaxRecords     = 2000
	DefaultEvaluationTraceMaxRecordBytes = 16 * 1024
	DefaultEvaluationTraceMaxCorrections = 8
	DefaultEvaluationTraceRetentionHours = 24
	DefaultEvaluationTraceMaxTraces      = 100
)

type EvaluationConfig struct {
	TraceCapture EvaluationTraceCaptureConfig `json:"trace_capture,omitempty"`
}

type EvaluationTraceCaptureConfig struct {
	Enabled        bool   `json:"enabled"                    env:"PICOCLAW_EVALUATION_TRACE_CAPTURE_ENABLED"`
	ContentMode    string `json:"content_mode,omitempty"     env:"PICOCLAW_EVALUATION_TRACE_CAPTURE_CONTENT_MODE"`
	StateDir       string `json:"state_dir,omitempty"        env:"PICOCLAW_EVALUATION_TRACE_CAPTURE_STATE_DIR"`
	MaxTraceBytes  int    `json:"max_trace_bytes,omitempty"  env:"PICOCLAW_EVALUATION_TRACE_CAPTURE_MAX_TRACE_BYTES"`
	MaxRecords     int    `json:"max_records,omitempty"      env:"PICOCLAW_EVALUATION_TRACE_CAPTURE_MAX_RECORDS"`
	MaxRecordBytes int    `json:"max_record_bytes,omitempty" env:"PICOCLAW_EVALUATION_TRACE_CAPTURE_MAX_RECORD_BYTES"`
	MaxCorrections int    `json:"max_corrections,omitempty"  env:"PICOCLAW_EVALUATION_TRACE_CAPTURE_MAX_CORRECTIONS"`
	RetentionHours int    `json:"retention_hours,omitempty"  env:"PICOCLAW_EVALUATION_TRACE_CAPTURE_RETENTION_HOURS"`
	MaxTraces      int    `json:"max_traces,omitempty"       env:"PICOCLAW_EVALUATION_TRACE_CAPTURE_MAX_TRACES"`
}

func (c EvaluationTraceCaptureConfig) EffectiveContentMode() string {
	if !c.Enabled {
		return "metadata_only"
	}
	switch c.ContentMode {
	case "redacted_content":
		return "redacted_content"
	default:
		// fixture mode is deliberately unavailable to production capture.
		return "metadata_only"
	}
}

func defaultEvaluationConfig() EvaluationConfig {
	return EvaluationConfig{TraceCapture: EvaluationTraceCaptureConfig{
		Enabled: false, ContentMode: "metadata_only",
		MaxTraceBytes:  DefaultEvaluationTraceMaxBytes,
		MaxRecords:     DefaultEvaluationTraceMaxRecords,
		MaxRecordBytes: DefaultEvaluationTraceMaxRecordBytes,
		MaxCorrections: DefaultEvaluationTraceMaxCorrections,
		RetentionHours: DefaultEvaluationTraceRetentionHours,
		MaxTraces:      DefaultEvaluationTraceMaxTraces,
	}}
}
