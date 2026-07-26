package config

const (
	DefaultDiagnosticTraceMaxBytes       = 2 * 1024 * 1024
	DefaultDiagnosticTraceMaxRecords     = 2000
	DefaultDiagnosticTraceMaxRecordBytes = 16 * 1024
	DefaultDiagnosticTraceRetentionHours = 24
	DefaultDiagnosticTraceMaxTraces      = 100
)

type DiagnosticsConfig struct {
	TraceCapture DiagnosticTraceCaptureConfig `json:"trace_capture,omitempty"`
}

type DiagnosticTraceCaptureConfig struct {
	Enabled        bool   `json:"enabled"                    env:"PICOCLAW_DIAGNOSTICS_TRACE_CAPTURE_ENABLED"`
	ContentMode    string `json:"content_mode,omitempty"     env:"PICOCLAW_DIAGNOSTICS_TRACE_CAPTURE_CONTENT_MODE"`
	StateDir       string `json:"state_dir,omitempty"        env:"PICOCLAW_DIAGNOSTICS_TRACE_CAPTURE_STATE_DIR"`
	MaxTraceBytes  int    `json:"max_trace_bytes,omitempty"  env:"PICOCLAW_DIAGNOSTICS_TRACE_CAPTURE_MAX_TRACE_BYTES"`
	MaxRecords     int    `json:"max_records,omitempty"      env:"PICOCLAW_DIAGNOSTICS_TRACE_CAPTURE_MAX_RECORDS"`
	MaxRecordBytes int    `json:"max_record_bytes,omitempty" env:"PICOCLAW_DIAGNOSTICS_TRACE_CAPTURE_MAX_RECORD_BYTES"`
	RetentionHours int    `json:"retention_hours,omitempty"  env:"PICOCLAW_DIAGNOSTICS_TRACE_CAPTURE_RETENTION_HOURS"`
	MaxTraces      int    `json:"max_traces,omitempty"       env:"PICOCLAW_DIAGNOSTICS_TRACE_CAPTURE_MAX_TRACES"`
}

func (c DiagnosticTraceCaptureConfig) EffectiveContentMode() string {
	if !c.Enabled {
		return "metadata_only"
	}
	switch c.ContentMode {
	case "redacted_content":
		return "redacted_content"
	default:
		return "metadata_only"
	}
}

func defaultDiagnosticsConfig() DiagnosticsConfig {
	return DiagnosticsConfig{TraceCapture: DiagnosticTraceCaptureConfig{
		Enabled: false, ContentMode: "metadata_only",
		MaxTraceBytes:  DefaultDiagnosticTraceMaxBytes,
		MaxRecords:     DefaultDiagnosticTraceMaxRecords,
		MaxRecordBytes: DefaultDiagnosticTraceMaxRecordBytes,
		RetentionHours: DefaultDiagnosticTraceRetentionHours,
		MaxTraces:      DefaultDiagnosticTraceMaxTraces,
	}}
}
