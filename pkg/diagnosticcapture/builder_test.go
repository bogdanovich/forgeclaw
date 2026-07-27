package diagnosticcapture

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/diagnostictrace"
)

func TestTraceBuilderSequencesAndDeduplicatesOrigins(t *testing.T) {
	builder := testBuilder(4, diagnostictrace.DefaultMaxTraceBytes)
	first := testBuilderRecord("event-1", diagnostictrace.RecordTurnStart, `{"status":"started"}`)
	if got := builder.Append(first, RecordCritical); got.Status != AppendAccepted {
		t.Fatalf("first append = %+v", got)
	}
	if got := builder.Append(first, RecordCritical); got.Status != AppendDuplicate {
		t.Fatalf("duplicate append = %+v", got)
	}
	second := testBuilderRecord("event-2", diagnostictrace.RecordTurnEnd, `{"status":"completed"}`)
	if got := builder.Append(second, RecordCritical); got.Status != AppendAccepted {
		t.Fatalf("second append = %+v", got)
	}
	trace := finalizeBuilder(t, builder)
	if len(trace.Records) != 2 || trace.Records[0].Sequence != 1 || trace.Records[1].Sequence != 2 {
		t.Fatalf("records = %+v", trace.Records)
	}
	if trace.Truncation.Incomplete {
		t.Fatalf("truncation = %+v", trace.Truncation)
	}
}

func TestTraceBuilderDropsOrdinaryAtRecordLimit(t *testing.T) {
	builder := testBuilder(1, diagnostictrace.DefaultMaxTraceBytes)
	builder.Append(testBuilderRecord("event-1", diagnostictrace.RecordTurnStart, `{}`), RecordCritical)
	result := builder.Append(
		testBuilderRecord("event-2", diagnostictrace.RecordModelRequest, `{}`),
		RecordOrdinary,
	)
	if result.Status != AppendDroppedOrdinary || result.Reason != "record_count_limit" {
		t.Fatalf("append = %+v", result)
	}
	trace := finalizeBuilder(t, builder)
	assertDrop(t, trace, "record_count_limit", diagnostictrace.RecordModelRequest, 1)
}

func TestTraceBuilderCriticalEvictsOrdinaryAtRecordLimit(t *testing.T) {
	builder := testBuilder(2, diagnostictrace.DefaultMaxTraceBytes)
	builder.Append(testBuilderRecord("event-1", diagnostictrace.RecordTurnStart, `{}`), RecordCritical)
	builder.Append(testBuilderRecord("event-2", diagnostictrace.RecordModelRequest, `{}`), RecordOrdinary)
	result := builder.Append(
		testBuilderRecord("event-3", diagnostictrace.RecordTurnEnd, `{}`),
		RecordCritical,
	)
	if result.Status != AppendAcceptedEvicting || result.DroppedKind != diagnostictrace.RecordModelRequest {
		t.Fatalf("append = %+v", result)
	}
	trace := finalizeBuilder(t, builder)
	if len(trace.Records) != 2 || trace.Records[0].Kind != diagnostictrace.RecordTurnStart ||
		trace.Records[1].Kind != diagnostictrace.RecordTurnEnd {
		t.Fatalf("records = %+v", trace.Records)
	}
	assertDrop(t, trace, "record_count_limit", diagnostictrace.RecordModelRequest, 1)
}

func TestTraceBuilderReportsAllCriticalRecordSaturation(t *testing.T) {
	builder := testBuilder(1, diagnostictrace.DefaultMaxTraceBytes)
	builder.Append(testBuilderRecord("event-1", diagnostictrace.RecordTurnStart, `{}`), RecordCritical)
	result := builder.Append(
		testBuilderRecord("event-2", diagnostictrace.RecordTurnEnd, `{}`),
		RecordCritical,
	)
	if result.Status != AppendDroppedCritical || result.Reason != "critical_record_count_limit" {
		t.Fatalf("append = %+v", result)
	}
	trace := finalizeBuilder(t, builder)
	assertDrop(t, trace, "critical_record_count_limit", diagnostictrace.RecordTurnEnd, 1)
}

func TestTraceBuilderReportsOversizedCriticalRecord(t *testing.T) {
	builder := testBuilder(2, diagnostictrace.DefaultMaxTraceBytes)
	builder.trace.Limits.MaxRecordBytes = 8
	result := builder.Append(
		testBuilderRecord("event-1", diagnostictrace.RecordTurnEnd, `{"status":"completed"}`),
		RecordCritical,
	)
	if result.Status != AppendDroppedCritical || result.Reason != "critical_record_size_limit" {
		t.Fatalf("append = %+v", result)
	}
	trace := finalizeBuilder(t, builder)
	assertDrop(t, trace, "critical_record_size_limit", diagnostictrace.RecordTurnEnd, 1)
}

func TestTraceBuilderByteLimitPrefersOrdinaryEvidence(t *testing.T) {
	builder := testBuilder(4, 1500)
	builder.trace.Limits.MaxRecordBytes = 4096
	builder.Append(testBuilderRecord("event-1", diagnostictrace.RecordTurnStart, `{}`), RecordCritical)
	builder.Append(
		testBuilderRecord(
			"event-2", diagnostictrace.RecordModelResponse,
			`{"content":"`+strings.Repeat("x", 2500)+`"}`,
		),
		RecordOrdinary,
	)
	trace := finalizeBuilder(t, builder)
	if len(trace.Records) != 1 || trace.Records[0].Kind != diagnostictrace.RecordTurnStart {
		t.Fatalf("records = %+v", trace.Records)
	}
	assertDrop(t, trace, "trace_size_limit", diagnostictrace.RecordModelResponse, 1)
}

func TestTraceBuilderByteLimitReportsCriticalEvidenceLoss(t *testing.T) {
	builder := testBuilder(4, 1500)
	builder.trace.Limits.MaxRecordBytes = 4096
	builder.Append(testBuilderRecord("event-1", diagnostictrace.RecordTurnStart, `{}`), RecordCritical)
	builder.Append(
		testBuilderRecord(
			"event-2", diagnostictrace.RecordTurnEnd,
			`{"content":"`+strings.Repeat("x", 2500)+`"}`,
		),
		RecordCritical,
	)
	trace := finalizeBuilder(t, builder)
	if !trace.Truncation.Incomplete || !containsString(trace.Truncation.Reasons, "critical_trace_size_limit") {
		t.Fatalf("truncation = %+v", trace.Truncation)
	}
	if trace.Truncation.DroppedByKind[diagnostictrace.RecordTurnEnd] != 1 {
		t.Fatalf("dropped kinds = %+v", trace.Truncation.DroppedByKind)
	}
}

func TestTraceBuilderMarksExternalLossAndRejectsUnknownClass(t *testing.T) {
	builder := testBuilder(2, diagnostictrace.DefaultMaxTraceBytes)
	builder.MarkIncomplete("runtime_event_backpressure", 3)
	result := builder.Append(testBuilderRecord("event-1", diagnostictrace.RecordTurnStart, `{}`), "unknown")
	if result.Status != AppendDroppedCritical || result.Reason != "invalid_record_class" {
		t.Fatalf("append = %+v", result)
	}
	trace := finalizeBuilder(t, builder)
	if trace.Truncation.DroppedRecords != 4 ||
		!containsString(trace.Truncation.Reasons, "runtime_event_backpressure") ||
		!containsString(trace.Truncation.Reasons, "invalid_record_class") {
		t.Fatalf("truncation = %+v", trace.Truncation)
	}
}

func testBuilder(maxRecords, maxTraceBytes int) *TraceBuilder {
	return NewTraceBuilder(diagnostictrace.Trace{
		SchemaVersion: diagnostictrace.SchemaVersionV1,
		TraceID:       "trace-builder-test",
		CreatedAt:     time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC),
		Policy:        diagnostictrace.CapturePolicy{ContentMode: diagnostictrace.ContentMetadataOnly},
		Limits: diagnostictrace.AppliedLimits{
			MaxTraceBytes: maxTraceBytes, MaxRecords: maxRecords,
			MaxRecordBytes: diagnostictrace.DefaultMaxRecordBytes,
		},
		Records: make([]diagnostictrace.Record, 0, maxRecords),
	})
}

func testBuilderRecord(id string, kind diagnostictrace.RecordKind, data string) diagnostictrace.Record {
	return diagnostictrace.Record{
		Kind: kind, Origin: diagnostictrace.Origin{Kind: "test", ID: id}, Data: json.RawMessage(data),
	}
}

func finalizeBuilder(t *testing.T, builder *TraceBuilder) diagnostictrace.Trace {
	t.Helper()
	trace, err := builder.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	return trace
}

func assertDrop(
	t *testing.T,
	trace diagnostictrace.Trace,
	reason string,
	kind diagnostictrace.RecordKind,
	count int,
) {
	t.Helper()
	if !trace.Truncation.Incomplete || trace.Truncation.DroppedRecords != count ||
		!containsString(trace.Truncation.Reasons, reason) ||
		trace.Truncation.DroppedByKind[kind] != count {
		t.Fatalf("truncation = %+v", trace.Truncation)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
