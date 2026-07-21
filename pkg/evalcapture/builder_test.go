package evalcapture

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/evaltrace"
)

func TestTraceBuilderSequencesAndDeduplicatesOrigins(t *testing.T) {
	builder := testBuilder(4, evaltrace.DefaultMaxTraceBytes)
	first := testBuilderRecord("event-1", evaltrace.RecordTurnStart, `{"status":"started"}`)
	if got := builder.Append(first, RecordCritical); got.Status != AppendAccepted {
		t.Fatalf("first append = %+v", got)
	}
	if got := builder.Append(first, RecordCritical); got.Status != AppendDuplicate {
		t.Fatalf("duplicate append = %+v", got)
	}
	second := testBuilderRecord("event-2", evaltrace.RecordTurnEnd, `{"status":"completed"}`)
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
	builder := testBuilder(1, evaltrace.DefaultMaxTraceBytes)
	builder.Append(testBuilderRecord("event-1", evaltrace.RecordTurnStart, `{}`), RecordCritical)
	result := builder.Append(
		testBuilderRecord("event-2", evaltrace.RecordModelRequest, `{}`),
		RecordOrdinary,
	)
	if result.Status != AppendDroppedOrdinary || result.Reason != "record_count_limit" {
		t.Fatalf("append = %+v", result)
	}
	trace := finalizeBuilder(t, builder)
	assertDrop(t, trace, "record_count_limit", evaltrace.RecordModelRequest, 1)
}

func TestTraceBuilderCriticalEvictsOrdinaryAtRecordLimit(t *testing.T) {
	builder := testBuilder(2, evaltrace.DefaultMaxTraceBytes)
	builder.Append(testBuilderRecord("event-1", evaltrace.RecordTurnStart, `{}`), RecordCritical)
	builder.Append(testBuilderRecord("event-2", evaltrace.RecordModelRequest, `{}`), RecordOrdinary)
	result := builder.Append(
		testBuilderRecord("event-3", evaltrace.RecordTurnEnd, `{}`),
		RecordCritical,
	)
	if result.Status != AppendAcceptedEvicting || result.DroppedKind != evaltrace.RecordModelRequest {
		t.Fatalf("append = %+v", result)
	}
	trace := finalizeBuilder(t, builder)
	if len(trace.Records) != 2 || trace.Records[0].Kind != evaltrace.RecordTurnStart ||
		trace.Records[1].Kind != evaltrace.RecordTurnEnd {
		t.Fatalf("records = %+v", trace.Records)
	}
	assertDrop(t, trace, "record_count_limit", evaltrace.RecordModelRequest, 1)
}

func TestTraceBuilderReportsAllCriticalRecordSaturation(t *testing.T) {
	builder := testBuilder(1, evaltrace.DefaultMaxTraceBytes)
	builder.Append(testBuilderRecord("event-1", evaltrace.RecordTurnStart, `{}`), RecordCritical)
	result := builder.Append(
		testBuilderRecord("event-2", evaltrace.RecordTurnEnd, `{}`),
		RecordCritical,
	)
	if result.Status != AppendDroppedCritical || result.Reason != "critical_record_count_limit" {
		t.Fatalf("append = %+v", result)
	}
	trace := finalizeBuilder(t, builder)
	assertDrop(t, trace, "critical_record_count_limit", evaltrace.RecordTurnEnd, 1)
}

func TestTraceBuilderReportsOversizedCriticalRecord(t *testing.T) {
	builder := testBuilder(2, evaltrace.DefaultMaxTraceBytes)
	builder.trace.Limits.MaxRecordBytes = 8
	result := builder.Append(
		testBuilderRecord("event-1", evaltrace.RecordTurnEnd, `{"status":"completed"}`),
		RecordCritical,
	)
	if result.Status != AppendDroppedCritical || result.Reason != "critical_record_size_limit" {
		t.Fatalf("append = %+v", result)
	}
	trace := finalizeBuilder(t, builder)
	assertDrop(t, trace, "critical_record_size_limit", evaltrace.RecordTurnEnd, 1)
}

func TestTraceBuilderByteLimitPrefersOrdinaryEvidence(t *testing.T) {
	builder := testBuilder(4, 1500)
	builder.trace.Limits.MaxRecordBytes = 4096
	builder.Append(testBuilderRecord("event-1", evaltrace.RecordTurnStart, `{}`), RecordCritical)
	builder.Append(
		testBuilderRecord(
			"event-2", evaltrace.RecordModelResponse,
			`{"content":"`+strings.Repeat("x", 2500)+`"}`,
		),
		RecordOrdinary,
	)
	trace := finalizeBuilder(t, builder)
	if len(trace.Records) != 1 || trace.Records[0].Kind != evaltrace.RecordTurnStart {
		t.Fatalf("records = %+v", trace.Records)
	}
	assertDrop(t, trace, "trace_size_limit", evaltrace.RecordModelResponse, 1)
}

func TestTraceBuilderByteLimitReportsCriticalEvidenceLoss(t *testing.T) {
	builder := testBuilder(4, 1500)
	builder.trace.Limits.MaxRecordBytes = 4096
	builder.Append(testBuilderRecord("event-1", evaltrace.RecordTurnStart, `{}`), RecordCritical)
	builder.Append(
		testBuilderRecord(
			"event-2", evaltrace.RecordTurnEnd,
			`{"content":"`+strings.Repeat("x", 2500)+`"}`,
		),
		RecordCritical,
	)
	trace := finalizeBuilder(t, builder)
	if !trace.Truncation.Incomplete || !containsString(trace.Truncation.Reasons, "critical_trace_size_limit") {
		t.Fatalf("truncation = %+v", trace.Truncation)
	}
	if trace.Truncation.DroppedByKind[evaltrace.RecordTurnEnd] != 1 {
		t.Fatalf("dropped kinds = %+v", trace.Truncation.DroppedByKind)
	}
}

func TestTraceBuilderMarksExternalLossAndRejectsUnknownClass(t *testing.T) {
	builder := testBuilder(2, evaltrace.DefaultMaxTraceBytes)
	builder.MarkIncomplete("runtime_event_backpressure", 3)
	result := builder.Append(testBuilderRecord("event-1", evaltrace.RecordTurnStart, `{}`), "unknown")
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
	return NewTraceBuilder(evaltrace.Trace{
		SchemaVersion: evaltrace.SchemaVersionV1,
		TraceID:       "trace-builder-test",
		CreatedAt:     time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC),
		Policy:        evaltrace.CapturePolicy{ContentMode: evaltrace.ContentMetadataOnly},
		Limits: evaltrace.AppliedLimits{
			MaxTraceBytes: maxTraceBytes, MaxRecords: maxRecords,
			MaxRecordBytes: evaltrace.DefaultMaxRecordBytes,
			MaxCorrections: evaltrace.DefaultMaxCorrections,
		},
		Records: make([]evaltrace.Record, 0, maxRecords),
	})
}

func testBuilderRecord(id string, kind evaltrace.RecordKind, data string) evaltrace.Record {
	return evaltrace.Record{
		Kind: kind, Origin: evaltrace.Origin{Kind: "test", ID: id}, Data: json.RawMessage(data),
	}
}

func finalizeBuilder(t *testing.T, builder *TraceBuilder) evaltrace.Trace {
	t.Helper()
	trace, err := builder.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	return trace
}

func assertDrop(
	t *testing.T,
	trace evaltrace.Trace,
	reason string,
	kind evaltrace.RecordKind,
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
