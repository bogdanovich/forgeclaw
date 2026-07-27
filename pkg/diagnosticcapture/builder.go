package diagnosticcapture

import (
	"errors"
	"strings"

	"github.com/bogdanovich/mintclaw/pkg/diagnostictrace"
)

// RecordClass states whether a diagnostic record should displace ordinary data.
type RecordClass string

const (
	RecordOrdinary RecordClass = "ordinary"
	RecordCritical RecordClass = "critical"
)

// AppendStatus describes how a record affected the bounded trace.
type AppendStatus string

const (
	AppendAccepted         AppendStatus = "accepted"
	AppendDuplicate        AppendStatus = "duplicate"
	AppendDroppedOrdinary  AppendStatus = "dropped_ordinary"
	AppendDroppedCritical  AppendStatus = "dropped_critical"
	AppendAcceptedEvicting AppendStatus = "accepted_evicting_ordinary"
)

// AppendResult makes every capacity decision visible to the source projector.
type AppendResult struct {
	Status      AppendStatus
	Reason      string
	DroppedKind diagnostictrace.RecordKind
}

// TraceBuilder owns bounded record assembly for one diagnostic trace.
// It is not safe for concurrent use; source projectors provide serialization.
type TraceBuilder struct {
	trace    diagnostictrace.Trace
	critical map[uint64]bool
	origins  map[string]struct{}
}

// NewTraceBuilder takes ownership of base and normalizes its limits.
func NewTraceBuilder(base diagnostictrace.Trace) *TraceBuilder {
	base.Limits = diagnostictrace.NormalizeLimits(base.Limits)
	builder := &TraceBuilder{
		trace: base, critical: make(map[uint64]bool), origins: make(map[string]struct{}),
	}
	for _, record := range base.Records {
		builder.critical[record.Sequence] = true
		if record.Origin.ID != "" {
			builder.origins[record.Origin.Kind+"\x00"+record.Origin.ID] = struct{}{}
		}
	}
	return builder
}

// TraceID returns the opaque identifier without exposing mutable trace state.
func (b *TraceBuilder) TraceID() string {
	if b == nil {
		return ""
	}
	return b.trace.TraceID
}

// SetOutcome records the terminal source outcome.
func (b *TraceBuilder) SetOutcome(outcome diagnostictrace.Outcome) {
	if b == nil {
		return
	}
	b.trace.Outcome = &outcome
}

// MarkIncomplete records loss that happened outside record admission.
func (b *TraceBuilder) MarkIncomplete(reason string, droppedRecords int) {
	if b == nil {
		return
	}
	b.trace.Truncation.Incomplete = true
	b.trace.Truncation.Reasons = appendUniqueReason(b.trace.Truncation.Reasons, reason)
	if droppedRecords > 0 {
		b.trace.Truncation.DroppedRecords += droppedRecords
	}
}

// Append admits one source record under the trace's bounded evidence policy.
func (b *TraceBuilder) Append(record diagnostictrace.Record, class RecordClass) AppendResult {
	if b == nil {
		return AppendResult{
			Status: AppendDroppedCritical, Reason: "builder_unavailable", DroppedKind: record.Kind,
		}
	}
	if class != RecordOrdinary && class != RecordCritical {
		b.recordDrop("invalid_record_class", record.Kind)
		return AppendResult{
			Status: AppendDroppedCritical, Reason: "invalid_record_class", DroppedKind: record.Kind,
		}
	}
	critical := class == RecordCritical
	originKey := record.Origin.Kind + "\x00" + record.Origin.ID
	if record.Origin.ID != "" {
		if _, exists := b.origins[originKey]; exists {
			return AppendResult{Status: AppendDuplicate}
		}
	}
	if len(record.Data) > b.trace.Limits.MaxRecordBytes {
		reason := "record_size_limit"
		status := AppendDroppedOrdinary
		if critical {
			reason, status = "critical_record_size_limit", AppendDroppedCritical
		}
		b.recordDrop(reason, record.Kind)
		return AppendResult{Status: status, Reason: reason, DroppedKind: record.Kind}
	}

	result := AppendResult{Status: AppendAccepted}
	if len(b.trace.Records) >= b.trace.Limits.MaxRecords {
		if !critical {
			b.recordDrop("record_count_limit", record.Kind)
			return AppendResult{
				Status: AppendDroppedOrdinary, Reason: "record_count_limit", DroppedKind: record.Kind,
			}
		}
		dropped, ok := b.evictOrdinary()
		if !ok {
			b.recordDrop("critical_record_count_limit", record.Kind)
			return AppendResult{
				Status: AppendDroppedCritical, Reason: "critical_record_count_limit",
				DroppedKind: record.Kind,
			}
		}
		b.recordDrop("record_count_limit", dropped.Kind)
		result = AppendResult{
			Status: AppendAcceptedEvicting, Reason: "record_count_limit", DroppedKind: dropped.Kind,
		}
	}
	record.Sequence = nextRecordSequence(b.trace.Records)
	b.trace.Records = append(b.trace.Records, record)
	b.critical[record.Sequence] = critical
	if record.Origin.ID != "" {
		b.origins[originKey] = struct{}{}
	}
	return result
}

// Finalize canonicalizes the trace and preferentially removes ordinary
// records when the encoded trace exceeds its byte limit.
func (b *TraceBuilder) Finalize() (diagnostictrace.Trace, error) {
	if b == nil {
		return diagnostictrace.Trace{}, errors.New("trace builder is unavailable")
	}
	for {
		finalized, err := diagnostictrace.Finalize(b.trace)
		if err == nil {
			return finalized, nil
		}
		if !strings.Contains(err.Error(), "trace exceeds byte limit") || len(b.trace.Records) == 0 {
			return diagnostictrace.Trace{}, err
		}
		dropped, critical := b.evictForByteLimit()
		reason := "trace_size_limit"
		if critical {
			reason = "critical_trace_size_limit"
		}
		b.recordDrop(reason, dropped.Kind)
	}
}

func (b *TraceBuilder) evictOrdinary() (diagnostictrace.Record, bool) {
	for i := len(b.trace.Records) - 1; i >= 0; i-- {
		if b.critical[b.trace.Records[i].Sequence] {
			continue
		}
		return b.removeRecord(i), true
	}
	return diagnostictrace.Record{}, false
}

func (b *TraceBuilder) evictForByteLimit() (diagnostictrace.Record, bool) {
	if dropped, ok := b.evictOrdinary(); ok {
		return dropped, false
	}
	index := len(b.trace.Records) - 1
	dropped := b.removeRecord(index)
	return dropped, true
}

func (b *TraceBuilder) removeRecord(index int) diagnostictrace.Record {
	dropped := b.trace.Records[index]
	b.trace.Records = append(b.trace.Records[:index], b.trace.Records[index+1:]...)
	delete(b.critical, dropped.Sequence)
	return dropped
}

func (b *TraceBuilder) recordDrop(reason string, kind diagnostictrace.RecordKind) {
	b.trace.Truncation.Incomplete = true
	b.trace.Truncation.DroppedRecords++
	b.trace.Truncation.Reasons = appendUniqueReason(b.trace.Truncation.Reasons, reason)
	if b.trace.Truncation.DroppedByKind == nil {
		b.trace.Truncation.DroppedByKind = make(map[diagnostictrace.RecordKind]int)
	}
	b.trace.Truncation.DroppedByKind[kind]++
}

func nextRecordSequence(records []diagnostictrace.Record) uint64 {
	var highest uint64
	for _, record := range records {
		if record.Sequence > highest {
			highest = record.Sequence
		}
	}
	return highest + 1
}

func appendUniqueReason(values []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}
