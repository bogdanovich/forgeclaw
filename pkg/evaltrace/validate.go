package evaltrace

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
)

var safeIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

var supportedKinds = map[RecordKind]struct{}{
	RecordTurnStart: {}, RecordTurnEnd: {}, RecordFinalOutcome: {},
	RecordModelRequest: {}, RecordModelResponse: {}, RecordModelRetry: {}, RecordModelFallbackAttempt: {},
	RecordToolCall: {}, RecordToolResult: {}, RecordToolSkipped: {}, RecordToolLoopDecision: {},
	RecordToolSteeringDecision: {},
	RecordSteeringEnqueued:     {}, RecordSteeringInjected: {}, RecordInterrupt: {},
	RecordTaskTransition: {}, RecordInteractionTransition: {},
	RecordDeliveryDecision: {}, RecordDeliveryAttempt: {}, RecordDeliveryOutcome: {},
	RecordContextCompaction: {}, RecordContextReconciliation: {}, RecordContextSnapshot: {},
	RecordRestartBoundary: {}, RecordInboundSpoolTransition: {}, RecordUserCorrection: {},
}

func Validate(trace Trace) error {
	if trace.SchemaVersion != SchemaVersionV1 {
		return fmt.Errorf("unsupported trace schema %q", trace.SchemaVersion)
	}
	if !safeIDPattern.MatchString(trace.TraceID) {
		return errors.New("trace_id must be a path-safe opaque identifier")
	}
	if trace.CreatedAt.IsZero() {
		return errors.New("created_at is required")
	}
	switch trace.Policy.ContentMode {
	case ContentMetadataOnly, ContentRedacted, ContentFixture:
	default:
		return fmt.Errorf("unsupported content mode %q", trace.Policy.ContentMode)
	}
	if trace.Policy.ContentMode == ContentRedacted && trace.Policy.Redactor == "" {
		return errors.New("redacted_content traces require a redactor version")
	}
	if trace.Policy.ContentMode == ContentFixture &&
		(trace.Source.FixtureID == "" || trace.Source.FixtureSource == "") {
		return errors.New("fixture traces require fixture_id and fixture_source")
	}
	limits := NormalizeLimits(trace.Limits)
	if trace.Limits != limits {
		return errors.New("trace limits are not normalized")
	}
	if len(trace.Records) > limits.MaxRecords {
		return fmt.Errorf("records exceed limit: %d > %d", len(trace.Records), limits.MaxRecords)
	}
	if len(trace.Corrections) > limits.MaxCorrections {
		return fmt.Errorf("corrections exceed limit: %d > %d", len(trace.Corrections), limits.MaxCorrections)
	}

	origins := make(map[string]string)
	sequences := make(map[uint64]struct{}, len(trace.Records))
	var previousSequence uint64
	var previousOffset int64
	for i, record := range trace.Records {
		if _, ok := supportedKinds[record.Kind]; !ok {
			return fmt.Errorf("record %d has unsupported kind %q", i+1, record.Kind)
		}
		if record.Sequence == 0 || (i > 0 && record.Sequence <= previousSequence) {
			return fmt.Errorf("record %d sequence is not strictly increasing", i+1)
		}
		if record.OffsetNanos < 0 || (i > 0 && record.OffsetNanos < previousOffset) {
			return fmt.Errorf("record %d offset is not monotonic", i+1)
		}
		if len(record.Data) > limits.MaxRecordBytes {
			return fmt.Errorf("record %d exceeds data limit", i+1)
		}
		if len(record.Data) > 0 && !json.Valid(record.Data) {
			return fmt.Errorf("record %d data is invalid JSON", i+1)
		}
		if len(record.Data) > 0 && !isJSONObject(record.Data) {
			return fmt.Errorf("record %d data must be a JSON object", i+1)
		}
		if record.Origin.Kind == "" {
			return fmt.Errorf("record %d origin kind is required", i+1)
		}
		if record.Origin.ID != "" {
			key := record.Origin.Kind + "\x00" + record.Origin.ID
			if prior, exists := origins[key]; exists && prior != record.Digest {
				return fmt.Errorf("record %d conflicts with duplicate origin", i+1)
			}
			if _, exists := origins[key]; exists {
				return fmt.Errorf("record %d duplicates origin", i+1)
			}
			origins[key] = record.Digest
		}
		if len(record.Digest) != sha256HexLength {
			return fmt.Errorf("record %d digest is invalid", i+1)
		}
		if _, err := hex.DecodeString(record.Digest); err != nil {
			return fmt.Errorf("record %d digest is invalid", i+1)
		}
		want, err := RecordDigest(record)
		if err != nil || want != record.Digest {
			return fmt.Errorf("record %d digest mismatch", i+1)
		}
		previousSequence = record.Sequence
		previousOffset = record.OffsetNanos
		sequences[record.Sequence] = struct{}{}
	}
	correctionIDs := make(map[string]struct{}, len(trace.Corrections))
	for i, correction := range trace.Corrections {
		if !safeIDPattern.MatchString(correction.CorrectionID) {
			return fmt.Errorf("correction %d has invalid id", i+1)
		}
		if _, exists := correctionIDs[correction.CorrectionID]; exists {
			return fmt.Errorf("correction %d duplicates id", i+1)
		}
		correctionIDs[correction.CorrectionID] = struct{}{}
		if len(correction.Note) > 4096 {
			return fmt.Errorf("correction %d note exceeds limit", i+1)
		}
		for _, ref := range correction.RecordRefs {
			if _, exists := sequences[ref]; !exists {
				return fmt.Errorf("correction %d references unknown record %d", i+1, ref)
			}
		}
	}
	data, err := json.Marshal(trace)
	if err != nil {
		return fmt.Errorf("encode trace: %w", err)
	}
	if len(data) > limits.MaxTraceBytes {
		return fmt.Errorf("trace exceeds byte limit: %d > %d", len(data), limits.MaxTraceBytes)
	}
	return nil
}

const sha256HexLength = 64

func isJSONObject(data json.RawMessage) bool {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value map[string]any
	return decoder.Decode(&value) == nil && value != nil
}
