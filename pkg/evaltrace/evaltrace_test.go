package evaltrace

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFinalizeCanonicalizesOrdersAndDigests(t *testing.T) {
	created := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	trace := baseTrace(created)
	trace.Records = []Record{
		{
			Sequence:    2,
			OffsetNanos: 2,
			Kind:        RecordToolResult,
			Origin:      Origin{Kind: "runtime_event", ID: "evt-2"},
			Data:        json.RawMessage(`{"z":1,"a":{"b":2,"a":1}}`),
		},
		{
			Sequence:    1,
			OffsetNanos: 1,
			Kind:        RecordToolCall,
			Origin:      Origin{Kind: "runtime_event", ID: "evt-1"},
			Data:        json.RawMessage(`{"tool":"read_file"}`),
		},
	}
	got, err := Finalize(trace)
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if got.Records[0].Sequence != 1 || got.Records[1].Sequence != 2 {
		t.Fatalf("records not ordered: %#v", got.Records)
	}
	if string(got.Records[1].Data) != `{"a":{"a":1,"b":2},"z":1}` {
		t.Fatalf("canonical data = %s", got.Records[1].Data)
	}
	for _, record := range got.Records {
		if len(record.Digest) != 64 {
			t.Fatalf("digest = %q", record.Digest)
		}
	}
	encodedA, _ := json.Marshal(got)
	gotAgain, err := Finalize(got)
	if err != nil {
		t.Fatalf("Finalize again: %v", err)
	}
	encodedB, _ := json.Marshal(gotAgain)
	if string(encodedA) != string(encodedB) {
		t.Fatal("finalization is not deterministic")
	}
}

func TestValidateRejectsSchemaOrderingDuplicateAndTampering(t *testing.T) {
	trace, err := Finalize(baseTrace(time.Now().UTC()))
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	tests := []struct {
		name string
		edit func(*Trace)
	}{
		{"schema", func(v *Trace) { v.SchemaVersion = "forgeclaw.eval_trace.v2" }},
		{"trace kind", func(v *Trace) { v.Metadata.TraceKind = "future" }},
		{"unsafe id", func(v *Trace) { v.TraceID = "../escape" }},
		{"unknown kind", func(v *Trace) { v.Records[0].Kind = "future.kind" }},
		{"tampered data", func(v *Trace) { v.Records[0].Data = json.RawMessage(`{"status":"changed"}`) }},
		{"array data", func(v *Trace) { v.Records[0].Data = json.RawMessage(`[]`) }},
		{"duplicate origin", func(v *Trace) { v.Records = append(v.Records, v.Records[0]); v.Records[1].Sequence = 2 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			copy := trace
			copy.Records = append([]Record(nil), trace.Records...)
			tc.edit(&copy)
			if err := Validate(copy); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestNormalizeLimitsUsesDefaultsAndHardCeilings(t *testing.T) {
	defaults := NormalizeLimits(AppliedLimits{})
	if defaults != DefaultLimits() {
		t.Fatalf("defaults = %#v", defaults)
	}
	hard := NormalizeLimits(AppliedLimits{
		MaxTraceBytes: 1 << 30, MaxRecords: 1 << 20,
		MaxRecordBytes: 1 << 20, MaxCorrections: 1000,
	})
	if hard.MaxTraceBytes != HardMaxTraceBytes || hard.MaxRecords != HardMaxRecords ||
		hard.MaxRecordBytes != HardMaxRecordBytes || hard.MaxCorrections != HardMaxCorrections {
		t.Fatalf("hard limits = %#v", hard)
	}
}

func TestValidateCorrectionAndContentPolicy(t *testing.T) {
	trace, err := Finalize(baseTrace(time.Now().UTC()))
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	trace.Corrections = []Correction{{CorrectionID: "correction-1", RecordRefs: []uint64{99}}}
	if err := Validate(trace); err == nil {
		t.Fatal("expected unknown correction reference error")
	}
	trace.Corrections[0].RecordRefs = []uint64{1}
	if err := Validate(trace); err != nil {
		t.Fatalf("valid correction: %v", err)
	}
	trace.Policy.ContentMode = ContentFixture
	if err := Validate(trace); err == nil {
		t.Fatal("expected fixture provenance error")
	}
	trace.Source.FixtureID = "fixture-1"
	trace.Source.FixtureSource = "commit:4de727cd"
	if err := Validate(trace); err != nil {
		t.Fatalf("valid fixture provenance: %v", err)
	}
	trace.Policy.ContentMode = ContentRedacted
	if err := Validate(trace); err == nil {
		t.Fatal("expected missing redactor error")
	}
}

func TestRedactorIsAllowlistedAndDoesNotSerializeSecrets(t *testing.T) {
	secret := "sk-super-secret-value"
	input := map[string]any{
		"status":  "failed",
		"body":    "Authorization: Bearer " + secret + " API_TOKEN=" + secret,
		"api_key": secret,
		"url":     "https://user:pass@example.test/path?token=" + secret + "&page=1",
		"nested":  map[string]any{"password": secret},
		"ignored": secret,
		"data":    "data:image/png;base64," + secret,
	}
	allow := map[string]FieldPolicy{
		"status": {Class: FieldMetadata}, "body": {Class: FieldContent},
		"api_key": {Class: FieldContent}, "url": {Class: FieldContent},
		"nested": {Class: FieldContent}, "data": {Class: FieldContent},
	}
	redactor := Redactor{Mode: ContentRedacted}
	projected := redactor.Project(input, allow)
	encoded, err := json.Marshal(projected)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	text := string(encoded)
	if strings.Contains(text, secret) || strings.Contains(text, "user:pass") {
		t.Fatalf("secret leaked: %s", text)
	}
	if _, ok := projected["api_key"]; ok {
		t.Fatal("sensitive key was projected")
	}
	if _, ok := projected["ignored"]; ok {
		t.Fatal("non-allowlisted field was projected")
	}
	if projected["nested"] != "[UNSUPPORTED]" || projected["data"] != "[DATA_URL REDACTED]" {
		t.Fatalf("structural redaction = %#v", projected)
	}
}

func TestMetadataOnlyReplacesContentWithLength(t *testing.T) {
	projected := (Redactor{Mode: ContentMetadataOnly}).Project(
		map[string]any{"status": "ok", "content": "private"},
		map[string]FieldPolicy{"status": {Class: FieldMetadata}, "content": {Class: FieldContent}},
	)
	if projected["status"] != "ok" || projected["content_len"] != len("private") {
		t.Fatalf("projected = %#v", projected)
	}
	if _, ok := projected["content"]; ok {
		t.Fatal("metadata-only projection retained content")
	}
}

func TestStoreRoundTripPermissionsPruneAndSymlinkDenial(t *testing.T) {
	root := filepath.Join(t.TempDir(), "traces")
	store := Store{Root: root, Retention: time.Hour, MaxTraces: 1}
	first, err := Finalize(baseTrace(time.Now().UTC()))
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	path, err := store.Save(first)
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if mode := fileMode(t, root); mode.Perm() != 0o700 {
		t.Fatalf("root mode = %o", mode.Perm())
	}
	if mode := fileMode(t, path); mode.Perm() != 0o600 {
		t.Fatalf("file mode = %o", mode.Perm())
	}
	loaded, err := store.Load(first.TraceID)
	if err != nil || loaded.TraceID != first.TraceID {
		t.Fatalf("Load = %#v, %v", loaded, err)
	}

	second := first
	second.TraceID = "trace-second"
	second.CreatedAt = first.CreatedAt.Add(time.Second)
	if _, saveErr := store.Save(second); saveErr != nil {
		t.Fatalf("Save second: %v", saveErr)
	}
	if chtimesErr := os.Chtimes(path, time.Now().Add(-2*time.Hour), time.Now().Add(-2*time.Hour)); chtimesErr != nil {
		t.Fatalf("Chtimes: %v", chtimesErr)
	}
	removed, err := store.Prune()
	if err != nil || removed != 1 {
		t.Fatalf("Prune = %d, %v", removed, err)
	}

	realRoot := filepath.Join(t.TempDir(), "real")
	if err := os.MkdirAll(realRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	linkRoot := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(realRoot, linkRoot); err != nil {
		t.Fatal(err)
	}
	if _, err := (Store{Root: linkRoot}).Save(first); err == nil {
		t.Fatal("expected symlink store rejection")
	}
}

func baseTrace(created time.Time) Trace {
	return Trace{
		SchemaVersion: SchemaVersionV1,
		TraceID:       "trace-test-1",
		CreatedAt:     created,
		Policy:        CapturePolicy{ContentMode: ContentMetadataOnly},
		Limits:        DefaultLimits(),
		Records: []Record{{
			Sequence: 1, Kind: RecordTurnEnd,
			Origin: Origin{Kind: "runtime_event", ID: "evt-1"},
			Data:   json.RawMessage(`{"status":"completed"}`),
		}},
	}
}

func fileMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode()
}
