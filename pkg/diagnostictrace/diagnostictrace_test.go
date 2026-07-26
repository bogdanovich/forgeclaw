package diagnostictrace

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

type panicJSONValue struct{}

func (panicJSONValue) MarshalJSON() ([]byte, error) {
	panic("must not invoke custom marshaler")
}

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
		{"schema", func(v *Trace) { v.SchemaVersion = "forgeclaw.diagnostic_trace.v2" }},
		{"removed evaluation schema", func(v *Trace) { v.SchemaVersion = "forgeclaw.eval_trace.v1" }},
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
		MaxRecordBytes: 1 << 20,
	})
	if hard.MaxTraceBytes != HardMaxTraceBytes || hard.MaxRecords != HardMaxRecords ||
		hard.MaxRecordBytes != HardMaxRecordBytes {
		t.Fatalf("hard limits = %#v", hard)
	}
}

func TestValidateContentPolicy(t *testing.T) {
	trace, err := Finalize(baseTrace(time.Now().UTC()))
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	trace.Policy.ContentMode = "fixture"
	if err := Validate(trace); err == nil {
		t.Fatal("expected unsupported content mode error")
	}
	trace.Policy.ContentMode = ContentRedacted
	if err := Validate(trace); err == nil {
		t.Fatal("expected missing redactor error")
	}
}

func TestRedactJSONRecursesFiltersCredentialsAndBoundsUTF8(t *testing.T) {
	knownSecret := "workspace-config-secret"
	privateKey := "-----BEGIN OPENSSH PRIVATE KEY-----\nprivate-material\n-----END OPENSSH PRIVATE KEY-----"
	input := map[string]any{
		"path": "/tmp/diagnostic.txt",
		"nested": map[string]any{
			"password": "arbitrary-password",
			"message":  "known=" + knownSecret,
			"key":      privateKey,
		},
		"tokens": []any{
			"ghp_1234567890abcdefghijklmnop",
			"Bearer abcdefghijklmnopqrstuvwxyz",
			"request https://alice:hunter2@example.test/path?token=opaque&page=1 failed",
			"Cookie: session=opaque-cookie",
		},
		"token_" + knownSecret:                 "sensitive-key-value",
		"ghp_abcdefghijklmnopqrstuvwxyz123456": "key-secret",
		"unicode":                              strings.Repeat("界", 20),
	}
	redactor := Redactor{
		Filter: func(value string) string {
			return strings.ReplaceAll(value, knownSecret, "[FILTERED]")
		},
	}
	got := redactor.RedactJSON(input, 1024)
	for _, forbidden := range []string{
		knownSecret, "arbitrary-password", "private-material", "ghp_1234567890",
		"ghp_abcdefghijklmnopqrstuvwxyz", "key-secret", "abcdefghijklmnopqrstuvwxyz",
		"hunter2", "token=opaque", "opaque-cookie",
		"sensitive-key-value",
	} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("redacted JSON leaked %q: %s", forbidden, got)
		}
	}
	for _, expected := range []string{"/tmp/diagnostic.txt", "[FILTERED]", "[REDACTED]", "[PRIVATE KEY REDACTED]"} {
		if !strings.Contains(got, expected) {
			t.Fatalf("redacted JSON lacks %q: %s", expected, got)
		}
	}
	if len(got) > 1024 || !utf8.ValidString(got) {
		t.Fatalf("bounded JSON length/encoding = %d, valid=%v", len(got), utf8.ValidString(got))
	}

	truncated := redactor.RedactText(strings.Repeat("界", 20), 17)
	if len(truncated) > 17 || !utf8.ValidString(truncated) {
		t.Fatalf("bounded text length/encoding = %d, valid=%v", len(truncated), utf8.ValidString(truncated))
	}
}

func TestRedactorContainsFilterPanicsWithoutLeakingInput(t *testing.T) {
	redactor := Redactor{
		Filter: func(string) string {
			panic("filter failure")
		},
	}
	if got := redactor.RedactText("content", 100); got != "" {
		t.Fatalf("RedactText() = %q", got)
	}
	if got := redactor.RedactJSON(map[string]any{"content": "value"}, 100); strings.Contains(got, "value") {
		t.Fatalf("RedactJSON() leaked input: %q", got)
	}
}

func TestRedactorBoundsWorkAndRejectsCustomJSONTypes(t *testing.T) {
	oversizedToken := "prefix ghp_" + strings.Repeat("a", 4096)
	got := (Redactor{}).RedactText(oversizedToken, 128)
	if len(got) > 128 || strings.Contains(got, "ghp_") {
		t.Fatalf("oversized token preview = %q", got)
	}
	if got := (Redactor{}).RedactText("value", 0); got != "" {
		t.Fatalf("zero-bound preview = %q", got)
	}
	got = (Redactor{}).RedactJSON(map[string]any{"custom": panicJSONValue{}}, 128)
	if !strings.Contains(got, "[UNSUPPORTED]") {
		t.Fatalf("custom JSON preview = %q", got)
	}
}

func TestRedactorChargesSensitiveMapEntriesToNodeBudget(t *testing.T) {
	input := make(map[string]any, maxRedactionNodes+100)
	for index := 0; index < maxRedactionNodes+100; index++ {
		input[fmt.Sprintf("token_%04d", index)] = "secret"
	}
	got := (Redactor{}).RedactJSON(input, 1<<20)
	if !strings.Contains(got, "[TRUNCATED]") {
		t.Fatalf("large sensitive map was not truncated")
	}
	if count := strings.Count(got, "[REDACTED]"); count >= len(input) {
		t.Fatalf("redacted entries = %d, want fewer than %d", count, len(input))
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

func TestStoreLoadClassifiesInvalidStoredContentAsCorrupt(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "corrupt-trace.json")
	if err := os.WriteFile(path, []byte(`{"truncated":`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := (Store{Root: root}).Load("corrupt-trace")
	var corrupt *CorruptTraceError
	if !errors.As(err, &corrupt) || corrupt.TraceID != "corrupt-trace" {
		t.Fatalf("Load error = %v, want CorruptTraceError", err)
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
