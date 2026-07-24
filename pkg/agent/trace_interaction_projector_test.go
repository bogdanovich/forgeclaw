package agent

import (
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/evalcapture"
	"github.com/sipeed/picoclaw/pkg/evaltrace"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/interactions"
)

func TestBuildInteractionTraceIsDeterministicAndMetadataOnly(t *testing.T) {
	startedAt := time.Date(2026, 7, 24, 7, 0, 0, 0, time.UTC)
	settings := traceCaptureSettingsFromConfig(traceTestConfig(t.TempDir()))
	settings.contentMode = evaltrace.ContentRedacted
	record := interactionTraceRecord(
		"interaction-secret", "session-secret", startedAt,
	)
	record.Status = interactions.StatusCancelled
	record.Outcome = interactions.OutcomeCanceled
	record.Revision = 2
	record.LastEventSeq = 2
	record.Questions = []interactions.Question{{
		ID: "secret", Question: "production-password",
	}}
	record.Answer = &interactions.Answer{Text: "answer-secret"}
	record.PromptSummary = "summary-secret"
	record.ApprovalAction = "approval-secret"
	events := []interactions.Event{
		interactionTraceEvent(record, 1, 1, interactions.EventCreated),
		interactionTraceEvent(record, 2, 2, interactions.EventCancelled),
	}
	events[1].Code = "diagnostic-secret"

	first, evidence := buildInteractionTrace(
		settings, "/workspace/one", record, events,
	)
	second, _ := buildInteractionTrace(
		settings, "/workspace/one", record, events,
	)
	firstTrace := finalizeInteractionTrace(t, first)
	secondTrace := finalizeInteractionTrace(t, second)
	if !reflect.DeepEqual(firstTrace, secondTrace) {
		t.Fatal("same durable interaction history produced different traces")
	}
	if !evidence.Complete || evidence.FirstSequence != 1 ||
		evidence.LastSequence != 2 || evidence.LastRevision != 2 {
		t.Fatalf("evidence = %#v", evidence)
	}
	if firstTrace.Policy.ContentMode != evaltrace.ContentMetadataOnly ||
		firstTrace.Policy.Redactor != "" ||
		firstTrace.Metadata.TraceKind != evaltrace.TraceKindInteraction {
		t.Fatalf("interaction trace policy = %#v", firstTrace)
	}
	data, err := json.Marshal(firstTrace)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{
		"session-secret",
		"production-password",
		"answer-secret",
		"summary-secret",
		"approval-secret",
		"diagnostic-secret",
	} {
		if strings.Contains(string(data), secret) {
			t.Fatalf("interaction trace leaked %q: %s", secret, data)
		}
	}
	var payload evaltrace.InteractionPayload
	if err := json.Unmarshal(firstTrace.Records[1].Data, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.CodeHash == "" || payload.EventType != string(interactions.EventCancelled) {
		t.Fatalf("terminal payload = %#v", payload)
	}
}

func TestBuildInteractionTraceUsesWorkspaceInIdentity(t *testing.T) {
	startedAt := time.Date(2026, 7, 24, 7, 0, 0, 0, time.UTC)
	record := interactionTraceRecord("interaction-shared", "session-shared", startedAt)
	events := []interactions.Event{
		interactionTraceEvent(record, 1, 1, interactions.EventCreated),
	}
	settings := traceCaptureSettingsFromConfig(traceTestConfig(t.TempDir()))
	left, _ := buildInteractionTrace(settings, "/workspace/a", record, events)
	right, _ := buildInteractionTrace(settings, "/workspace/b", record, events)
	if left.builder.TraceID() == right.builder.TraceID() {
		t.Fatalf("workspace collision produced trace ID %q", left.builder.TraceID())
	}
}

func TestInteractionTraceProjectorProjectsLiveTerminalHistory(t *testing.T) {
	workspace := t.TempDir()
	registry := newInteractionTraceRegistry(time.Date(2026, 7, 24, 8, 0, 0, 0, time.UTC))
	traces, submit := collectInteractionTraces(t)
	projector := newInteractionTraceProjector(
		traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
		submit,
	)
	projector.current = func(traceCaptureSettings, string, evaltrace.Trace) bool {
		return false
	}
	t.Cleanup(projector.close)
	projector.attach(workspace, registry)

	record := cancelInteractionForTrace(
		t, registry, "interaction-live", "session-live",
	)
	got := traces()
	if len(got) != 1 || got[0].Outcome == nil ||
		got[0].Outcome.Status != string(interactions.StatusCancelled) ||
		len(got[0].Records) != int(record.LastEventSeq) {
		t.Fatalf("live traces = %#v", got)
	}
	want, _ := buildInteractionTrace(
		traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
		workspace,
		record,
		registry.ListEvents(record.ID),
	)
	if !reflect.DeepEqual(got[0], finalizeInteractionTrace(t, want)) {
		t.Fatal("live projection differs from deterministic durable-history build")
	}
}

func TestInteractionTraceProjectorReconcilesTerminalSnapshot(t *testing.T) {
	workspace := t.TempDir()
	registry := newInteractionTraceRegistry(time.Date(2026, 7, 24, 9, 0, 0, 0, time.UTC))
	record := cancelInteractionForTrace(
		t, registry, "interaction-startup", "session-startup",
	)
	traces, submit := collectInteractionTraces(t)
	projector := newInteractionTraceProjector(
		traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
		submit,
	)
	projector.current = func(traceCaptureSettings, string, evaltrace.Trace) bool {
		return false
	}
	t.Cleanup(projector.close)
	projector.attach(workspace, registry)

	got := traces()
	if len(got) != 1 || len(got[0].Records) != int(record.LastEventSeq) {
		t.Fatalf("startup traces = %#v", got)
	}
}

func TestInteractionTraceProjectorSkipsCurrentTerminalTrace(t *testing.T) {
	workspace := t.TempDir()
	registry := newInteractionTraceRegistry(time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC))
	cancelInteractionForTrace(
		t, registry, "interaction-current", "session-current",
	)
	var submitted atomic.Int32
	projector := newInteractionTraceProjector(
		traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
		func(traceCaptureSettings, *activeTraceCapture) error {
			submitted.Add(1)
			return nil
		},
	)
	projector.current = func(traceCaptureSettings, string, evaltrace.Trace) bool {
		return true
	}
	t.Cleanup(projector.close)
	projector.attach(workspace, registry)
	if submitted.Load() != 0 {
		t.Fatalf("current terminal trace was resubmitted %d time(s)", submitted.Load())
	}
}

func TestInteractionTraceProjectorBuffersLiveCommitDuringReconciliation(t *testing.T) {
	workspace := t.TempDir()
	registry := newInteractionTraceRegistry(time.Date(2026, 7, 24, 11, 0, 0, 0, time.UTC))
	cancelInteractionForTrace(
		t, registry, "interaction-snapshot", "session-snapshot",
	)
	traces, submit := collectInteractionTraces(t)
	projector := newInteractionTraceProjector(
		traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
		submit,
	)
	entered := make(chan struct{})
	release := make(chan struct{})
	projector.current = func(traceCaptureSettings, string, evaltrace.Trace) bool {
		close(entered)
		<-release
		return true
	}
	attached := make(chan struct{})
	go func() {
		projector.attach(workspace, registry)
		close(attached)
	}()
	<-entered
	cancelInteractionForTrace(
		t, registry, "interaction-post-boundary", "session-post-boundary",
	)
	if got := traces(); len(got) != 0 {
		t.Fatalf("live event escaped before snapshot activation: %#v", got)
	}
	close(release)
	select {
	case <-attached:
	case <-time.After(time.Second):
		t.Fatal("projector did not finish gated startup reconciliation")
	}
	t.Cleanup(projector.close)
	got := traces()
	if len(got) != 1 ||
		got[0].Records[0].Correlation.InteractionID != "interaction-post-boundary" {
		t.Fatalf("buffered live trace = %#v", got)
	}
}

func TestInteractionTraceProjectorRetriesCapacityWithoutNewEvent(t *testing.T) {
	workspace := t.TempDir()
	registry := newInteractionTraceRegistry(time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC))
	cancelInteractionForTrace(
		t, registry, "interaction-retry", "session-retry",
	)
	var attempts atomic.Int32
	completed := make(chan struct{})
	projector := newInteractionTraceProjector(
		traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
		func(_ traceCaptureSettings, active *activeTraceCapture) error {
			if attempts.Add(1) == 1 {
				return &evalcapture.AdmissionError{
					Reason: evalcapture.ReasonCapacity,
					Class:  evalcapture.ClassCritical,
				}
			}
			_ = finalizeInteractionTrace(t, active)
			select {
			case <-completed:
			default:
				close(completed)
			}
			return nil
		},
	)
	projector.current = func(traceCaptureSettings, string, evaltrace.Trace) bool {
		return false
	}
	t.Cleanup(projector.close)
	projector.attach(workspace, registry)
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("capacity-rejected interaction trace was not retried")
	}
	if attempts.Load() < 2 {
		t.Fatalf("submission attempts = %d", attempts.Load())
	}
}

func TestInteractionTraceProjectorMarksPrunedHistoryIncomplete(t *testing.T) {
	workspace := t.TempDir()
	now := time.Date(2026, 7, 24, 13, 0, 0, 0, time.UTC)
	registry := interactions.NewRegistryWithOptions("", interactions.Options{
		Now: func() time.Time { return now }, MaxEvents: 1,
	})
	record := cancelInteractionForTrace(
		t, registry, "interaction-pruned", "session-pruned",
	)
	traces, submit := collectInteractionTraces(t)
	projector := newInteractionTraceProjector(
		traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
		submit,
	)
	projector.current = func(traceCaptureSettings, string, evaltrace.Trace) bool {
		return false
	}
	t.Cleanup(projector.close)
	projector.attach(workspace, registry)
	got := traces()
	if len(got) != 1 || !got[0].Truncation.Incomplete ||
		got[0].Truncation.DroppedRecords != int(record.LastEventSeq-1) ||
		len(got[0].Records) != 1 {
		t.Fatalf("pruned trace = %#v", got)
	}
}

func TestInteractionTraceProjectorReconcilesAfterEnable(t *testing.T) {
	workspace := t.TempDir()
	registry := newInteractionTraceRegistry(time.Date(2026, 7, 24, 14, 0, 0, 0, time.UTC))
	traces, submit := collectInteractionTraces(t)
	projector := newInteractionTraceProjector(traceCaptureSettings{}, submit)
	projector.current = func(traceCaptureSettings, string, evaltrace.Trace) bool {
		return false
	}
	t.Cleanup(projector.close)
	projector.attach(workspace, registry)
	cancelInteractionForTrace(
		t, registry, "interaction-disabled", "session-disabled",
	)
	if got := traces(); len(got) != 0 {
		t.Fatalf("disabled projector submitted traces: %#v", got)
	}
	projector.updateSettings(
		traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
	)
	if got := traces(); len(got) != 1 {
		t.Fatalf("enable reconciliation traces = %#v", got)
	}
}

func TestTraceCaptureManagerPersistsTerminalInteractionSnapshot(t *testing.T) {
	workspace := t.TempDir()
	eventBus := runtimeevents.NewBus()
	cfg := traceTestConfig(workspace)
	manager := newTraceCaptureManager(cfg, eventBus)
	al := &AgentLoop{cfg: cfg, traceCapture: manager}
	registry := al.interactionRegistryForWorkspace(workspace)
	cancelInteractionForTrace(
		t, registry, "interaction-manager", "session-manager",
	)
	path := waitForTraceFile(t, workspace)
	manager.close()
	if err := eventBus.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var trace evaltrace.Trace
	if unmarshalErr := json.Unmarshal(data, &trace); unmarshalErr != nil {
		t.Fatal(unmarshalErr)
	}
	if trace.Metadata.TraceKind != evaltrace.TraceKindInteraction ||
		trace.Policy.ContentMode != evaltrace.ContentMetadataOnly ||
		trace.Outcome == nil ||
		trace.Outcome.Status != string(interactions.StatusCancelled) {
		t.Fatalf("persisted interaction trace = %#v", trace)
	}
	reloaded := interactions.NewRegistry(interactions.WorkspaceStorePath(workspace))
	reloadedRecord, ok := reloaded.Get("interaction-manager")
	if !ok {
		t.Fatal("reloaded interaction record is missing")
	}
	rebuilt, _ := buildInteractionTrace(
		traceCaptureSettingsFromConfig(cfg),
		workspace,
		reloadedRecord,
		reloaded.ListEvents(reloadedRecord.ID),
	)
	rebuiltTrace := finalizeInteractionTrace(t, rebuilt)
	if !interactionTracesEqual(trace, rebuiltTrace) {
		t.Fatalf("live trace differs from restart rebuild:\nlive=%#v\nrebuilt=%#v", trace, rebuiltTrace)
	}
	oldModTime := time.Now().Add(-time.Minute).Truncate(time.Second)
	if chtimesErr := os.Chtimes(path, oldModTime, oldModTime); chtimesErr != nil {
		t.Fatal(chtimesErr)
	}
	secondBus := runtimeevents.NewBus()
	secondManager := newTraceCaptureManager(cfg, secondBus)
	secondLoop := &AgentLoop{cfg: cfg, traceCapture: secondManager}
	_ = secondLoop.interactionRegistryForWorkspace(workspace)
	secondManager.close()
	if closeErr := secondBus.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.ModTime().Equal(oldModTime) {
		t.Fatalf("current interaction trace was rewritten at %s", info.ModTime())
	}
}

func TestInteractionTraceProjectorSeparatesIdenticalIDsAcrossWorkspaces(t *testing.T) {
	workspaceA := t.TempDir()
	workspaceB := t.TempDir()
	now := time.Date(2026, 7, 24, 15, 0, 0, 0, time.UTC)
	registryA := newInteractionTraceRegistry(now)
	registryB := newInteractionTraceRegistry(now)
	traces, submit := collectInteractionTraces(t)
	projector := newInteractionTraceProjector(
		traceCaptureSettingsFromConfig(traceTestConfig(workspaceA)),
		submit,
	)
	projector.current = func(traceCaptureSettings, string, evaltrace.Trace) bool {
		return false
	}
	t.Cleanup(projector.close)
	projector.attach(workspaceA, registryA)
	projector.attach(workspaceB, registryB)
	cancelInteractionForTrace(
		t, registryA, "interaction-shared", "session-a",
	)
	cancelInteractionForTrace(
		t, registryB, "interaction-shared", "session-b",
	)
	got := traces()
	if len(got) != 2 || got[0].TraceID == got[1].TraceID {
		t.Fatalf("workspace traces = %#v", got)
	}
}

func collectInteractionTraces(
	t *testing.T,
) (func() []evaltrace.Trace, func(traceCaptureSettings, *activeTraceCapture) error) {
	t.Helper()
	var mu sync.Mutex
	var traces []evaltrace.Trace
	return func() []evaltrace.Trace {
			mu.Lock()
			defer mu.Unlock()
			return append([]evaltrace.Trace(nil), traces...)
		}, func(_ traceCaptureSettings, active *activeTraceCapture) error {
			trace := finalizeInteractionTrace(t, active)
			mu.Lock()
			traces = append(traces, trace)
			mu.Unlock()
			return nil
		}
}

func finalizeInteractionTrace(
	t *testing.T,
	active *activeTraceCapture,
) evaltrace.Trace {
	t.Helper()
	if active == nil || active.builder == nil {
		t.Fatal("interaction trace builder is unavailable")
	}
	trace, err := active.builder.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	return trace
}

func newInteractionTraceRegistry(now time.Time) *interactions.Registry {
	return interactions.NewRegistryWithOptions("", interactions.Options{
		Now: func() time.Time { return now },
	})
}

func cancelInteractionForTrace(
	t *testing.T,
	registry *interactions.Registry,
	id string,
	session string,
) interactions.Record {
	t.Helper()
	record, err := registry.Create(interactions.CreateRequest{
		ID:   id,
		Kind: interactions.KindQuestion,
		Route: interactions.Route{
			AgentID: "main", SessionKey: session, Channel: "telegram",
			ChatID: "chat-" + session, SenderID: "sender-" + session,
		},
		Origin: interactions.Origin{
			TurnID: "turn-" + session, ToolCallID: "call-" + session,
			ToolName: "request_user_input",
		},
		Questions: []interactions.Question{{
			ID: "environment", Question: "Which environment?",
		}},
		ExpiresAt: time.Now().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	record, err = registry.Cancel(record.ID, record.Revision, "test_cancel")
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func interactionTraceRecord(
	id string,
	session string,
	startedAt time.Time,
) interactions.Record {
	return interactions.Record{
		ID: id, Kind: interactions.KindQuestion,
		Status: interactions.StatusCreated, Revision: 1, LastEventSeq: 1,
		Route: interactions.Route{
			AgentID: "main", SessionKey: session, Channel: "telegram",
			ChatID: "chat-secret", SenderID: "sender-secret",
		},
		Origin: interactions.Origin{
			TurnID: "turn-1", ToolCallID: "call-1",
			ToolName: "request_user_input", TaskID: "task-1",
		},
		CreatedAt: startedAt.UnixMilli(), UpdatedAt: startedAt.UnixMilli(),
	}
}

func interactionTraceEvent(
	record interactions.Record,
	sequence int64,
	revision int64,
	eventType interactions.EventType,
) interactions.Event {
	status := interactions.StatusCreated
	from := interactions.Status("")
	if eventType == interactions.EventCancelled {
		status = interactions.StatusCancelled
		from = interactions.StatusCreated
	}
	return interactions.Event{
		SchemaVersion:  interactions.EventSchemaVersion,
		EventID:        record.ID + ":" + string(eventType),
		CommitSequence: uint64(sequence),
		InteractionID:  record.ID,
		Type:           eventType,
		From:           from,
		To:             status,
		Outcome:        record.Outcome,
		Revision:       revision,
		Sequence:       sequence,
		EmittedAt:      record.CreatedAt + sequence - 1,
	}
}

func TestInteractionTraceAdmissionOnlyRetriesCapacity(t *testing.T) {
	if interactionTraceAdmissionCanRetry(errors.New("storage failed")) {
		t.Fatal("ordinary error was classified as retryable admission")
	}
	if !interactionTraceAdmissionCanRetry(&evalcapture.AdmissionError{
		Reason: evalcapture.ReasonCapacity,
	}) {
		t.Fatal("capacity rejection was not classified as retryable")
	}
}
