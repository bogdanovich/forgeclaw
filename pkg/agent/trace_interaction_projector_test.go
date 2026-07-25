package agent

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
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
		firstTrace.Metadata.TraceKind != evaltrace.TraceKindInteraction ||
		firstTrace.Metadata.ProjectionRevision != 2 {
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
	if payload.CodeHash == "" ||
		payload.EventType != string(interactions.EventCancelled) {
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

func TestInteractionTraceSourceLoadsExactPendingRevision(t *testing.T) {
	workspace := t.TempDir()
	now := time.Date(2026, 7, 24, 8, 0, 0, 0, time.UTC)
	registry := newInteractionTraceRegistry(workspace, now)
	if err := registry.SetTraceCaptureProtection(true, 32); err != nil {
		t.Fatal(err)
	}
	record := cancelInteractionForTrace(
		t, registry, "interaction-source", "session-source", now,
	)
	source := &interactionTraceSource{
		workspace: workspace,
		registry:  registry,
		settings:  traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
	}
	keys, err := source.Pending(context.Background(), 10)
	if err != nil || len(keys) != 1 {
		t.Fatalf("pending = (%v, %v)", keys, err)
	}
	candidate, ok, err := source.LoadLatest(context.Background(), keys[0])
	if err != nil || !ok ||
		candidate.Revision != uint64(record.LastEventSeq) ||
		candidate.Trace.Metadata.ProjectionRevision != uint64(record.LastEventSeq) ||
		candidate.Trace.Metadata.TraceKind != evaltrace.TraceKindInteraction {
		t.Fatalf("candidate = (%#v, %v, %v)", candidate, ok, err)
	}
}

func TestTraceCaptureManagerPersistsAndAcknowledgesInteraction(t *testing.T) {
	workspace := t.TempDir()
	eventBus := runtimeevents.NewBus()
	cfg := traceTestConfig(workspace)
	manager := newTraceCaptureManager(cfg, eventBus)
	al := &AgentLoop{cfg: cfg, traceCapture: manager}
	registry := al.interactionRegistryForWorkspace(workspace)
	now := time.Now().UTC()
	record := cancelInteractionForTrace(
		t, registry, "interaction-manager", "session-manager", now,
	)
	waitForInteractionTraceMarkerCleared(t, registry, record.ID)
	path := waitForTraceFile(t, workspace)
	manager.close()
	if err := eventBus.Close(); err != nil {
		t.Fatal(err)
	}
	trace := readCapturedTrace(t, path)
	if trace.Metadata.TraceKind != evaltrace.TraceKindInteraction ||
		trace.Metadata.ProjectionRevision != uint64(record.LastEventSeq) ||
		trace.Policy.ContentMode != evaltrace.ContentMetadataOnly ||
		trace.Outcome == nil ||
		trace.Outcome.Status != string(interactions.StatusCancelled) {
		t.Fatalf("persisted interaction trace = %#v", trace)
	}
	reloaded := interactions.NewRegistry(
		interactions.WorkspaceStorePath(workspace),
	)
	stored, ok := reloaded.Get(record.ID)
	if !ok || stored.TraceCapturePending {
		t.Fatalf("acknowledged interaction = (%#v, %v)", stored, ok)
	}
}

func TestInteractionTraceProjectorReconcilesPendingRestart(t *testing.T) {
	workspace := t.TempDir()
	now := time.Now().UTC()
	registry := newInteractionTraceRegistry(workspace, now)
	if err := registry.SetTraceCaptureProtection(true, 32); err != nil {
		t.Fatal(err)
	}
	record := cancelInteractionForTrace(
		t, registry, "interaction-restart", "session-restart", now,
	)
	if err := registry.SetTraceCaptureProtection(false, 0); err != nil {
		t.Fatal(err)
	}
	pending, ok := registry.Get(record.ID)
	if !ok || !pending.TraceCapturePending {
		t.Fatalf("restart source is not durable: (%#v, %v)", pending, ok)
	}

	eventBus := runtimeevents.NewBus()
	cfg := traceTestConfig(workspace)
	manager := newTraceCaptureManager(cfg, eventBus)
	al := &AgentLoop{cfg: cfg, traceCapture: manager}
	reloaded := al.interactionRegistryForWorkspace(workspace)
	waitForInteractionTraceMarkerCleared(t, reloaded, record.ID)
	trace := readCapturedTrace(t, waitForTraceFile(t, workspace))
	manager.close()
	if err := eventBus.Close(); err != nil {
		t.Fatal(err)
	}
	if trace.Metadata.ProjectionRevision != uint64(record.LastEventSeq) ||
		trace.Outcome == nil ||
		trace.Outcome.Status != string(interactions.StatusCancelled) {
		t.Fatalf("restart trace = %#v", trace)
	}
}

func TestInteractionTraceShutdownDrainsRegistryPendingSource(t *testing.T) {
	workspace := t.TempDir()
	eventBus := runtimeevents.NewBus()
	cfg := traceTestConfig(workspace)
	manager := newTraceCaptureManager(cfg, eventBus)
	al := &AgentLoop{cfg: cfg, traceCapture: manager}
	registry := al.interactionRegistryForWorkspace(workspace)
	now := time.Now().UTC()
	record := cancelInteractionForTrace(
		t, registry, "interaction-shutdown", "session-shutdown", now,
	)

	manager.close()
	if err := eventBus.Close(); err != nil {
		t.Fatal(err)
	}
	trace := readCapturedTrace(t, waitForTraceFile(t, workspace))
	if trace.Metadata.ProjectionRevision != uint64(record.LastEventSeq) {
		t.Fatalf("shutdown trace revision = %d", trace.Metadata.ProjectionRevision)
	}
	reloaded := interactions.NewRegistry(
		interactions.WorkspaceStorePath(workspace),
	)
	stored, ok := reloaded.Get(record.ID)
	if !ok || stored.TraceCapturePending {
		t.Fatalf("shutdown confirmation = (%#v, %v)", stored, ok)
	}
}

func TestInteractionTraceShutdownFindsTerminalFromAnotherRegistryInstance(
	t *testing.T,
) {
	workspace := t.TempDir()
	eventBus := runtimeevents.NewBus()
	cfg := traceTestConfig(workspace)
	manager := newTraceCaptureManager(cfg, eventBus)
	al := &AgentLoop{cfg: cfg, traceCapture: manager}
	_ = al.interactionRegistryForWorkspace(workspace)
	other := interactions.NewRegistry(
		interactions.WorkspaceStorePath(workspace),
	)
	now := time.Now().UTC()
	record := cancelInteractionForTrace(
		t,
		other,
		"interaction-cross-instance-shutdown",
		"session-cross-instance-shutdown",
		now,
	)
	manager.close()
	if err := eventBus.Close(); err != nil {
		t.Fatal(err)
	}
	trace := readCapturedTrace(t, waitForTraceFile(t, workspace))
	if trace.Metadata.ProjectionRevision != uint64(record.LastEventSeq) ||
		trace.Outcome == nil ||
		trace.Outcome.Status != string(interactions.StatusCancelled) {
		t.Fatalf("cross-instance shutdown trace = %#v", trace)
	}
	reloaded := interactions.NewRegistry(
		interactions.WorkspaceStorePath(workspace),
	)
	stored, ok := reloaded.Get(record.ID)
	if !ok || stored.TraceCapturePending {
		t.Fatalf("cross-instance shutdown confirmation = (%#v, %v)", stored, ok)
	}
}

func TestInteractionTraceShutdownPreservesActiveJournal(t *testing.T) {
	workspace := t.TempDir()
	eventBus := runtimeevents.NewBus()
	cfg := traceTestConfig(workspace)
	manager := newTraceCaptureManager(cfg, eventBus)
	al := &AgentLoop{cfg: cfg, traceCapture: manager}
	registry := al.interactionRegistryForWorkspace(workspace)
	now := time.Now().UTC()
	record, err := registry.Create(interactions.CreateRequest{
		ID:   "interaction-active-shutdown",
		Kind: interactions.KindQuestion,
		Route: interactions.Route{
			AgentID: "main", SessionKey: "session-active-shutdown",
			Channel: "telegram", ChatID: "chat-active-shutdown",
			SenderID: "sender-active-shutdown",
		},
		Origin: interactions.Origin{
			TurnID: "turn-active-shutdown", ToolCallID: "call-active-shutdown",
			ToolName: "request_user_input",
		},
		Questions: []interactions.Question{{
			ID: "environment", Question: "Which environment?",
		}},
		ExpiresAt: now.Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	manager.close()
	if err := eventBus.Close(); err != nil {
		t.Fatal(err)
	}
	reloaded := interactions.NewRegistry(
		interactions.WorkspaceStorePath(workspace),
	)
	stored, journal, ok := reloaded.GetTraceCapture(record.ID)
	if err := reloaded.LastLoadError(); err != nil || !ok ||
		stored.TraceCapturePending || len(journal) == 0 ||
		journal[len(journal)-1].Sequence != stored.LastEventSeq {
		t.Fatalf(
			"active shutdown journal = (%#v, %#v, %v, %v)",
			stored,
			journal,
			ok,
			err,
		)
	}
}

func TestReconcileInteractionTraceDoesNotDowngradeSameRevision(t *testing.T) {
	workspace := t.TempDir()
	settings := traceCaptureSettingsFromConfig(traceTestConfig(workspace))
	startedAt := time.Now().UTC()
	record := interactionTraceRecord(
		"interaction-reconcile", "session-reconcile", startedAt,
	)
	record.Status = interactions.StatusCancelled
	record.Revision = 2
	record.LastEventSeq = 2
	record.Outcome = interactions.OutcomeCanceled
	complete, _ := buildInteractionTrace(settings, workspace, record, []interactions.Event{
		interactionTraceEvent(record, 1, 1, interactions.EventCreated),
		interactionTraceEvent(record, 2, 2, interactions.EventCancelled),
	})
	completeTrace := finalizeInteractionTrace(t, complete)
	policy := evalcapture.Policy{
		Root: traceStoreRoot(settings, workspace),
	}
	if _, err := (evaltrace.Store{Root: policy.Root}).Save(completeTrace); err != nil {
		t.Fatal(err)
	}
	incomplete, _ := buildInteractionTrace(settings, workspace, record, []interactions.Event{
		interactionTraceEvent(record, 2, 2, interactions.EventCancelled),
	})
	selected, persist, err := reconcileStoredInteractionTrace(
		policy,
		finalizeInteractionTrace(t, incomplete),
	)
	if err != nil || persist || selected.Truncation.Incomplete {
		t.Fatalf("reconciliation downgraded trace: (%#v, %v, %v)", selected, persist, err)
	}
}

func TestReconcileInteractionTracePreservesPrependedSameRevisionEvidence(
	t *testing.T,
) {
	workspace := t.TempDir()
	settings := traceCaptureSettingsFromConfig(traceTestConfig(workspace))
	startedAt := time.Now().UTC()
	record := interactionTraceRecord(
		"interaction-prepended", "session-prepended", startedAt,
	)
	record.Status = interactions.StatusCancelled
	record.Revision = 5
	record.LastEventSeq = 5
	record.Outcome = interactions.OutcomeCanceled
	events := []interactions.Event{
		interactionTraceEvent(record, 2, 2, interactions.EventWaiting),
		interactionTraceEvent(record, 3, 3, interactions.EventAnswerClaimed),
		interactionTraceEvent(record, 4, 4, interactions.EventResumeStarted),
		interactionTraceEvent(record, 5, 5, interactions.EventCancelled),
	}
	short, _ := buildInteractionTrace(settings, workspace, record, events[2:])
	shortTrace := finalizeInteractionTrace(t, short)
	policy := evalcapture.Policy{
		Root: traceStoreRoot(settings, workspace),
	}
	if _, err := (evaltrace.Store{Root: policy.Root}).Save(shortTrace); err != nil {
		t.Fatal(err)
	}
	richer, _ := buildInteractionTrace(settings, workspace, record, events)
	richerTrace := finalizeInteractionTrace(t, richer)
	selected, persist, err := reconcileStoredInteractionTrace(
		policy,
		richerTrace,
	)
	if err != nil || !persist ||
		len(selected.Records) != len(richerTrace.Records) ||
		selected.Records[0].Origin != richerTrace.Records[0].Origin {
		t.Fatalf(
			"prepended evidence reconciliation = (%#v, %v, %v)",
			selected,
			persist,
			err,
		)
	}
}

func waitForInteractionTraceMarkerCleared(
	t *testing.T,
	registry *interactions.Registry,
	interactionID string,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		record, ok := registry.Get(interactionID)
		if ok && !record.TraceCapturePending {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	record, _ := registry.Get(interactionID)
	t.Fatalf("interaction trace marker did not clear: %#v", record)
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

func newInteractionTraceRegistry(
	workspace string,
	now time.Time,
) *interactions.Registry {
	return interactions.NewRegistryWithOptions(
		interactions.WorkspaceStorePath(workspace),
		interactions.Options{Now: func() time.Time { return now }},
	)
}

func cancelInteractionForTrace(
	t *testing.T,
	registry *interactions.Registry,
	id string,
	session string,
	now time.Time,
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
		ExpiresAt: now.Add(24 * time.Hour),
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
