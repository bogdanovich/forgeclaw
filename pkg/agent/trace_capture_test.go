package agent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/evaltrace"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/interactions"
	taskregistry "github.com/sipeed/picoclaw/pkg/tasks"
)

func TestTraceCaptureRecordsBoundedRedactedTurn(t *testing.T) {
	workspace := t.TempDir()
	cfg := traceTestConfig(workspace)
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(cfg, eventBus)
	t.Cleanup(func() {
		manager.close()
		_ = eventBus.Close()
	})

	secret := "sk-secret-that-must-not-appear"
	start := time.Now().UTC()
	scope := runtimeevents.Scope{
		AgentID:    "main",
		SessionKey: "session:" + secret,
		TurnID:     "turn-1",
		Channel:    "telegram",
		ChatID:     "chat-1",
	}
	publishCaptureEvent(t, eventBus, runtimeevents.Event{
		ID: "evt-start", Kind: runtimeevents.KindAgentTurnStart, Time: start,
		Source: runtimeevents.Source{Component: "agent"}, Scope: scope,
		Payload: TurnStartPayload{UserMessage: "use " + secret, Workspace: workspace},
	})
	publishCaptureEvent(t, eventBus, runtimeevents.Event{
		ID: "evt-tool", Kind: runtimeevents.KindAgentToolExecStart, Time: start.Add(time.Millisecond),
		Source: runtimeevents.Source{Component: "agent"}, Scope: scope,
		Payload: ToolExecStartPayload{
			Tool:      "read_file",
			Arguments: map[string]any{"token": secret},
		},
	})
	publishCaptureEvent(t, eventBus, runtimeevents.Event{
		ID: "evt-fallback", Kind: runtimeevents.KindAgentLLMFallbackAttempt, Time: start.Add(2 * time.Millisecond),
		Source: runtimeevents.Source{Component: "agent"}, Scope: scope,
		Payload: LLMFallbackAttemptPayload{
			Provider:    "openai",
			Model:       "fallback",
			IdentityKey: "model:fallback",
			Attempt:     2,
			Status:      "succeeded",
		},
	})
	publishCaptureEvent(t, eventBus, runtimeevents.Event{
		ID: "evt-context", Kind: runtimeevents.KindAgentContextSnapshot, Time: start.Add(3 * time.Millisecond),
		Source: runtimeevents.Source{Component: "agent"}, Scope: scope,
		Payload: ContextSnapshotPayload{
			MessageCount:     3,
			SnapshotHash:     "snapshot-hash",
			GoalHash:         "goal-hash",
			ToolPairingValid: true,
		},
	})
	publishCaptureEvent(t, eventBus, runtimeevents.Event{
		ID: "evt-end", Kind: runtimeevents.KindAgentTurnEnd, Time: start.Add(4 * time.Millisecond),
		Source: runtimeevents.Source{Component: "agent"}, Scope: scope,
		Payload: TurnEndPayload{
			Status:          TurnEndStatusCompleted,
			Workspace:       workspace,
			FinalContent:    "done " + secret,
			FinalContentLen: 12,
		},
	})

	tracePath := waitForTraceFile(t, workspace)
	data, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), secret) {
		t.Fatalf("trace leaked secret: %s", data)
	}
	var trace evaltrace.Trace
	if err := json.Unmarshal(data, &trace); err != nil {
		t.Fatalf("decode trace: %v", err)
	}
	if err := evaltrace.Validate(trace); err != nil {
		t.Fatalf("validate trace: %v", err)
	}
	if trace.Metadata.TraceKind != evaltrace.TraceKindTurn {
		t.Fatalf("trace kind = %q", trace.Metadata.TraceKind)
	}
	if trace.Outcome == nil || trace.Outcome.Status != string(TurnEndStatusCompleted) {
		t.Fatalf("outcome = %#v", trace.Outcome)
	}
	if len(trace.Records) != 5 {
		t.Fatalf("records = %d, want 5", len(trace.Records))
	}
	if mode := fileModeForTraceTest(t, tracePath); mode.Perm() != 0o600 {
		t.Fatalf("trace mode = %o", mode.Perm())
	}
}

func TestTraceCaptureDisabledWritesNothing(t *testing.T) {
	workspace := t.TempDir()
	cfg := config.DefaultConfig()
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(cfg, eventBus)
	if manager.sub != nil || manager.persistCh != nil {
		t.Fatal("disabled capture started background workers")
	}
	start := time.Now().UTC()
	scope := runtimeevents.Scope{TurnID: "turn-disabled"}
	eventBus.Publish(
		context.Background(),
		runtimeevents.Event{
			ID:      "start",
			Kind:    runtimeevents.KindAgentTurnStart,
			Time:    start,
			Scope:   scope,
			Payload: TurnStartPayload{Workspace: workspace},
		},
	)
	eventBus.Publish(
		context.Background(),
		runtimeevents.Event{
			ID:      "end",
			Kind:    runtimeevents.KindAgentTurnEnd,
			Time:    start.Add(time.Millisecond),
			Scope:   scope,
			Payload: TurnEndPayload{Workspace: workspace, Status: TurnEndStatusCompleted},
		},
	)
	manager.close()
	_ = eventBus.Close()
	if matches, _ := filepath.Glob(filepath.Join(workspace, "state", "evaluation", "traces", "*.json")); len(
		matches,
	) != 0 {
		t.Fatalf("disabled capture wrote traces: %v", matches)
	}
}

func TestTraceCaptureStartsLazilyAfterConfigEnable(t *testing.T) {
	workspace := t.TempDir()
	cfg := config.DefaultConfig()
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(cfg, eventBus)
	enabled := traceTestConfig(workspace)
	manager.updateConfig(enabled)
	if manager.sub == nil || manager.persistCh == nil {
		t.Fatal("enabling capture did not start workers")
	}
	start := time.Now().UTC()
	scope := runtimeevents.Scope{TurnID: "turn-enabled"}
	publishCaptureEvent(
		t,
		eventBus,
		runtimeevents.Event{
			ID:      "start",
			Kind:    runtimeevents.KindAgentTurnStart,
			Time:    start,
			Scope:   scope,
			Payload: TurnStartPayload{Workspace: workspace},
		},
	)
	publishCaptureEvent(
		t,
		eventBus,
		runtimeevents.Event{
			ID:      "end",
			Kind:    runtimeevents.KindAgentTurnEnd,
			Time:    start.Add(time.Millisecond),
			Scope:   scope,
			Payload: TurnEndPayload{Workspace: workspace, Status: TurnEndStatusCompleted},
		},
	)
	_ = waitForTraceFile(t, workspace)
	manager.close()
	_ = eventBus.Close()
}

func TestTraceCaptureWaitsForExpectedDeliveryOutcome(t *testing.T) {
	workspace := t.TempDir()
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(traceTestConfig(workspace), eventBus)
	t.Cleanup(func() {
		manager.close()
		_ = eventBus.Close()
	})

	start := time.Now().UTC()
	scope := runtimeevents.Scope{
		TurnID:     "turn-delivery",
		SessionKey: "session-delivery",
		Channel:    "telegram",
		ChatID:     "chat-delivery",
	}
	publishCaptureEvent(
		t,
		eventBus,
		runtimeevents.Event{
			ID:      "start",
			Kind:    runtimeevents.KindAgentTurnStart,
			Time:    start,
			Scope:   scope,
			Payload: TurnStartPayload{Workspace: workspace},
		},
	)
	publishCaptureEvent(
		t,
		eventBus,
		runtimeevents.Event{
			ID:    "end",
			Kind:  runtimeevents.KindAgentTurnEnd,
			Time:  start.Add(time.Millisecond),
			Scope: scope,
			Payload: TurnEndPayload{
				Workspace:        workspace,
				Status:           TurnEndStatusCompleted,
				DeliveryExpected: true,
			},
		},
	)
	if matches, _ := filepath.Glob(filepath.Join(workspace, "state", "evaluation", "traces", "*.json")); len(
		matches,
	) != 0 {
		t.Fatalf("trace persisted before delivery outcome: %v", matches)
	}
	publishCaptureEvent(t, eventBus, runtimeevents.Event{
		ID: "sent", Kind: runtimeevents.KindChannelMessageOutboundSent, Time: start.Add(2 * time.Millisecond),
		Scope:   runtimeevents.Scope{Channel: "telegram", ChatID: "chat-delivery"},
		Payload: channels.ChannelOutboundPayload{ContentLen: 4},
	})

	tracePath := waitForTraceFile(t, workspace)
	data, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	var trace evaltrace.Trace
	if err := json.Unmarshal(data, &trace); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, record := range trace.Records {
		found = found || record.Kind == evaltrace.RecordDeliveryOutcome
	}
	if !found {
		t.Fatalf("trace does not contain delivery outcome: %#v", trace.Records)
	}
}

func TestTraceCaptureCreatesCanonicalTerminalTaskTrace(t *testing.T) {
	workspace := t.TempDir()
	cfg := traceTestConfig(workspace)
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(cfg, eventBus)
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	manager.attachTaskRegistry(workspace, registry)
	if err := registry.Upsert(taskregistry.Record{
		TaskID: "task-1", Task: "test", RequesterSessionKey: "session-1",
		Status: taskregistry.StatusRunning, DeliveryStatus: taskregistry.DeliveryPending,
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := registry.AppendEvent("task-1", taskregistry.EventTaskDeliveryDecision, map[string]string{
		"completion_id": "completion-1", "mode": "user_only", "will_user": "true",
	}); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	if err := registry.Update("task-1", func(record *taskregistry.Record) {
		record.Status = taskregistry.StatusSucceeded
		record.DeliveryStatus = taskregistry.DeliveryDelivered
		record.LastCompletionID = "completion-1"
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	tracePath := waitForTraceFile(t, workspace)
	data, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	var trace evaltrace.Trace
	if err := json.Unmarshal(data, &trace); err != nil {
		t.Fatal(err)
	}
	if trace.Outcome == nil || trace.Outcome.Status != string(taskregistry.StatusSucceeded) {
		t.Fatalf("outcome = %#v", trace.Outcome)
	}
	if trace.Metadata.TraceKind != evaltrace.TraceKindTask {
		t.Fatalf("trace kind = %q", trace.Metadata.TraceKind)
	}
	foundDecision, foundOutcome := false, false
	for _, record := range trace.Records {
		foundDecision = foundDecision || record.Kind == evaltrace.RecordDeliveryDecision
		foundOutcome = foundOutcome || record.Kind == evaltrace.RecordDeliveryOutcome
	}
	if !foundDecision || !foundOutcome {
		t.Fatalf("missing delivery evidence: %#v", trace.Records)
	}
	manager.close()
	_ = eventBus.Close()
}

func TestTraceCaptureBackfillsDurableTaskHistoryOnRestartReconciliation(t *testing.T) {
	workspace := t.TempDir()
	initial := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	if err := initial.Upsert(taskregistry.Record{
		TaskID: "task-restart", Task: "test", RequesterSessionKey: "session-1",
		Status: taskregistry.StatusRunning, DeliveryStatus: taskregistry.DeliveryPending,
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	cfg := traceTestConfig(workspace)
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(cfg, eventBus)
	reloaded := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	manager.attachTaskRegistry(workspace, reloaded)
	if changed, err := reloaded.MarkActiveLost("runtime restarted"); err != nil || changed != 1 {
		t.Fatalf("MarkActiveLost = %d, %v", changed, err)
	}
	tracePath := waitForTraceFile(t, workspace)
	data, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	var trace evaltrace.Trace
	if err := json.Unmarshal(data, &trace); err != nil {
		t.Fatal(err)
	}
	if len(trace.Records) < 4 {
		t.Fatalf("expected backfilled and reconciled events, got %d", len(trace.Records))
	}
	if trace.Records[0].Origin.ID == "" || trace.Records[0].Kind != evaltrace.RecordTaskTransition {
		t.Fatalf("first historical record = %#v", trace.Records[0])
	}
	manager.close()
	_ = eventBus.Close()
}

func TestTraceCaptureBackfillsDurableInteractionHistoryAfterRestart(t *testing.T) {
	workspace := t.TempDir()
	secret := "answer-that-must-not-appear"
	initial := interactions.NewRegistry(interactions.WorkspaceStorePath(workspace))
	record, err := initial.Create(interactions.CreateRequest{
		ID:   "interaction-restart",
		Kind: interactions.KindQuestion,
		Route: interactions.Route{
			AgentID: "main", SessionKey: "session-secret", Channel: "telegram",
			ChatID: "chat-secret", SenderID: "sender-secret",
		},
		Origin: interactions.Origin{
			TurnID: "turn-1", ToolCallID: "call-1", ToolName: "request_user_input",
			TaskID: "task-1",
		},
		Questions: []interactions.Question{{
			ID: "environment", Question: "Which environment contains " + secret + "?",
		}},
		PromptSummary: "sensitive " + secret,
		ExpiresAt:     time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	record, err = initial.RecordDeliveryAttempt(record.ID, record.Revision, true, "")
	if err != nil {
		t.Fatal(err)
	}
	record, err = initial.MarkWaiting(record.ID, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	record, err = initial.ClaimAnswer(record.ID, record.Revision, interactions.Answer{
		Text: secret, MessageID: "message-secret",
	}, interactions.OutcomeAnswered)
	if err != nil {
		t.Fatal(err)
	}
	record, err = initial.MarkResuming(record.ID, record.Revision)
	if err != nil {
		t.Fatal(err)
	}

	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(traceTestConfig(workspace), eventBus)
	t.Cleanup(func() {
		manager.close()
		_ = eventBus.Close()
	})
	reloaded := interactions.NewRegistry(interactions.WorkspaceStorePath(workspace))
	manager.attachInteractionRegistry(workspace, reloaded)
	loaded, ok := reloaded.Get(record.ID)
	if !ok {
		t.Fatal("reloaded interaction not found")
	}
	if _, resolveErr := reloaded.Resolve(loaded.ID, loaded.Revision); resolveErr != nil {
		t.Fatal(resolveErr)
	}

	tracePath := waitForTraceFile(t, workspace)
	data, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), secret) || strings.Contains(string(data), "session-secret") ||
		strings.Contains(string(data), "chat-secret") || strings.Contains(string(data), "sender-secret") {
		t.Fatalf("interaction trace leaked private content: %s", data)
	}
	var trace evaltrace.Trace
	if err := json.Unmarshal(data, &trace); err != nil {
		t.Fatal(err)
	}
	if err := evaltrace.Validate(trace); err != nil {
		t.Fatal(err)
	}
	if trace.Metadata.TraceKind != evaltrace.TraceKindInteraction {
		t.Fatalf("trace kind = %q", trace.Metadata.TraceKind)
	}
	if trace.Outcome == nil || trace.Outcome.Status != string(interactions.StatusResolved) {
		t.Fatalf("outcome = %#v", trace.Outcome)
	}
	if len(trace.Records) != 6 {
		t.Fatalf("records = %d, want 6", len(trace.Records))
	}
	for _, item := range trace.Records {
		if item.Kind != evaltrace.RecordInteractionTransition ||
			item.Correlation.InteractionID != record.ID ||
			item.Correlation.ToolCallID != "call-1" || item.Scope.TaskID != "task-1" {
			t.Fatalf("interaction correlation = %#v", item)
		}
	}
}

func TestTraceCaptureNormalizesInteractionAgainstActiveTurnClock(t *testing.T) {
	workspace := t.TempDir()
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(traceTestConfig(workspace), eventBus)
	t.Cleanup(func() {
		manager.close()
		_ = eventBus.Close()
	})
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	registry := interactions.NewRegistryWithOptions(
		interactions.WorkspaceStorePath(workspace),
		interactions.Options{Now: func() time.Time { return now }},
	)
	manager.attachInteractionRegistry(workspace, registry)
	scope := runtimeevents.Scope{
		AgentID: "main", SessionKey: "session-1", TurnID: "turn-1",
		Channel: "telegram", ChatID: "chat-1",
	}
	publishCaptureEvent(t, eventBus, runtimeevents.Event{
		ID: "turn-start", Kind: runtimeevents.KindAgentTurnStart, Time: now,
		Scope: scope, Payload: TurnStartPayload{Workspace: workspace},
	})
	publishCaptureEvent(t, eventBus, runtimeevents.Event{
		ID: "tool-start", Kind: runtimeevents.KindAgentToolExecStart, Time: now.Add(time.Millisecond),
		Scope: scope, Payload: ToolExecStartPayload{Tool: "request_user_input"},
	})
	waitForActiveTurnRecords(t, manager, "turn-1", 2)
	now = now.Add(2 * time.Millisecond)
	if _, err := registry.Create(interactions.CreateRequest{
		ID:   "interaction-turn-clock",
		Kind: interactions.KindQuestion,
		Route: interactions.Route{
			AgentID: "main", SessionKey: "session-1", Channel: "telegram",
			ChatID: "chat-1", SenderID: "sender-1",
		},
		Origin: interactions.Origin{
			TurnID: "turn-1", ToolCallID: "call-1", ToolName: "request_user_input",
		},
		Questions: []interactions.Question{{ID: "environment", Question: "Which environment?"}},
		ExpiresAt: now.Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	waitForActiveTurnRecords(t, manager, "turn-1", 3)
	publishCaptureEvent(t, eventBus, runtimeevents.Event{
		ID: "turn-end", Kind: runtimeevents.KindAgentTurnEnd, Time: now.Add(time.Millisecond),
		Scope: scope, Payload: TurnEndPayload{
			Workspace: workspace, Status: TurnEndStatusSuspended,
		},
	})

	data, err := os.ReadFile(waitForTraceFile(t, workspace))
	if err != nil {
		t.Fatal(err)
	}
	var trace evaltrace.Trace
	if err := json.Unmarshal(data, &trace); err != nil {
		t.Fatal(err)
	}
	if err := evaltrace.Validate(trace); err != nil {
		t.Fatalf("turn trace is invalid: %v", err)
	}
	if len(trace.Records) != 4 {
		t.Fatalf("records = %d, want 4", len(trace.Records))
	}
	interactionRecord := trace.Records[2]
	if interactionRecord.Kind != evaltrace.RecordInteractionTransition ||
		interactionRecord.OffsetNanos != int64(2*time.Millisecond) {
		t.Fatalf("interaction record = %#v", interactionRecord)
	}
}

func waitForActiveTurnRecords(
	t *testing.T,
	manager *traceCaptureManager,
	turnID string,
	want int,
) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		manager.mu.Lock()
		trace := manager.turns[turnID]
		got := 0
		if trace != nil {
			got = len(trace.trace.Records)
		}
		manager.mu.Unlock()
		if got >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d records in turn %s", want, turnID)
}

func TestTraceStoreRootRejectsRelativeTraversal(t *testing.T) {
	workspace := t.TempDir()
	settings := traceCaptureSettings{stateDir: "../../outside"}
	want := filepath.Join(workspace, "state", "evaluation", "traces")
	if got := traceStoreRoot(settings, workspace); got != want {
		t.Fatalf("traceStoreRoot = %q, want %q", got, want)
	}
}

func traceTestConfig(workspace string) *config.Config {
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = workspace
	cfg.Evaluation.TraceCapture.Enabled = true
	cfg.Evaluation.TraceCapture.ContentMode = "metadata_only"
	return cfg
}

func publishCaptureEvent(t *testing.T, eventBus runtimeevents.Bus, event runtimeevents.Event) {
	t.Helper()
	result := eventBus.Publish(context.Background(), event)
	if result.Delivered == 0 {
		t.Fatalf("event %s was not delivered: %#v", event.Kind, result)
	}
}

func waitForTraceFile(t *testing.T, workspace string) string {
	t.Helper()
	pattern := filepath.Join(workspace, "state", "evaluation", "traces", "*.json")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		matches, _ := filepath.Glob(pattern)
		if len(matches) > 0 {
			return matches[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for trace at %s", pattern)
	return ""
}

func fileModeForTraceTest(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode()
}
