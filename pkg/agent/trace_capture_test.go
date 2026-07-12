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

func TestTraceCaptureStoresStandaloneEvolutionTransition(t *testing.T) {
	workspace := t.TempDir()
	cfg := traceTestConfig(workspace)
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(cfg, eventBus)
	eventTime := time.Now().UTC()
	publishCaptureEvent(t, eventBus, runtimeevents.Event{
		ID: "evolution-1", Kind: runtimeevents.KindAgentEvolutionTransition, Time: eventTime,
		Source: runtimeevents.Source{Component: "agent"},
		Payload: EvolutionTransitionPayload{
			Workspace: workspace, RecordID: "record-1", DraftID: "draft-1",
			SkillName: "test-skill", Action: "draft_saved", Status: "candidate",
			ProvenanceIDs: []string{"record-1"},
		},
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
	if len(trace.Records) != 1 || trace.Records[0].Kind != evaltrace.RecordEvolutionDraft {
		t.Fatalf("evolution trace = %#v", trace)
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
