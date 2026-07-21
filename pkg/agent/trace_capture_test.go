package agent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
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
		TraceScope: runtimeevents.NewTraceScope(workspace, "turn-1"),
		AgentID:    "main",
		SessionKey: "session:" + secret,
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
	if manager.sub != nil || manager.writer != nil {
		t.Fatal("disabled capture started background workers")
	}
	start := time.Now().UTC()
	scope := runtimeevents.Scope{
		TraceScope: runtimeevents.NewTraceScope(workspace, "turn-disabled"),
	}
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
	if manager.sub == nil || manager.writer == nil {
		t.Fatal("enabling capture did not start workers")
	}
	start := time.Now().UTC()
	scope := runtimeevents.Scope{
		TraceScope: runtimeevents.NewTraceScope(workspace, "turn-enabled"),
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
		TraceScope: runtimeevents.NewTraceScope(workspace, "turn-delivery"),
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
		ID: "observed", Kind: runtimeevents.KindChannelMessageOutboundSent, Time: start.Add(2 * time.Millisecond),
		Scope: runtimeevents.Scope{Channel: "telegram", ChatID: "chat-delivery"},
		Payload: channels.ChannelOutboundPayload{
			TraceScopes: []runtimeevents.TraceScope{scope.TraceScope}, ContentLen: 4,
		},
	})
	if matches, _ := filepath.Glob(filepath.Join(workspace, "state", "evaluation", "traces", "*.json")); len(
		matches,
	) != 0 {
		t.Fatalf("non-settling delivery event persisted trace: %v", matches)
	}
	publishCaptureEvent(t, eventBus, runtimeevents.Event{
		ID: "sent", Kind: runtimeevents.KindChannelMessageOutboundSent, Time: start.Add(3 * time.Millisecond),
		Scope: runtimeevents.Scope{Channel: "telegram", ChatID: "chat-delivery"},
		Payload: channels.ChannelOutboundPayload{
			TraceScopes: []runtimeevents.TraceScope{scope.TraceScope}, TraceSettlement: true, ContentLen: 4,
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
	found := false
	for _, record := range trace.Records {
		found = found || record.Kind == evaltrace.RecordDeliveryOutcome
	}
	if !found {
		t.Fatalf("trace does not contain delivery outcome: %#v", trace.Records)
	}
}

func TestTraceCaptureSeparatesIdenticalTurnIDsAcrossWorkspaces(t *testing.T) {
	workspaceA := t.TempDir()
	workspaceB := t.TempDir()
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(traceTestConfig(workspaceA), eventBus)
	t.Cleanup(func() {
		manager.close()
		_ = eventBus.Close()
	})

	startedAt := time.Now().UTC()
	for index, item := range []struct {
		workspace string
		tool      string
	}{
		{workspace: workspaceA, tool: "tool-a"},
		{workspace: workspaceB, tool: "tool-b"},
	} {
		scope := runtimeevents.Scope{
			TraceScope: runtimeevents.NewTraceScope(item.workspace, "shared-turn"),
			SessionKey: "shared-session", Channel: "telegram", ChatID: "shared-chat",
		}
		publishCaptureEvent(t, eventBus, runtimeevents.Event{
			ID: "start-" + item.tool, Kind: runtimeevents.KindAgentTurnStart,
			Time: startedAt.Add(time.Duration(index) * time.Millisecond), Scope: scope,
			Payload: TurnStartPayload{Workspace: item.workspace},
		})
		publishCaptureEvent(t, eventBus, runtimeevents.Event{
			ID: "tool-" + item.tool, Kind: runtimeevents.KindAgentToolExecStart,
			Time: startedAt.Add(time.Duration(index+2) * time.Millisecond), Scope: scope,
			Payload: ToolExecStartPayload{Tool: item.tool},
		})
		publishCaptureEvent(t, eventBus, runtimeevents.Event{
			ID: "end-" + item.tool, Kind: runtimeevents.KindAgentTurnEnd,
			Time: startedAt.Add(time.Duration(index+4) * time.Millisecond), Scope: scope,
			Payload: TurnEndPayload{Workspace: item.workspace, Status: TurnEndStatusCompleted},
		})
	}

	traceA := readCapturedTrace(t, waitForTraceFile(t, workspaceA))
	traceB := readCapturedTrace(t, waitForTraceFile(t, workspaceB))
	if traceA.TraceID == traceB.TraceID {
		t.Fatalf("workspace-colliding turns share trace ID %q", traceA.TraceID)
	}
	assertCapturedTools(t, traceA, "tool-a")
	assertCapturedTools(t, traceB, "tool-b")
}

func TestTraceCaptureSettlesEveryScopeOnAggregatedDelivery(t *testing.T) {
	workspace := t.TempDir()
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(traceTestConfig(workspace), eventBus)
	t.Cleanup(func() {
		manager.close()
		_ = eventBus.Close()
	})

	startedAt := time.Now().UTC()
	traceScopes := []runtimeevents.TraceScope{
		runtimeevents.NewTraceScope(workspace, "turn-1"),
		runtimeevents.NewTraceScope(workspace, "turn-2"),
	}
	for index, traceScope := range traceScopes {
		scope := runtimeevents.Scope{TraceScope: traceScope}
		publishCaptureEvent(t, eventBus, runtimeevents.Event{
			ID: "start-" + traceScope.TurnID, Kind: runtimeevents.KindAgentTurnStart,
			Time: startedAt.Add(time.Duration(index) * time.Millisecond), Scope: scope,
			Payload: TurnStartPayload{Workspace: workspace},
		})
		publishCaptureEvent(t, eventBus, runtimeevents.Event{
			ID: "end-" + traceScope.TurnID, Kind: runtimeevents.KindAgentTurnEnd,
			Time: startedAt.Add(time.Duration(index+2) * time.Millisecond), Scope: scope,
			Payload: TurnEndPayload{
				Workspace: workspace, Status: TurnEndStatusCompleted, DeliveryExpected: true,
			},
		})
	}
	publishCaptureEvent(t, eventBus, runtimeevents.Event{
		ID: "aggregated-sent", Kind: runtimeevents.KindChannelMessageOutboundSent,
		Time: startedAt.Add(5 * time.Millisecond),
		Payload: channels.ChannelOutboundPayload{
			TraceScopes: traceScopes, TraceSettlement: true, ContentLen: 8,
		},
	})

	for _, path := range waitForTraceFiles(t, workspace, 2) {
		trace := readCapturedTrace(t, path)
		found := false
		for _, record := range trace.Records {
			found = found || record.Kind == evaltrace.RecordDeliveryOutcome
		}
		if !found {
			t.Fatalf("aggregated trace %s lacks delivery outcome", trace.Metadata.RootTurnID)
		}
	}
}

func TestTraceCaptureRetainsEarlyTerminalDeliveryUntilTurnEnd(t *testing.T) {
	workspace := t.TempDir()
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(traceTestConfig(workspace), eventBus)
	t.Cleanup(func() {
		manager.close()
		_ = eventBus.Close()
	})

	startedAt := time.Now().UTC()
	traceScope := runtimeevents.NewTraceScope(workspace, "turn-early-delivery")
	scope := runtimeevents.Scope{TraceScope: traceScope}
	publishCaptureEvent(t, eventBus, runtimeevents.Event{
		ID: "start", Kind: runtimeevents.KindAgentTurnStart, Time: startedAt, Scope: scope,
		Payload: TurnStartPayload{Workspace: workspace},
	})
	publishCaptureEvent(t, eventBus, runtimeevents.Event{
		ID: "sent", Kind: runtimeevents.KindChannelMessageOutboundSent,
		Time: startedAt.Add(time.Millisecond),
		Payload: channels.ChannelOutboundPayload{
			TraceScopes: []runtimeevents.TraceScope{traceScope}, TraceSettlement: true,
		},
	})
	if matches, _ := filepath.Glob(filepath.Join(workspace, "state", "evaluation", "traces", "*.json")); len(
		matches,
	) != 0 {
		t.Fatalf("trace persisted before turn end: %v", matches)
	}
	publishCaptureEvent(t, eventBus, runtimeevents.Event{
		ID: "end", Kind: runtimeevents.KindAgentTurnEnd,
		Time: startedAt.Add(2 * time.Millisecond), Scope: scope,
		Payload: TurnEndPayload{
			Workspace: workspace, Status: TurnEndStatusCompleted, DeliveryExpected: true,
		},
	})

	trace := readCapturedTrace(t, waitForTraceFile(t, workspace))
	if len(trace.Records) != 3 {
		t.Fatalf("early-settled trace records = %d, want 3", len(trace.Records))
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

func TestTaskTraceProjectorSeparatesIdenticalIDsAcrossWorkspaces(t *testing.T) {
	workspaceA, workspaceB := t.TempDir(), t.TempDir()
	cfg := traceTestConfig(workspaceA)
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(cfg, eventBus)
	registryA := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspaceA))
	registryB := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspaceB))
	manager.attachTaskRegistry(workspaceA, registryA)
	manager.attachTaskRegistry(workspaceB, registryB)

	finish := func(registry *taskregistry.Registry, session string) {
		t.Helper()
		if err := registry.Upsert(taskregistry.Record{
			TaskID: "shared-task", Task: "test", RequesterSessionKey: session,
			Status: taskregistry.StatusRunning, DeliveryStatus: taskregistry.DeliveryPending,
		}); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		if err := registry.Update("shared-task", func(record *taskregistry.Record) {
			record.Status = taskregistry.StatusSucceeded
			record.DeliveryStatus = taskregistry.DeliveryDelivered
		}); err != nil {
			t.Fatalf("Update: %v", err)
		}
	}
	finish(registryA, "session-a")
	finish(registryB, "session-b")

	traceA := readCapturedTrace(t, waitForTraceFile(t, workspaceA))
	traceB := readCapturedTrace(t, waitForTraceFile(t, workspaceB))
	if traceA.TraceID == traceB.TraceID {
		t.Fatalf("workspace traces share id %q", traceA.TraceID)
	}
	if traceA.Metadata.SessionHash == traceB.Metadata.SessionHash {
		t.Fatalf("workspace traces share session hash %q", traceA.Metadata.SessionHash)
	}
	for _, trace := range []evaltrace.Trace{traceA, traceB} {
		if trace.Outcome == nil || trace.Outcome.Status != string(taskregistry.StatusSucceeded) {
			t.Fatalf("outcome = %#v", trace.Outcome)
		}
		for _, record := range trace.Records {
			if record.Scope.TaskID != "shared-task" {
				t.Fatalf("record task id = %q", record.Scope.TaskID)
			}
		}
	}
	manager.close()
	_ = eventBus.Close()
}

func TestTaskTraceProjectorEnablesAfterRegistryAttachment(t *testing.T) {
	workspace := t.TempDir()
	cfg := config.DefaultConfig()
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(cfg, eventBus)
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	manager.attachTaskRegistry(workspace, registry)
	manager.updateConfig(traceTestConfig(workspace))

	if err := registry.Upsert(taskregistry.Record{
		TaskID: "task-enabled", Task: "test", RequesterSessionKey: "session-enabled",
		Status: taskregistry.StatusRunning, DeliveryStatus: taskregistry.DeliveryPending,
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := registry.Update("task-enabled", func(record *taskregistry.Record) {
		record.Status = taskregistry.StatusSucceeded
		record.DeliveryStatus = taskregistry.DeliveryDelivered
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	trace := readCapturedTrace(t, waitForTraceFile(t, workspace))
	if trace.Outcome == nil || trace.Outcome.Status != string(taskregistry.StatusSucceeded) {
		t.Fatalf("outcome = %#v", trace.Outcome)
	}
	manager.close()
	_ = eventBus.Close()
}

func TestTaskTraceProjectorPersistsIncompleteTraceOnClose(t *testing.T) {
	workspace := t.TempDir()
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(traceTestConfig(workspace), eventBus)
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	manager.attachTaskRegistry(workspace, registry)
	if err := registry.Upsert(taskregistry.Record{
		TaskID: "task-active", Task: "test", RequesterSessionKey: "session-active",
		Status: taskregistry.StatusRunning, DeliveryStatus: taskregistry.DeliveryPending,
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	manager.close()
	trace := readCapturedTrace(t, waitForTraceFile(t, workspace))
	if !trace.Truncation.Incomplete ||
		!slices.Contains(trace.Truncation.Reasons, "runtime_closed_before_terminal_task_delivery") {
		t.Fatalf("truncation = %+v", trace.Truncation)
	}
	_ = eventBus.Close()
}

func TestTaskTraceProjectorProjectsEachEventOnceAcrossCallbackOrder(t *testing.T) {
	workspace := t.TempDir()
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	if err := registry.Upsert(taskregistry.Record{
		TaskID: "task-race", Task: "test", RequesterSessionKey: "session-race",
		Status: taskregistry.StatusRunning, DeliveryStatus: taskregistry.DeliveryPending,
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := registry.Update("task-race", func(record *taskregistry.Record) {
		record.Status = taskregistry.StatusSucceeded
		record.DeliveryStatus = taskregistry.DeliveryDelivered
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	history := registry.ListEvents("task-race")
	if len(history) < 2 {
		t.Fatalf("history has %d events, want at least 2", len(history))
	}
	record, ok := registry.Get("task-race")
	if !ok {
		t.Fatal("terminal task record not found")
	}

	orders := map[string][]int{
		"terminal callback first": {len(history) - 1, 0},
		"early callback first":    {0, len(history) - 1},
	}
	var want evaltrace.Trace
	for name, order := range orders {
		t.Run(name, func(t *testing.T) {
			var traces []evaltrace.Trace
			projector := newTaskTraceProjector(
				traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
				func(_ traceCaptureSettings, active *activeTraceCapture) bool {
					trace, err := active.builder.Finalize()
					if err != nil {
						t.Fatalf("Finalize: %v", err)
					}
					traces = append(traces, trace)
					return true
				},
			)
			for _, index := range order {
				projector.observe(workspace, registry, taskregistry.EventObservation{
					Event: history[index], Record: record, FinalForTask: true,
				})
			}
			if len(traces) != 1 {
				t.Fatalf("persisted %d traces, want 1", len(traces))
			}
			if len(traces[0].Records) != len(history) {
				t.Fatalf("trace has %d records, want %d", len(traces[0].Records), len(history))
			}
			if want.Records == nil {
				want = traces[0]
			} else if !reflect.DeepEqual(traces[0], want) {
				t.Fatalf("trace differs by callback order\ngot:  %#v\nwant: %#v", traces[0], want)
			}
		})
	}
}

func TestTaskTraceProjectorSeparatesReusedTaskIDGenerations(t *testing.T) {
	workspace := t.TempDir()
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	finish := func(createdAt int64) taskregistry.Record {
		t.Helper()
		if err := registry.Upsert(taskregistry.Record{
			TaskID: "reused", Task: "test", RequesterSessionKey: "session-reused",
			CreatedAt: createdAt, Status: taskregistry.StatusRunning,
			DeliveryStatus: taskregistry.DeliveryPending,
		}); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		if err := registry.Update("reused", func(record *taskregistry.Record) {
			record.Status = taskregistry.StatusSucceeded
			record.DeliveryStatus = taskregistry.DeliveryDelivered
		}); err != nil {
			t.Fatalf("Update: %v", err)
		}
		record, ok := registry.Get("reused")
		if !ok {
			t.Fatal("terminal task record not found")
		}
		return record
	}
	firstRecord := finish(1_000)
	firstEnd := len(registry.ListEvents("reused")) - 1
	secondRecord := finish(1_000)
	history := registry.ListEvents("reused")
	secondStart := slices.IndexFunc(history, func(event taskregistry.TaskEvent) bool {
		return event.Type == taskregistry.EventTaskUpserted && event.Seq > history[firstEnd].Seq
	})
	if secondStart < 0 {
		t.Fatal("second task generation boundary not found")
	}

	var traces []evaltrace.Trace
	projector := newTaskTraceProjector(
		traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
		func(_ traceCaptureSettings, active *activeTraceCapture) bool {
			trace, err := active.builder.Finalize()
			if err != nil {
				t.Fatalf("Finalize: %v", err)
			}
			traces = append(traces, trace)
			return true
		},
	)
	projector.observe(workspace, registry, taskregistry.EventObservation{
		Event: history[firstEnd], Record: firstRecord, FinalForTask: true,
	})
	projector.observe(workspace, registry, taskregistry.EventObservation{
		Event: history[len(history)-1], Record: secondRecord, FinalForTask: true,
	})

	if len(traces) != 2 {
		t.Fatalf("persisted %d traces, want 2", len(traces))
	}
	if traces[0].TraceID == traces[1].TraceID {
		t.Fatalf("task generations share trace id %q", traces[0].TraceID)
	}
	if got, want := len(traces[1].Records), len(history)-secondStart; got != want {
		t.Fatalf("second trace has %d records, want %d", got, want)
	}
	firstOrigins := make(map[string]struct{}, len(traces[0].Records))
	for _, record := range traces[0].Records {
		firstOrigins[record.Origin.ID] = struct{}{}
	}
	for _, record := range traces[1].Records {
		if _, duplicate := firstOrigins[record.Origin.ID]; duplicate {
			t.Fatalf("second trace reused first-generation event %q", record.Origin.ID)
		}
	}
}

func TestTaskTraceProjectorRetriesTerminalTraceAfterAdmissionRejection(t *testing.T) {
	workspace := t.TempDir()
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	if err := registry.Upsert(taskregistry.Record{
		TaskID: "retry", Task: "test", RequesterSessionKey: "session-retry",
		Status: taskregistry.StatusRunning, DeliveryStatus: taskregistry.DeliveryPending,
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := registry.Update("retry", func(record *taskregistry.Record) {
		record.Status = taskregistry.StatusSucceeded
		record.DeliveryStatus = taskregistry.DeliveryDelivered
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	history := registry.ListEvents("retry")
	record, ok := registry.Get("retry")
	if !ok {
		t.Fatal("terminal task record not found")
	}
	terminal := taskregistry.EventObservation{
		Event: history[len(history)-1], Record: record, FinalForTask: true,
	}

	attempts := 0
	var admitted evaltrace.Trace
	projector := newTaskTraceProjector(
		traceCaptureSettingsFromConfig(traceTestConfig(workspace)),
		func(_ traceCaptureSettings, active *activeTraceCapture) bool {
			attempts++
			if attempts == 1 {
				return false
			}
			trace, err := active.builder.Finalize()
			if err != nil {
				t.Fatalf("Finalize: %v", err)
			}
			admitted = trace
			return true
		},
	)
	projector.observe(workspace, registry, terminal)
	if len(projector.traces) != 1 || len(projector.completed) != 0 {
		t.Fatalf("after rejection: traces=%d completed=%d", len(projector.traces), len(projector.completed))
	}
	projector.observe(workspace, registry, terminal)
	if attempts != 2 {
		t.Fatalf("admission attempts = %d, want 2", attempts)
	}
	if len(projector.traces) != 0 || len(projector.completed) != 1 {
		t.Fatalf("after admission: traces=%d completed=%d", len(projector.traces), len(projector.completed))
	}
	if admitted.Outcome == nil || admitted.Outcome.Status != string(taskregistry.StatusSucceeded) {
		t.Fatalf("admitted outcome = %#v", admitted.Outcome)
	}
	if len(admitted.Records) != len(history) {
		t.Fatalf("admitted records = %d, want %d", len(admitted.Records), len(history))
	}
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
	return waitForTraceFiles(t, workspace, 1)[0]
}

func waitForTraceFiles(t *testing.T, workspace string, count int) []string {
	t.Helper()
	pattern := filepath.Join(workspace, "state", "evaluation", "traces", "*.json")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		matches, _ := filepath.Glob(pattern)
		if len(matches) >= count {
			return matches
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d trace(s) at %s", count, pattern)
	return nil
}

func readCapturedTrace(t *testing.T, path string) evaltrace.Trace {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var trace evaltrace.Trace
	if err := json.Unmarshal(data, &trace); err != nil {
		t.Fatal(err)
	}
	return trace
}

func assertCapturedTools(t *testing.T, trace evaltrace.Trace, want ...string) {
	t.Helper()
	got := make([]string, 0, len(want))
	for _, record := range trace.Records {
		if record.Kind != evaltrace.RecordToolCall {
			continue
		}
		var payload evaltrace.ToolPayload
		if err := json.Unmarshal(record.Data, &payload); err != nil {
			t.Fatal(err)
		}
		got = append(got, payload.Tool)
	}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("captured tools = %v, want %v", got, want)
	}
}

func fileModeForTraceTest(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode()
}
