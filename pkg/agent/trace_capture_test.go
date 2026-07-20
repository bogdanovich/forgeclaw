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

func TestTraceCaptureNormalizesTurnEndScope(t *testing.T) {
	workspace := t.TempDir()
	cfg := traceTestConfig(workspace)
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(cfg, eventBus)
	t.Cleanup(func() {
		manager.close()
		_ = eventBus.Close()
	})

	startedAt := time.Now().UTC()
	paddedScope := runtimeevents.Scope{
		TraceScope: runtimeevents.TraceScope{
			Workspace: "  " + workspace + "  ",
			TurnID:    "  turn-padded  ",
		},
	}
	publishCaptureEvent(t, eventBus, runtimeevents.Event{
		ID: "start-padded", Kind: runtimeevents.KindAgentTurnStart, Time: startedAt,
		Source: runtimeevents.Source{Component: "agent"}, Scope: paddedScope,
		Payload: TurnStartPayload{Workspace: workspace},
	})
	publishCaptureEvent(t, eventBus, runtimeevents.Event{
		ID: "end-padded", Kind: runtimeevents.KindAgentTurnEnd, Time: startedAt.Add(time.Millisecond),
		Source: runtimeevents.Source{Component: "agent"}, Scope: paddedScope,
		Payload: TurnEndPayload{Status: TurnEndStatusCompleted, Workspace: workspace},
	})

	tracePath := waitForTraceFile(t, workspace)
	data, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	var trace evaltrace.Trace
	if err := json.Unmarshal(data, &trace); err != nil {
		t.Fatalf("decode trace: %v", err)
	}
	if trace.Metadata.RootTurnID != "turn-padded" {
		t.Fatalf("root turn ID = %q, want normalized value", trace.Metadata.RootTurnID)
	}
	if trace.Outcome == nil || trace.Outcome.Status != string(TurnEndStatusCompleted) {
		t.Fatalf("outcome = %#v", trace.Outcome)
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
	if manager.sub == nil || manager.persistCh == nil {
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
		ID: "reasoning-sent", Kind: runtimeevents.KindChannelMessageOutboundSent,
		Time: start.Add(2 * time.Millisecond),
		Scope: runtimeevents.Scope{
			TraceScope: scope.TraceScope, Channel: "telegram", ChatID: "chat-delivery",
		},
		Payload: channels.ChannelOutboundPayload{
			TraceScopes: []runtimeevents.TraceScope{scope.TraceScope}, ContentLen: 9,
		},
	})
	tracePattern := filepath.Join(workspace, "state", "evaluation", "traces", "*.json")
	if matches, _ := filepath.Glob(tracePattern); len(matches) != 0 {
		t.Fatalf("non-final delivery settled trace: %v", matches)
	}
	publishCaptureEvent(t, eventBus, runtimeevents.Event{
		ID: "sent", Kind: runtimeevents.KindChannelMessageOutboundSent, Time: start.Add(3 * time.Millisecond),
		Scope: runtimeevents.Scope{
			TraceScope: scope.TraceScope, Channel: "telegram", ChatID: "chat-delivery",
		},
		Payload: channels.ChannelOutboundPayload{
			TraceScopes: []runtimeevents.TraceScope{scope.TraceScope}, ContentLen: 4,
			TraceSettlement: true,
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

func TestTraceCaptureScopesDuplicateTurnIDsByWorkspace(t *testing.T) {
	workspaceA, workspaceB := t.TempDir(), t.TempDir()
	manager := newTraceCaptureManager(traceTestConfig(workspaceA), nil)
	start := time.Now().UTC()
	const turnID = "turn-shared"
	for index, workspace := range []string{workspaceA, workspaceB} {
		manager.observeRuntimeEvent(runtimeevents.Event{
			ID: "start-" + string(rune('a'+index)), Kind: runtimeevents.KindAgentTurnStart,
			Time: start.Add(time.Duration(index) * time.Millisecond),
			Scope: runtimeevents.Scope{
				TraceScope: runtimeevents.NewTraceScope(workspace, turnID),
				SessionKey: "session-shared",
			},
			Payload: TurnStartPayload{Workspace: workspace},
		})
	}
	manager.observeRuntimeEvent(runtimeevents.Event{
		ID: "ambiguous", Kind: runtimeevents.KindAgentLLMFallbackAttempt,
		Time: start.Add(2 * time.Millisecond),
		Scope: runtimeevents.Scope{
			SessionKey: "session-shared", Channel: "telegram", ChatID: "chat-shared",
		},
		Payload: LLMFallbackAttemptPayload{
			Provider: "openai", Model: "ambiguous", Attempt: 2, Status: "succeeded",
		},
	})
	manager.observeRuntimeEvent(runtimeevents.Event{
		ID: "workspace-a", Kind: runtimeevents.KindAgentLLMFallbackAttempt,
		Time: start.Add(3 * time.Millisecond),
		Scope: runtimeevents.Scope{
			TraceScope: runtimeevents.NewTraceScope(workspaceA, turnID),
		},
		Payload: LLMFallbackAttemptPayload{
			Provider: "openai", Model: "workspace-a", Attempt: 2, Status: "succeeded",
		},
	})

	manager.mu.Lock()
	traceA := manager.turns[runtimeevents.NewTraceScope(workspaceA, turnID)]
	traceB := manager.turns[runtimeevents.NewTraceScope(workspaceB, turnID)]
	recordsA, recordsB := len(traceA.trace.Records), len(traceB.trace.Records)
	manager.mu.Unlock()
	manager.close()
	if recordsA != 2 || recordsB != 1 {
		t.Fatalf("duplicate turn IDs crossed workspaces: a=%d b=%d", recordsA, recordsB)
	}
}

func TestTraceCaptureDurableIDsIncludeWorkspace(t *testing.T) {
	workspaceA, workspaceB := t.TempDir(), t.TempDir()
	stateDir := t.TempDir()
	cfg := traceTestConfig(workspaceA)
	cfg.Evaluation.TraceCapture.StateDir = stateDir
	manager := newTraceCaptureManager(cfg, nil)
	startedAt := time.Now().UTC()

	for index, workspace := range []string{workspaceA, workspaceB} {
		scope := runtimeevents.Scope{
			TraceScope: runtimeevents.NewTraceScope(workspace, "turn-shared"),
		}
		manager.observeRuntimeEvent(runtimeevents.Event{
			ID: "start-" + string(rune('a'+index)), Kind: runtimeevents.KindAgentTurnStart,
			Time: startedAt, Scope: scope, Payload: TurnStartPayload{Workspace: workspace},
		})
		manager.observeRuntimeEvent(runtimeevents.Event{
			ID: "end-" + string(rune('a'+index)), Kind: runtimeevents.KindAgentTurnEnd,
			Time: startedAt.Add(time.Millisecond), Scope: scope,
			Payload: TurnEndPayload{Status: TurnEndStatusCompleted, Workspace: workspace},
		})
	}
	manager.close()

	tracePaths, err := filepath.Glob(filepath.Join(stateDir, "traces", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(tracePaths) != 2 {
		t.Fatalf("durable traces = %d, want 2: %v", len(tracePaths), tracePaths)
	}
	if filepath.Base(tracePaths[0]) == filepath.Base(tracePaths[1]) {
		t.Fatalf("workspace-scoped traces share a durable ID: %v", tracePaths)
	}
}

func TestTraceCaptureSettlesAllScopesFromOneOutbound(t *testing.T) {
	workspace := t.TempDir()
	manager := newTraceCaptureManager(traceTestConfig(workspace), nil)
	start := time.Now().UTC()
	traceScopes := []runtimeevents.TraceScope{
		runtimeevents.NewTraceScope(workspace, "turn-one"),
		runtimeevents.NewTraceScope(workspace, "turn-two"),
	}
	for index, traceScope := range traceScopes {
		scope := runtimeevents.Scope{TraceScope: traceScope, SessionKey: "session-shared"}
		manager.observeRuntimeEvent(runtimeevents.Event{
			ID: "start-" + traceScope.TurnID, Kind: runtimeevents.KindAgentTurnStart,
			Time: start.Add(time.Duration(index) * time.Millisecond), Scope: scope,
			Payload: TurnStartPayload{Workspace: workspace},
		})
		manager.observeRuntimeEvent(runtimeevents.Event{
			ID: "end-" + traceScope.TurnID, Kind: runtimeevents.KindAgentTurnEnd,
			Time: start.Add(time.Duration(index+2) * time.Millisecond), Scope: scope,
			Payload: TurnEndPayload{
				Workspace: workspace, Status: TurnEndStatusCompleted, DeliveryExpected: true,
			},
		})
	}
	manager.observeRuntimeEvent(runtimeevents.Event{
		ID: "sent", Kind: runtimeevents.KindChannelMessageOutboundSent,
		Time:  start.Add(5 * time.Millisecond),
		Scope: runtimeevents.Scope{TraceScope: traceScopes[0]},
		Payload: channels.ChannelOutboundPayload{
			TraceScopes: traceScopes, ContentLen: 4, TraceSettlement: true,
		},
	})
	manager.close()

	paths, err := filepath.Glob(filepath.Join(workspace, "state", "evaluation", "traces", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 {
		t.Fatalf("persisted traces = %v, want two", paths)
	}
	for _, path := range paths {
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		var trace evaltrace.Trace
		if err := json.Unmarshal(data, &trace); err != nil {
			t.Fatal(err)
		}
		foundDelivery := false
		for _, record := range trace.Records {
			foundDelivery = foundDelivery || record.Kind == evaltrace.RecordDeliveryOutcome
		}
		if !foundDelivery || trace.Truncation.Incomplete {
			t.Fatalf("trace %s was not cleanly settled: %#v", path, trace)
		}
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

func TestTraceCaptureScopesDuplicateTaskIDsByWorkspace(t *testing.T) {
	workspaceA, workspaceB := t.TempDir(), t.TempDir()
	stateDir := t.TempDir()
	cfg := traceTestConfig(workspaceA)
	cfg.Evaluation.TraceCapture.StateDir = stateDir
	manager := newTraceCaptureManager(cfg, nil)

	registries := []*taskregistry.Registry{
		taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspaceA)),
		taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspaceB)),
	}
	for index, registry := range registries {
		workspace := []string{workspaceA, workspaceB}[index]
		manager.attachTaskRegistry(workspace, registry)
		if err := registry.Upsert(taskregistry.Record{
			TaskID: "subagent-0", Task: "workspace task",
			Status: taskregistry.StatusRunning, DeliveryStatus: taskregistry.DeliveryPending,
		}); err != nil {
			t.Fatalf("Upsert workspace %d: %v", index, err)
		}
	}

	manager.mu.Lock()
	active := len(manager.tasks)
	manager.mu.Unlock()
	if active != 2 {
		t.Fatalf("active task traces = %d, want 2", active)
	}
	for index, registry := range registries {
		if err := registry.Update("subagent-0", func(record *taskregistry.Record) {
			record.Status = taskregistry.StatusSucceeded
			record.DeliveryStatus = taskregistry.DeliveryDelivered
		}); err != nil {
			t.Fatalf("Update workspace %d: %v", index, err)
		}
	}
	manager.close()

	tracePaths, err := filepath.Glob(filepath.Join(stateDir, "traces", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(tracePaths) != 2 {
		t.Fatalf("durable task traces = %d, want 2: %v", len(tracePaths), tracePaths)
	}
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
