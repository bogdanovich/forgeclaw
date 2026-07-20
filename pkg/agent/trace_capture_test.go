package agent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
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

func TestTraceCaptureReconcilesAttachedRegistryAfterConfigEnable(t *testing.T) {
	workspace := t.TempDir()
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(config.DefaultConfig(), eventBus)
	registry := interactions.NewRegistry(interactions.WorkspaceStorePath(workspace))
	manager.attachInteractionRegistry(workspace, registry)
	record := createTraceTestInteraction(t, registry, "interaction-enable-reconcile", "session-1")
	record, err := registry.Cancel(record.ID, record.Revision, "fixture_cancel")
	if err != nil {
		t.Fatal(err)
	}
	if matches, _ := filepath.Glob(
		filepath.Join(workspace, "state", "evaluation", "traces", "*.json"),
	); len(matches) != 0 {
		t.Fatalf("disabled capture wrote traces: %v", matches)
	}
	manager.updateConfig(traceTestConfig(workspace))
	tracePath := filepath.Join(
		workspace,
		"state",
		"evaluation",
		"traces",
		opaqueTraceID("interaction", record.ID, time.UnixMilli(record.CreatedAt))+".json",
	)
	waitForTracePath(t, tracePath)
	manager.close()
	_ = eventBus.Close()
	trace := loadTraceFile(t, tracePath)
	if trace.Truncation.Incomplete || trace.Outcome == nil || len(trace.Records) != 2 {
		t.Fatalf("trace = %#v", trace)
	}
}

func TestTraceCaptureRejectsStaleInteractionObserverGeneration(t *testing.T) {
	workspace := t.TempDir()
	manager := newTraceCaptureManager(traceTestConfig(workspace), nil)
	manager.mu.Lock()
	manager.interactionGens[workspace] = 1
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		manager.observeInteractionRegistryEventGeneration(workspace, 1, interactions.EventObservation{
			Event: interactions.Event{
				InteractionID: "interaction-stale-observer",
				Type:          interactions.EventCreated, To: interactions.StatusCreated,
				Sequence: 1, Revision: 1, EmittedAt: time.Now().UnixMilli(),
			},
			Record: interactions.Record{
				ID: "interaction-stale-observer", Status: interactions.StatusCreated,
				CreatedAt: time.Now().UnixMilli(),
			},
		})
		close(done)
	}()
	<-started
	manager.interactionGens[workspace] = 2
	manager.mu.Unlock()
	<-done

	manager.mu.Lock()
	_, captured := manager.interactions["interaction-stale-observer"]
	manager.mu.Unlock()
	manager.close()
	if captured {
		t.Fatal("stale observer generation captured an interaction event")
	}
}

func TestTraceCapturePreservesActiveInteractionAcrossEnabledConfigReload(t *testing.T) {
	workspace := t.TempDir()
	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(traceTestConfig(workspace), eventBus)
	registry := interactions.NewRegistry(interactions.WorkspaceStorePath(workspace))
	manager.attachInteractionRegistry(workspace, registry)
	record := createTraceTestInteraction(t, registry, "interaction-enabled-reload", "session-1")
	manager.updateConfig(traceTestConfig(workspace))
	record, err := registry.Cancel(record.ID, record.Revision, "fixture_cancel")
	if err != nil {
		t.Fatal(err)
	}
	tracePath := filepath.Join(
		workspace,
		"state",
		"evaluation",
		"traces",
		opaqueTraceID("interaction", record.ID, time.UnixMilli(record.CreatedAt))+".json",
	)
	waitForTracePath(t, tracePath)
	manager.close()
	_ = eventBus.Close()
	trace := loadTraceFile(t, tracePath)
	if trace.Truncation.Incomplete || trace.Outcome == nil || len(trace.Records) != 2 {
		t.Fatalf("trace = %#v", trace)
	}
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

func TestTraceCaptureReconcilesTerminalInteractionOnRegistryAttachment(t *testing.T) {
	workspace := t.TempDir()
	registry := interactions.NewRegistry(interactions.WorkspaceStorePath(workspace))
	record, err := registry.Create(interactions.CreateRequest{
		ID:   "interaction-terminal-reconcile",
		Kind: interactions.KindQuestion,
		Route: interactions.Route{
			AgentID: "main", SessionKey: "session-1", Channel: "telegram",
			ChatID: "chat-1", SenderID: "sender-1",
		},
		Origin: interactions.Origin{
			TurnID: "turn-1", ToolCallID: "call-1", ToolName: "request_user_input",
		},
		Questions: []interactions.Question{{ID: "environment", Question: "Which environment?"}},
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	record, err = registry.RecordDeliveryAttempt(record.ID, record.Revision, true, "")
	if err != nil {
		t.Fatal(err)
	}
	record, err = registry.MarkWaiting(record.ID, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	record, err = registry.ClaimAnswer(
		record.ID,
		record.Revision,
		interactions.Answer{Text: "staging"},
		interactions.OutcomeAnswered,
	)
	if err != nil {
		t.Fatal(err)
	}
	record, err = registry.MarkResuming(record.ID, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = registry.Resolve(record.ID, record.Revision); err != nil {
		t.Fatal(err)
	}
	history := registry.ListEvents(record.ID)
	wantTraceID := opaqueTraceID("interaction", record.ID, time.UnixMilli(record.CreatedAt))

	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(traceTestConfig(workspace), eventBus)
	manager.attachInteractionRegistry(workspace, registry)
	tracePath := waitForTraceFile(t, workspace)
	manager.close()
	_ = eventBus.Close()
	if filepath.Base(tracePath) != wantTraceID+".json" {
		t.Fatalf("trace path = %s, want %s.json", tracePath, wantTraceID)
	}
	data, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	var trace evaltrace.Trace
	if err = json.Unmarshal(data, &trace); err != nil {
		t.Fatal(err)
	}
	if err = evaltrace.Validate(trace); err != nil {
		t.Fatal(err)
	}
	if trace.Outcome == nil || trace.Outcome.Status != string(interactions.StatusResolved) ||
		len(trace.Records) != len(history) {
		t.Fatalf("reconciled trace = %#v", trace)
	}

	oldModTime := time.Now().Add(-time.Minute).Truncate(time.Second)
	if err = os.Chtimes(tracePath, oldModTime, oldModTime); err != nil {
		t.Fatal(err)
	}
	reloaded := interactions.NewRegistry(interactions.WorkspaceStorePath(workspace))
	secondBus := runtimeevents.NewBus()
	secondManager := newTraceCaptureManager(traceTestConfig(workspace), secondBus)
	secondManager.attachInteractionRegistry(workspace, reloaded)
	secondManager.close()
	_ = secondBus.Close()
	info, err := os.Stat(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	if !info.ModTime().Equal(oldModTime) {
		t.Fatalf("existing trace was rewritten: modtime = %s", info.ModTime())
	}
}

func TestTraceCaptureReplacesIncompleteTraceDuringTerminalReconciliation(t *testing.T) {
	workspace := t.TempDir()
	registry := interactions.NewRegistry(interactions.WorkspaceStorePath(workspace))
	firstBus := runtimeevents.NewBus()
	firstManager := newTraceCaptureManager(traceTestConfig(workspace), firstBus)
	firstManager.attachInteractionRegistry(workspace, registry)
	record, err := registry.Create(interactions.CreateRequest{
		ID:   "interaction-replace-incomplete",
		Kind: interactions.KindQuestion,
		Route: interactions.Route{
			AgentID: "main", SessionKey: "session-1", Channel: "telegram",
			ChatID: "chat-1", SenderID: "sender-1",
		},
		Origin: interactions.Origin{
			TurnID: "turn-1", ToolCallID: "call-1", ToolName: "request_user_input",
		},
		Questions: []interactions.Question{{ID: "environment", Question: "Which environment?"}},
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	record, err = registry.RecordDeliveryAttempt(record.ID, record.Revision, true, "")
	if err != nil {
		t.Fatal(err)
	}
	record, err = registry.MarkWaiting(record.ID, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	firstManager.close()
	_ = firstBus.Close()
	traceID := opaqueTraceID("interaction", record.ID, time.UnixMilli(record.CreatedAt))
	tracePath := filepath.Join(
		workspace,
		"state",
		"evaluation",
		"traces",
		traceID+".json",
	)
	oldTrace := loadTraceFile(t, tracePath)
	if !oldTrace.Truncation.Incomplete || oldTrace.Outcome != nil {
		t.Fatalf("initial trace = %#v", oldTrace)
	}

	record, err = registry.ClaimAnswer(
		record.ID,
		record.Revision,
		interactions.Answer{Text: "staging"},
		interactions.OutcomeAnswered,
	)
	if err != nil {
		t.Fatal(err)
	}
	record, err = registry.MarkResuming(record.ID, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = registry.Resolve(record.ID, record.Revision); err != nil {
		t.Fatal(err)
	}

	reloaded := interactions.NewRegistry(interactions.WorkspaceStorePath(workspace))
	secondBus := runtimeevents.NewBus()
	secondManager := newTraceCaptureManager(traceTestConfig(workspace), secondBus)
	secondManager.attachInteractionRegistry(workspace, reloaded)
	secondManager.close()
	_ = secondBus.Close()
	recovered := loadTraceFile(t, tracePath)
	if recovered.Truncation.Incomplete || recovered.Outcome == nil ||
		recovered.Outcome.Status != string(interactions.StatusResolved) {
		t.Fatalf("recovered trace = %#v", recovered)
	}
}

func TestTraceCapturePersistsIncompleteTerminalTraceWithoutRetainedEvents(t *testing.T) {
	workspace := t.TempDir()
	registry := interactions.NewRegistryWithOptions(
		interactions.WorkspaceStorePath(workspace),
		interactions.Options{MaxEvents: 1},
	)
	record, err := registry.Create(interactions.CreateRequest{
		ID:   "interaction-without-events",
		Kind: interactions.KindQuestion,
		Route: interactions.Route{
			AgentID: "main", SessionKey: "session-1", Channel: "telegram",
			ChatID: "chat-1", SenderID: "sender-1",
		},
		Origin: interactions.Origin{
			TurnID: "turn-1", ToolCallID: "call-1", ToolName: "request_user_input",
		},
		Questions: []interactions.Question{{ID: "environment", Question: "Which environment?"}},
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	record, err = registry.Cancel(record.ID, record.Revision, "fixture_cancel")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = registry.Create(interactions.CreateRequest{
		ID:   "interaction-evicts-events",
		Kind: interactions.KindQuestion,
		Route: interactions.Route{
			AgentID: "main", SessionKey: "session-2", Channel: "telegram",
			ChatID: "chat-2", SenderID: "sender-2",
		},
		Origin: interactions.Origin{
			TurnID: "turn-2", ToolCallID: "call-2", ToolName: "request_user_input",
		},
		Questions: []interactions.Question{{ID: "environment", Question: "Which environment?"}},
		ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	if history := registry.ListEvents(record.ID); len(history) != 0 {
		t.Fatalf("retained history = %+v", history)
	}

	eventBus := runtimeevents.NewBus()
	manager := newTraceCaptureManager(traceTestConfig(workspace), eventBus)
	manager.attachInteractionRegistry(workspace, registry)
	manager.close()
	_ = eventBus.Close()
	traceID := opaqueTraceID("interaction", record.ID, time.UnixMilli(record.CreatedAt))
	trace := loadTraceFile(t, filepath.Join(
		workspace,
		"state",
		"evaluation",
		"traces",
		traceID+".json",
	))
	if !trace.Truncation.Incomplete || len(trace.Records) != 0 || trace.Outcome == nil {
		t.Fatalf("trace = %#v", trace)
	}
	if !slices.Contains(trace.Truncation.Reasons, "interaction_event_history_missing") {
		t.Fatalf("truncation reasons = %v", trace.Truncation.Reasons)
	}
}

func TestTraceCaptureDoesNotResurrectTerminalTraceExpiredByRetention(t *testing.T) {
	workspace := t.TempDir()
	now := time.Now().UTC()
	registry := interactions.NewRegistryWithOptions(
		interactions.WorkspaceStorePath(workspace),
		interactions.Options{Now: func() time.Time { return now.Add(-2 * time.Hour) }},
	)
	record := createTraceTestInteraction(t, registry, "interaction-expired-trace", "session-1")
	if _, err := registry.Cancel(record.ID, record.Revision, "fixture_cancel"); err != nil {
		t.Fatal(err)
	}
	cfg := traceTestConfig(workspace)
	cfg.Evaluation.TraceCapture.RetentionHours = 1
	manager := newTraceCaptureManager(cfg, nil)
	manager.attachInteractionRegistry(workspace, registry)
	manager.close()

	traceID := opaqueTraceID("interaction", record.ID, time.UnixMilli(record.CreatedAt))
	tracePath := filepath.Join(workspace, "state", "evaluation", "traces", traceID+".json")
	if _, err := os.Stat(tracePath); !os.IsNotExist(err) {
		t.Fatalf("expired terminal trace was recreated: %v", err)
	}
}

func TestTraceCaptureDoesNotResurrectTerminalTracePrunedByCount(t *testing.T) {
	workspace := t.TempDir()
	now := time.Now().UTC()
	clock := now.Add(-time.Hour)
	registry := interactions.NewRegistryWithOptions(
		interactions.WorkspaceStorePath(workspace),
		interactions.Options{Now: func() time.Time { return clock }},
	)
	older := createTraceTestInteraction(t, registry, "interaction-count-older", "session-1")
	if _, err := registry.Cancel(older.ID, older.Revision, "fixture_cancel"); err != nil {
		t.Fatal(err)
	}
	clock = now
	newer := createTraceTestInteraction(t, registry, "interaction-count-newer", "session-2")
	if _, err := registry.Cancel(newer.ID, newer.Revision, "fixture_cancel"); err != nil {
		t.Fatal(err)
	}
	cfg := traceTestConfig(workspace)
	cfg.Evaluation.TraceCapture.MaxTraces = 1

	first := newTraceCaptureManager(cfg, nil)
	first.attachInteractionRegistry(workspace, registry)
	first.close()
	olderPath := filepath.Join(
		workspace, "state", "evaluation", "traces",
		opaqueTraceID("interaction", older.ID, time.UnixMilli(older.CreatedAt))+".json",
	)
	newerPath := filepath.Join(
		workspace, "state", "evaluation", "traces",
		opaqueTraceID("interaction", newer.ID, time.UnixMilli(newer.CreatedAt))+".json",
	)
	if _, err := os.Stat(olderPath); !os.IsNotExist(err) {
		t.Fatalf("older trace survived count pruning: %v", err)
	}
	if _, err := os.Stat(newerPath); err != nil {
		t.Fatalf("newer trace missing after count pruning: %v", err)
	}

	second := newTraceCaptureManager(cfg, nil)
	second.attachInteractionRegistry(workspace, registry)
	second.close()
	if _, err := os.Stat(olderPath); !os.IsNotExist(err) {
		t.Fatalf("count-pruned trace was resurrected: %v", err)
	}
	if _, err := os.Stat(newerPath); err != nil {
		t.Fatalf("newer trace displaced during reconciliation: %v", err)
	}
}

func TestTraceCaptureUsesTerminalLifecycleTimeForLiveTraceRetention(t *testing.T) {
	workspace := t.TempDir()
	terminalTime := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Millisecond)
	registry := interactions.NewRegistryWithOptions(
		interactions.WorkspaceStorePath(workspace),
		interactions.Options{Now: func() time.Time { return terminalTime }},
	)
	manager := newTraceCaptureManager(traceTestConfig(workspace), nil)
	manager.attachInteractionRegistry(workspace, registry)
	record := createTraceTestInteraction(t, registry, "interaction-live-retention", "session-1")
	record, err := registry.Cancel(record.ID, record.Revision, "fixture_cancel")
	if err != nil {
		t.Fatal(err)
	}
	tracePath := filepath.Join(
		workspace, "state", "evaluation", "traces",
		opaqueTraceID("interaction", record.ID, time.UnixMilli(record.CreatedAt))+".json",
	)
	waitForTracePath(t, tracePath)
	manager.close()
	info, err := os.Stat(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	want := time.UnixMilli(record.ResolvedAt)
	if !info.ModTime().Equal(want) {
		t.Fatalf("live trace retention time = %s, want %s", info.ModTime(), want)
	}
}

func TestCompleteTerminalInteractionTraceRequiresFinalSequenceAndRevision(t *testing.T) {
	workspace := t.TempDir()
	registry := interactions.NewRegistry(interactions.WorkspaceStorePath(workspace))
	record := createTraceTestInteraction(t, registry, "interaction-terminal-envelope", "session-1")
	record, err := registry.Cancel(record.ID, record.Revision, "fixture_cancel")
	if err != nil {
		t.Fatal(err)
	}
	history := registry.ListEvents(record.ID)
	settings := traceCaptureSettingsFromConfig(traceTestConfig(workspace))
	partial := buildInteractionTrace(settings, workspace, record, history[:len(history)-1])
	partial.trace.Truncation = evaltrace.Truncation{}
	partial.trace.Outcome = &evaltrace.Outcome{Status: string(record.Status)}
	if completeTraceMatchesTerminalInteraction(partial.trace, record) {
		t.Fatal("terminal envelope accepted a trace without its final transition")
	}
	complete := buildInteractionTrace(settings, workspace, record, history)
	complete.trace.Outcome = &evaltrace.Outcome{Status: string(record.Status)}
	if !completeTraceMatchesTerminalInteraction(complete.trace, record) {
		t.Fatal("complete terminal lifecycle was rejected")
	}
}

func TestBuildInteractionTraceKeepsIdentityWhenHistoryPrefixIsEvicted(t *testing.T) {
	workspace := t.TempDir()
	createdAt := time.Date(2026, 7, 19, 18, 0, 0, 0, time.UTC)
	state := interactions.Record{
		ID: "interaction-stable-trace-id", Kind: interactions.KindQuestion,
		Status: interactions.StatusWaiting, CreatedAt: createdAt.UnixMilli(),
		Route:  interactions.Route{AgentID: "main", SessionKey: "session-1"},
		Origin: interactions.Origin{TurnID: "turn-1", ToolCallID: "call-1"},
	}
	history := []interactions.Event{
		{
			EventID: "event-1", InteractionID: state.ID, Type: interactions.EventCreated,
			To: interactions.StatusCreated, Revision: 1, Sequence: 1,
			EmittedAt: createdAt.UnixMilli(),
		},
		{
			EventID: "event-2", InteractionID: state.ID, Type: interactions.EventWaiting,
			From: interactions.StatusCreated, To: interactions.StatusWaiting,
			Revision: 2, Sequence: 2, EmittedAt: createdAt.Add(time.Minute).UnixMilli(),
		},
	}
	settings := traceCaptureSettingsFromConfig(traceTestConfig(workspace))
	full := buildInteractionTrace(settings, workspace, state, history)
	truncated := buildInteractionTrace(settings, workspace, state, history[1:])
	if full.trace.TraceID != truncated.trace.TraceID ||
		!full.startedAt.Equal(truncated.startedAt) || !full.startedAt.Equal(createdAt) {
		t.Fatalf(
			"trace identities differ: full=%s/%s truncated=%s/%s",
			full.trace.TraceID,
			full.startedAt,
			truncated.trace.TraceID,
			truncated.startedAt,
		)
	}
	if !truncated.trace.Truncation.Incomplete ||
		truncated.trace.Records[0].OffsetNanos != time.Minute.Nanoseconds() {
		t.Fatalf("truncated trace = %#v", truncated.trace)
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

func TestAppendCaptureRecordReportsCriticalDropAtRecordLimit(t *testing.T) {
	trace := &activeTraceCapture{
		critical: make(map[uint64]bool),
		origins:  make(map[string]struct{}),
		trace: evaltrace.Trace{
			Limits: evaltrace.AppliedLimits{MaxRecords: 1, MaxRecordBytes: 1024},
		},
	}
	appendCaptureRecord(trace, evaltrace.Record{
		Kind:   evaltrace.RecordInteractionTransition,
		Origin: evaltrace.Origin{Kind: "interaction_event", ID: "event-1"},
	}, true)
	appendCaptureRecord(trace, evaltrace.Record{
		Kind:   evaltrace.RecordInteractionTransition,
		Origin: evaltrace.Origin{Kind: "interaction_event", ID: "event-2"},
	}, true)
	if len(trace.trace.Records) != 1 {
		t.Fatalf("records = %d, want 1", len(trace.trace.Records))
	}
	if !trace.trace.Truncation.Incomplete || trace.trace.Truncation.DroppedRecords != 1 {
		t.Fatalf("truncation = %+v", trace.trace.Truncation)
	}
	if got := trace.trace.Truncation.DroppedByKind[evaltrace.RecordInteractionTransition]; got != 1 {
		t.Fatalf("dropped interaction records = %d, want 1", got)
	}
	if !slices.Contains(trace.trace.Truncation.Reasons, "record_count_limit") {
		t.Fatalf("truncation reasons = %v", trace.trace.Truncation.Reasons)
	}
}

func TestAppendCaptureRecordReportsEvictedNoncriticalRecord(t *testing.T) {
	trace := &activeTraceCapture{
		critical: make(map[uint64]bool),
		origins:  make(map[string]struct{}),
		trace: evaltrace.Trace{
			Limits: evaltrace.AppliedLimits{MaxRecords: 1, MaxRecordBytes: 1024},
		},
	}
	appendCaptureRecord(trace, evaltrace.Record{
		Kind:   evaltrace.RecordModelRequest,
		Origin: evaltrace.Origin{Kind: "runtime_event", ID: "event-1"},
	}, false)
	appendCaptureRecord(trace, evaltrace.Record{
		Kind:   evaltrace.RecordInteractionTransition,
		Origin: evaltrace.Origin{Kind: "interaction_event", ID: "event-2"},
	}, true)
	if len(trace.trace.Records) != 1 ||
		trace.trace.Records[0].Kind != evaltrace.RecordInteractionTransition {
		t.Fatalf("records = %+v", trace.trace.Records)
	}
	if !trace.trace.Truncation.Incomplete || trace.trace.Truncation.DroppedRecords != 1 {
		t.Fatalf("truncation = %+v", trace.trace.Truncation)
	}
	if got := trace.trace.Truncation.DroppedByKind[evaltrace.RecordModelRequest]; got != 1 {
		t.Fatalf("dropped model requests = %d, want 1", got)
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

func waitForTracePath(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for trace at %s", path)
}

func createTraceTestInteraction(
	t *testing.T,
	registry *interactions.Registry,
	id string,
	sessionKey string,
) interactions.Record {
	t.Helper()
	record, err := registry.Create(interactions.CreateRequest{
		ID: id, Kind: interactions.KindQuestion,
		Route: interactions.Route{
			AgentID: "main", SessionKey: sessionKey, Channel: "telegram",
			ChatID: "chat-1", SenderID: "sender-1",
		},
		Origin: interactions.Origin{
			TurnID: "turn-1", ToolCallID: "call-1", ToolName: "request_user_input",
		},
		Questions: []interactions.Question{{ID: "environment", Question: "Which environment?"}},
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func loadTraceFile(t *testing.T, path string) evaltrace.Trace {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var trace evaltrace.Trace
	if err = json.Unmarshal(data, &trace); err != nil {
		t.Fatal(err)
	}
	if err = evaltrace.Validate(trace); err != nil {
		t.Fatal(err)
	}
	return trace
}

func fileModeForTraceTest(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode()
}
