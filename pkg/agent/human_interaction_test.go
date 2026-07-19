package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/interactions"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/session"
	taskregistry "github.com/sipeed/picoclaw/pkg/tasks"
	"github.com/sipeed/picoclaw/pkg/tools"
)

type interactionChannelManager struct {
	*recordingChannelManager
	sent    chan bus.OutboundMessage
	sendErr error
}

type durableApprovalHook struct {
	actionSummary string
	revoked       bool
}

func (h *durableApprovalHook) ApproveTool(
	context.Context,
	*ToolApprovalRequest,
) (ApprovalDecision, error) {
	if h.revoked {
		return ApprovalDecision{Reason: "policy revoked human override"}, nil
	}
	return ApprovalDecision{RequireHuman: true, ActionSummary: h.actionSummary}, nil
}

type approvalCountingTool struct {
	executions int
}

func (*approvalCountingTool) Name() string { return "approval_counting" }

func (*approvalCountingTool) Description() string { return "Run a protected test action" }

func (*approvalCountingTool) Parameters() map[string]any {
	return map[string]any{"type": "object"}
}

func (t *approvalCountingTool) Execute(context.Context, map[string]any) *tools.ToolResult {
	t.executions++
	return tools.NewToolResult("protected action completed")
}

type approvalContextTool struct {
	executions int
	inbound    bus.InboundContext
}

func (*approvalContextTool) Name() string { return "approval_context" }

func (*approvalContextTool) Description() string { return "Capture protected inbound context" }

func (*approvalContextTool) Parameters() map[string]any {
	return map[string]any{"type": "object"}
}

func (t *approvalContextTool) Execute(ctx context.Context, _ map[string]any) *tools.ToolResult {
	t.executions++
	t.inbound = tools.ToolInboundContext(ctx)
	return tools.NewToolResult("protected context captured")
}

type interactionOwnershipBus struct {
	*bus.MessageBus
	mu       sync.Mutex
	acked    []string
	released []string
}

func (b *interactionOwnershipBus) AckInbound(ctx context.Context, msg bus.InboundMessage) error {
	b.mu.Lock()
	b.acked = append(b.acked, msg.SpoolID)
	b.mu.Unlock()
	return b.MessageBus.AckInbound(ctx, msg)
}

func (b *interactionOwnershipBus) ReleaseInbound(
	ctx context.Context,
	msg bus.InboundMessage,
	cause error,
) error {
	b.mu.Lock()
	b.released = append(b.released, msg.SpoolID)
	b.mu.Unlock()
	return b.MessageBus.ReleaseInbound(ctx, msg, cause)
}

func (b *interactionOwnershipBus) counts() (int, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.acked), len(b.released)
}

func newInteractionChannelManager() *interactionChannelManager {
	return &interactionChannelManager{
		recordingChannelManager: &recordingChannelManager{},
		sent:                    make(chan bus.OutboundMessage, 16),
	}
}

func (m *interactionChannelManager) SendMessage(_ context.Context, msg bus.OutboundMessage) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.sent <- msg
	return nil
}

func (m *interactionChannelManager) SendMessageDefiniteRetryOnly(
	ctx context.Context,
	msg bus.OutboundMessage,
) error {
	return m.SendMessage(ctx, msg)
}

func TestCorruptHumanInteractionStoreFailsClosed(t *testing.T) {
	workspace := t.TempDir()
	storePath := interactions.WorkspaceStorePath(workspace)
	if err := os.MkdirAll(filepath.Dir(storePath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(storePath, []byte("not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	al := &AgentLoop{}
	if !al.hasNonterminalInteraction(workspace, "session") {
		t.Fatal("corrupt interaction state did not block normal inbound handling")
	}
}

func testToolSuspensionRequest(workspace string) ToolSuspensionRequest {
	return ToolSuspensionRequest{
		Workspace: workspace,
		Prompt: interactions.SuspensionRequest{
			Kind: interactions.KindQuestion,
			Questions: []interactions.Question{{
				ID: "deploy_mode", Header: "Deploy", Question: "Which mode should be used?",
				Options: []interactions.Option{
					{Label: "Canary", Description: "Deploy one profile first."},
					{Label: "All", Description: "Deploy every profile now."},
				},
			}},
			Timeout: time.Hour,
		},
		Route: interactions.Route{
			AgentID: "main", SessionKey: "session-1", RouteSessionKey: "route-1",
			Channel: "telegram", AccountID: "primary", ChatID: "chat-1", ChatType: "direct",
			SenderID: "user-1",
		},
		Origin: interactions.Origin{
			TurnID: "turn-1", ToolCallID: "call-question", ToolName: "request_user_input",
		},
	}
}

func TestHumanInteractionRuntimePersistsAndQueuesPromptBeforeWaiting(t *testing.T) {
	messageBus := bus.NewMessageBus()
	manager := newInteractionChannelManager()
	al := &AgentLoop{cfg: config.DefaultConfig(), bus: messageBus, channelManager: manager}
	workspace := t.TempDir()

	disposition, err := al.humanInteractionRuntime().SuspendToolCall(
		t.Context(),
		testToolSuspensionRequest(workspace),
	)
	if err != nil || !disposition.Durable || disposition.InteractionID == "" {
		t.Fatalf("SuspendToolCall() = (%#v, %v)", disposition, err)
	}
	record, ok := al.interactionRegistryForWorkspace(workspace).Get(disposition.InteractionID)
	if !ok || record.Status != interactions.StatusWaiting || record.DeliveryTries != 1 {
		t.Fatalf("record = %#v", record)
	}
	select {
	case outbound := <-manager.sent:
		if !strings.Contains(outbound.Content, "Input needed ["+record.ShortID+"]") ||
			!strings.Contains(outbound.Content, "Canary") ||
			outbound.Context.Raw[interactionIDMetadata] != record.ID ||
			outbound.Context.Raw["delivery_key"] != interactionDeliveryKey(record.ID, "prompt") ||
			outbound.Context.Account != "primary" {
			t.Fatalf("outbound prompt = %#v", outbound)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for interaction prompt")
	}
}

func TestInteractionEventsProjectOwningTaskState(t *testing.T) {
	workspace := t.TempDir()
	al := &AgentLoop{cfg: config.DefaultConfig()}
	tasks := al.taskRegistryForWorkspace(workspace)
	if err := tasks.Upsert(taskregistry.Record{
		TaskID: "task-1", Status: taskregistry.StatusRunning,
		DeliveryStatus: taskregistry.DeliveryPending,
	}); err != nil {
		t.Fatal(err)
	}
	record := interactions.Record{
		ID: "interaction-1", ShortID: "abc123", Status: interactions.StatusWaiting,
		PromptSummary: "Choose a deployment mode",
		Origin:        interactions.Origin{TaskID: "task-1"},
	}
	al.observeInteractionEvent(workspace, interactions.EventObservation{
		Event: interactions.Event{Type: interactions.EventWaiting}, Record: record,
	})
	task, _ := tasks.Get("task-1")
	if task.Status != taskregistry.StatusWaitingForInput ||
		task.InteractionShortID != "abc123" {
		t.Fatalf("waiting task = %#v", task)
	}

	record.Status = interactions.StatusClaimed
	al.observeInteractionEvent(workspace, interactions.EventObservation{
		Event: interactions.Event{Type: interactions.EventAnswerClaimed}, Record: record,
	})
	task, _ = tasks.Get("task-1")
	if task.Status != taskregistry.StatusRunning || task.InteractionShortID != "" {
		t.Fatalf("resumed task = %#v", task)
	}

	record.Status = interactions.StatusFailed
	record.FailureDetail = "continuation failed"
	al.observeInteractionEvent(workspace, interactions.EventObservation{
		Event: interactions.Event{Type: interactions.EventFailed}, Record: record,
	})
	task, _ = tasks.Get("task-1")
	if task.Status != taskregistry.StatusFailed || task.Error != "continuation failed" {
		t.Fatalf("failed task = %#v", task)
	}
}

func TestTaskInteractionFinalHonorsParentOnlyDelivery(t *testing.T) {
	al, agent, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
	defer cleanup()
	workspace := agent.Workspace
	tasks := al.taskRegistryForWorkspace(workspace)
	if err := tasks.Upsert(taskregistry.Record{
		TaskID: "subagent-parent", Runtime: taskregistry.RuntimeSubagent,
		TaskKind: "spawn", Task: "finish in parent", Status: taskregistry.StatusRunning,
		DeliveryStatus: taskregistry.DeliveryPending,
		DeliveryMode:   string(tools.AsyncDeliveryParentOnly),
		InteractionID:  "interaction-parent",
		Channel:        "telegram", ChatID: "chat-1", RequesterSessionKey: "owner-session",
	}); err != nil {
		t.Fatal(err)
	}
	registry := al.interactionRegistryForWorkspace(workspace)
	record, err := registry.Create(interactions.CreateRequest{
		ID: "interaction-parent", Kind: interactions.KindQuestion,
		Route: interactions.Route{
			AgentID: agent.ID, SessionKey: "owner-session", RouteSessionKey: "route-owner",
			Channel: "telegram", ChatID: "chat-1", SenderID: "user-1",
		},
		Origin: interactions.Origin{
			TurnID: "turn-task", ToolCallID: "call-task", ToolName: "request_user_input",
			TaskID: "subagent-parent", ContinuationSessionKey: "task-session",
		},
		Questions: []interactions.Question{{ID: "confirm", Question: "Proceed?"}},
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
	record, err = registry.ClaimAnswer(record.ID, record.Revision, interactions.Answer{
		Text: "yes", Values: map[string]string{"confirm": "yes"}, ReceivedAt: time.Now().UnixMilli(),
	}, interactions.OutcomeAnswered)
	if err != nil {
		t.Fatal(err)
	}
	record, err = registry.MarkResuming(record.ID, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	inbound := bus.InboundContext{
		Channel: "telegram", ChatID: "chat-1", SenderID: "user-1",
	}
	if err := al.deliverTaskInteractionFinal(
		t.Context(), registry, workspace, record, inbound, "raw child final",
	); err != nil {
		t.Fatalf("deliverTaskInteractionFinal() error = %v", err)
	}
	task, _ := tasks.Get("subagent-parent")
	if task.Status != taskregistry.StatusSucceeded ||
		task.DeliveryStatus != taskregistry.DeliverySessionQueued {
		t.Fatalf("parent-only task = %#v", task)
	}
	resolved, _ := registry.Get(record.ID)
	if resolved.Status != interactions.StatusResolved ||
		resolved.FinalDeliveryState != interactions.DeliveryStateDelivered {
		t.Fatalf("interaction after parent handoff = %#v", resolved)
	}
	events := registry.ListEvents(record.ID)
	startedAt, completedAt := -1, -1
	for i, event := range events {
		if event.Type != interactions.EventFinalDelivery {
			continue
		}
		switch event.Code {
		case "delivery_started":
			startedAt = i
		case "delivery_completed":
			completedAt = i
		}
	}
	if startedAt < 0 || completedAt <= startedAt {
		t.Fatalf("task delivery was not durably started before completion: %#v", events)
	}
	msgBus := al.bus.(*bus.MessageBus)
	select {
	case outbound := <-msgBus.OutboundChan():
		if outbound.Content == "raw child final" {
			t.Fatalf("parent-only delivery leaked raw child final: %#v", outbound)
		}
	case <-time.After(time.Second):
		t.Fatal("parent-only completion was not processed")
	}
}

func TestNewAgentLoopRegistersRequestUserInputByDefault(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = t.TempDir()
	al := NewAgentLoop(cfg, bus.NewMessageBus(), &simpleConvProvider{})
	defer al.Close()
	agent := al.GetRegistry().GetDefaultAgent()
	if agent == nil || !agent.Tools.HasRegistered("request_user_input") {
		t.Fatal("request_user_input is not registered by default")
	}
	if _, ok := al.interactionRegistries.Load(agent.Workspace); !ok {
		t.Fatal("interaction registry was not initialized for recovery")
	}
}

func TestDisabledRequestUserInputStillInitializesRecoveryRegistry(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = t.TempDir()
	cfg.Tools.RequestUserInput.Enabled = false
	al := NewAgentLoop(cfg, bus.NewMessageBus(), &simpleConvProvider{})
	defer al.Close()
	agent := al.GetRegistry().GetDefaultAgent()
	if agent == nil {
		t.Fatal("missing default agent")
	}
	if agent.Tools.HasRegistered("request_user_input") {
		t.Fatal("disabled request_user_input was registered")
	}
	if _, ok := al.interactionRegistries.Load(agent.Workspace); !ok {
		t.Fatal("disabled tool prevented durable interaction recovery")
	}
}

func TestHumanInteractionPromptFailureRemainsAmbiguousAndDoesNotRetry(t *testing.T) {
	manager := newInteractionChannelManager()
	manager.sendErr = errors.New("delivery failed")
	al := &AgentLoop{cfg: config.DefaultConfig(), bus: failingMessageBus{}, channelManager: manager}
	workspace := t.TempDir()
	disposition, err := al.humanInteractionRuntime().SuspendToolCall(
		t.Context(),
		testToolSuspensionRequest(workspace),
	)
	if err == nil || !disposition.Durable {
		t.Fatalf("SuspendToolCall() = (%#v, %v), want durable delivery error", disposition, err)
	}
	record, _ := al.interactionRegistryForWorkspace(workspace).Get(disposition.InteractionID)
	if record.Status != interactions.StatusCreated || record.DeliveryError == "" ||
		record.PromptDeliveryState != interactions.DeliveryStateAmbiguous {
		t.Fatalf("record after failed delivery = %#v", record)
	}

	manager.sendErr = nil
	if al.retryInteractionPrompt(
		t.Context(),
		al.interactionRegistryForWorkspace(workspace),
		record,
	) {
		t.Fatal("ambiguous prompt delivery was retried")
	}
	record, _ = al.interactionRegistryForWorkspace(workspace).Get(disposition.InteractionID)
	if record.Status != interactions.StatusCreated || record.DeliveryTries != 1 {
		t.Fatalf("record after refused retry = %#v", record)
	}
	select {
	case duplicate := <-manager.sent:
		t.Fatalf("ambiguous prompt was duplicated: %#v", duplicate)
	default:
	}
}

func TestHumanInteractionDefiniteNotSentPromptRetries(t *testing.T) {
	manager := newInteractionChannelManager()
	manager.sendErr = channels.DefiniteNotSentDeliveryError(errors.New("worker unavailable"))
	al := &AgentLoop{cfg: config.DefaultConfig(), channelManager: manager}
	workspace := t.TempDir()
	disposition, err := al.humanInteractionRuntime().SuspendToolCall(
		t.Context(),
		testToolSuspensionRequest(workspace),
	)
	if err == nil || !disposition.Durable {
		t.Fatalf("SuspendToolCall() = (%#v, %v), want durable not-sent error", disposition, err)
	}
	registry := al.interactionRegistryForWorkspace(workspace)
	record, _ := registry.Get(disposition.InteractionID)
	if record.PromptDeliveryState != interactions.DeliveryStateNotSent {
		t.Fatalf("definite failure state = %#v", record)
	}

	manager.sendErr = nil
	if !al.retryInteractionPrompt(t.Context(), registry, record) {
		t.Fatal("definite not-sent prompt was not retried")
	}
	record, _ = registry.Get(record.ID)
	if record.Status != interactions.StatusWaiting || !record.PromptDelivered ||
		record.DeliveryTries != 2 {
		t.Fatalf("record after definite retry = %#v", record)
	}
	select {
	case <-manager.sent:
	default:
		t.Fatal("retry did not send the prompt")
	}
}

func TestRecoveryDoesNotResendPromptAfterAmbiguousCrashWindow(t *testing.T) {
	al, agent, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
	defer cleanup()
	manager := newInteractionChannelManager()
	al.channelManager = manager
	sessionKey := "session-ambiguous-prompt"
	agent.Sessions.AddFullMessage(sessionKey, providers.Message{Role: "user", Content: "Deploy this"})
	agent.Sessions.AddFullMessage(sessionKey, providers.Message{
		Role: "assistant",
		ToolCalls: []providers.ToolCall{{
			ID: "call-question", Name: "request_user_input",
			Function: &providers.FunctionCall{Name: "request_user_input", Arguments: `{}`},
		}},
	})
	request := testToolSuspensionRequest(agent.Workspace)
	request.Route.SessionKey = sessionKey
	registry := al.interactionRegistryForWorkspace(agent.Workspace)
	record, err := registry.Create(interactions.CreateRequest{
		Kind: request.Prompt.Kind, Route: request.Route, Origin: request.Origin,
		Questions: request.Prompt.Questions, ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	record, err = registry.BeginPromptDelivery(record.ID, record.Revision)
	if err != nil || record.PromptDeliveryState != interactions.DeliveryStateSending {
		t.Fatalf("begin prompt delivery = (%#v, %v)", record, err)
	}

	if recovered := al.RecoverHumanInteractions(t.Context()); recovered != 1 {
		t.Fatalf("RecoverHumanInteractions() = %d, want 1", recovered)
	}
	record, _ = registry.Get(record.ID)
	if record.Status != interactions.StatusResolved ||
		record.Outcome != interactions.OutcomeDeliveryUnknown {
		t.Fatalf("record after ambiguous prompt recovery = %#v", record)
	}
	select {
	case outbound := <-manager.sent:
		if strings.Contains(outbound.Content, "Input needed") {
			t.Fatalf("recovery resent ambiguous prompt: %#v", outbound)
		}
	default:
		t.Fatal("recovery did not deliver the delivery-unknown continuation")
	}
	select {
	case duplicate := <-manager.sent:
		t.Fatalf("recovery emitted a duplicate message: %#v", duplicate)
	default:
	}
}

func TestRecoveryDoesNotResendAmbiguousFinal(t *testing.T) {
	al, agent, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
	defer cleanup()
	manager := newInteractionChannelManager()
	al.channelManager = manager
	sessionKey := "session-ambiguous-final"
	agent.Sessions.AddFullMessage(sessionKey, providers.Message{
		Role: "assistant", ToolCalls: []providers.ToolCall{{ID: "call-question"}},
	})
	request := testToolSuspensionRequest(agent.Workspace)
	request.Route.SessionKey = sessionKey
	registry := al.interactionRegistryForWorkspace(agent.Workspace)
	record, err := registry.Create(interactions.CreateRequest{
		Kind: request.Prompt.Kind, Route: request.Route, Origin: request.Origin,
		Questions: request.Prompt.Questions, ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	record, _ = registry.RecordDeliveryAttempt(record.ID, record.Revision, true, "")
	record, _ = registry.MarkWaiting(record.ID, record.Revision)
	record, _ = registry.ClaimAnswer(record.ID, record.Revision, interactions.Answer{
		Text: "Canary", Values: map[string]string{"deploy_mode": "Canary"},
	}, interactions.OutcomeAnswered)
	if ensureErr := al.ensureInteractionToolResult(t.Context(), agent, record); ensureErr != nil {
		t.Fatal(ensureErr)
	}
	record, _ = registry.MarkResuming(record.ID, record.Revision)
	agent.Sessions.AddFullMessage(sessionKey, providers.Message{Role: "assistant", Content: "Final response"})
	record, err = registry.BeginFinalDelivery(record.ID, record.Revision)
	if err != nil || record.FinalDeliveryState != interactions.DeliveryStateSending {
		t.Fatalf("begin final delivery = (%#v, %v)", record, err)
	}

	if recovered := al.RecoverHumanInteractions(t.Context()); recovered != 1 {
		t.Fatalf("RecoverHumanInteractions() = %d, want 1", recovered)
	}
	record, _ = registry.Get(record.ID)
	if record.Status != interactions.StatusFailed || record.FailureCode != "final_delivery_ambiguous" {
		t.Fatalf("record after ambiguous final recovery = %#v", record)
	}
	select {
	case duplicate := <-manager.sent:
		t.Fatalf("recovery resent ambiguous final: %#v", duplicate)
	default:
	}
}

func TestRecoveryRetriesDefinitelyNotSentFinal(t *testing.T) {
	al, agent, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
	defer cleanup()
	manager := newInteractionChannelManager()
	al.channelManager = manager
	sessionKey := "session-not-sent-final"
	agent.Sessions.AddFullMessage(sessionKey, providers.Message{
		Role: "assistant", ToolCalls: []providers.ToolCall{{ID: "call-question"}},
	})
	request := testToolSuspensionRequest(agent.Workspace)
	request.Route.SessionKey = sessionKey
	registry := al.interactionRegistryForWorkspace(agent.Workspace)
	record, err := registry.Create(interactions.CreateRequest{
		Kind: request.Prompt.Kind, Route: request.Route, Origin: request.Origin,
		Questions: request.Prompt.Questions, ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	record, _ = registry.RecordDeliveryAttempt(record.ID, record.Revision, true, "")
	record, _ = registry.MarkWaiting(record.ID, record.Revision)
	record, _ = registry.ClaimAnswer(record.ID, record.Revision, interactions.Answer{
		Text: "Canary", Values: map[string]string{"deploy_mode": "Canary"},
	}, interactions.OutcomeAnswered)
	if ensureErr := al.ensureInteractionToolResult(t.Context(), agent, record); ensureErr != nil {
		t.Fatal(ensureErr)
	}
	record, _ = registry.MarkResuming(record.ID, record.Revision)
	agent.Sessions.AddFullMessage(sessionKey, providers.Message{Role: "assistant", Content: "Final response"})
	record, _ = registry.BeginFinalDelivery(record.ID, record.Revision)
	record, err = registry.CompleteFinalDelivery(
		record.ID,
		record.Revision,
		false,
		false,
		"worker unavailable",
	)
	if err != nil || record.FinalDeliveryState != interactions.DeliveryStateNotSent {
		t.Fatalf("complete not-sent final = (%#v, %v)", record, err)
	}

	if recovered := al.RecoverHumanInteractions(t.Context()); recovered != 1 {
		t.Fatalf("RecoverHumanInteractions() = %d, want 1", recovered)
	}
	record, _ = registry.Get(record.ID)
	if record.Status != interactions.StatusResolved || !record.FinalDelivered {
		t.Fatalf("record after not-sent final recovery = %#v", record)
	}
	select {
	case outbound := <-manager.sent:
		if outbound.Content != "Final response" {
			t.Fatalf("retried final = %#v", outbound)
		}
	default:
		t.Fatal("definitely not-sent final was not retried")
	}
}

func TestRecoveryCommitsAcknowledgedPromptWithoutDuplicateSend(t *testing.T) {
	manager := newInteractionChannelManager()
	al := &AgentLoop{cfg: config.DefaultConfig(), channelManager: manager}
	workspace := t.TempDir()
	request := testToolSuspensionRequest(workspace)
	registry := al.interactionRegistryForWorkspace(workspace)
	record, err := registry.Create(interactions.CreateRequest{
		Kind: request.Prompt.Kind, Route: request.Route, Origin: request.Origin,
		Questions: request.Prompt.Questions, ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	record, err = registry.RecordDeliveryAttempt(record.ID, record.Revision, true, "")
	if err != nil || !record.PromptDelivered || record.Status != interactions.StatusCreated {
		t.Fatalf("acknowledged created record = (%#v, %v)", record, err)
	}
	if recovered := al.RecoverHumanInteractions(t.Context()); recovered != 1 {
		t.Fatalf("RecoverHumanInteractions() = %d, want 1", recovered)
	}
	record, _ = registry.Get(record.ID)
	if record.Status != interactions.StatusWaiting || record.DeliveryTries != 1 {
		t.Fatalf("recovered record = %#v", record)
	}
	select {
	case duplicate := <-manager.sent:
		t.Fatalf("recovery duplicated acknowledged prompt: %#v", duplicate)
	default:
	}
}

func TestParseInteractionAnswerSupportsExplicitAndStructuredReplies(t *testing.T) {
	record := interactions.Record{
		ShortID: "ABC123",
		Questions: []interactions.Question{
			{ID: "target", Question: "Where?"},
			{ID: "mode", Question: "How?"},
		},
	}
	answer, err := parseInteractionAnswer(
		record,
		"/answer abc123 target: staging\nmode: canary",
		"message-1",
	)
	if err != nil {
		t.Fatalf("parseInteractionAnswer() error = %v", err)
	}
	if answer.Values["target"] != "staging" || answer.Values["mode"] != "canary" ||
		answer.MessageID != "message-1" {
		t.Fatalf("answer = %#v", answer)
	}
	if _, err := parseInteractionAnswer(record, "target: staging", "message-2"); err == nil {
		t.Fatal("parseInteractionAnswer() accepted incomplete multi-question answer")
	}
	prompt := renderInteractionPrompt(record)
	if !strings.Contains(prompt, "[target]") || !strings.Contains(prompt, "[mode]") {
		t.Fatalf("multi-question prompt omitted canonical IDs: %q", prompt)
	}
	if _, err := parseInteractionAnswer(
		record,
		"target: staging\nmode: canary",
		"message-3",
	); err != nil {
		t.Fatalf("rendered question IDs did not round-trip through parser: %v", err)
	}
}

func TestApprovalPromptAndAnswerUseFixedPolicyChoices(t *testing.T) {
	record := interactions.Record{
		Kind: interactions.KindApproval, ShortID: "APR123",
		PromptSummary:  "Run a protected deployment command?",
		ApprovalAction: "Tool: deploy\nAction: Run a protected deployment command?",
	}
	prompt := renderInteractionPrompt(record)
	if !strings.Contains(prompt, "Approval needed [APR123]") ||
		!strings.Contains(prompt, record.PromptSummary) ||
		!strings.Contains(prompt, record.ApprovalAction) ||
		!strings.Contains(prompt, "allow_once") || !strings.Contains(prompt, "deny") {
		t.Fatalf("approval prompt = %q", prompt)
	}
	answer, err := parseInteractionAnswer(record, "/answer apr123 allow once", "message-approval")
	if err != nil || answer.Text != "allow_once" || answer.MessageID != "message-approval" {
		t.Fatalf("allow answer = (%#v, %v)", answer, err)
	}
	answer, err = parseInteractionAnswer(record, "deny", "message-deny")
	if err != nil || answer.Text != "deny" {
		t.Fatalf("deny answer = (%#v, %v)", answer, err)
	}
	if _, err = parseInteractionAnswer(record, "always", "message-invalid"); err == nil {
		t.Fatal("approval parser accepted a persistent grant")
	}
}

func TestDurableHumanApprovalAllowsOrDeniesOriginalToolCall(t *testing.T) {
	for _, test := range []struct {
		name           string
		answer         string
		outcome        interactions.Outcome
		wantExecutions int
		wantConsumed   bool
		revokePolicy   bool
		mutateArgs     bool
	}{
		{name: "allow once", answer: "allow_once", outcome: interactions.OutcomeAllowed, wantExecutions: 1, wantConsumed: true},
		{name: "deny", answer: "deny", outcome: interactions.OutcomeDenied},
		{name: "policy revoked", answer: "allow_once", outcome: interactions.OutcomeAllowed, revokePolicy: true},
		{name: "arguments changed", answer: "allow_once", outcome: interactions.OutcomeAllowed, mutateArgs: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := &sequenceProvider{responses: []*providers.LLMResponse{
				{ToolCalls: []providers.ToolCall{{
					ID: "call-protected", Name: "approval_counting",
					Arguments: map[string]any{"token": "secret-value"},
					Function: &providers.FunctionCall{
						Name: "approval_counting", Arguments: `{"token":"secret-value"}`,
					},
				}}},
				{Content: "approval flow finished", FinishReason: "stop"},
			}}
			al, agent, cleanup := newTurnCoordTestLoop(t, provider)
			defer cleanup()
			manager := newInteractionChannelManager()
			al.channelManager = manager
			tool := &approvalCountingTool{}
			agent.Tools.Register(tool)
			hook := &durableApprovalHook{
				actionSummary: "Run the protected test action",
			}
			if err := al.MountHook(NamedHook("durable-approval", hook)); err != nil {
				t.Fatal(err)
			}
			inbound := &bus.InboundContext{
				Channel: "telegram", ChatID: "chat-1", SenderID: "user-1",
			}
			turnStatus := TurnEndStatusCompleted
			response, err := al.runAgentLoop(t.Context(), agent, processOptions{
				TurnStatus: &turnStatus,
				Dispatch: DispatchRequest{
					RouteSessionKey: "route-approval", SessionKey: "session-approval",
					UserMessage: "run protected action", InboundContext: inbound,
				},
				DefaultResponse: defaultResponse, EnableSummary: true, SendResponse: false,
			})
			if err != nil || response != "" || turnStatus != TurnEndStatusSuspended || tool.executions != 0 {
				t.Fatalf(
					"initial approval turn = (%q, %q, executions=%d, err=%v)",
					response,
					turnStatus,
					tool.executions,
					err,
				)
			}
			registry := al.interactionRegistryForWorkspace(agent.Workspace)
			record, ok := activeInteractionForSession(registry, "session-approval")
			if !ok || record.Kind != interactions.KindApproval ||
				record.Status != interactions.StatusWaiting || record.Origin.ArgumentHash == "" {
				t.Fatalf("approval interaction = %#v", record)
			}
			select {
			case prompt := <-manager.sent:
				if !strings.Contains(prompt.Content, "Approval needed") ||
					!strings.Contains(prompt.Content, "Tool: approval_counting") ||
					!strings.Contains(prompt.Content, "Action: Run the protected test action") ||
					strings.Contains(prompt.Content, "secret-value") {
					t.Fatalf("approval prompt = %#v", prompt)
				}
			case <-time.After(time.Second):
				t.Fatal("approval prompt was not delivered")
			}
			if test.revokePolicy {
				hook.revoked = true
			}
			if test.mutateArgs {
				history := agent.Sessions.GetHistory("session-approval")
				for messageIndex := range history {
					for callIndex := range history[messageIndex].ToolCalls {
						call := &history[messageIndex].ToolCalls[callIndex]
						if call.ID == "call-protected" {
							call.Arguments = map[string]any{"token": "changed-after-approval"}
							call.Function.Arguments = `{"token":"changed-after-approval"}`
						}
					}
				}
				agent.Sessions.SetHistory("session-approval", history)
			}
			record, err = registry.ClaimAnswer(record.ID, record.Revision, interactions.Answer{
				Text: test.answer, MessageID: "approval-answer", ReceivedAt: time.Now().UnixMilli(),
			}, test.outcome)
			if err != nil {
				t.Fatal(err)
			}
			if err = al.resumeClaimedInteraction(
				t.Context(), registry, agent.Workspace, agent, nil, *inbound, record,
			); err != nil {
				t.Fatalf("resumeClaimedInteraction() error = %v", err)
			}
			resolved, _ := registry.Get(record.ID)
			if resolved.Status != interactions.StatusResolved ||
				(resolved.ApprovalConsumedAt != 0) != test.wantConsumed ||
				tool.executions != test.wantExecutions {
				t.Fatalf("resolved approval = %#v, executions=%d", resolved, tool.executions)
			}
			select {
			case final := <-manager.sent:
				if final.Content != "approval flow finished" {
					t.Fatalf("approval final = %#v", final)
				}
			case <-time.After(time.Second):
				t.Fatal("approval continuation final was not delivered")
			}
		})
	}
}

func TestHumanApprovalNeverRendersGenericArguments(t *testing.T) {
	provider := &sequenceProvider{responses: []*providers.LLMResponse{
		{ToolCalls: []providers.ToolCall{{
			ID: "call-opaque", Name: "approval_counting",
			Arguments: map[string]any{"source": "-----BEGIN PRIVATE KEY-----\nsecret"},
			Function: &providers.FunctionCall{
				Name: "approval_counting", Arguments: `{"source":"-----BEGIN PRIVATE KEY-----\\nsecret"}`,
			},
		}}},
		{Content: "approval flow finished", FinishReason: "stop"},
	}}
	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()
	manager := newInteractionChannelManager()
	al.channelManager = manager
	tool := &approvalCountingTool{}
	agent.Tools.Register(tool)
	if err := al.MountHook(NamedHook("opaque-approval", &durableApprovalHook{
		actionSummary: "Rotate production signing material",
	})); err != nil {
		t.Fatal(err)
	}
	inbound := &bus.InboundContext{
		Channel: "telegram", ChatID: "chat-1", SenderID: "user-1",
	}
	turnStatus := TurnEndStatusCompleted
	response, err := al.runAgentLoop(t.Context(), agent, processOptions{
		TurnStatus: &turnStatus,
		Dispatch: DispatchRequest{
			RouteSessionKey: "route-opaque", SessionKey: "session-opaque",
			UserMessage: "run opaque action", InboundContext: inbound,
		},
		DefaultResponse: defaultResponse, EnableSummary: true, SendResponse: false,
	})
	if err != nil || response != "" ||
		turnStatus != TurnEndStatusSuspended || tool.executions != 0 {
		t.Fatalf(
			"opaque approval turn = (%q, %q, executions=%d, err=%v)",
			response,
			turnStatus,
			tool.executions,
			err,
		)
	}
	record, ok := activeInteractionForSession(
		al.interactionRegistryForWorkspace(agent.Workspace), "session-opaque",
	)
	if !ok || strings.Contains(record.ApprovalAction, "PRIVATE KEY") ||
		record.ApprovalAction != "Tool: approval_counting\nAction: Rotate production signing material" {
		t.Fatalf("approval interaction = %#v", record)
	}
	select {
	case prompt := <-manager.sent:
		if strings.Contains(prompt.Content, "PRIVATE KEY") ||
			!strings.Contains(prompt.Content, record.ApprovalAction) {
			t.Fatalf("approval prompt = %#v", prompt)
		}
	case <-time.After(time.Second):
		t.Fatal("approval prompt was not delivered")
	}
}

func TestApprovalRecoveryNeverReexecutesConsumedOrTimedOutCall(t *testing.T) {
	for _, test := range []struct {
		name        string
		consume     bool
		wantOutcome interactions.Outcome
	}{
		{name: "consumed before crash", consume: true, wantOutcome: interactions.OutcomeDeliveryUnknown},
		{name: "timed out", wantOutcome: interactions.OutcomeTimedOut},
	} {
		t.Run(test.name, func(t *testing.T) {
			al, agent, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
			defer cleanup()
			manager := newInteractionChannelManager()
			al.channelManager = manager
			tool := &approvalCountingTool{}
			agent.Tools.Register(tool)
			sessionKey := "session-approval-recovery"
			args := map[string]any{"token": "recovery-secret"}
			agent.Sessions.AddFullMessage(sessionKey, providers.Message{
				Role: "assistant", ToolCalls: []providers.ToolCall{{
					ID: "call-approval-recovery", Name: tool.Name(), Arguments: args,
					Function: &providers.FunctionCall{
						Name: tool.Name(), Arguments: `{"token":"recovery-secret"}`,
					},
				}},
			})
			argumentHash, err := interactions.HashArguments(agent.Workspace, args)
			if err != nil {
				t.Fatal(err)
			}
			expiresAt := time.Now().Add(time.Minute)
			registry := al.interactionRegistryForWorkspace(agent.Workspace)
			record, err := registry.Create(interactions.CreateRequest{
				Kind: interactions.KindApproval,
				Route: interactions.Route{
					AgentID: agent.ID, SessionKey: sessionKey, RouteSessionKey: "route-recovery",
					Channel: "telegram", ChatID: "chat-1", SenderID: "user-1",
				},
				Origin: interactions.Origin{
					TurnID: "turn-recovery", ToolCallID: "call-approval-recovery",
					ToolName: tool.Name(), ArgumentHash: argumentHash,
					ExecutionContext: &bus.InboundContext{
						Channel: "telegram", ChatID: "chat-1", SenderID: "user-1",
					},
				},
				PromptSummary:  "Run recovery action",
				ApprovalAction: "Tool: approval_counting\nAction: Run recovery action",
				ExpiresAt:      expiresAt,
			})
			if err != nil {
				t.Fatal(err)
			}
			record, _ = registry.RecordDeliveryAttempt(record.ID, record.Revision, true, "")
			record, _ = registry.MarkWaiting(record.ID, record.Revision)
			if test.consume {
				record, err = registry.ClaimAnswer(record.ID, record.Revision, interactions.Answer{
					Text: "allow_once", MessageID: "answer-recovery", ReceivedAt: time.Now().UnixMilli(),
				}, interactions.OutcomeAllowed)
				if err != nil {
					t.Fatal(err)
				}
				record, err = registry.MarkResuming(record.ID, record.Revision)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = registry.ConsumeApproval(
					record.ID, record.Revision, record.Origin.ToolCallID,
					record.Origin.ToolName, record.Origin.ArgumentHash,
				); err != nil {
					t.Fatal(err)
				}
			} else {
				claimed, claimErr := registry.ClaimOverdue(expiresAt.Add(time.Second))
				if claimErr != nil || len(claimed) != 1 {
					t.Fatalf("ClaimOverdue() = (%#v, %v)", claimed, claimErr)
				}
			}
			if recovered := al.RecoverHumanInteractions(t.Context()); recovered != 1 {
				t.Fatalf("RecoverHumanInteractions() = %d, want 1", recovered)
			}
			resolved, _ := registry.Get(record.ID)
			if resolved.Status != interactions.StatusResolved || tool.executions != 0 {
				t.Fatalf("recovered approval = %#v, executions=%d", resolved, tool.executions)
			}
			_, resultIndex := interactionToolPairIndexes(
				agent.Sessions.GetHistory(sessionKey), record.Origin.ToolCallID,
			)
			if resultIndex < 0 {
				t.Fatal("recovery did not pair the protected tool call")
			}
			result := agent.Sessions.GetHistory(sessionKey)[resultIndex]
			if !strings.Contains(result.Content, string(test.wantOutcome)) {
				t.Fatalf("recovery tool result = %q", result.Content)
			}
		})
	}
}

func TestApprovalRecoveryUsesPersistedOriginalExecutionContext(t *testing.T) {
	provider := &sequenceProvider{responses: []*providers.LLMResponse{
		{ToolCalls: []providers.ToolCall{{
			ID: "call-context", Name: "approval_context",
			Arguments: map[string]any{"target": "production"},
			Function: &providers.FunctionCall{
				Name: "approval_context", Arguments: `{"target":"production"}`,
			},
		}}},
		{Content: "context approval finished", FinishReason: "stop"},
	}}
	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()
	al.channelManager = newInteractionChannelManager()
	tool := &approvalContextTool{}
	agent.Tools.Register(tool)
	if err := al.MountHook(NamedHook("context-approval", &durableApprovalHook{
		actionSummary: "Run the context-sensitive action",
	})); err != nil {
		t.Fatal(err)
	}
	original := &bus.InboundContext{
		Channel: "telegram", Account: "bot-1", ChatID: "chat-1", ChatType: "group",
		TopicID: "topic-1", SpaceID: "space-1", SpaceType: "workspace",
		SenderID: "user-1", ActorID: "actor-1", MessageID: "origin-message",
		OriginID: "origin-1", OriginType: "forward", SourceRef: "source-1",
		ReplyToMessageID: "origin-reply", ReplyToSenderID: "reply-user",
		ReplyHandles: map[string]string{"telegram": "reply-handle"},
		Raw:          map[string]string{"thread_ts": "original-thread", "transport": "original"},
	}
	turnStatus := TurnEndStatusCompleted
	if response, err := al.runAgentLoop(t.Context(), agent, processOptions{
		TurnStatus: &turnStatus,
		Dispatch: DispatchRequest{
			RouteSessionKey: "route-context", SessionKey: "session-context",
			UserMessage: "run context action", InboundContext: original,
		},
		DefaultResponse: defaultResponse, EnableSummary: true, SendResponse: false,
	}); err != nil || response != "" || turnStatus != TurnEndStatusSuspended {
		t.Fatalf("initial approval turn = (%q, %q, %v)", response, turnStatus, err)
	}

	// Mutate every map supplied by the caller, then force a registry reload to
	// model process restart before the approval answer arrives.
	original.ReplyHandles["telegram"] = "mutated"
	original.Raw["thread_ts"] = "mutated"
	al.interactionRegistries.Delete(agent.Workspace)
	registry := al.interactionRegistryForWorkspace(agent.Workspace)
	record, ok := activeInteractionForSession(registry, "session-context")
	if !ok || record.Origin.ExecutionContext == nil {
		t.Fatalf("reloaded approval interaction = %#v", record)
	}
	record, err := registry.ClaimAnswer(record.ID, record.Revision, interactions.Answer{
		Text: "allow_once", MessageID: "answer-message", ReceivedAt: time.Now().UnixMilli(),
	}, interactions.OutcomeAllowed)
	if err != nil {
		t.Fatal(err)
	}
	answerContext := bus.InboundContext{
		Channel: "telegram", Account: "bot-1", ChatID: "chat-1", ChatType: "group",
		TopicID: "topic-1", SpaceID: "space-1", SpaceType: "workspace",
		SenderID: "user-1", MessageID: "answer-message", ReplyToMessageID: "answer-reply",
		ReplyHandles: map[string]string{"telegram": "answer-handle"},
		Raw:          map[string]string{"thread_ts": "answer-thread"},
	}
	if err = al.resumeClaimedInteraction(
		t.Context(), registry, agent.Workspace, agent, nil, answerContext, record,
	); err != nil {
		t.Fatalf("resumeClaimedInteraction() error = %v", err)
	}
	if tool.executions != 1 {
		t.Fatalf("protected tool executions = %d, want 1", tool.executions)
	}
	if tool.inbound.MessageID != "origin-message" ||
		tool.inbound.ReplyToMessageID != "origin-reply" ||
		tool.inbound.ReplyHandles["telegram"] != "reply-handle" ||
		tool.inbound.Raw["thread_ts"] != "original-thread" ||
		tool.inbound.ActorID != "actor-1" || tool.inbound.SourceRef != "source-1" {
		t.Fatalf("protected tool inbound context = %#v", tool.inbound)
	}
}

func TestExpiredAllowOnceNeverExecutesProtectedTool(t *testing.T) {
	al, agent, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
	defer cleanup()
	al.channelManager = newInteractionChannelManager()
	tool := &approvalCountingTool{}
	agent.Tools.Register(tool)
	sessionKey := "session-expired-approval"
	args := map[string]any{"target": "production"}
	agent.Sessions.AddFullMessage(sessionKey, providers.Message{
		Role: "assistant", ToolCalls: []providers.ToolCall{{
			ID: "call-expired", Name: tool.Name(), Arguments: args,
			Function: &providers.FunctionCall{
				Name: tool.Name(), Arguments: `{"target":"production"}`,
			},
		}},
	})
	argumentHash, err := interactions.HashArguments(agent.Workspace, args)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)
	registry := interactions.NewRegistryWithOptions(
		interactions.WorkspaceStorePath(agent.Workspace),
		interactions.Options{Now: func() time.Time { return now }},
	)
	al.interactionRegistries.Store(agent.Workspace, registry)
	inbound := &bus.InboundContext{
		Channel: "telegram", ChatID: "chat-1", SenderID: "user-1",
		MessageID: "origin-message",
	}
	record, err := registry.Create(interactions.CreateRequest{
		Kind: interactions.KindApproval,
		Route: interactions.Route{
			AgentID: agent.ID, SessionKey: sessionKey, RouteSessionKey: "route-expired",
			Channel: "telegram", ChatID: "chat-1", SenderID: "user-1",
		},
		Origin: interactions.Origin{
			TurnID: "turn-expired", ToolCallID: "call-expired", ToolName: tool.Name(),
			ArgumentHash: argumentHash, ExecutionContext: inbound,
		},
		PromptSummary:  "Run the protected action",
		ApprovalAction: "Tool: approval_counting\nAction: Run the protected action",
		ExpiresAt:      now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	record, _ = registry.RecordDeliveryAttempt(record.ID, record.Revision, true, "")
	record, _ = registry.MarkWaiting(record.ID, record.Revision)
	now = time.UnixMilli(record.ExpiresAt)
	record, err = registry.ClaimAnswer(record.ID, record.Revision, interactions.Answer{
		Text: "allow_once", MessageID: "late-allow", ReceivedAt: now.UnixMilli(),
	}, interactions.OutcomeAllowed)
	if err != nil {
		t.Fatal(err)
	}
	if record.Outcome != interactions.OutcomeTimedOut {
		t.Fatalf("expired approval outcome = %q, want timed_out", record.Outcome)
	}
	if err = al.resumeClaimedInteraction(
		t.Context(), registry, agent.Workspace, agent, nil, *inbound, record,
	); err != nil {
		t.Fatalf("resumeClaimedInteraction() error = %v", err)
	}
	resolved, _ := registry.Get(record.ID)
	if tool.executions != 0 || resolved.ApprovalConsumedAt != 0 ||
		resolved.Status != interactions.StatusResolved {
		t.Fatalf("expired approval = %#v, executions=%d", resolved, tool.executions)
	}
}

func TestInteractionRouteAuthorizationRequiresTrustedEnvelope(t *testing.T) {
	route := interactions.Route{
		SessionKey: "session-1", RouteSessionKey: "route-1", Channel: "telegram",
		AccountID: "primary", ChatID: "chat-1", TopicID: "topic-1", SenderID: "user-1",
	}
	target := &inboundDispatchTarget{
		SessionKey: "session-1",
		Allocation: session.Allocation{RouteScopeKey: "route-1"},
	}
	inbound := bus.InboundContext{
		Channel: "telegram", Account: "primary", ChatID: "chat-1", TopicID: "topic-1",
		SenderID: "user-1",
	}
	if !interactionRouteAuthorizes(route, target, inbound) {
		t.Fatal("matching trusted envelope was rejected")
	}
	inbound.SenderID = "user-2"
	if interactionRouteAuthorizes(route, target, inbound) {
		t.Fatal("different sender was authorized")
	}
	inbound.SenderID = "user-1"
	inbound.TopicID = "topic-2"
	if interactionRouteAuthorizes(route, target, inbound) {
		t.Fatal("different topic was authorized")
	}
}

func TestInteractionIngressOnlyClaimsAuthorizedAnswers(t *testing.T) {
	workspace := t.TempDir()
	al := &AgentLoop{cfg: config.DefaultConfig()}
	request := testToolSuspensionRequest(workspace)
	registry := al.interactionRegistryForWorkspace(workspace)
	record, err := registry.Create(interactions.CreateRequest{
		Kind: request.Prompt.Kind, Route: request.Route, Origin: request.Origin,
		Questions: request.Prompt.Questions, ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	record, _ = registry.RecordDeliveryAttempt(record.ID, record.Revision, true, "")
	_, _ = registry.MarkWaiting(record.ID, record.Revision)
	target := &inboundDispatchTarget{
		Agent: &AgentInstance{Workspace: workspace}, SessionKey: request.Route.SessionKey,
		Allocation: session.Allocation{RouteScopeKey: request.Route.RouteSessionKey},
	}
	msg := bus.InboundMessage{Content: "Canary", Context: inboundContextForInteraction(request.Route)}
	if !al.shouldHandleInteractionInbound(msg, target) {
		t.Fatal("authorized plain answer was not claimed")
	}
	msg.Content = "unrelated message"
	msg.Context.SenderID = "someone-else"
	if al.shouldHandleInteractionInbound(msg, target) {
		t.Fatal("unrelated sender message was consumed as an interaction answer")
	}
	msg.Content = "/reset"
	msg.Context.SenderID = request.Route.SenderID
	if al.shouldHandleInteractionInbound(msg, target) {
		t.Fatal("control command was consumed as an interaction answer")
	}
	if err := al.cancelInteractionForControlMessage(t.Context(), msg, target); err != nil {
		t.Fatal(err)
	}
	record, _ = registry.Get(record.ID)
	if record.Status != interactions.StatusCancelled {
		t.Fatalf("reset did not cancel pending interaction: %#v", record)
	}
}

func TestInteractionIngressRetainsClaimedAnswerReplayOwnership(t *testing.T) {
	workspace := t.TempDir()
	al := &AgentLoop{cfg: config.DefaultConfig()}
	request := testToolSuspensionRequest(workspace)
	registry := al.interactionRegistryForWorkspace(workspace)
	record, err := registry.Create(interactions.CreateRequest{
		Kind: request.Prompt.Kind, Route: request.Route, Origin: request.Origin,
		Questions: request.Prompt.Questions, ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	record, _ = registry.RecordDeliveryAttempt(record.ID, record.Revision, true, "")
	record, _ = registry.MarkWaiting(record.ID, record.Revision)
	record, err = registry.ClaimAnswer(record.ID, record.Revision, interactions.Answer{
		Text: "Canary", Values: map[string]string{"deploy_mode": "Canary"}, MessageID: "answer-1",
	}, interactions.OutcomeAnswered)
	if err != nil {
		t.Fatal(err)
	}
	target := &inboundDispatchTarget{
		Agent: &AgentInstance{Workspace: workspace}, SessionKey: request.Route.SessionKey,
		Allocation: session.Allocation{RouteScopeKey: request.Route.RouteSessionKey},
	}
	msg := bus.InboundMessage{Content: "Canary", Context: inboundContextForInteraction(request.Route)}
	msg.Context.MessageID = "answer-1"
	if !al.shouldHandleInteractionInbound(msg, target) {
		t.Fatal("claimed answer replay escaped interaction dispatch")
	}
	if !interactionInboundReplaysAnswer(record, msg.Context) {
		t.Fatal("persisted answer replay was not recognized")
	}
	msg.Context.MessageID = "answer-2"
	if !al.shouldHandleInteractionInbound(msg, target) {
		t.Fatal("new authorized message escaped the owned interaction session")
	}
	if interactionInboundReplaysAnswer(record, msg.Context) {
		t.Fatal("different message was recognized as the persisted answer")
	}
	msg.Context.SenderID = "user-2"
	if al.shouldHandleInteractionInbound(msg, target) {
		t.Fatal("unrelated sender was consumed by the claimed interaction")
	}
}

func TestClaimedAnswerIsNotReleasedAfterResumeFailure(t *testing.T) {
	al, agent, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
	defer cleanup()
	tracker := &interactionOwnershipBus{MessageBus: al.bus.(*bus.MessageBus)}
	al.bus = tracker
	sessionKey := "session-claimed-spool-ownership"
	request := testToolSuspensionRequest(agent.Workspace)
	request.Route.SessionKey = sessionKey
	registry := al.interactionRegistryForWorkspace(agent.Workspace)
	record, err := registry.Create(interactions.CreateRequest{
		Kind: request.Prompt.Kind, Route: request.Route, Origin: request.Origin,
		Questions: request.Prompt.Questions, ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	record, _ = registry.RecordDeliveryAttempt(record.ID, record.Revision, true, "")
	if _, err = registry.MarkWaiting(record.ID, record.Revision); err != nil {
		t.Fatal(err)
	}
	target := &inboundDispatchTarget{
		Agent: agent, SessionKey: sessionKey,
		Allocation: session.Allocation{RouteScopeKey: request.Route.RouteSessionKey},
	}
	msg := bus.InboundMessage{
		Content: "Canary", SpoolID: "spool-claimed-answer",
		Context: inboundContextForInteraction(request.Route),
	}
	msg.Context.MessageID = "answer-claimed"
	claim, claimed := al.claimRuntimeSession(sessionKey, "test-claimed-spool")
	if !claimed {
		t.Fatal("failed to claim test session")
	}

	// No originating tool call exists, so continuation fails after ClaimAnswer.
	newInboundTurnCoordinator(al).runInteractionWorker(t.Context(), msg, target, claim)
	acked, released := tracker.counts()
	if acked != 1 || released != 0 {
		t.Fatalf("spool ownership = acked:%d released:%d, want 1/0", acked, released)
	}
	record, _ = registry.Get(record.ID)
	if record.Status != interactions.StatusClaimed {
		t.Fatalf("record status = %q, want claimed recovery ownership", record.Status)
	}
}

func TestAdditionalMessageDuringResumeIsDeferred(t *testing.T) {
	al, agent, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
	defer cleanup()
	tracker := &interactionOwnershipBus{MessageBus: al.bus.(*bus.MessageBus)}
	al.bus = tracker
	sessionKey := "session-resume-additional-input"
	request := testToolSuspensionRequest(agent.Workspace)
	request.Route.SessionKey = sessionKey
	registry := al.interactionRegistryForWorkspace(agent.Workspace)
	record, err := registry.Create(interactions.CreateRequest{
		Kind: request.Prompt.Kind, Route: request.Route, Origin: request.Origin,
		Questions: request.Prompt.Questions, ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	record, _ = registry.RecordDeliveryAttempt(record.ID, record.Revision, true, "")
	record, _ = registry.MarkWaiting(record.ID, record.Revision)
	if _, err = registry.ClaimAnswer(record.ID, record.Revision, interactions.Answer{
		Text: "Canary", Values: map[string]string{"deploy_mode": "Canary"}, MessageID: "answer-1",
	}, interactions.OutcomeAnswered); err != nil {
		t.Fatal(err)
	}
	target := &inboundDispatchTarget{
		Agent: agent, SessionKey: sessionKey,
		Allocation: session.Allocation{RouteScopeKey: request.Route.RouteSessionKey},
	}
	msg := bus.InboundMessage{
		Content: "Use staging instead", SpoolID: "spool-correction",
		Context: inboundContextForInteraction(request.Route),
	}
	msg.Context.MessageID = "answer-2"
	claim, claimed := al.claimRuntimeSession(sessionKey, "test-active-resume")
	if !claimed {
		t.Fatal("failed to claim test session")
	}
	defer claim.releaseIfOwned()

	newInboundTurnCoordinator(al).handleInteractionInbound(t.Context(), msg, target)
	acked, released := tracker.counts()
	if acked != 0 || released != 0 {
		t.Fatalf("deferred spool ownership = acked:%d released:%d, want 0/0", acked, released)
	}
	if got := al.pendingSteeringCountForScope(sessionKey); got != 1 {
		t.Fatalf("deferred queue depth = %d, want 1", got)
	}
	queued := al.dequeueSteeringMessagesForTurn(sessionKey, request.Route.SenderID)
	if len(queued) != 1 || queued[0].InboundSpoolID != "spool-correction" {
		t.Fatalf("deferred message = %#v", queued)
	}
}

func TestReloadWhileWaitingResumesAgainstPersistedSession(t *testing.T) {
	al, agent, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
	defer cleanup()
	manager := newInteractionChannelManager()
	al.channelManager = manager
	sessionKey := "session-reload-waiting"
	agent.Sessions.AddFullMessage(sessionKey, providers.Message{Role: "user", Content: "Deploy this"})
	agent.Sessions.AddFullMessage(sessionKey, providers.Message{
		Role: "assistant",
		ToolCalls: []providers.ToolCall{{
			ID: "call-reload-question", Name: "request_user_input",
			Function: &providers.FunctionCall{Name: "request_user_input", Arguments: `{}`},
		}},
	})
	request := testToolSuspensionRequest(agent.Workspace)
	request.Route.SessionKey = sessionKey
	request.Origin.ToolCallID = "call-reload-question"
	registry := al.interactionRegistryForWorkspace(agent.Workspace)
	record, err := registry.Create(interactions.CreateRequest{
		Kind: request.Prompt.Kind, Route: request.Route, Origin: request.Origin,
		Questions: request.Prompt.Questions, ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	record, _ = registry.RecordDeliveryAttempt(record.ID, record.Revision, true, "")
	if _, err = registry.MarkWaiting(record.ID, record.Revision); err != nil {
		t.Fatal(err)
	}

	reloaded := *al.GetConfig()
	if err = al.ReloadProviderAndConfig(t.Context(), &simpleConvProvider{}, &reloaded); err != nil {
		t.Fatal(err)
	}
	reloadedAgent, ok := al.GetRegistry().GetAgent(agent.ID)
	if !ok || reloadedAgent == nil {
		t.Fatal("reloaded agent is unavailable")
	}
	target := &inboundDispatchTarget{
		Agent: reloadedAgent, SessionKey: sessionKey,
		Allocation: session.Allocation{RouteScopeKey: request.Route.RouteSessionKey},
	}
	msg := bus.InboundMessage{
		Content: "Canary", SpoolID: "spool-reload-answer",
		Context: inboundContextForInteraction(request.Route),
	}
	msg.Context.MessageID = "answer-after-reload"
	ownership, err := al.processInteractionInbound(t.Context(), msg, target)
	if err != nil || ownership != interactionInboundClaimed {
		t.Fatalf("processInteractionInbound() = (%v, %v)", ownership, err)
	}
	record, _ = registry.Get(record.ID)
	if record.Status != interactions.StatusResolved || !record.FinalDelivered {
		t.Fatalf("record after reload answer = %#v", record)
	}
	select {
	case outbound := <-manager.sent:
		if strings.TrimSpace(outbound.Content) == "" {
			t.Fatalf("reload continuation outbound = %#v", outbound)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reload continuation")
	}
}

func TestStopCancellationPairsSuspendedToolCall(t *testing.T) {
	al, agent, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
	defer cleanup()
	sessionKey := "session-stop-interaction"
	agent.Sessions.AddFullMessage(sessionKey, providers.Message{Role: "user", Content: "Deploy this"})
	agent.Sessions.AddFullMessage(sessionKey, providers.Message{
		Role: "assistant",
		ToolCalls: []providers.ToolCall{{
			ID: "call-question", Name: "request_user_input",
			Function: &providers.FunctionCall{Name: "request_user_input", Arguments: `{}`},
		}},
	})
	request := testToolSuspensionRequest(agent.Workspace)
	request.Route.SessionKey = sessionKey
	registry := al.interactionRegistryForWorkspace(agent.Workspace)
	record, err := registry.Create(interactions.CreateRequest{
		Kind: request.Prompt.Kind, Route: request.Route, Origin: request.Origin,
		Questions: request.Prompt.Questions, ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	record, _ = registry.RecordDeliveryAttempt(record.ID, record.Revision, true, "")
	record, _ = registry.MarkWaiting(record.ID, record.Revision)
	target := &inboundDispatchTarget{
		Agent: agent, SessionKey: sessionKey,
		Allocation: session.Allocation{RouteScopeKey: request.Route.RouteSessionKey},
	}
	msg := bus.InboundMessage{Content: "/stop", Context: inboundContextForInteraction(request.Route)}
	if err := al.cancelInteractionForControlMessage(t.Context(), msg, target); err != nil {
		t.Fatal(err)
	}
	record, _ = registry.Get(record.ID)
	if record.Status != interactions.StatusCancelled {
		t.Fatalf("record status = %q, want canceled", record.Status)
	}
	_, resultIndex := interactionToolPairIndexes(agent.Sessions.GetHistory(sessionKey), "call-question")
	if resultIndex < 0 {
		t.Fatal("stop left the suspended tool call unpaired")
	}
	result := agent.Sessions.GetHistory(sessionKey)[resultIndex]
	if !strings.Contains(result.Content, `"outcome":"canceled"`) {
		t.Fatalf("cancellation tool result = %q", result.Content)
	}
}

func TestRecoveryCompletesDurableStopCancellation(t *testing.T) {
	al, agent, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
	defer cleanup()
	sessionKey := "session-stop-cancel-recovery"
	agent.Sessions.AddFullMessage(sessionKey, providers.Message{
		Role: "assistant",
		ToolCalls: []providers.ToolCall{{
			ID: "call-question", Name: "request_user_input",
			Function: &providers.FunctionCall{Name: "request_user_input", Arguments: `{}`},
		}},
	})
	request := testToolSuspensionRequest(agent.Workspace)
	request.Route.SessionKey = sessionKey
	registry := al.interactionRegistryForWorkspace(agent.Workspace)
	record, err := registry.Create(interactions.CreateRequest{
		Kind: request.Prompt.Kind, Route: request.Route, Origin: request.Origin,
		Questions: request.Prompt.Questions, ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	record, _ = registry.RecordDeliveryAttempt(record.ID, record.Revision, true, "")
	record, _ = registry.MarkWaiting(record.ID, record.Revision)
	record, err = registry.BeginCancellation(record.ID, record.Revision, "session_control_stop")
	if err != nil || record.Status != interactions.StatusCanceling {
		t.Fatalf("begin cancellation = (%#v, %v)", record, err)
	}

	if recovered := al.RecoverHumanInteractions(t.Context()); recovered != 1 {
		t.Fatalf("RecoverHumanInteractions() = %d, want 1", recovered)
	}
	record, _ = registry.Get(record.ID)
	if record.Status != interactions.StatusCancelled {
		t.Fatalf("record after cancellation recovery = %#v", record)
	}
	_, resultIndex := interactionToolPairIndexes(agent.Sessions.GetHistory(sessionKey), "call-question")
	if resultIndex < 0 {
		t.Fatal("cancellation recovery left the tool call unpaired")
	}
	result := agent.Sessions.GetHistory(sessionKey)[resultIndex]
	if !strings.Contains(result.Content, `"outcome":"canceled"`) {
		t.Fatalf("recovered cancellation result = %q", result.Content)
	}
}

func TestDeferredInteractionIngressQueuesWithoutChangingHistory(t *testing.T) {
	al, agent, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
	defer cleanup()
	sessionKey := "session-deferred-interaction"
	agent.Sessions.AddFullMessage(sessionKey, providers.Message{Role: "assistant", Content: "existing"})
	target := &inboundDispatchTarget{Agent: agent, SessionKey: sessionKey}
	msg := bus.InboundMessage{
		Content: "unrelated turn", SenderID: "user-2", SpoolID: "spool-2",
		Context: bus.InboundContext{
			Channel: "telegram", ChatID: "chat-1", SenderID: "user-2", MessageID: "message-2",
		},
	}
	newInboundTurnCoordinator(al).deferInteractionInbound(t.Context(), msg, target)
	if got := al.pendingSteeringCountForScope(sessionKey); got != 1 {
		t.Fatalf("deferred queue depth = %d, want 1", got)
	}
	history := agent.Sessions.GetHistory(sessionKey)
	if len(history) != 1 || history[0].Content != "existing" {
		t.Fatalf("deferred ingress changed history: %#v", history)
	}
	queued := al.dequeueSteeringMessagesForTurn(sessionKey, "user-2")
	if len(queued) != 1 || queued[0].InboundSpoolID != "spool-2" ||
		!strings.Contains(queued[0].Content, "unrelated turn") {
		t.Fatalf("deferred message = %#v", queued)
	}
}

func TestResumeClaimedInteractionAppendsOneToolResultAndResolves(t *testing.T) {
	provider := &simpleConvProvider{}
	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()
	manager := newInteractionChannelManager()
	al.channelManager = manager
	workspace := agent.Workspace
	sessionKey := "session-resume"
	agent.Sessions.AddFullMessage(sessionKey, providers.Message{Role: "user", Content: "Deploy this"})
	agent.Sessions.AddFullMessage(sessionKey, providers.Message{
		Role: "assistant",
		ToolCalls: []providers.ToolCall{{
			ID: "call-question", Name: "request_user_input",
			Function: &providers.FunctionCall{Name: "request_user_input", Arguments: `{}`},
		}},
	})
	registry := al.interactionRegistryForWorkspace(workspace)
	request := testToolSuspensionRequest(workspace)
	request.Route.SessionKey = sessionKey
	record, err := registry.Create(interactions.CreateRequest{
		Kind: request.Prompt.Kind, Route: request.Route, Origin: request.Origin,
		Questions: request.Prompt.Questions, ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	record, _ = registry.RecordDeliveryAttempt(record.ID, record.Revision, true, "")
	record, _ = registry.MarkWaiting(record.ID, record.Revision)
	record, err = registry.ClaimAnswer(record.ID, record.Revision, interactions.Answer{
		Text: "Canary", Values: map[string]string{"deploy_mode": "Canary"}, MessageID: "answer-1",
	}, interactions.OutcomeAnswered)
	if err != nil {
		t.Fatal(err)
	}
	inbound := inboundContextForInteraction(record.Route)
	scope := &session.SessionScope{
		Version: 1, AgentID: agent.ID, Channel: record.Route.Channel,
		RouteScopeKey: record.Route.RouteSessionKey,
	}
	if err := al.resumeClaimedInteraction(
		t.Context(), registry, agent.Workspace, agent, scope, inbound, record,
	); err != nil {
		t.Fatalf("resumeClaimedInteraction() error = %v", err)
	}
	resolved, _ := registry.Get(record.ID)
	if resolved.Status != interactions.StatusResolved || !resolved.FinalDelivered {
		t.Fatalf("record status = %q, want resolved", resolved.Status)
	}
	history := agent.Sessions.GetHistory(sessionKey)
	toolResults := 0
	for _, message := range history {
		if message.Role == "tool" && message.ToolCallID == "call-question" {
			toolResults++
			if !strings.Contains(message.Content, `"deploy_mode":"Canary"`) {
				t.Fatalf("tool result = %q", message.Content)
			}
		}
	}
	if toolResults != 1 {
		t.Fatalf("matching tool results = %d, want 1", toolResults)
	}
	select {
	case outbound := <-manager.sent:
		if strings.TrimSpace(outbound.Content) == "" {
			t.Fatalf("final outbound = %#v", outbound)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for resumed final response")
	}
}

func TestRecoverHumanInteractionsResumesDurableClaimAfterRestartWindow(t *testing.T) {
	provider := &simpleConvProvider{}
	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()
	manager := newInteractionChannelManager()
	al.channelManager = manager
	sessionKey := "session-recover-interaction"
	agent.Sessions.AddFullMessage(sessionKey, providers.Message{Role: "user", Content: "Deploy this"})
	agent.Sessions.AddFullMessage(sessionKey, providers.Message{
		Role: "assistant",
		ToolCalls: []providers.ToolCall{{
			ID: "call-question", Name: "request_user_input",
			Function: &providers.FunctionCall{Name: "request_user_input", Arguments: `{}`},
		}},
	})
	request := testToolSuspensionRequest(agent.Workspace)
	request.Route.SessionKey = sessionKey
	registry := al.interactionRegistryForWorkspace(agent.Workspace)
	record, err := registry.Create(interactions.CreateRequest{
		Kind: request.Prompt.Kind, Route: request.Route, Origin: request.Origin,
		Questions: request.Prompt.Questions, ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	record, _ = registry.RecordDeliveryAttempt(record.ID, record.Revision, true, "")
	record, _ = registry.MarkWaiting(record.ID, record.Revision)
	record, err = registry.ClaimAnswer(record.ID, record.Revision, interactions.Answer{
		Text: "Canary", Values: map[string]string{"deploy_mode": "Canary"}, MessageID: "answer-recover",
	}, interactions.OutcomeAnswered)
	if err != nil {
		t.Fatal(err)
	}
	if record.Status != interactions.StatusClaimed {
		t.Fatalf("status before recovery = %q", record.Status)
	}
	if err := al.enqueueSteeringMessageWithSender(sessionKey, agent.ID, "user-2", providers.Message{
		Role: "user", Content: "Check the deployment after recovery.",
	}); err != nil {
		t.Fatal(err)
	}

	if recovered := al.RecoverHumanInteractions(t.Context()); recovered != 1 {
		t.Fatalf("RecoverHumanInteractions() = %d, want 1", recovered)
	}
	record, _ = registry.Get(record.ID)
	if record.Status != interactions.StatusResolved || !record.FinalDelivered {
		t.Fatalf("status after recovery = %q", record.Status)
	}
	if got := al.pendingSteeringCountForScope(sessionKey); got != 0 {
		t.Fatalf("deferred queue depth after recovery = %d, want 0", got)
	}
	foundDeferred := false
	for _, message := range agent.Sessions.GetHistory(sessionKey) {
		if message.Role == "user" && strings.Contains(message.Content, "Check the deployment") {
			foundDeferred = true
			break
		}
	}
	if !foundDeferred {
		t.Fatal("recovery did not continue the deferred inbound message")
	}
	select {
	case outbound := <-manager.sent:
		if strings.TrimSpace(outbound.Content) == "" {
			t.Fatalf("recovery outbound = %#v", outbound)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for recovered continuation")
	}
}

func TestRecoverResumingInteractionReplaysPersistedFinalWithoutModelCall(t *testing.T) {
	provider := &toolCallRespProvider{toolName: "must_not_run", response: "must not run"}
	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()
	manager := newInteractionChannelManager()
	al.channelManager = manager
	sessionKey := "session-recover-final"
	agent.Sessions.AddFullMessage(sessionKey, providers.Message{
		Role: "assistant", ToolCalls: []providers.ToolCall{{ID: "call-question"}},
	})
	request := testToolSuspensionRequest(agent.Workspace)
	request.Route.SessionKey = sessionKey
	registry := al.interactionRegistryForWorkspace(agent.Workspace)
	record, err := registry.Create(interactions.CreateRequest{
		Kind: request.Prompt.Kind, Route: request.Route, Origin: request.Origin,
		Questions: request.Prompt.Questions, ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	record, _ = registry.RecordDeliveryAttempt(record.ID, record.Revision, true, "")
	record, _ = registry.MarkWaiting(record.ID, record.Revision)
	record, _ = registry.ClaimAnswer(record.ID, record.Revision, interactions.Answer{
		Text: "Canary", Values: map[string]string{"deploy_mode": "Canary"},
	}, interactions.OutcomeAnswered)
	if ensureErr := al.ensureInteractionToolResult(t.Context(), agent, record); ensureErr != nil {
		t.Fatal(ensureErr)
	}
	record, err = registry.MarkResuming(record.ID, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	agent.Sessions.AddFullMessage(sessionKey, providers.Message{Role: "assistant", Content: "Recovered final"})

	if recovered := al.RecoverHumanInteractions(t.Context()); recovered != 1 {
		t.Fatalf("RecoverHumanInteractions() = %d, want 1", recovered)
	}
	if provider.callCount != 0 {
		t.Fatalf("provider calls = %d, want 0", provider.callCount)
	}
	record, _ = registry.Get(record.ID)
	if record.Status != interactions.StatusResolved || !record.FinalDelivered {
		t.Fatalf("status = %q, want resolved", record.Status)
	}
	select {
	case outbound := <-manager.sent:
		if outbound.Content != "Recovered final" ||
			outbound.Context.Raw["delivery_key"] != interactionDeliveryKey(record.ID, "final") {
			t.Fatalf("outbound = %#v", outbound)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for replayed final")
	}
}

func TestInteractionFinalAfterToolResultRequiresMatchingOrder(t *testing.T) {
	history := []providers.Message{
		{Role: "assistant", Content: "old"},
		{Role: "assistant", ToolCalls: []providers.ToolCall{{ID: "call-1"}}},
		{Role: "tool", ToolCallID: "call-1", Content: "answer"},
		{Role: "assistant", Content: "continued"},
	}
	if content, ok := interactionFinalAfterToolResult(history, "call-1"); !ok || content != "continued" {
		t.Fatalf("interactionFinalAfterToolResult() = (%q, %v)", content, ok)
	}
	if _, ok := interactionFinalAfterToolResult(history, "other"); ok {
		t.Fatal("unmatched tool result produced a final response")
	}
}

func TestInteractionPairingIgnoresReusedToolCallIDFromOlderRound(t *testing.T) {
	history := []providers.Message{
		{Role: "assistant", ToolCalls: []providers.ToolCall{{ID: "call-reused"}}},
		{Role: "tool", ToolCallID: "call-reused", Content: "old result"},
		{Role: "assistant", Content: "old final"},
		{Role: "user", Content: "new request"},
		{Role: "assistant", ToolCalls: []providers.ToolCall{{ID: "call-reused"}}},
	}
	origin, result := interactionToolPairIndexes(history, "call-reused")
	if origin != 4 || result != -1 {
		t.Fatalf("interactionToolPairIndexes() = (%d, %d), want (4, -1)", origin, result)
	}
	if _, ok := interactionFinalAfterToolResult(history, "call-reused"); ok {
		t.Fatal("older reused result was treated as current continuation")
	}
}

func TestRecoverHumanInteractionsTerminalizesCatalogedOrphanWorkspace(t *testing.T) {
	provider := &simpleConvProvider{}
	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()

	catalogRoot := t.TempDir()
	orphanWorkspace := filepath.Join(catalogRoot, "removed-agent-workspace")
	catalog := interactions.NewWorkspaceCatalog(catalogRoot)
	if err := catalog.Register(orphanWorkspace); err != nil {
		t.Fatal(err)
	}
	al.interactionCatalog = catalog

	registry := interactions.NewRegistry(interactions.WorkspaceStorePath(orphanWorkspace))
	request := testToolSuspensionRequest(orphanWorkspace)
	request.Route.AgentID = "removed-agent"
	record, err := registry.Create(interactions.CreateRequest{
		Kind: request.Prompt.Kind, Route: request.Route, Origin: request.Origin,
		Questions: request.Prompt.Questions, ExpiresAt: time.Now().Add(time.Hour),
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

	if recovered := al.RecoverHumanInteractions(t.Context()); recovered != 1 {
		t.Fatalf("RecoverHumanInteractions() = %d, want 1", recovered)
	}
	loaded := al.interactionRegistryForWorkspace(orphanWorkspace)
	terminal, ok := loaded.Get(record.ID)
	if !ok || terminal.Status != interactions.StatusFailed ||
		terminal.FailureCode != "agent_unavailable" {
		t.Fatalf("orphan record = %#v", terminal)
	}
	if current, ok := al.GetRegistry().GetAgent(agent.ID); !ok || current == nil {
		t.Fatal("active agent was disturbed while recovering orphan workspace")
	}
}

func TestDrainQueuedSteeringStopsWhileInteractionIsNonterminal(t *testing.T) {
	provider := &simpleConvProvider{}
	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()
	sessionKey := "session-suspended-steering"
	request := testToolSuspensionRequest(agent.Workspace)
	request.Route.SessionKey = sessionKey
	registry := al.interactionRegistryForWorkspace(agent.Workspace)
	record, err := registry.Create(interactions.CreateRequest{
		Kind: request.Prompt.Kind, Route: request.Route, Origin: request.Origin,
		Questions: request.Prompt.Questions, ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	record, err = registry.RecordDeliveryAttempt(record.ID, record.Revision, true, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = registry.MarkWaiting(record.ID, record.Revision); err != nil {
		t.Fatal(err)
	}
	if err = al.enqueueSteeringMessageWithSender(sessionKey, agent.ID, "user-2", providers.Message{
		Role: "user", Content: "message that arrived during suspension",
	}); err != nil {
		t.Fatal(err)
	}

	continued, err := al.drainQueuedSteeringContinuations(t.Context(), &continuationTarget{
		SessionKey: sessionKey,
		Channel:    "telegram",
		ChatID:     "chat-1",
		Workspace:  agent.Workspace,
	})
	if err != nil || continued != "" {
		t.Fatalf("drain = (%q, %v), want empty success", continued, err)
	}
	if got := al.pendingSteeringCountForScope(sessionKey); got != 1 {
		t.Fatalf("deferred queue depth = %d, want 1", got)
	}
}

func TestRecoveryKeepsCatalogEntryWhenRegistryLoadFails(t *testing.T) {
	provider := &simpleConvProvider{}
	al, _, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()
	catalogRoot := t.TempDir()
	workspace := filepath.Join(catalogRoot, "corrupt-workspace")
	storePath := interactions.WorkspaceStorePath(workspace)
	if err := os.MkdirAll(filepath.Dir(storePath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(storePath, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	catalog := interactions.NewWorkspaceCatalog(catalogRoot)
	if err := catalog.Register(workspace); err != nil {
		t.Fatal(err)
	}
	al.interactionCatalog = catalog

	al.RecoverHumanInteractions(t.Context())
	workspaces, err := catalog.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(workspaces) != 1 || workspaces[0] != workspace {
		t.Fatalf("catalog workspaces = %#v, want corrupt store retained", workspaces)
	}
}
