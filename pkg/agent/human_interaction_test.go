package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/channels"
	"github.com/bogdanovich/mintclaw/pkg/config"
	runtimeevents "github.com/bogdanovich/mintclaw/pkg/events"
	"github.com/bogdanovich/mintclaw/pkg/interactions"
	"github.com/bogdanovich/mintclaw/pkg/providers"
	"github.com/bogdanovich/mintclaw/pkg/session"
	taskregistry "github.com/bogdanovich/mintclaw/pkg/tasks"
	"github.com/bogdanovich/mintclaw/pkg/tools"
)

type interactionChannelManager struct {
	*recordingChannelManager
	sent    chan bus.OutboundMessage
	sendErr error
}

type blockingInteractionProvider struct {
	started chan struct{}
	release chan struct{}

	mu       sync.Mutex
	calls    int
	messages [][]providers.Message
}

func newBlockingInteractionProvider() *blockingInteractionProvider {
	return &blockingInteractionProvider{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

func (p *blockingInteractionProvider) Chat(
	ctx context.Context,
	messages []providers.Message,
	_ []providers.ToolDefinition,
	_ string,
	_ map[string]any,
) (*providers.LLMResponse, error) {
	p.mu.Lock()
	p.calls++
	p.messages = append(p.messages, append([]providers.Message(nil), messages...))
	p.mu.Unlock()
	select {
	case p.started <- struct{}{}:
	default:
	}
	select {
	case <-p.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &providers.LLMResponse{
		Content: "DUPLICATE_INTERACTION_OK: первый", FinishReason: "stop",
	}, nil
}

func (p *blockingInteractionProvider) GetDefaultModel() string {
	return "blocking-interaction-model"
}

func (p *blockingInteractionProvider) snapshot() (int, [][]providers.Message) {
	p.mu.Lock()
	defer p.mu.Unlock()
	messages := make([][]providers.Message, len(p.messages))
	for i := range p.messages {
		messages[i] = append([]providers.Message(nil), p.messages[i]...)
	}
	return p.calls, messages
}

type durableApprovalHook struct {
	actionSummary string
	revoked       bool
	calls         int
}

func (h *durableApprovalHook) ApproveTool(
	context.Context,
	*ToolApprovalRequest,
) (ApprovalDecision, error) {
	h.calls++
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

type approvalBindingTool struct {
	executions           int
	bindingCalls         []string
	bindingContinuations []bool
	executionIDs         []string
	workspaces           []string
}

func (*approvalBindingTool) Name() string { return "approval_binding" }

func (*approvalBindingTool) Description() string { return "Run a prepared protected action" }

func (*approvalBindingTool) Parameters() map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": map[string]any{"mutable": map[string]any{"type": "string"}},
		"required":   []any{"mutable"},
	}
}

func (t *approvalBindingTool) ApprovalArguments(
	ctx context.Context,
	_ map[string]any,
) (map[string]any, error) {
	t.bindingCalls = append(t.bindingCalls, tools.ToolCallID(ctx))
	t.bindingContinuations = append(
		t.bindingContinuations,
		tools.ToolApprovalContinuation(ctx),
	)
	t.executionIDs = append(t.executionIDs, tools.ToolExecutionID(ctx))
	t.workspaces = append(t.workspaces, tools.ToolWorkspace(ctx))
	return map[string]any{"plan_hash": "prepared-plan-hash"}, nil
}

func (t *approvalBindingTool) Execute(context.Context, map[string]any) *tools.ToolResult {
	t.executions++
	return tools.NewToolResult("prepared action completed")
}

func TestToolExecutionIdentityDoesNotRepeatAcrossAgentLoops(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = t.TempDir()
	cfg.Agents.Defaults.ModelName = "test-model"
	executionIDs := make([]string, 0, 2)
	turnIDs := make([]string, 0, 2)
	for range 2 {
		loop := NewAgentLoop(
			cfg,
			bus.NewMessageBus(),
			&sequenceProvider{},
			WithIsolatedToolBootstrap(),
		)
		agent := loop.registry.GetDefaultAgent()
		scope := loop.newTurnEventScope(agent.ID, agent.Workspace, "session-1", nil)
		state := newTurnState(
			agent,
			processOptions{Dispatch: DispatchRequest{SessionKey: "session-1"}},
			scope,
		)
		executionIDs = append(executionIDs, state.executionID)
		turnIDs = append(turnIDs, state.turnID)
		loop.Close()
	}
	if turnIDs[0] != turnIDs[1] {
		t.Fatalf("test setup did not reproduce turn counter reset: %#v", turnIDs)
	}
	if executionIDs[0] == "" || executionIDs[1] == "" ||
		executionIDs[0] == executionIDs[1] {
		t.Fatalf("execution identities repeated across loops: %#v", executionIDs)
	}
}

type approvalContextTool struct {
	executions int
	inbound    bus.InboundContext
	bypass     bool
	continued  bool
}

func (*approvalContextTool) Name() string { return "approval_context" }

func (*approvalContextTool) Description() string { return "Capture protected inbound context" }

func (*approvalContextTool) Parameters() map[string]any {
	return map[string]any{"type": "object"}
}

func (t *approvalContextTool) Execute(ctx context.Context, _ map[string]any) *tools.ToolResult {
	t.executions++
	t.inbound = tools.ToolInboundContext(ctx)
	t.bypass = tools.ToolApprovalBypass(ctx)
	t.continued = tools.ToolApprovalContinuation(ctx)
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

func prepareWaitingControlInteraction(
	t *testing.T,
	al *AgentLoop,
	agent *AgentInstance,
	msg bus.InboundMessage,
	taskID string,
) (interactions.Record, *inboundDispatchTarget) {
	t.Helper()
	target, ok := al.resolveSteeringTarget(msg)
	if !ok {
		t.Fatal("failed to resolve interaction control target")
	}
	route := interactions.Route{
		AgentID:         agent.ID,
		SessionKey:      target.SessionKey,
		RouteSessionKey: target.Allocation.RouteScopeKey,
		Channel:         msg.Context.Channel,
		AccountID:       msg.Context.Account,
		ChatID:          msg.Context.ChatID,
		ChatType:        msg.Context.ChatType,
		TopicID:         msg.Context.TopicID,
		SenderID:        msg.Context.SenderID,
	}
	origin := interactions.Origin{
		TurnID:     "turn-control",
		ToolCallID: "call-control-question",
		ToolName:   "request_user_input",
		TaskID:     taskID,
	}
	agent.Sessions.AddFullMessage(target.SessionKey, providers.Message{
		Role: "assistant",
		ToolCalls: []providers.ToolCall{{
			ID: origin.ToolCallID, Name: origin.ToolName,
			Function: &providers.FunctionCall{Name: origin.ToolName, Arguments: `{}`},
		}},
	})
	registry := al.interactionRegistryForWorkspace(agent.Workspace)
	record, err := registry.Create(interactions.CreateRequest{
		Kind: interactions.KindQuestion, Route: route, Origin: origin,
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
	return record, target
}

func countInteractionToolResults(history []providers.Message, toolCallID string) int {
	count := 0
	for _, message := range history {
		if message.Role == "tool" && message.ToolCallID == toolCallID {
			count++
		}
	}
	return count
}

func TestHumanInteractionRuntimePersistsAndQueuesPromptBeforeWaiting(t *testing.T) {
	messageBus := bus.NewMessageBus()
	manager := newInteractionChannelManager()
	al := &AgentLoop{cfg: config.DefaultConfig(), bus: messageBus, channelManager: manager}
	workspace := t.TempDir()

	request := testToolSuspensionRequest(workspace)
	request.ExecutionContext = &bus.InboundContext{MessageID: "question-origin"}
	disposition, err := al.humanInteractionRuntime().SuspendToolCall(t.Context(), request)
	if err != nil || !disposition.Durable || disposition.InteractionID == "" {
		t.Fatalf("SuspendToolCall() = (%#v, %v)", disposition, err)
	}
	record, ok := al.interactionRegistryForWorkspace(workspace).Get(disposition.InteractionID)
	if !ok || record.Status != interactions.StatusWaiting || record.DeliveryTries != 1 {
		t.Fatalf("record = %#v", record)
	}
	select {
	case outbound := <-manager.sent:
		if !strings.Contains(outbound.Content, "Which mode should be used?") ||
			!strings.Contains(outbound.Content, "Canary") ||
			!strings.Contains(outbound.Content, "`/answer "+record.ShortID+" …`") ||
			strings.Contains(outbound.Content, "Input needed") ||
			strings.Contains(outbound.Content, "Reply with your answer") ||
			outbound.Context.Raw[interactionIDMetadata] != record.ID ||
			outbound.Context.Raw["delivery_key"] != interactionDeliveryKey(record.ID, "prompt") ||
			outbound.Context.Account != "primary" || outbound.ReplyToMessageID != "" ||
			bus.OutboundMetadataFromMessage(outbound).IsApprovalPrompt() {
			t.Fatalf("outbound prompt = %#v", outbound)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for interaction prompt")
	}
}

func TestNonTelegramApprovalPromptDoesNotProjectTelegramControls(t *testing.T) {
	manager := newInteractionChannelManager()
	al := &AgentLoop{cfg: config.DefaultConfig(), channelManager: manager}
	record := interactions.Record{
		ID: "approval-slack", ShortID: "apr123", Kind: interactions.KindApproval,
		Route: interactions.Route{
			AgentID: "main", SessionKey: "session-1", Channel: "slack", ChatID: "chat-1",
		},
		Origin: interactions.Origin{
			ToolName: "protected", ExecutionContext: &bus.InboundContext{MessageID: "origin-message"},
		},
		ApprovalAction: "Run protected action",
	}

	if err := al.humanInteractionRuntime().publishPrompt(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	prompt := <-manager.sent
	if prompt.ReplyToMessageID != "" || bus.OutboundMetadataFromMessage(prompt).IsApprovalPrompt() {
		t.Fatalf("non-Telegram approval prompt = %#v", prompt)
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
	if al.interactionContinuationExpectsUserDelivery(workspace, record) {
		t.Fatal("parent-only interaction must not wait for user delivery settlement")
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
		t.Context(), registry, workspace, record, inbound, "raw child final", nil,
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

func TestParentOnlyTaskApprovalRemovesTelegramControlsWithoutLeakingResult(t *testing.T) {
	al, agent, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
	defer cleanup()
	manager := newInteractionChannelManager()
	al.channelManager = manager
	workspace := agent.Workspace
	tasks := al.taskRegistryForWorkspace(workspace)
	if err := tasks.Upsert(taskregistry.Record{
		TaskID: "approval-parent", Runtime: taskregistry.RuntimeSubagent,
		TaskKind: "spawn", Task: "finish approval in parent", Status: taskregistry.StatusRunning,
		DeliveryStatus: taskregistry.DeliveryPending,
		DeliveryMode:   string(tools.AsyncDeliveryParentOnly),
		InteractionID:  "interaction-approval-parent",
		Channel:        "telegram", ChatID: "chat-1", RequesterSessionKey: "owner-session",
	}); err != nil {
		t.Fatal(err)
	}
	argumentHash := strings.Repeat("a", 64)
	registry := al.interactionRegistryForWorkspace(workspace)
	record, err := registry.Create(interactions.CreateRequest{
		ID: "interaction-approval-parent", Kind: interactions.KindApproval,
		Route: interactions.Route{
			AgentID: agent.ID, SessionKey: "owner-session", RouteSessionKey: "route-owner",
			Channel: "telegram", ChatID: "chat-1", SenderID: "user-1",
		},
		Origin: interactions.Origin{
			TurnID: "turn-task", ToolCallID: "call-task", ToolName: "protected",
			TaskID: "approval-parent", ContinuationSessionKey: "task-session",
			ArgumentHash: argumentHash,
			ExecutionContext: &bus.InboundContext{
				Channel: "telegram", ChatID: "chat-1", SenderID: "user-1", MessageID: "origin-message",
			},
		},
		PromptSummary:  "Approve protected task action",
		ApprovalAction: "Run protected task action",
		ExpiresAt:      time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	record, _ = registry.RecordDeliveryAttempt(record.ID, record.Revision, true, "")
	record, _ = registry.MarkWaiting(record.ID, record.Revision)
	record, err = registry.ClaimAnswer(record.ID, record.Revision, interactions.Answer{
		Text: "allow_once", MessageID: "typed-fallback-answer", ReceivedAt: time.Now().UnixMilli(),
	}, interactions.OutcomeAllowed)
	if err != nil {
		t.Fatal(err)
	}
	record, err = registry.MarkResuming(record.ID, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	record, err = registry.ConsumeApproval(
		record.ID, record.Revision, record.Origin.ToolCallID, record.Origin.ToolName, argumentHash,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := al.deliverInteractionFinal(
		t.Context(), registry, workspace, record,
		bus.InboundContext{
			Channel: "telegram", ChatID: "chat-1", SenderID: "user-1", MessageID: "recovery-message",
		},
		"raw child final", nil,
	); err != nil {
		t.Fatal(err)
	}

	select {
	case acknowledgement := <-manager.sent:
		if acknowledgement.Content == "raw child final" ||
			acknowledgement.ReplyToMessageID != "typed-fallback-answer" ||
			!bus.OutboundMetadataFromMessage(acknowledgement).RemovesInteractionControls() {
			t.Fatalf("approval control acknowledgement = %#v", acknowledgement)
		}
	case <-time.After(time.Second):
		t.Fatal("parent-only approval did not remove Telegram controls")
	}
	task, _ := tasks.Get("approval-parent")
	if task.Status != taskregistry.StatusSucceeded ||
		task.DeliveryStatus != taskregistry.DeliverySessionQueued {
		t.Fatalf("parent-only approval task = %#v", task)
	}
	resolved, _ := registry.Get(record.ID)
	if resolved.Status != interactions.StatusResolved ||
		resolved.FinalDeliveryState != interactions.DeliveryStateDelivered {
		t.Fatalf("parent-only approval interaction = %#v", resolved)
	}
	select {
	case outbound := <-al.bus.(*bus.MessageBus).OutboundChan():
		if outbound.Content == "raw child final" {
			t.Fatalf("parent-only approval leaked raw child final: %#v", outbound)
		}
	default:
	}
}

func TestTaskInteractionFinalCarriesResumeScopeToUserDelivery(t *testing.T) {
	al, agent, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
	defer cleanup()
	workspace := agent.Workspace
	tasks := al.taskRegistryForWorkspace(workspace)
	if err := tasks.Upsert(taskregistry.Record{
		TaskID: "subagent-user", Runtime: taskregistry.RuntimeSubagent,
		TaskKind: "spawn", Task: "finish for user", Status: taskregistry.StatusRunning,
		DeliveryStatus: taskregistry.DeliveryPending,
		DeliveryMode:   string(tools.AsyncDeliveryUserOnly),
		InteractionID:  "interaction-user",
		Channel:        "telegram", ChatID: "chat-1", RequesterSessionKey: "owner-session",
	}); err != nil {
		t.Fatal(err)
	}
	registry := al.interactionRegistryForWorkspace(workspace)
	record, err := registry.Create(interactions.CreateRequest{
		ID: "interaction-user", Kind: interactions.KindQuestion,
		Route: interactions.Route{
			AgentID: agent.ID, SessionKey: "owner-session", RouteSessionKey: "route-owner",
			Channel: "telegram", ChatID: "chat-1", SenderID: "user-1",
		},
		Origin: interactions.Origin{
			TurnID: "turn-task", ToolCallID: "call-task", ToolName: "request_user_input",
			TaskID: "subagent-user", ContinuationSessionKey: "task-session",
		},
		Questions: []interactions.Question{{ID: "confirm", Question: "Proceed?"}},
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	record, _ = registry.RecordDeliveryAttempt(record.ID, record.Revision, true, "")
	record, _ = registry.MarkWaiting(record.ID, record.Revision)
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
	if !al.interactionContinuationExpectsUserDelivery(workspace, record) {
		t.Fatal("user-only interaction must wait for user delivery settlement")
	}
	traceScope := runtimeevents.NewTraceScope(workspace, "resume-turn")
	if err := al.deliverTaskInteractionFinal(
		t.Context(), registry, workspace, record,
		bus.InboundContext{Channel: "telegram", ChatID: "chat-1", SenderID: "user-1"},
		"raw child final", []runtimeevents.TraceScope{traceScope},
	); err != nil {
		t.Fatalf("deliverTaskInteractionFinal() error = %v", err)
	}
	select {
	case outbound := <-al.bus.(*bus.MessageBus).OutboundChan():
		if outbound.Content != "raw child final" || !outbound.TraceSettlement ||
			len(outbound.TraceScopes) != 1 || outbound.TraceScopes[0] != traceScope {
			t.Fatalf("task user delivery = %#v", outbound)
		}
	case <-time.After(time.Second):
		t.Fatal("user-only task completion was not queued")
	}
	resolved, _ := registry.Get(record.ID)
	if resolved.Status != interactions.StatusResolved ||
		resolved.FinalDeliveryState != interactions.DeliveryStateDelivered {
		t.Fatalf("interaction after user delivery = %#v", resolved)
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

func TestParseInteractionAnswerSupportsWhitespaceDelimitedCommands(t *testing.T) {
	singleRecord := interactions.Record{
		ShortID: "ABC12345",
		Questions: []interactions.Question{
			{ID: "confirmation", Question: "Proceed?"},
		},
	}
	multipleRecord := interactions.Record{
		ShortID: "13CCBF94",
		Questions: []interactions.Question{
			{ID: "test_region", Question: "Where?"},
			{ID: "test_mode", Question: "How?"},
		},
	}
	tests := []struct {
		name       string
		record     interactions.Record
		content    string
		wantText   string
		wantValues map[string]string
		wantError  bool
	}{
		{
			name: "single question command", record: singleRecord,
			content: "/answer abc12345 yes", wantText: "yes",
			wantValues: map[string]string{"confirmation": "yes"},
		},
		{
			name: "multiple answers first pair on command line", record: multipleRecord,
			content:  "/answer 13ccbf94 test_region: eu\ntest_mode: balanced",
			wantText: "test_region: eu\ntest_mode: balanced",
			wantValues: map[string]string{
				"test_region": "eu",
				"test_mode":   "balanced",
			},
		},
		{
			name: "production newline after id regression", record: multipleRecord,
			content:  "/answer 13ccbf94\ntest_region: eu\ntest_mode: balanced",
			wantText: "test_region: eu\ntest_mode: balanced",
			wantValues: map[string]string{
				"test_region": "eu",
				"test_mode":   "balanced",
			},
		},
		{
			name: "tab separator", record: singleRecord,
			content: "/answer abc12345\tyes", wantText: "yes",
			wantValues: map[string]string{"confirmation": "yes"},
		},
		{
			name: "crlf multiline body", record: multipleRecord,
			content:  "/answer\r\n13ccbf94\r\ntest_region: eu\r\ntest_mode: balanced",
			wantText: "test_region: eu\r\ntest_mode: balanced",
			wantValues: map[string]string{
				"test_region": "eu",
				"test_mode":   "balanced",
			},
		},
		{
			name: "unicode and surrounding whitespace", record: multipleRecord,
			content:  "\u2003/answer\u200313ccbf94\u2003 test_region: eu \n test_mode: balanced \u2003",
			wantText: "test_region: eu \n test_mode: balanced",
			wantValues: map[string]string{
				"test_region": "eu",
				"test_mode":   "balanced",
			},
		},
		{
			name: "telegram bot mention", record: multipleRecord,
			content:  "/answer@MintClawBot 13ccbf94\ntest_region: eu\ntest_mode: balanced",
			wantText: "test_region: eu\ntest_mode: balanced",
			wantValues: map[string]string{
				"test_region": "eu",
				"test_mode":   "balanced",
			},
		},
		{
			name: "plain message answer", record: singleRecord,
			content: "yes", wantText: "yes",
			wantValues: map[string]string{"confirmation": "yes"},
		},
		{
			name: "wrong short id", record: singleRecord,
			content: "/answer wrong-id yes", wantError: true,
		},
		{
			name: "missing short id", record: singleRecord,
			content: "/answer", wantError: true,
		},
		{
			name: "missing answer body", record: singleRecord,
			content: "/answer abc12345 \t\r\n", wantError: true,
		},
		{
			name: "unknown question id", record: multipleRecord,
			content: "/answer 13ccbf94\ntest_region: eu\nunknown: balanced", wantError: true,
		},
		{
			name: "duplicate question id", record: multipleRecord,
			content: "/answer 13ccbf94\ntest_region: eu\ntest_region: us", wantError: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			answer, parseErr := parseInteractionAnswer(test.record, test.content, "message-1")
			if test.wantError {
				if parseErr == nil {
					t.Fatalf("parseInteractionAnswer(%q) accepted malformed answer: %#v", test.content, answer)
				}
				return
			}
			if parseErr != nil {
				t.Fatalf("parseInteractionAnswer(%q) error = %v", test.content, parseErr)
			}
			if answer.Text != test.wantText || answer.MessageID != "message-1" {
				t.Fatalf("answer = %#v, want text %q and message id message-1", answer, test.wantText)
			}
			for questionID, want := range test.wantValues {
				if answer.Values[questionID] != want {
					t.Errorf("answer.Values[%q] = %q, want %q", questionID, answer.Values[questionID], want)
				}
			}
		})
	}
	if _, incompleteErr := parseInteractionAnswer(
		multipleRecord,
		"test_region: eu",
		"message-incomplete",
	); incompleteErr == nil {
		t.Fatal("parseInteractionAnswer() accepted incomplete multi-question answer")
	}
	_, _, matched, err := parseInteractionAnswerEnvelope("/answerfoo 13ccbf94 yes")
	if matched || err != nil {
		t.Fatalf("/answerfoo envelope = (matched:%v, err:%v), want non-command", matched, err)
	}

	prompt := renderInteractionPrompt(multipleRecord)
	if !strings.Contains(prompt, "`test_region`") || !strings.Contains(prompt, "`test_mode`") {
		t.Fatalf("multi-question prompt omitted canonical IDs: %q", prompt)
	}
	if _, roundTripErr := parseInteractionAnswer(
		multipleRecord,
		"test_region: eu\ntest_mode: balanced",
		"message-plain",
	); roundTripErr != nil {
		t.Fatalf("rendered question IDs did not round-trip through parser: %v", roundTripErr)
	}
	templateStart := strings.Index(prompt, "`/answer")
	if templateStart < 0 {
		t.Fatalf("rendered prompt omitted answer template: %q", prompt)
	}
	renderedSubmission := strings.ReplaceAll(prompt[templateStart:], "`", "")
	renderedSubmission = strings.Replace(renderedSubmission, "test_region: …", "test_region: eu", 1)
	renderedSubmission = strings.Replace(renderedSubmission, "test_mode: …", "test_mode: balanced", 1)
	answer, err := parseInteractionAnswer(multipleRecord, renderedSubmission, "message-rendered")
	if err != nil || answer.Values["test_region"] != "eu" ||
		answer.Values["test_mode"] != "balanced" {
		t.Fatalf("rendered answer template did not round-trip: (%#v, %v)", answer, err)
	}
}

func TestMalformedMultilineAnswerCanRetryAndResumeExactlyOnce(t *testing.T) {
	provider := &sequenceProvider{responses: []*providers.LLMResponse{{
		Content: "Interaction resumed.",
	}}}
	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()
	manager := newInteractionChannelManager()
	al.channelManager = manager
	tracker := &interactionOwnershipBus{MessageBus: al.bus.(*bus.MessageBus)}
	al.bus = tracker

	sessionKey := "session-multiline-answer-retry"
	agent.Sessions.AddFullMessage(sessionKey, providers.Message{Role: "user", Content: "Configure the test"})
	agent.Sessions.AddFullMessage(sessionKey, providers.Message{
		Role: "assistant",
		ToolCalls: []providers.ToolCall{{
			ID: "call-multiline-question", Name: "request_user_input",
			Function: &providers.FunctionCall{Name: "request_user_input", Arguments: `{}`},
		}},
	})
	request := testToolSuspensionRequest(agent.Workspace)
	request.Route.SessionKey = sessionKey
	request.Origin.ToolCallID = "call-multiline-question"
	request.Prompt.Questions = []interactions.Question{
		{ID: "test_region", Question: "Which region?"},
		{ID: "test_mode", Question: "Which mode?"},
	}
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
	waitingRevision := record.Revision
	target := &inboundDispatchTarget{
		Agent: agent, SessionKey: sessionKey,
		Allocation: session.Allocation{RouteScopeKey: request.Route.RouteSessionKey},
	}

	malformed := bus.InboundMessage{
		Content: fmt.Sprintf(
			"/answer %s\ntest_region: eu\nunknown_question: balanced",
			record.ShortID,
		),
		SpoolID: "spool-malformed-answer",
		Context: inboundContextForInteraction(request.Route),
	}
	malformed.Context.MessageID = "malformed-answer"
	ownership, err := al.processInteractionInbound(t.Context(), malformed, target)
	if err != nil || ownership != interactionInboundCallerOwned {
		t.Fatalf("malformed processInteractionInbound() = (%v, %v)", ownership, err)
	}
	afterMalformed, _ := registry.Get(record.ID)
	if afterMalformed.Status != interactions.StatusWaiting ||
		afterMalformed.Revision != waitingRevision ||
		afterMalformed.Answer != nil {
		t.Fatalf("malformed answer mutated waiting interaction: %#v", afterMalformed)
	}
	if acked, released := tracker.counts(); acked != 0 || released != 0 {
		t.Fatalf("malformed answer ownership = acked:%d released:%d, want 0/0", acked, released)
	}

	valid := bus.InboundMessage{
		Content: fmt.Sprintf(
			"/answer %s\ntest_region: eu\ntest_mode: balanced",
			record.ShortID,
		),
		SpoolID: "spool-valid-answer",
		Context: inboundContextForInteraction(request.Route),
	}
	valid.Context.MessageID = "valid-answer"
	ownership, err = al.processInteractionInbound(t.Context(), valid, target)
	if err != nil || ownership != interactionInboundClaimed {
		t.Fatalf("valid processInteractionInbound() = (%v, %v)", ownership, err)
	}
	resolved, _ := registry.Get(record.ID)
	if resolved.Status != interactions.StatusResolved || resolved.Answer == nil ||
		resolved.Answer.Values["test_region"] != "eu" ||
		resolved.Answer.Values["test_mode"] != "balanced" {
		t.Fatalf("resolved interaction = %#v", resolved)
	}
	if acked, released := tracker.counts(); acked != 1 || released != 0 {
		t.Fatalf("valid retry ownership = acked:%d released:%d, want 1/0", acked, released)
	}
	provider.mu.Lock()
	providerCalls := provider.callCount
	provider.mu.Unlock()
	if providerCalls != 1 {
		t.Fatalf("resumption provider calls = %d, want 1", providerCalls)
	}
	toolResults := 0
	for _, message := range agent.Sessions.GetHistory(sessionKey) {
		if message.Role == "tool" && message.ToolCallID == "call-multiline-question" {
			toolResults++
		}
	}
	if toolResults != 1 {
		t.Fatalf("matching tool results = %d, want 1", toolResults)
	}
	select {
	case outbound := <-manager.sent:
		if outbound.Content != "Interaction resumed." {
			t.Fatalf("resumed final response = %#v", outbound)
		}
	default:
		t.Fatal("resumed final response was not delivered")
	}
}

func TestRenderInteractionPromptUsesAgentAuthoredLanguage(t *testing.T) {
	tests := []struct {
		name   string
		record interactions.Record
		want   string
	}{
		{
			name: "Russian single question with options",
			record: interactions.Record{
				ShortID: "16131195",
				Questions: []interactions.Question{{
					ID: "environment", Question: "Какую среду выбрать?",
					Options: []interactions.Option{
						{Label: "development", Description: "Среда разработки."},
						{Label: "staging", Description: "Предпродовая среда."},
						{Label: "production", Description: "Боевая среда."},
					},
				}},
			},
			want: "Какую среду выбрать?\n\n" +
				"• development — Среда разработки.\n" +
				"• staging — Предпродовая среда.\n" +
				"• production — Боевая среда.\n\n" +
				"`/answer 16131195 …`",
		},
		{
			name: "Japanese single question with header",
			record: interactions.Record{
				ShortID: "8f03c2aa",
				Questions: []interactions.Question{{
					ID: "region", Header: "地域", Question: "デプロイ先を選んでください。",
				}},
			},
			want: "地域\n\nデプロイ先を選んでください。\n\n`/answer 8f03c2aa …`",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := renderInteractionPrompt(test.record); got != test.want {
				t.Fatalf("renderInteractionPrompt() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestRenderMultipleQuestionPromptUsesNeutralAnswerTemplate(t *testing.T) {
	record := interactions.Record{
		ShortID: "16131195",
		Questions: []interactions.Question{
			{ID: "region", Header: "Регион", Question: "Какой регион использовать?"},
			{ID: "mode", Header: "Режим", Question: "Какой режим развёртывания выбрать?"},
		},
	}
	want := "1. `region` Регион\nКакой регион использовать?\n\n" +
		"2. `mode` Режим\nКакой режим развёртывания выбрать?\n\n" +
		"`/answer 16131195`\n`region: …`\n`mode: …`"
	got := renderInteractionPrompt(record)
	if got != want {
		t.Fatalf("renderInteractionPrompt() = %q, want %q", got, want)
	}
	for _, removed := range []string{"Input needed", "Reply with", "question_id", "<answer>"} {
		if strings.Contains(got, removed) {
			t.Fatalf("prompt retained runtime prose %q: %q", removed, got)
		}
	}
}

func TestParseSingleInteractionAnswerSupportsDirectAndCommandReplies(t *testing.T) {
	record := interactions.Record{
		ShortID: "16131195",
		Questions: []interactions.Question{{
			ID: "environment", Question: "Какую среду выбрать?",
		}},
	}
	for _, reply := range []string{"production", "/answer 16131195 production"} {
		answer, err := parseInteractionAnswer(record, reply, "message-single")
		if err != nil || answer.Text != "production" || answer.MessageID != "message-single" {
			t.Fatalf("parseInteractionAnswer(%q) = (%#v, %v)", reply, answer, err)
		}
	}
}

func TestApprovalPromptAndAnswerUseFixedPolicyChoices(t *testing.T) {
	record := interactions.Record{
		Kind: interactions.KindApproval, ShortID: "APR123",
		Origin:         interactions.Origin{ToolName: "deploy"},
		PromptSummary:  "Run a protected deployment command?",
		ApprovalAction: "Run a protected deployment command?",
	}
	prompt := renderInteractionPrompt(record)
	want := "deploy\nRun a protected deployment command?\n\n" +
		"`/answer APR123 allow_once`\n`/answer APR123 deny`"
	if prompt != want {
		t.Fatalf("approval prompt = %q, want %q", prompt, want)
	}
	for _, removed := range []string{"Approval needed", "Requested action", "Reply"} {
		if strings.Contains(prompt, removed) {
			t.Fatalf("approval prompt retained runtime prose %q: %q", removed, prompt)
		}
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

func TestApprovalAnswerOutcomeIsChannelIndependent(t *testing.T) {
	for _, channel := range []string{"telegram", "slack"} {
		for _, test := range []struct {
			answer string
			want   interactions.Outcome
		}{
			{answer: "allow_once", want: interactions.OutcomeAllowed},
			{answer: "deny", want: interactions.OutcomeDenied},
		} {
			record := interactions.Record{
				Kind:  interactions.KindApproval,
				Route: interactions.Route{Channel: channel},
			}
			got := interactionAnswerOutcome(record, interactions.Answer{Text: test.answer})
			if got != test.want {
				t.Fatalf(
					"interactionAnswerOutcome(channel=%q, answer=%q) = %q, want %q",
					channel,
					test.answer,
					got,
					test.want,
				)
			}
		}
	}
	question := interactions.Record{Kind: interactions.KindQuestion}
	if got := interactionAnswerOutcome(question, interactions.Answer{Text: "allow_once"}); got != interactions.OutcomeAnswered {
		t.Fatalf("question outcome = %q, want answered", got)
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
		{
			name: "allow once", answer: "allow_once", outcome: interactions.OutcomeAllowed,
			wantExecutions: 1, wantConsumed: true,
		},
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
				Channel: "telegram", ChatID: "chat-1", SenderID: "user-1", MessageID: "origin-message",
			}
			turnStatus := TurnEndStatusCompleted
			response, err := al.runAgentLoop(t.Context(), agent, processOptions{
				TurnStatus:            &turnStatus,
				InteractionSessionKey: "owner-session",
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
			record, ok := activeInteractionForSession(registry, "owner-session")
			if !ok || record.Kind != interactions.KindApproval ||
				record.Status != interactions.StatusWaiting || record.Origin.ArgumentHash == "" {
				t.Fatalf("approval interaction = %#v", record)
			}
			select {
			case prompt := <-manager.sent:
				if !strings.Contains(prompt.Content, "approval_counting") ||
					!strings.Contains(prompt.Content, "Run the protected test action") ||
					!strings.Contains(prompt.Content, "`/answer "+record.ShortID+" allow_once`") ||
					strings.Contains(prompt.Content, "Approval needed") ||
					strings.Contains(prompt.Content, "secret-value") ||
					prompt.ReplyToMessageID != "origin-message" ||
					!bus.OutboundMetadataFromMessage(prompt).IsApprovalPrompt() {
					t.Fatalf("approval prompt = %#v", prompt)
				}
			case <-time.After(time.Second):
				t.Fatal("approval prompt was not delivered")
			}
			if len(manager.dismissedSessions) != 1 ||
				manager.dismissedSessions[0] != "telegram:chat-1:session-approval" {
				t.Fatalf("suspension feedback cleanup = %#v", manager.dismissedSessions)
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
				if final.Content != "approval flow finished" ||
					!bus.OutboundMetadataFromMessage(final).RemovesInteractionControls() ||
					final.ReplyToMessageID != "approval-answer" {
					t.Fatalf("approval final = %#v", final)
				}
			case <-time.After(time.Second):
				t.Fatal("approval continuation final was not delivered")
			}
			wantDismissed := []string{
				"telegram:chat-1:session-approval",
				"telegram:chat-1:owner-session",
			}
			if test.outcome == interactions.OutcomeAllowed {
				wantDismissed = []string{
					"telegram:chat-1:session-approval",
					"telegram:chat-1:session-approval",
					"telegram:chat-1:owner-session",
				}
			}
			if strings.Join(manager.dismissedSessions, "\n") != strings.Join(wantDismissed, "\n") {
				t.Fatalf(
					"feedback cleanup = %#v, want %#v",
					manager.dismissedSessions,
					wantDismissed,
				)
			}
		})
	}
}

func TestDurableHumanApprovalBindsTrustedPreparedArguments(t *testing.T) {
	provider := &sequenceProvider{responses: []*providers.LLMResponse{
		{ToolCalls: []providers.ToolCall{{
			ID:        "call-prepared",
			Name:      "approval_binding",
			Arguments: map[string]any{"mutable": "model-value"},
			Function: &providers.FunctionCall{
				Name: "approval_binding", Arguments: `{"mutable":"model-value"}`,
			},
		}}},
		{Content: "approval flow finished", FinishReason: "stop"},
	}}
	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()
	manager := newInteractionChannelManager()
	al.channelManager = manager
	tool := &approvalBindingTool{}
	agent.Tools.Register(tool)
	if err := al.MountHook(NamedHook("prepared-approval", &durableApprovalHook{
		actionSummary: "Run the prepared protected action",
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
			RouteSessionKey: "route-prepared", SessionKey: "session-prepared",
			UserMessage: "run prepared action", InboundContext: inbound,
		},
		DefaultResponse: defaultResponse, EnableSummary: true, SendResponse: false,
	})
	if err != nil || response != "" || turnStatus != TurnEndStatusSuspended {
		t.Fatalf("initial approval turn = (%q, %q, %v)", response, turnStatus, err)
	}
	registry := al.interactionRegistryForWorkspace(agent.Workspace)
	record, ok := activeInteractionForSession(registry, "session-prepared")
	if !ok {
		t.Fatal("approval interaction not found")
	}
	wantHash, err := interactions.HashArguments(agent.Workspace, map[string]any{
		"plan_hash": "prepared-plan-hash",
	})
	if err != nil {
		t.Fatal(err)
	}
	if record.Origin.ArgumentHash != wantHash {
		t.Fatalf("argument hash = %q, want trusted binding %q", record.Origin.ArgumentHash, wantHash)
	}
	record, err = registry.ClaimAnswer(record.ID, record.Revision, interactions.Answer{
		Text: "allow_once", MessageID: "approval-answer", ReceivedAt: time.Now().UnixMilli(),
	}, interactions.OutcomeAllowed)
	if err != nil {
		t.Fatal(err)
	}
	if err = al.resumeClaimedInteraction(
		t.Context(), registry, agent.Workspace, agent, nil, *inbound, record,
	); err != nil {
		t.Fatal(err)
	}
	resolved, _ := registry.Get(record.ID)
	if resolved.ApprovalConsumedAt == 0 || tool.executions != 1 {
		t.Fatalf("resolved approval = %#v, executions=%d", resolved, tool.executions)
	}
	if len(tool.bindingCalls) != 2 ||
		tool.bindingCalls[0] != "call-prepared" ||
		tool.bindingCalls[1] != "call-prepared" {
		t.Fatalf("approval binding calls = %#v", tool.bindingCalls)
	}
	if len(tool.bindingContinuations) != 2 ||
		tool.bindingContinuations[0] ||
		!tool.bindingContinuations[1] {
		t.Fatalf("approval continuation markers = %#v", tool.bindingContinuations)
	}
	if len(tool.executionIDs) != 2 ||
		tool.executionIDs[0] != record.Origin.ExecutionID ||
		tool.executionIDs[1] != record.Origin.ExecutionID {
		t.Fatalf(
			"approval execution identities = %#v, origin = %q",
			tool.executionIDs,
			record.Origin.ExecutionID,
		)
	}
	if len(tool.workspaces) != 2 ||
		tool.workspaces[0] != agent.Workspace ||
		tool.workspaces[1] != agent.Workspace {
		t.Fatalf("approval workspaces = %#v", tool.workspaces)
	}
}

func TestDurableHumanApprovalDoesNotPrepareAfterPolicyRevocation(t *testing.T) {
	provider := &sequenceProvider{responses: []*providers.LLMResponse{
		{ToolCalls: []providers.ToolCall{{
			ID: "call-revoked", Name: "approval_binding",
			Arguments: map[string]any{"mutable": "model-value"},
			Function: &providers.FunctionCall{
				Name: "approval_binding", Arguments: `{"mutable":"model-value"}`,
			},
		}}},
		{Content: "approval denied", FinishReason: "stop"},
	}}
	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()
	al.channelManager = newInteractionChannelManager()
	tool := &approvalBindingTool{}
	agent.Tools.Register(tool)
	hook := &durableApprovalHook{actionSummary: "Run the prepared protected action"}
	if err := al.MountHook(NamedHook("revoked-prepared-approval", hook)); err != nil {
		t.Fatal(err)
	}
	inbound := &bus.InboundContext{
		Channel: "telegram", ChatID: "chat-1", SenderID: "user-1",
	}
	turnStatus := TurnEndStatusCompleted
	if _, err := al.runAgentLoop(t.Context(), agent, processOptions{
		TurnStatus: &turnStatus,
		Dispatch: DispatchRequest{
			RouteSessionKey: "route-revoked", SessionKey: "session-revoked",
			UserMessage: "run prepared action", InboundContext: inbound,
		},
		DefaultResponse: defaultResponse, EnableSummary: true, SendResponse: false,
	}); err != nil || turnStatus != TurnEndStatusSuspended {
		t.Fatalf("initial approval turn = (%q, %v)", turnStatus, err)
	}
	registry := al.interactionRegistryForWorkspace(agent.Workspace)
	record, ok := activeInteractionForSession(registry, "session-revoked")
	if !ok || len(tool.bindingCalls) != 1 {
		t.Fatalf("initial approval = (%#v, binding calls=%#v)", record, tool.bindingCalls)
	}
	hook.revoked = true
	record, err := registry.ClaimAnswer(record.ID, record.Revision, interactions.Answer{
		Text: "allow_once", MessageID: "approval-answer", ReceivedAt: time.Now().UnixMilli(),
	}, interactions.OutcomeAllowed)
	if err != nil {
		t.Fatal(err)
	}
	if err = al.resumeClaimedInteraction(
		t.Context(), registry, agent.Workspace, agent, nil, *inbound, record,
	); err != nil {
		t.Fatal(err)
	}
	resolved, _ := registry.Get(record.ID)
	if resolved.ApprovalConsumedAt != 0 || tool.executions != 0 ||
		len(tool.bindingCalls) != 1 {
		t.Fatalf(
			"revoked approval = (%#v, executions=%d, binding calls=%#v)",
			resolved,
			tool.executions,
			tool.bindingCalls,
		)
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
		record.ApprovalAction != "Rotate production signing material" {
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
				ApprovalAction: "Run recovery action",
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
	for _, test := range []struct {
		name              string
		expireBeforeClaim bool
		reloadAfterClaim  bool
	}{
		{name: "expired when answer is claimed", expireBeforeClaim: true},
		{name: "expired before consumption after restart", reloadAfterClaim: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := &sequenceProvider{responses: []*providers.LLMResponse{{
				Content: "approval expired", FinishReason: "stop",
			}}}
			al, agent, cleanup := newTurnCoordTestLoop(t, provider)
			defer cleanup()
			al.channelManager = newInteractionChannelManager()
			tool := &approvalCountingTool{}
			agent.Tools.Register(tool)
			if err := al.MountHook(NamedHook("expiry-approval", &durableApprovalHook{
				actionSummary: "Run the protected action",
			})); err != nil {
				t.Fatal(err)
			}
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
			registryPath := interactions.WorkspaceStorePath(agent.Workspace)
			registry := interactions.NewRegistryWithOptions(
				registryPath,
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
				ApprovalAction: "Run the protected action",
				ExpiresAt:      now.Add(time.Minute),
			})
			if err != nil {
				t.Fatal(err)
			}
			record, _ = registry.RecordDeliveryAttempt(record.ID, record.Revision, true, "")
			record, _ = registry.MarkWaiting(record.ID, record.Revision)
			if test.expireBeforeClaim {
				now = time.UnixMilli(record.ExpiresAt)
			} else {
				now = time.UnixMilli(record.ExpiresAt - 1)
			}
			record, err = registry.ClaimAnswer(record.ID, record.Revision, interactions.Answer{
				Text: "allow_once", MessageID: "allow-once", ReceivedAt: now.UnixMilli(),
			}, interactions.OutcomeAllowed)
			if err != nil {
				t.Fatal(err)
			}
			if test.reloadAfterClaim {
				now = time.UnixMilli(record.ExpiresAt)
				registry = interactions.NewRegistryWithOptions(
					registryPath,
					interactions.Options{Now: func() time.Time { return now }},
				)
				if registry.LastLoadError() != nil {
					t.Fatal(registry.LastLoadError())
				}
				al.interactionRegistries.Store(agent.Workspace, registry)
				record, _ = registry.Get(record.ID)
			}
			if err = al.resumeClaimedInteraction(
				t.Context(), registry, agent.Workspace, agent, nil, *inbound, record,
			); err != nil {
				t.Fatalf("resumeClaimedInteraction() error = %v", err)
			}
			resolved, _ := registry.Get(record.ID)
			if tool.executions != 0 || resolved.ApprovalConsumedAt != 0 ||
				resolved.Status != interactions.StatusResolved ||
				resolved.Outcome != interactions.OutcomeTimedOut {
				t.Fatalf("expired approval = %#v, executions=%d", resolved, tool.executions)
			}
		})
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
	al, agent, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
	defer cleanup()
	workspace := agent.Workspace
	request := testToolSuspensionRequest(workspace)
	agent.Sessions.AddFullMessage(request.Route.SessionKey, providers.Message{
		Role: "assistant",
		ToolCalls: []providers.ToolCall{{
			ID: request.Origin.ToolCallID, Name: request.Origin.ToolName,
			Function: &providers.FunctionCall{Name: request.Origin.ToolName, Arguments: `{}`},
		}},
	})
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
	waitingRevision := record.Revision
	target := &inboundDispatchTarget{
		Agent: agent, SessionKey: request.Route.SessionKey,
		RouteClaimKey: runtimeRouteClaimKey(request.Route.RouteSessionKey, ""),
		Allocation:    session.Allocation{RouteScopeKey: request.Route.RouteSessionKey},
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
	msg.Content = "/answerfoo"
	if al.shouldHandleInteractionInbound(msg, target) {
		t.Fatal("command-prefix collision was consumed as an interaction answer")
	}
	for _, malformedCommand := range []string{
		fmt.Sprintf("/answer@bot@junk %s yes", record.ShortID),
		fmt.Sprintf("/answer@bot/path %s yes", record.ShortID),
		fmt.Sprintf("/answer@ %s yes", record.ShortID),
	} {
		msg.Content = malformedCommand
		if al.shouldHandleInteractionInbound(msg, target) {
			t.Errorf("malformed answer command %q was consumed", malformedCommand)
		}
		current, _ := registry.Get(record.ID)
		if current.Status != interactions.StatusWaiting ||
			current.Revision != waitingRevision ||
			current.Answer != nil {
			t.Errorf("malformed answer command %q mutated interaction: %#v", malformedCommand, current)
		}
	}
	msg.Content = "/reset"
	result, err := al.cancelInteractionForControlMessage(t.Context(), msg, target)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Matched || !result.Canceled || result.Failed || result.CommandHandled {
		t.Fatalf("reset cancellation result = %#v", result)
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
		RouteClaimKey: runtimeRouteClaimKey(request.Route.RouteSessionKey, ""),
		Allocation:    session.Allocation{RouteScopeKey: request.Route.RouteSessionKey},
	}
	msg := bus.InboundMessage{
		Content: "Canary", SpoolID: "spool-claimed-answer",
		Context: inboundContextForInteraction(request.Route),
	}
	msg.Context.MessageID = "answer-claimed"
	scope := newRuntimeSessionScope(agent.Workspace, sessionKey)
	claim, claimed := al.claimRuntimeSession(scope, "test-claimed-spool")
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
	scope := newRuntimeSessionScope(agent.Workspace, sessionKey)
	claim, claimed := al.claimRuntimeSession(scope, "test-active-resume")
	if !claimed {
		t.Fatal("failed to claim test session")
	}
	defer claim.releaseIfOwned()

	newInboundTurnCoordinator(al).handleInteractionInbound(t.Context(), msg, target)
	acked, released := tracker.counts()
	if acked != 0 || released != 0 {
		t.Fatalf("deferred spool ownership = acked:%d released:%d, want 0/0", acked, released)
	}
	if got := al.pendingSteeringCountForScope(scope); got != 1 {
		t.Fatalf("deferred queue depth = %d, want 1", got)
	}
	queued := al.dequeueSteeringMessagesForTurn(scope, request.Route.SenderID)
	if len(queued) != 1 || queued[0].InboundSpoolID != "spool-correction" {
		t.Fatalf("deferred message = %#v", queued)
	}
}

func TestConcurrentExplicitInteractionAnswersNeverBecomeSteering(t *testing.T) {
	provider := newBlockingInteractionProvider()
	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()
	manager := newInteractionChannelManager()
	al.channelManager = manager
	tracker := &interactionOwnershipBus{MessageBus: al.bus.(*bus.MessageBus)}
	al.bus = tracker

	sessionKey := "session-concurrent-explicit-answers"
	agent.Sessions.AddFullMessage(sessionKey, providers.Message{Role: "user", Content: "Choose a value"})
	agent.Sessions.AddFullMessage(sessionKey, providers.Message{
		Role: "assistant",
		ToolCalls: []providers.ToolCall{{
			ID: "call-concurrent-answer", Name: "request_user_input",
			Function: &providers.FunctionCall{Name: "request_user_input", Arguments: `{}`},
		}},
	})
	request := testToolSuspensionRequest(agent.Workspace)
	request.Route.SessionKey = sessionKey
	request.Route.TopicID = "topic-1"
	request.Origin.ToolCallID = "call-concurrent-answer"
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
		Agent:         agent,
		SessionKey:    sessionKey,
		RouteClaimKey: runtimeRouteClaimKey(request.Route.RouteSessionKey, ""),
		Allocation:    session.Allocation{RouteScopeKey: request.Route.RouteSessionKey},
	}
	first := bus.InboundMessage{
		Content: "/answer " + record.ShortID + " первый", SpoolID: "spool-answer-first",
		Context: inboundContextForInteraction(request.Route),
	}
	first.Context.MessageID = "answer-first"
	coordinator := newInboundTurnCoordinator(al)
	if !coordinator.routeExplicitInteractionAnswer(t.Context(), first, target) {
		t.Fatal("first explicit answer was not classified as interaction protocol")
	}
	select {
	case <-provider.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the accepted interaction continuation")
	}

	contenders := []bus.InboundMessage{
		first,
		{
			Content: "/answer " + record.ShortID + " первый", SpoolID: "spool-answer-same",
			Context: inboundContextForInteraction(request.Route),
		},
		{
			Content: "/answer " + record.ShortID + " второй", SpoolID: "spool-answer-second",
			Context: inboundContextForInteraction(request.Route),
		},
		{
			Content: "/answer deadbeef второй", SpoolID: "spool-answer-wrong-id",
			Context: inboundContextForInteraction(request.Route),
		},
		{
			Content: "/answer " + record.ShortID + " второй", SpoolID: "spool-answer-wrong-sender",
			Context: inboundContextForInteraction(request.Route),
		},
		{
			Content: "/answer " + record.ShortID + " второй", SpoolID: "spool-answer-wrong-chat",
			Context: inboundContextForInteraction(request.Route),
		},
		{
			Content: "/answer " + record.ShortID + " второй", SpoolID: "spool-answer-wrong-topic",
			Context: inboundContextForInteraction(request.Route),
		},
	}
	contenders[0].SpoolID = "spool-answer-replay"
	contenders[1].Context.MessageID = "answer-same"
	contenders[2].Context.MessageID = "answer-second"
	contenders[3].Context.MessageID = "answer-wrong-id"
	contenders[4].Context.MessageID = "answer-wrong-sender"
	contenders[4].Context.SenderID = "user-2"
	contenders[5].Context.MessageID = "answer-wrong-chat"
	contenders[5].Context.ChatID = "chat-2"
	contenders[6].Context.MessageID = "answer-wrong-topic"
	contenders[6].Context.TopicID = "topic-2"
	for _, contender := range contenders {
		if !coordinator.routeExplicitInteractionAnswer(t.Context(), contender, target) {
			t.Fatalf("explicit contender escaped protocol routing: %q", contender.Content)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		acked, released := tracker.counts()
		if acked == 1+len(contenders) {
			if released != 0 {
				t.Fatalf("interaction ingress released %d spool entries", released)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("spool acknowledgements = %d, want %d", acked, 1+len(contenders))
		}
		time.Sleep(time.Millisecond)
	}
	scope := newRuntimeSessionScope(agent.Workspace, sessionKey)
	if got := al.pendingSteeringCountForScope(scope); got != 0 {
		t.Fatalf("explicit answer contenders entered steering queue: %d", got)
	}
	close(provider.release)

	deadline = time.Now().Add(2 * time.Second)
	for {
		record, _ = registry.Get(record.ID)
		if record.Status == interactions.StatusResolved {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("interaction did not resolve: %#v", record)
		}
		time.Sleep(time.Millisecond)
	}
	if record.Answer == nil || record.Answer.Text != "первый" ||
		record.Answer.MessageID != "answer-first" {
		t.Fatalf("durable winner = %#v", record.Answer)
	}
	if record.ResumeTries != 1 || record.FinalDeliveryTries != 1 || !record.FinalDelivered {
		t.Fatalf("continuation/delivery counts = %#v", record)
	}
	eventCounts := map[interactions.EventType]int{}
	for _, event := range registry.ListEvents(record.ID) {
		eventCounts[event.Type]++
	}
	if eventCounts[interactions.EventAnswerClaimed] != 1 ||
		eventCounts[interactions.EventResumeStarted] != 1 {
		t.Fatalf("interaction event counts = %#v", eventCounts)
	}
	calls, modelCalls := provider.snapshot()
	if calls != 1 || len(modelCalls) != 1 {
		t.Fatalf("provider calls = %d, messages = %d", calls, len(modelCalls))
	}
	for _, message := range modelCalls[0] {
		if strings.Contains(message.Content, "/answer") || strings.Contains(message.Content, "второй") {
			t.Fatalf("losing answer entered model context: %#v", modelCalls[0])
		}
	}
	for _, message := range agent.Sessions.GetHistory(sessionKey) {
		if strings.Contains(message.Content, "/answer") || strings.Contains(message.Content, "второй") {
			t.Fatalf("losing answer entered conversation history: %#v", message)
		}
	}
	finals := 0
	for len(manager.sent) > 0 {
		outbound := <-manager.sent
		if outbound.Content == "DUPLICATE_INTERACTION_OK: первый" {
			finals++
		}
	}
	if finals != 1 {
		t.Fatalf("final responses = %d, want 1", finals)
	}

	terminalReplay := first
	terminalReplay.SpoolID = "spool-answer-terminal-replay"
	if !coordinator.routeExplicitInteractionAnswer(t.Context(), terminalReplay, target) {
		t.Fatal("terminal replay escaped interaction protocol routing")
	}
	acked, released := tracker.counts()
	if acked != 2+len(contenders) || released != 0 {
		t.Fatalf("terminal replay ownership = acked:%d released:%d", acked, released)
	}
	if callsAfterReplay, _ := provider.snapshot(); callsAfterReplay != 1 {
		t.Fatalf("terminal replay started another continuation: %d calls", callsAfterReplay)
	}
}

func TestExplicitAnswerContentionReleasesBeforeDurableAnswerAdmission(t *testing.T) {
	for _, test := range []struct {
		name        string
		markWaiting bool
		status      interactions.Status
	}{
		{name: "created_after_delivery", status: interactions.StatusCreated},
		{name: "waiting", markWaiting: true, status: interactions.StatusWaiting},
	} {
		t.Run(test.name, func(t *testing.T) {
			al, agent, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
			defer cleanup()
			tracker := &interactionOwnershipBus{MessageBus: al.bus.(*bus.MessageBus)}
			al.bus = tracker
			sessionKey := "session-answer-admission-contention-" + test.name
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
			if test.markWaiting {
				record, _ = registry.MarkWaiting(record.ID, record.Revision)
			}
			target := &inboundDispatchTarget{
				Agent: agent, SessionKey: sessionKey,
				Allocation: session.Allocation{RouteScopeKey: request.Route.RouteSessionKey},
			}
			scope := newRuntimeSessionScope(agent.Workspace, sessionKey)
			blocker, claimed := al.claimRuntimeSession(scope, "answer-admission-blocker")
			if !claimed {
				t.Fatal("failed to claim the interaction session blocker")
			}
			defer blocker.releaseIfOwned()

			contender := bus.InboundMessage{
				Content: "/answer " + record.ShortID + " valid",
				SpoolID: "spool-admission-contender-" + test.name,
				Context: inboundContextForInteraction(request.Route),
			}
			contender.Context.MessageID = "admission-contender-" + test.name
			if !newInboundTurnCoordinator(al).routeExplicitInteractionAnswer(
				t.Context(),
				contender,
				target,
			) {
				t.Fatal("pre-admission contender escaped interaction protocol routing")
			}
			deadline := time.Now().Add(2 * time.Second)
			for {
				acked, released := tracker.counts()
				if released == 1 {
					if acked != 0 {
						t.Fatalf("contender was acknowledged before a durable claim: %d", acked)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("timed out waiting for contended answer transport release")
				}
				time.Sleep(time.Millisecond)
			}
			record, _ = registry.Get(record.ID)
			if record.Status != test.status || record.Answer != nil {
				t.Fatalf("runtime contention chose a durable answer: %#v", record)
			}
			if got := al.pendingSteeringCountForScope(scope); got != 0 {
				t.Fatalf("released answer entered steering queue: %d", got)
			}
			for _, event := range registry.ListEvents(record.ID) {
				if event.Type == interactions.EventAnswerClaimed ||
					event.Type == interactions.EventResumeStarted {
					t.Fatalf("contention emitted durable transition: %#v", event)
				}
			}
		})
	}
}

func TestRetainedAnswerReplayPrecedesNewActiveInteractionWrongID(t *testing.T) {
	provider := &sequenceProvider{}
	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()
	tracker := &interactionOwnershipBus{MessageBus: al.bus.(*bus.MessageBus)}
	al.bus = tracker
	sessionKey := "session-retained-replay"
	request := testToolSuspensionRequest(agent.Workspace)
	request.Route.SessionKey = sessionKey
	registry := al.interactionRegistryForWorkspace(agent.Workspace)
	first, err := registry.Create(interactions.CreateRequest{
		Kind: request.Prompt.Kind, Route: request.Route, Origin: request.Origin,
		Questions: request.Prompt.Questions, ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	first, _ = registry.RecordDeliveryAttempt(first.ID, first.Revision, true, "")
	first, _ = registry.MarkWaiting(first.ID, first.Revision)
	first, err = registry.ClaimAnswer(first.ID, first.Revision, interactions.Answer{
		Text: "первый", Values: map[string]string{"deploy_mode": "первый"},
		MessageID: "retained-answer",
	}, interactions.OutcomeAnswered)
	if err != nil {
		t.Fatal(err)
	}
	first, err = registry.MarkResuming(first.ID, first.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = registry.Resolve(first.ID, first.Revision); err != nil {
		t.Fatal(err)
	}
	request.Origin.ToolCallID = "call-next-question"
	second, err := registry.Create(interactions.CreateRequest{
		Kind: request.Prompt.Kind, Route: request.Route, Origin: request.Origin,
		Questions: request.Prompt.Questions, ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	second, _ = registry.RecordDeliveryAttempt(second.ID, second.Revision, true, "")
	second, _ = registry.MarkWaiting(second.ID, second.Revision)
	target := &inboundDispatchTarget{
		Agent: agent, SessionKey: sessionKey,
		Allocation: session.Allocation{RouteScopeKey: request.Route.RouteSessionKey},
	}
	replay := bus.InboundMessage{
		Content: "/answer " + first.ShortID + " первый", SpoolID: "spool-retained-replay",
		Context: inboundContextForInteraction(request.Route),
	}
	replay.Context.MessageID = "retained-answer"
	if !newInboundTurnCoordinator(al).routeExplicitInteractionAnswer(t.Context(), replay, target) {
		t.Fatal("retained replay escaped interaction protocol routing")
	}
	acked, released := tracker.counts()
	if acked != 1 || released != 0 {
		t.Fatalf("retained replay ownership = acked:%d released:%d", acked, released)
	}
	second, _ = registry.Get(second.ID)
	if second.Status != interactions.StatusWaiting || second.Answer != nil {
		t.Fatalf("retained replay mutated the new interaction: %#v", second)
	}
	if got := al.pendingSteeringCountForScope(
		newRuntimeSessionScope(agent.Workspace, sessionKey),
	); got != 0 {
		t.Fatalf("retained replay entered steering queue: %d", got)
	}
	provider.mu.Lock()
	calls := provider.callCount
	provider.mu.Unlock()
	if calls != 0 {
		t.Fatalf("retained replay started %d continuation(s)", calls)
	}
	select {
	case outbound := <-tracker.OutboundChan():
		t.Fatalf("retained replay produced a notice: %#v", outbound)
	default:
	}
}

func TestReloadedClaimedInteractionRejectsLosingExplicitAnswer(t *testing.T) {
	provider := &sequenceProvider{}
	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()
	tracker := &interactionOwnershipBus{MessageBus: al.bus.(*bus.MessageBus)}
	al.bus = tracker
	sessionKey := "session-reloaded-claimed-answer"
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
		Text: "первый", Values: map[string]string{"deploy_mode": "первый"},
		MessageID: "answer-first",
	}, interactions.OutcomeAnswered)
	if err != nil {
		t.Fatal(err)
	}
	reloaded := *al.GetConfig()
	if err = al.ReloadProviderAndConfig(t.Context(), provider, &reloaded); err != nil {
		t.Fatal(err)
	}
	reloadedAgent, ok := al.GetRegistry().GetAgent(agent.ID)
	if !ok {
		t.Fatal("reloaded agent is unavailable")
	}
	target := &inboundDispatchTarget{
		Agent: reloadedAgent, SessionKey: sessionKey,
		Allocation: session.Allocation{RouteScopeKey: request.Route.RouteSessionKey},
	}
	loser := bus.InboundMessage{
		Content: "/answer " + record.ShortID + " второй", SpoolID: "spool-reloaded-loser",
		Context: inboundContextForInteraction(request.Route),
	}
	loser.Context.MessageID = "answer-second"
	if !newInboundTurnCoordinator(al).routeExplicitInteractionAnswer(t.Context(), loser, target) {
		t.Fatal("reloaded losing answer escaped interaction protocol routing")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		acked, released := tracker.counts()
		if acked == 1 {
			if released != 0 {
				t.Fatalf("reloaded loser released %d spool entries", released)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for reloaded loser acknowledgement")
		}
		time.Sleep(time.Millisecond)
	}
	record, _ = registry.Get(record.ID)
	if record.Status != interactions.StatusClaimed || record.Answer == nil ||
		record.Answer.MessageID != "answer-first" {
		t.Fatalf("reloaded interaction was mutated: %#v", record)
	}
	provider.mu.Lock()
	calls := provider.callCount
	provider.mu.Unlock()
	if calls != 0 {
		t.Fatalf("reloaded losing answer started %d continuation(s)", calls)
	}
	scope := newRuntimeSessionScope(reloadedAgent.Workspace, sessionKey)
	if got := al.pendingSteeringCountForScope(scope); got != 0 {
		t.Fatalf("reloaded losing answer entered steering queue: %d", got)
	}
	for _, message := range reloadedAgent.Sessions.GetHistory(sessionKey) {
		if strings.Contains(message.Content, "второй") {
			t.Fatalf("reloaded losing answer entered history: %#v", message)
		}
	}
}

func TestTaskInteractionConcurrentExplicitAnswersStartOneContinuation(t *testing.T) {
	provider := newBlockingInteractionProvider()
	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()
	manager := newInteractionChannelManager()
	al.channelManager = manager
	tracker := &interactionOwnershipBus{MessageBus: al.bus.(*bus.MessageBus)}
	al.bus = tracker

	sessionKey := "session-task-concurrent-answer"
	continuationSessionKey := "task-continuation-concurrent-answer"
	agent.Sessions.AddFullMessage(continuationSessionKey, providers.Message{
		Role: "assistant",
		ToolCalls: []providers.ToolCall{{
			ID: "call-task-concurrent-answer", Name: "request_user_input",
			Function: &providers.FunctionCall{Name: "request_user_input", Arguments: `{}`},
		}},
	})
	request := testToolSuspensionRequest(agent.Workspace)
	request.Route.SessionKey = sessionKey
	request.Origin.ToolCallID = "call-task-concurrent-answer"
	request.Origin.TaskID = "task-concurrent-answer"
	request.Origin.ContinuationSessionKey = continuationSessionKey
	tasks := al.taskRegistryForWorkspace(agent.Workspace)
	if err := tasks.Upsert(taskregistry.Record{
		TaskID: "task-concurrent-answer", Runtime: taskregistry.RuntimeSubagent,
		TaskKind: "spawn", Task: "complete after input", Status: taskregistry.StatusRunning,
		DeliveryStatus: taskregistry.DeliveryPending,
		DeliveryMode:   string(tools.AsyncDeliveryUserOnly),
		InteractionID:  "interaction-task-concurrent-answer",
		Channel:        request.Route.Channel, ChatID: request.Route.ChatID,
		RequesterSessionKey: sessionKey,
	}); err != nil {
		t.Fatal(err)
	}
	registry := al.interactionRegistryForWorkspace(agent.Workspace)
	record, err := registry.Create(interactions.CreateRequest{
		ID:   "interaction-task-concurrent-answer",
		Kind: request.Prompt.Kind, Route: request.Route, Origin: request.Origin,
		Questions: request.Prompt.Questions, ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	record, _ = registry.RecordDeliveryAttempt(record.ID, record.Revision, true, "")
	record, _ = registry.MarkWaiting(record.ID, record.Revision)
	target := &inboundDispatchTarget{
		Agent:         agent,
		SessionKey:    sessionKey,
		RouteClaimKey: runtimeRouteClaimKey(request.Route.RouteSessionKey, ""),
		Allocation:    session.Allocation{RouteScopeKey: request.Route.RouteSessionKey},
	}
	first := bus.InboundMessage{
		Content: "/answer " + record.ShortID + " первый", SpoolID: "spool-task-first",
		Context: inboundContextForInteraction(request.Route),
	}
	first.Context.MessageID = "task-answer-first"
	coordinator := newInboundTurnCoordinator(al)
	coordinator.routeExplicitInteractionAnswer(t.Context(), first, target)
	select {
	case <-provider.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for task continuation")
	}
	second := bus.InboundMessage{
		Content: "/answer " + record.ShortID + " второй", SpoolID: "spool-task-second",
		Context: inboundContextForInteraction(request.Route),
	}
	second.Context.MessageID = "task-answer-second"
	coordinator.routeExplicitInteractionAnswer(t.Context(), second, target)
	deadline := time.Now().Add(2 * time.Second)
	for {
		acked, _ := tracker.counts()
		if acked == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for task answer acknowledgements")
		}
		time.Sleep(time.Millisecond)
	}
	if got := al.pendingSteeringCountForScope(
		newRuntimeSessionScope(agent.Workspace, sessionKey),
	); got != 0 {
		t.Fatalf("task losing answer entered steering queue: %d", got)
	}
	close(provider.release)
	deadline = time.Now().Add(2 * time.Second)
	for {
		record, _ = registry.Get(record.ID)
		task, _ := tasks.Get("task-concurrent-answer")
		if record.Status == interactions.StatusResolved && task.Status == taskregistry.StatusSucceeded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("task interaction did not complete once: record=%#v task=%#v", record, task)
		}
		time.Sleep(time.Millisecond)
	}
	if record.Answer == nil || record.Answer.Text != "первый" ||
		record.ResumeTries != 1 || record.FinalDeliveryTries != 1 {
		t.Fatalf("task interaction winner/counts = %#v", record)
	}
	if calls, _ := provider.snapshot(); calls != 1 {
		t.Fatalf("task provider calls = %d, want 1", calls)
	}
	for _, message := range agent.Sessions.GetHistory(continuationSessionKey) {
		if strings.Contains(message.Content, "второй") || strings.Contains(message.Content, "/answer") {
			t.Fatalf("task losing answer entered continuation history: %#v", message)
		}
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
		Agent:         agent,
		SessionKey:    sessionKey,
		RouteClaimKey: runtimeRouteClaimKey(request.Route.RouteSessionKey, ""),
		Allocation:    session.Allocation{RouteScopeKey: request.Route.RouteSessionKey},
	}
	msg := bus.InboundMessage{Content: "/stop", Context: inboundContextForInteraction(request.Route)}
	cancellation, err := al.cancelInteractionForControlMessage(t.Context(), msg, target)
	if err != nil {
		t.Fatal(err)
	}
	if !cancellation.Matched || !cancellation.Canceled ||
		cancellation.Failed || !cancellation.CommandHandled {
		t.Fatalf("stop cancellation result = %#v", cancellation)
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

func TestWaitingForegroundInteractionStopUsesSuccessfulStopContract(t *testing.T) {
	provider := &sequenceProvider{}
	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()
	msg := testInboundMessage(bus.InboundMessage{
		Content:    "/stop",
		SessionKey: session.BuildOpaqueSessionKey("agent:main:test:interaction-stop"),
		Context: bus.InboundContext{
			Channel: "telegram", Account: "primary", ChatID: "chat-1", ChatType: "direct",
			TopicID: "topic-1", SenderID: "user-1", MessageID: "stop-1",
		},
	})
	record, _ := prepareWaitingControlInteraction(t, al, agent, msg, "")

	newInboundTurnCoordinator(al).handleInbound(t.Context(), msg)

	messageBus := al.bus.(*bus.MessageBus)
	select {
	case outbound := <-messageBus.OutboundChan():
		want := "Task stopped. Current task was canceled."
		if outbound.Content != want {
			t.Fatalf("stop reply = %q, want %q", outbound.Content, want)
		}
		if strings.Contains(outbound.Content, "No active task to stop.") {
			t.Fatalf("stop reply used inactive-task contract: %q", outbound.Content)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for interaction /stop reply")
	}

	record, _ = al.interactionRegistryForWorkspace(agent.Workspace).Get(record.ID)
	if record.Status != interactions.StatusCancelled ||
		record.FailureCode != "session_control_stop" {
		t.Fatalf("stopped interaction = %#v", record)
	}
	history := agent.Sessions.GetHistory(record.Route.SessionKey)
	if got := countInteractionToolResults(history, record.Origin.ToolCallID); got != 1 {
		t.Fatalf("cancellation tool results = %d, want 1", got)
	}
	provider.mu.Lock()
	calls := provider.callCount
	provider.mu.Unlock()
	if calls != 0 {
		t.Fatalf("model calls after interaction stop = %d, want 0", calls)
	}
}

func TestWaitingTaskInteractionStopTerminalizesTaskWithoutDelivery(t *testing.T) {
	provider := &sequenceProvider{}
	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()
	msg := testInboundMessage(bus.InboundMessage{
		Content:    "/stop",
		SessionKey: session.BuildOpaqueSessionKey("agent:main:test:task-interaction-stop"),
		Context: bus.InboundContext{
			Channel: "telegram", Account: "primary", ChatID: "chat-task", ChatType: "direct",
			TopicID: "topic-task", SenderID: "user-task", MessageID: "stop-task",
		},
	})
	tasks := al.taskRegistryForWorkspace(agent.Workspace)
	if err := tasks.Upsert(taskregistry.Record{
		TaskID: "task-waiting-stop", Runtime: taskregistry.RuntimeSubagent,
		Status: taskregistry.StatusRunning, DeliveryStatus: taskregistry.DeliveryPending,
	}); err != nil {
		t.Fatal(err)
	}
	record, _ := prepareWaitingControlInteraction(t, al, agent, msg, "task-waiting-stop")
	task, _ := tasks.Get("task-waiting-stop")
	if task.Status != taskregistry.StatusWaitingForInput {
		t.Fatalf("task before stop = %#v", task)
	}

	newInboundTurnCoordinator(al).handleInbound(t.Context(), msg)
	select {
	case outbound := <-al.bus.(*bus.MessageBus).OutboundChan():
		if outbound.Content != "Task stopped. Current task was canceled." {
			t.Fatalf("task stop reply = %q", outbound.Content)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for task interaction /stop reply")
	}

	task, _ = tasks.Get("task-waiting-stop")
	if task.Status != taskregistry.StatusCancelled ||
		task.DeliveryStatus != taskregistry.DeliveryNotApplicable ||
		task.EndedAt == 0 || tasks.Stats().ProtectedTaskCount != 0 {
		t.Fatalf("task after stop = %#v, stats=%#v", task, tasks.Stats())
	}
	record, _ = al.interactionRegistryForWorkspace(agent.Workspace).Get(record.ID)
	if record.Status != interactions.StatusCancelled {
		t.Fatalf("task interaction after stop = %#v", record)
	}
	provider.mu.Lock()
	calls := provider.callCount
	provider.mu.Unlock()
	if calls != 0 {
		t.Fatalf("task continuation model calls after stop = %d, want 0", calls)
	}
	select {
	case outbound := <-al.bus.(*bus.MessageBus).OutboundChan():
		t.Fatalf("unexpected task completion after cancellation: %#v", outbound)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestTaskBoundInteractionCancellationReturnsTaskID(t *testing.T) {
	al, agent, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
	defer cleanup()
	msg := testInboundMessage(bus.InboundMessage{
		Content:    "/stop",
		SessionKey: session.BuildOpaqueSessionKey("agent:main:test:task-id-stop"),
		Context: bus.InboundContext{
			Channel: "telegram", ChatID: "chat-task", ChatType: "direct",
			SenderID: "user-task",
		},
	})
	tasks := al.taskRegistryForWorkspace(agent.Workspace)
	if err := tasks.Upsert(taskregistry.Record{
		TaskID: "task-associated", Status: taskregistry.StatusRunning,
		DeliveryStatus: taskregistry.DeliveryPending,
	}); err != nil {
		t.Fatal(err)
	}
	_, target := prepareWaitingControlInteraction(t, al, agent, msg, "task-associated")

	result, err := al.cancelInteractionForControlMessage(t.Context(), msg, target)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Matched || !result.Canceled || result.Failed ||
		!result.CommandHandled || result.TaskID != "task-associated" {
		t.Fatalf("task-bound cancellation result = %#v", result)
	}
}

func TestInteractionControlCancellationReportsFailure(t *testing.T) {
	al, agent, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
	defer cleanup()
	msg := testInboundMessage(bus.InboundMessage{
		Content:    "/stop",
		SessionKey: session.BuildOpaqueSessionKey("agent:main:test:failed-interaction-stop"),
		Context: bus.InboundContext{
			Channel: "telegram", ChatID: "chat-1", ChatType: "direct", SenderID: "user-1",
		},
	})
	record, target := prepareWaitingControlInteraction(t, al, agent, msg, "")
	agent.Sessions.SetHistory(record.Route.SessionKey, nil)

	result, err := al.cancelInteractionForControlMessage(t.Context(), msg, target)
	if err == nil {
		t.Fatal("cancellation succeeded without the originating tool call")
	}
	if !result.Matched || result.Canceled || !result.Failed || result.CommandHandled {
		t.Fatalf("failed cancellation result = %#v", result)
	}
	record, _ = al.interactionRegistryForWorkspace(agent.Workspace).Get(record.ID)
	if record.Status != interactions.StatusCanceling ||
		record.FailureCode != "session_control_stop" {
		t.Fatalf("interaction after interrupted cancellation = %#v", record)
	}
}

func TestRepeatedStopDoesNotDuplicateInteractionCancellation(t *testing.T) {
	provider := &sequenceProvider{}
	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()
	msg := testInboundMessage(bus.InboundMessage{
		Content:    "/stop",
		SessionKey: session.BuildOpaqueSessionKey("agent:main:test:repeated-interaction-stop"),
		Context: bus.InboundContext{
			Channel: "telegram", ChatID: "chat-1", ChatType: "direct",
			SenderID: "user-1", MessageID: "stop-first",
		},
	})
	record, _ := prepareWaitingControlInteraction(t, al, agent, msg, "")
	coordinator := newInboundTurnCoordinator(al)
	coordinator.handleInbound(t.Context(), msg)
	select {
	case <-al.bus.(*bus.MessageBus).OutboundChan():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first stop reply")
	}

	msg.Context.MessageID = "stop-second"
	coordinator.handleInbound(t.Context(), msg)
	select {
	case outbound := <-al.bus.(*bus.MessageBus).OutboundChan():
		if outbound.Content != "No active task to stop." {
			t.Fatalf("repeated stop reply = %q", outbound.Content)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for repeated stop reply")
	}

	history := agent.Sessions.GetHistory(record.Route.SessionKey)
	if got := countInteractionToolResults(history, record.Origin.ToolCallID); got != 1 {
		t.Fatalf("repeated stop cancellation tool results = %d, want 1", got)
	}
	provider.mu.Lock()
	calls := provider.callCount
	provider.mu.Unlock()
	if calls != 0 {
		t.Fatalf("model calls after repeated stop = %d, want 0", calls)
	}
}

func TestInteractionControlCancellationRequiresAuthorizedRoute(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*bus.InboundMessage)
	}{
		{name: "channel", mutate: func(msg *bus.InboundMessage) { msg.Context.Channel = "discord" }},
		{name: "account", mutate: func(msg *bus.InboundMessage) { msg.Context.Account = "secondary" }},
		{name: "sender", mutate: func(msg *bus.InboundMessage) { msg.Context.SenderID = "other-user" }},
		{name: "chat", mutate: func(msg *bus.InboundMessage) { msg.Context.ChatID = "other-chat" }},
		{name: "topic", mutate: func(msg *bus.InboundMessage) { msg.Context.TopicID = "other-topic" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			al, agent, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
			defer cleanup()
			msg := testInboundMessage(bus.InboundMessage{
				Content:    "/stop",
				SessionKey: session.BuildOpaqueSessionKey("agent:main:test:unauthorized-" + tt.name),
				Context: bus.InboundContext{
					Channel: "telegram", Account: "primary", ChatID: "chat-1", ChatType: "direct",
					TopicID: "topic-1", SenderID: "user-1",
				},
			})
			record, target := prepareWaitingControlInteraction(t, al, agent, msg, "")
			tt.mutate(&msg)

			result, err := al.cancelInteractionForControlMessage(t.Context(), msg, target)
			if err != nil {
				t.Fatal(err)
			}
			if result.Matched || result.Canceled || result.Failed || result.CommandHandled {
				t.Fatalf("unauthorized cancellation result = %#v", result)
			}
			record, _ = al.interactionRegistryForWorkspace(agent.Workspace).Get(record.ID)
			if record.Status != interactions.StatusWaiting {
				t.Fatalf("unauthorized route canceled interaction: %#v", record)
			}
		})
	}
}

func TestSessionControlCommandsCancelInteractionAndContinueNormally(t *testing.T) {
	tests := []struct {
		command   string
		replyText string
	}{
		{command: "/new", replyText: "Started a fresh session and cleared the current goal."},
		{command: "/reset", replyText: "Started a fresh session."},
		{command: "/clear", replyText: "Chat history cleared!"},
	}
	for _, tt := range tests {
		t.Run(tt.command, func(t *testing.T) {
			provider := &sequenceProvider{}
			al, agent, cleanup := newTurnCoordTestLoop(t, provider)
			defer cleanup()
			msg := testInboundMessage(bus.InboundMessage{
				Content:    tt.command,
				SessionKey: session.BuildOpaqueSessionKey("agent:main:test:control-" + tt.command[1:]),
				Context: bus.InboundContext{
					Channel: "telegram", Account: "primary", ChatID: "chat-1", ChatType: "direct",
					TopicID: "topic-1", SenderID: "user-1",
				},
			})
			record, _ := prepareWaitingControlInteraction(t, al, agent, msg, "")

			newInboundTurnCoordinator(al).handleInbound(t.Context(), msg)
			select {
			case outbound := <-al.bus.(*bus.MessageBus).OutboundChan():
				if !strings.Contains(outbound.Content, tt.replyText) {
					t.Fatalf("%s reply = %q", tt.command, outbound.Content)
				}
				if strings.Contains(outbound.Content, "Task stopped.") {
					t.Fatalf("%s emitted an extra stop acknowledgement: %q", tt.command, outbound.Content)
				}
			case <-time.After(time.Second):
				t.Fatalf("timed out waiting for %s reply", tt.command)
			}

			record, _ = al.interactionRegistryForWorkspace(agent.Workspace).Get(record.ID)
			wantCode := "session_control_" + tt.command[1:]
			if record.Status != interactions.StatusCancelled || record.FailureCode != wantCode {
				t.Fatalf("%s interaction = %#v", tt.command, record)
			}
			provider.mu.Lock()
			calls := provider.callCount
			provider.mu.Unlock()
			if calls != 0 {
				t.Fatalf("%s model calls = %d, want 0", tt.command, calls)
			}
		})
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
	scope := newRuntimeSessionScope(agent.Workspace, sessionKey)
	if got := al.pendingSteeringCountForScope(scope); got != 1 {
		t.Fatalf("deferred queue depth = %d, want 1", got)
	}
	history := agent.Sessions.GetHistory(sessionKey)
	if len(history) != 1 || history[0].Content != "existing" {
		t.Fatalf("deferred ingress changed history: %#v", history)
	}
	queued := al.dequeueSteeringMessagesForTurn(scope, "user-2")
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
	scope := newRuntimeSessionScope(agent.Workspace, sessionKey)
	if err := al.enqueueSteeringMessageWithSender(scope, agent.ID, "user-2", providers.Message{
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
	if got := al.pendingSteeringCountForScope(scope); got != 0 {
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

func TestInteractionFinalAfterToolResultDoesNotDuplicateHandledAttachment(t *testing.T) {
	history := []providers.Message{
		{Role: "assistant", ToolCalls: []providers.ToolCall{{ID: "call-media"}}},
		{Role: "tool", ToolCallID: "call-media", Content: "delivered"},
		{
			Role:    "assistant",
			Content: handledToolResponseSummary,
			Attachments: []providers.Attachment{{
				Ref: "media://delivered",
			}},
		},
	}
	if content, ok := interactionFinalAfterToolResult(history, "call-media"); !ok || content != "" {
		t.Fatalf("interactionFinalAfterToolResult() = (%q, %v), want empty handled final", content, ok)
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
	scope := newRuntimeSessionScope(agent.Workspace, sessionKey)
	if err = al.enqueueSteeringMessageWithSender(scope, agent.ID, "user-2", providers.Message{
		Role: "user", Content: "message that arrived during suspension",
	}); err != nil {
		t.Fatal(err)
	}

	continued, err := al.drainQueuedSteeringContinuations(t.Context(), &continuationTarget{
		AgentID:    agent.ID,
		SessionKey: sessionKey,
		Channel:    "telegram",
		ChatID:     "chat-1",
		Workspace:  agent.Workspace,
	})
	if err != nil || continued != "" {
		t.Fatalf("drain = (%q, %v), want empty success", continued, err)
	}
	if got := al.pendingSteeringCountForScope(scope); got != 1 {
		t.Fatalf("deferred queue depth = %d, want 1", got)
	}
}

func TestDrainDeferredInteractionIngressReleasesSteeringAfterAggregateRejection(t *testing.T) {
	rejection := errors.New("deferred aggregate rejected")
	al, agent, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
	defer cleanup()
	trackingBus := &finalResponseAdmissionTestBus{
		MessageBus: al.bus.(*bus.MessageBus),
		publishErr: rejection,
	}
	al.bus = trackingBus
	sessionKey := "session-deferred-admission"
	if err := al.enqueueSteeringMessageWithSender(
		newRuntimeSessionScope(agent.Workspace, sessionKey),
		agent.ID,
		"user-2",
		providers.Message{
			Role:           "user",
			Content:        "deferred interaction follow-up",
			InboundSpoolID: "spool-deferred",
		},
	); err != nil {
		t.Fatal(err)
	}

	err := al.drainDeferredInteractionIngress(t.Context(), agent.Workspace, interactions.Route{
		AgentID: agent.ID, SessionKey: sessionKey, Channel: "telegram", ChatID: "chat-1",
	}, bus.InboundContext{Channel: "telegram", ChatID: "chat-1"})

	if !errors.Is(err, rejection) {
		t.Fatalf("drainDeferredInteractionIngress() error = %v, want %v", err, rejection)
	}
	acked, released, cause := trackingBus.ownership()
	if len(acked) != 0 || !containsExactly(released, "spool-deferred") {
		t.Fatalf("deferred ownership = acked:%v released:%v", acked, released)
	}
	if !errors.Is(cause, rejection) {
		t.Fatalf("release cause = %v, want %v", cause, rejection)
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
