package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/interactions"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/session"
)

type interactionChannelManager struct {
	*recordingChannelManager
	sent    chan bus.OutboundMessage
	sendErr error
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
	if err := al.resumeClaimedInteraction(t.Context(), agent, scope, inbound, record); err != nil {
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
