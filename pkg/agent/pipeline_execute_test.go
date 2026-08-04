package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/config"
	runtimeevents "github.com/bogdanovich/mintclaw/pkg/events"
	"github.com/bogdanovich/mintclaw/pkg/interactions"
	"github.com/bogdanovich/mintclaw/pkg/providers"
	"github.com/bogdanovich/mintclaw/pkg/routing"
	"github.com/bogdanovich/mintclaw/pkg/session"
	"github.com/bogdanovich/mintclaw/pkg/tools"
	"github.com/bogdanovich/mintclaw/pkg/tools/loopguard"
)

type pipelineLoopGuardTool struct {
	executions int
}

type toolResultFailingJournal struct {
	session.SessionStore
	err error
}

type fixedToolResultTool struct {
	name       string
	result     *tools.ToolResult
	executions int
}

type boundApprovalSuspensionTool struct {
	executions       int
	preparationCalls int
	continued        bool
}

func (*boundApprovalSuspensionTool) Name() string { return "bound_approval" }
func (*boundApprovalSuspensionTool) Description() string {
	return "request approval bound to trusted prepared authority"
}

func (*boundApprovalSuspensionTool) Parameters() map[string]any {
	return map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"value": map[string]any{"type": "string"}},
		"required":             []string{"value"},
		"additionalProperties": false,
	}
}

func (tool *boundApprovalSuspensionTool) ApprovalArguments(
	_ context.Context,
	_ map[string]any,
) (map[string]any, error) {
	tool.preparationCalls++
	return map[string]any{"prepared_action_id": "prepared_1", "action_hash": "trusted_hash"}, nil
}

func (tool *boundApprovalSuspensionTool) Execute(ctx context.Context, _ map[string]any) *tools.ToolResult {
	tool.executions++
	if tools.ToolApprovalContinuation(ctx) {
		tool.continued = true
		return tools.NewToolResult("approved")
	}
	return &tools.ToolResult{Silent: true, Suspension: &tools.SuspensionRequest{
		Kind: interactions.KindApproval, PromptSummary: "Publish the prepared browser action", Timeout: time.Minute,
	}}
}

func (t *fixedToolResultTool) Name() string        { return t.name }
func (t *fixedToolResultTool) Description() string { return "fixed tool result" }
func (t *fixedToolResultTool) Parameters() map[string]any {
	return map[string]any{"type": "object"}
}

func (t *fixedToolResultTool) Execute(context.Context, map[string]any) *tools.ToolResult {
	t.executions++
	return t.result
}

type recordingToolResultDelivery struct {
	outboundCalls int
	syncCalls     int
}

func (d *recordingToolResultDelivery) PublishOutbound(context.Context, bus.OutboundMessage) error {
	d.outboundCalls++
	return nil
}

func (*recordingToolResultDelivery) GetStreamer(
	context.Context,
	string,
	string,
	string,
	string,
	runtimeevents.TraceScope,
) (bus.Streamer, bool) {
	return nil, false
}

func (d *recordingToolResultDelivery) applySyncToolResultDelivery(
	_ context.Context,
	_ *turnState,
	result *tools.ToolResult,
	_ string,
) ([]providers.Attachment, *tools.ToolResult) {
	if result != nil && (result.ResponseHandled || result.ImmediateDelivery) {
		d.syncCalls++
	}
	return nil, result
}

type toolResultRespondHook struct {
	result *tools.ToolResult
}

func (h *toolResultRespondHook) BeforeLLM(
	_ context.Context,
	req *LLMHookRequest,
) (*LLMHookRequest, HookDecision) {
	return req, HookDecision{Action: HookActionContinue}
}

func (h *toolResultRespondHook) AfterLLM(
	_ context.Context,
	resp *LLMHookResponse,
) (*LLMHookResponse, HookDecision) {
	return resp, HookDecision{Action: HookActionContinue}
}

func (h *toolResultRespondHook) BeforeTool(
	_ context.Context,
	req *ToolCallHookRequest,
) (*ToolCallHookRequest, HookDecision) {
	next := req.Clone()
	next.HookResult = h.result
	return next, HookDecision{Action: HookActionRespond}
}

func (h *toolResultRespondHook) AfterTool(
	_ context.Context,
	resp *ToolResultHookResponse,
) (*ToolResultHookResponse, HookDecision) {
	return resp, HookDecision{Action: HookActionContinue}
}

func (*toolResultRespondHook) ApproveTool(context.Context, *ToolApprovalRequest) ApprovalDecision {
	return ApprovalDecision{Approved: true}
}

func (s *toolResultFailingJournal) AppendTurnMessage(
	ctx context.Context,
	sessionKey string,
	msg providers.Message,
) error {
	if msg.Role == "tool" {
		return s.err
	}
	return s.SessionStore.AppendTurnMessage(ctx, sessionKey, msg)
}

type fakeToolSuspensionManager struct {
	requests     []ToolSuspensionRequest
	consumptions []ToolApprovalConsumptionRequest
	disposition  ToolSuspensionDisposition
	err          error
}

func (m *fakeToolSuspensionManager) SuspendToolCall(
	_ context.Context,
	request ToolSuspensionRequest,
) (ToolSuspensionDisposition, error) {
	m.requests = append(m.requests, request)
	return m.disposition, m.err
}

func (m *fakeToolSuspensionManager) ConsumeApproval(
	_ context.Context,
	request ToolApprovalConsumptionRequest,
) error {
	m.consumptions = append(m.consumptions, request)
	return nil
}

func TestToolResultContextStatus(t *testing.T) {
	tests := []struct {
		name   string
		result *tools.ToolResult
		want   providers.ToolResultStatus
	}{
		{name: "success", result: tools.NewToolResult("ok"), want: providers.ToolResultStatusSuccess},
		{name: "error", result: tools.ErrorResult("failed"), want: providers.ToolResultStatusError},
		{
			name:   "async unresolved",
			result: &tools.ToolResult{ForLLM: "started", Async: true},
			want:   providers.ToolResultStatusUnresolved,
		},
		{name: "nil unresolved", want: providers.ToolResultStatusUnresolved},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := toolResultContextStatus(test.result); got != test.want {
				t.Fatalf("status = %q, want %q", got, test.want)
			}
		})
	}
}

func TestPipelineToolResultJournalFailureLeavesDurableUnresolvedIntent(t *testing.T) {
	registry := tools.NewToolRegistry()
	tool := &steeringSafetyTestTool{name: "side-effect", safety: tools.SteeringSafetyNonCancellable}
	registry.Register(tool)
	baseStore := session.NewSessionManager("")
	journalErr := errors.New("tool result fsync failed")
	store := &toolResultFailingJournal{SessionStore: baseStore, err: journalErr}
	agent := &AgentInstance{ID: "main", Tools: registry, Sessions: store}
	ts := &turnState{
		agent: agent, agentID: agent.ID, turnID: "turn-journal-failure",
		sessionKey: "session-journal-failure",
		opts:       processOptions{Dispatch: DispatchRequest{SessionKey: "session-journal-failure"}},
	}
	toolCall := providers.ToolCall{ID: "call-side-effect", Name: tool.Name(), Arguments: map[string]any{}}
	intent := providers.Message{Role: "assistant", ToolCalls: []providers.ToolCall{toolCall}}
	if err := store.AppendTurnMessage(t.Context(), ts.sessionKey, intent); err != nil {
		t.Fatalf("persist tool intent: %v", err)
	}
	exec := newTurnExecution(agent, ts.opts, nil, "", nil)
	exec.messages = []providers.Message{intent}
	exec.normalizedToolCalls = []providers.ToolCall{toolCall}
	exec.assistantToolCallsPersisted = true

	outcome := (&Pipeline{}).ExecuteTools(t.Context(), t.Context(), ts, exec, 1)

	if tool.executions != 1 {
		t.Fatalf("tool executions = %d, want 1", tool.executions)
	}
	if !errors.Is(outcome.JournalErr, journalErr) {
		t.Fatalf("journal error = %v, want %v", outcome.JournalErr, journalErr)
	}
	history := baseStore.GetHistory(ts.sessionKey)
	if len(history) != 1 || history[0].Role != "assistant" || len(history[0].ToolCalls) != 1 {
		t.Fatalf("durable history = %+v, want unresolved assistant tool intent", history)
	}
}

func TestPipelineToolResultJournalFailurePreventsEveryDeliveryMode(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result func() *tools.ToolResult
		hook   bool
	}{
		{
			name: "normal for-user",
			result: func() *tools.ToolResult {
				return &tools.ToolResult{ForLLM: "normal result", ForUser: "normal delivery"}
			},
		},
		{
			name: "response handled",
			result: func() *tools.ToolResult {
				return (&tools.ToolResult{ForLLM: "handled result", ForUser: "handled delivery"}).WithResponseHandled()
			},
		},
		{
			name: "immediate delivery",
			result: func() *tools.ToolResult {
				return (&tools.ToolResult{ForLLM: "immediate result", ForUser: "immediate delivery"}).WithImmediateDelivery()
			},
		},
		{
			name: "hook response",
			result: func() *tools.ToolResult {
				return &tools.ToolResult{ForLLM: "hook result", ForUser: "hook delivery"}
			},
			hook: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := tools.NewToolRegistry()
			tool := &fixedToolResultTool{name: "delivery-test", result: tc.result()}
			registry.Register(tool)
			baseStore := session.NewSessionManager("")
			journalErr := errors.New("tool result journal failed")
			store := &toolResultFailingJournal{SessionStore: baseStore, err: journalErr}
			agent := &AgentInstance{ID: "main", Tools: registry, Sessions: store}
			ts := &turnState{
				agent: agent, agentID: agent.ID, turnID: "turn-delivery-failure",
				sessionKey: "session-delivery-failure",
				opts: processOptions{
					SendResponse: true,
					Dispatch:     DispatchRequest{SessionKey: "session-delivery-failure"},
				},
			}
			toolCall := providers.ToolCall{ID: "call-delivery", Name: tool.Name(), Arguments: map[string]any{}}
			intent := providers.Message{Role: "assistant", ToolCalls: []providers.ToolCall{toolCall}}
			if err := store.AppendTurnMessage(t.Context(), ts.sessionKey, intent); err != nil {
				t.Fatal(err)
			}
			exec := newTurnExecution(agent, ts.opts, nil, "", []providers.Message{intent})
			exec.normalizedToolCalls = []providers.ToolCall{toolCall}
			exec.assistantToolCallsPersisted = true
			delivery := &recordingToolResultDelivery{}
			pipeline := &Pipeline{
				Runtime: PipelineRuntimeServices{Bus: delivery},
				Interaction: PipelineInteractionServices{
					SyncToolDelivery: delivery,
				},
			}
			if tc.hook {
				pipeline.Interaction.Hooks = &toolResultRespondHook{result: tc.result()}
			}

			outcome := pipeline.ExecuteTools(t.Context(), t.Context(), ts, exec, 1)

			if !errors.Is(outcome.JournalErr, journalErr) {
				t.Fatalf("journal error = %v, want %v", outcome.JournalErr, journalErr)
			}
			if delivery.outboundCalls != 0 || delivery.syncCalls != 0 {
				t.Fatalf(
					"delivery calls = outbound:%d sync:%d, want none",
					delivery.outboundCalls,
					delivery.syncCalls,
				)
			}
			if tc.hook && tool.executions != 0 {
				t.Fatalf("hook-response tool executions = %d, want 0", tool.executions)
			}
			history := baseStore.GetHistory(ts.sessionKey)
			if len(history) != 1 || history[0].Role != "assistant" || len(history[0].ToolCalls) != 1 {
				t.Fatalf("durable history = %+v, want unresolved assistant intent", history)
			}
		})
	}
}

func TestPipelineDeliveryOnlyArtifactStaysOutOfProviderHistory(t *testing.T) {
	const (
		mediaRef = "media://private-browser-screenshot"
		hostPath = "/private/workspace/state/media/browser-screenshot.png"
	)
	store := session.NewSessionManager("")
	commitCalls := 0
	result := tools.NewToolResult(`{"artifact":{"ref":"transfer-artifact://opaque"}}`).
		WithOutboundDelivery(tools.OutboundDelivery{
			Media: []bus.MediaPart{{
				Type: "image", Ref: mediaRef, Filename: "browser-screenshot.png", ContentType: "image/png",
			}},
			Recovery: &bus.OutboundRecovery{
				Kind:        bus.OutboundRecoveryBrowserScreenshot,
				ArtifactRef: "transfer-artifact://opaque", MediaRef: mediaRef,
				WorkspaceID: "private_workspace", AgentID: "browser", ActorID: "private_actor",
				RouteID: "private_route", SessionID: "private_session", ToolCallID: "private_call",
			},
		}).
		WithOutboundCommit(func(context.Context) error {
			commitCalls++
			return nil
		}).
		WithImmediateDelivery()
	tool := &fixedToolResultTool{name: "browser_observe", result: result}
	registry := tools.NewToolRegistry()
	registry.Register(tool)
	agent := &AgentInstance{ID: "browser", Tools: registry, Sessions: store}
	ts := &turnState{
		agent: agent, agentID: agent.ID, turnID: "turn-browser-screenshot",
		sessionKey: "session-browser-screenshot",
		opts: processOptions{
			SendResponse: true,
			Dispatch:     DispatchRequest{SessionKey: "session-browser-screenshot"},
		},
	}
	toolCall := providers.ToolCall{ID: "call-browser", Name: tool.Name(), Arguments: map[string]any{}}
	intent := providers.Message{Role: "assistant", ToolCalls: []providers.ToolCall{toolCall}}
	if err := store.AppendTurnMessage(t.Context(), ts.sessionKey, intent); err != nil {
		t.Fatal(err)
	}
	exec := newTurnExecution(agent, ts.opts, nil, "", []providers.Message{intent})
	exec.normalizedToolCalls = []providers.ToolCall{toolCall}
	exec.assistantToolCallsPersisted = true
	deliveryCalls := 0
	delivery := &syncToolResultDelivery{deliverToUser: func(
		deliveryCtx context.Context,
		_ *turnState,
		got *tools.ToolResult,
		_ string,
	) ([]providers.Attachment, toolResultDeliveryOutcome, error) {
		deliveryCalls++
		journaled := store.GetHistory(ts.sessionKey)
		if commitCalls != 0 || got.Outbound == nil || got.Outbound.Recovery == nil ||
			len(got.Outbound.Media) != 1 ||
			got.Outbound.Media[0].Ref != mediaRef || len(got.Media) != 0 ||
			len(got.ArtifactTags) != 0 || len(journaled) != 2 || journaled[1].Role != "tool" {
			t.Fatalf(
				"delivery result = %#v; commit calls = %d; journaled = %#v",
				got, commitCalls, journaled,
			)
		}
		if err := commitToolResultOutbound(deliveryCtx, got); err != nil {
			return nil, toolResultDeliveryNone, err
		}
		return nil, toolResultDeliveryQueued, nil
	}}
	pipeline := &Pipeline{Interaction: PipelineInteractionServices{SyncToolDelivery: delivery}}
	pipeline.ExecuteTools(t.Context(), t.Context(), ts, exec, 1)

	history := store.GetHistory(ts.sessionKey)
	if deliveryCalls != 1 || commitCalls != 1 || len(history) != 2 ||
		history[1].Role != "tool" || len(history[1].Media) != 0 ||
		strings.Contains(history[1].Content, mediaRef) || strings.Contains(history[1].Content, hostPath) ||
		strings.Contains(history[1].Content, "private_workspace") {
		t.Fatalf(
			"delivery calls = %d, commit calls = %d, history = %#v",
			deliveryCalls, commitCalls, history,
		)
	}
}

func TestPipelineSuppressedToolDeliveryRetainsHandledAndImmediateMedia(t *testing.T) {
	for _, tc := range []struct {
		name      string
		immediate bool
		hook      bool
	}{
		{name: "normal handled"},
		{name: "normal immediate", immediate: true},
		{name: "hook handled", hook: true},
		{name: "hook immediate", immediate: true, hook: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := (&tools.ToolResult{
				ForLLM:  "media result",
				ForUser: "media delivery",
				Media:   []string{"media://suppressed-result"},
			}).WithResponseHandled()
			if tc.immediate {
				result = (&tools.ToolResult{
					ForLLM:  "media result",
					ForUser: "media delivery",
					Media:   []string{"media://suppressed-result"},
				}).WithImmediateDelivery()
			}

			registry := tools.NewToolRegistry()
			tool := &fixedToolResultTool{name: "suppressed-media", result: result}
			registry.Register(tool)
			store := session.NewSessionManager("")
			agent := &AgentInstance{ID: "main", Tools: registry, Sessions: store}
			ts := &turnState{
				agent: agent, agentID: agent.ID, turnID: "turn-suppressed-media",
				sessionKey: "session-suppressed-media",
				opts: processOptions{
					SuppressToolUserDelivery: true,
					SendResponse:             true,
					Dispatch:                 DispatchRequest{SessionKey: "session-suppressed-media"},
				},
			}
			toolCall := providers.ToolCall{ID: "call-media", Name: tool.Name(), Arguments: map[string]any{}}
			intent := providers.Message{Role: "assistant", ToolCalls: []providers.ToolCall{toolCall}}
			if err := store.AppendTurnMessage(t.Context(), ts.sessionKey, intent); err != nil {
				t.Fatal(err)
			}
			exec := newTurnExecution(agent, ts.opts, nil, "", []providers.Message{intent})
			exec.normalizedToolCalls = []providers.ToolCall{toolCall}
			exec.assistantToolCallsPersisted = true
			delivery := &recordingToolResultDelivery{}
			pipeline := &Pipeline{
				Runtime: PipelineRuntimeServices{Bus: delivery},
				Interaction: PipelineInteractionServices{
					SyncToolDelivery: delivery,
				},
			}
			if tc.hook {
				pipeline.Interaction.Hooks = &toolResultRespondHook{result: result}
			}

			outcome := pipeline.ExecuteTools(t.Context(), t.Context(), ts, exec, 1)

			if outcome.JournalErr != nil {
				t.Fatalf("journal error = %v", outcome.JournalErr)
			}
			if delivery.outboundCalls != 0 || delivery.syncCalls != 0 {
				t.Fatalf(
					"delivery calls = outbound:%d sync:%d, want none",
					delivery.outboundCalls,
					delivery.syncCalls,
				)
			}
			if len(exec.completionMedia) != 1 || exec.completionMedia[0].Ref != "media://suppressed-result" {
				t.Fatalf("completion media = %+v, want suppressed result", exec.completionMedia)
			}
			history := store.GetHistory(ts.sessionKey)
			if len(history) != 2 || len(history[1].Media) != 1 ||
				history[1].Media[0] != "media://suppressed-result" {
				t.Fatalf("durable history = %+v, want tool media", history)
			}
		})
	}
}

func TestPipelineToolCallIntentJournalFailurePreventsExecution(t *testing.T) {
	registry := tools.NewToolRegistry()
	tool := &steeringSafetyTestTool{name: "must-not-run", safety: tools.SteeringSafetyNonCancellable}
	registry.Register(tool)
	agent := &AgentInstance{ID: "main", Tools: registry, Sessions: session.NewSessionManager("")}
	ts := &turnState{agent: agent, opts: processOptions{Dispatch: DispatchRequest{SessionKey: "intent-fail"}}}
	exec := newTurnExecution(agent, ts.opts, nil, "", nil)
	exec.normalizedToolCalls = []providers.ToolCall{{ID: "call-1", Name: tool.Name()}}
	exec.assistantToolCallsWriteErr = errors.New("assistant intent rename failed")

	outcome := (&Pipeline{}).ExecuteTools(t.Context(), t.Context(), ts, exec, 1)

	if tool.executions != 0 {
		t.Fatalf("tool executions = %d, want 0", tool.executions)
	}
	if !errors.Is(outcome.JournalErr, exec.assistantToolCallsWriteErr) {
		t.Fatalf("journal error = %v, want %v", outcome.JournalErr, exec.assistantToolCallsWriteErr)
	}
}

func TestPipelineAllowAllBypassesApprovalHook(t *testing.T) {
	registry := tools.NewToolRegistry()
	tool := &approvalContextTool{}
	registry.Register(tool)
	agent := &AgentInstance{
		ID:       "main",
		Tools:    registry,
		Sessions: session.NewSessionManager(""),
	}
	ts := &turnState{
		agent: agent, agentID: "main", turnID: "turn-allow-all",
		sessionKey: "allow-all", workspace: t.TempDir(),
		opts: processOptions{
			NoHistory: true,
			Dispatch:  DispatchRequest{SessionKey: "allow-all"},
			ApprovalGrant: &ToolApprovalGrant{
				InteractionID:      "approval-before-allow-all",
				Revision:           1,
				OriginExecutionID:  "original-execution",
				OriginArgumentHash: strings.Repeat("a", 64),
			},
		},
	}
	exec := newTurnExecution(agent, ts.opts, nil, "", nil)
	exec.normalizedToolCalls = []providers.ToolCall{{
		ID: "call-allow-all", Name: tool.Name(), Arguments: map[string]any{},
	}}
	hook := &durableApprovalHook{actionSummary: "must not be requested"}
	hooks := NewHookManager(nil)
	defer hooks.Close()
	if err := hooks.Mount(NamedHook("approval", hook)); err != nil {
		t.Fatal(err)
	}
	cfg := config.DefaultConfig()
	cfg.Tools.Approval.Mode = config.ToolApprovalModeAllowAll
	manager := &fakeToolSuspensionManager{}
	pipeline := &Pipeline{
		Cfg: cfg,
		Interaction: PipelineInteractionServices{
			Hooks:      hooks,
			Suspension: manager,
		},
	}

	pipeline.ExecuteTools(t.Context(), t.Context(), ts, exec, 1)

	if hook.calls != 0 {
		t.Fatalf("approval hook calls = %d, want 0", hook.calls)
	}
	if tool.executions != 1 || !tool.bypass || tool.continued {
		t.Fatalf(
			"tool executions = %d, bypass = %v, continued = %v",
			tool.executions,
			tool.bypass,
			tool.continued,
		)
	}
	if len(manager.consumptions) != 1 || ts.opts.ApprovalGrant != nil {
		t.Fatalf(
			"approval consumptions = %d, retained grant = %#v",
			len(manager.consumptions),
			ts.opts.ApprovalGrant,
		)
	}
	if got := manager.consumptions[0].Origin.ArgumentHash; got != strings.Repeat("a", 64) {
		t.Fatalf("consumed argument hash = %q, want original retained hash", got)
	}
}

type replacementNodeTool struct{ approvalContextTool }

func (*replacementNodeTool) Name() string { return "nodes_invoke" }

func TestToolApprovalBypassRequiresTrustedNodeTool(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Tools.Approval.BypassNodeTargets = []string{"vpn"}
	arguments := map[string]any{"target": "vpn"}
	registry := tools.NewToolRegistry()
	registry.Register(tools.NewNodeInvokeTool(nil, nil))

	if bypass, _ := toolApprovalBypass(cfg, registry, "nodes_invoke", arguments); !bypass {
		t.Fatal("trusted node tool did not receive the configured target bypass")
	}
	if bypass, _ := toolApprovalBypass(
		cfg,
		registry,
		"nodes_invoke",
		map[string]any{"target": "approval-test"},
	); bypass {
		t.Fatal("unlisted target received the configured target bypass")
	}
	if bypass, _ := toolApprovalBypass(cfg, registry, "nodes_invoke", map[string]any{}); bypass {
		t.Fatal("node tool without an explicit target received the configured bypass")
	}

	registry.Register(&replacementNodeTool{})
	if bypass, _ := toolApprovalBypass(cfg, registry, "nodes_invoke", arguments); bypass {
		t.Fatal("replacement nodes_* tool received trusted node approval bypass")
	}
}

func TestPipelineSuspendsDurablyWithoutFabricatingPendingToolResult(t *testing.T) {
	registry := tools.NewToolRegistry()
	requestTool, err := tools.NewRequestUserInputTool(tools.RequestUserInputToolOptions{})
	if err != nil {
		t.Fatalf("NewRequestUserInputTool() error = %v", err)
	}
	deferredTool := &steeringSafetyTestTool{
		name: "deferred-write", safety: tools.SteeringSafetyCancellable,
	}
	registry.Register(requestTool)
	registry.Register(deferredTool)
	store := session.NewSessionManager("")
	agent := &AgentInstance{ID: "main", Tools: registry, Sessions: store}
	inbound := bus.InboundContext{
		Channel: "telegram", Account: "primary", ChatID: "chat-1", ChatType: "group",
		TopicID: "topic-1", SpaceID: "space-1", SpaceType: "workspace", SenderID: "user-1",
	}
	ts := &turnState{
		agent: agent, agentID: agent.ID, turnID: "turn-suspend", sessionKey: "session-suspend",
		channel: inbound.Channel, chatID: inbound.ChatID,
		opts: processOptions{TaskID: "task-suspend", Dispatch: DispatchRequest{
			RouteSessionKey: "route-suspend", SessionKey: "session-suspend", InboundContext: &inbound,
		}},
	}
	exec := newTurnExecution(agent, ts.opts, nil, "", nil)
	exec.normalizedToolCalls = []providers.ToolCall{
		{ID: "call-question", Name: requestTool.Name(), Arguments: map[string]any{
			"questions": []any{map[string]any{"id": "mode", "question": "Which mode?"}},
		}},
		{ID: "call-deferred", Name: deferredTool.Name()},
	}
	exec.assistantToolCallsPersisted = true
	manager := &fakeToolSuspensionManager{
		disposition: ToolSuspensionDisposition{InteractionID: "interaction-1", Durable: true},
	}
	emitter := &captureRuntimeEmitter{}
	pipeline := &Pipeline{
		Runtime:     PipelineRuntimeServices{Events: emitter},
		Interaction: PipelineInteractionServices{Suspension: manager},
	}

	control := pipeline.ExecuteTools(t.Context(), t.Context(), ts, exec, 1)
	if control.Control != ToolControlSuspend {
		t.Fatalf("control = %v, want suspend", control.Control)
	}
	if deferredTool.executions != 0 {
		t.Fatalf("deferred tool executions = %d, want 0", deferredTool.executions)
	}
	if control.SuspendedInteractionID != "interaction-1" || len(manager.requests) != 1 {
		t.Fatalf("suspension = %q, requests = %#v", control.SuspendedInteractionID, manager.requests)
	}
	request := manager.requests[0]
	if request.Origin.ToolCallID != "call-question" || request.Origin.TurnID != ts.turnID ||
		request.Origin.TaskID != "task-suspend" ||
		request.Origin.ArgumentHash != "" || request.ApprovalAction != "" ||
		request.Route.SenderID != "user-1" || request.Route.AccountID != "primary" ||
		request.Route.TopicID != "topic-1" || request.Route.SpaceID != "space-1" {
		t.Fatalf("trusted suspension request = %#v", request)
	}
	if len(exec.messages) != 1 || exec.messages[0].ToolCallID != "call-deferred" {
		t.Fatalf("messages = %#v, want only deferred sibling result", exec.messages)
	}
	for _, message := range exec.messages {
		if message.ToolCallID == "call-question" {
			t.Fatalf("pending call received fabricated result: %#v", message)
		}
	}
	foundSuspendedEvent := false
	for _, event := range emitter.events {
		payload, ok := event.payload.(ToolExecEndPayload)
		if event.kind == runtimeevents.KindAgentToolExecEnd && ok && payload.Suspended &&
			payload.InteractionID == "interaction-1" {
			foundSuspendedEvent = true
		}
	}
	if !foundSuspendedEvent {
		t.Fatal("missing suspended tool execution event")
	}
}

func TestPipelineForwardsAndCancelsSuspensionDomainResolution(t *testing.T) {
	newTool := func(called chan interactions.Outcome) *fixedToolResultTool {
		return &fixedToolResultTool{name: "domain_suspension", result: &tools.ToolResult{
			Silent: true,
			Suspension: &interactions.SuspensionRequest{
				Kind: interactions.KindQuestion, PromptSummary: "Release browser control", Timeout: time.Minute,
				Questions: []interactions.Question{{
					ID: "release", Header: "Browser control", Question: "Release browser control?",
				}},
			},
			SuspensionResolution: func(_ context.Context, outcome interactions.Outcome) error {
				called <- outcome
				return nil
			},
		}}
	}
	t.Run("durable", func(t *testing.T) {
		called := make(chan interactions.Outcome, 1)
		tool := newTool(called)
		registry := tools.NewToolRegistry()
		registry.Register(tool)
		agent := &AgentInstance{ID: "browser", Tools: registry, Sessions: session.NewSessionManager("")}
		inbound := bus.InboundContext{
			Channel: "telegram", Account: "primary", ChatID: "chat-domain", SenderID: "user-domain",
		}
		ts := &turnState{
			agent: agent, agentID: agent.ID, turnID: "turn-domain", sessionKey: "session-domain",
			channel: inbound.Channel, chatID: inbound.ChatID,
			opts: processOptions{Dispatch: DispatchRequest{
				SessionKey: "session-domain", InboundContext: &inbound,
			}},
		}
		exec := newTurnExecution(agent, ts.opts, nil, "", nil)
		exec.normalizedToolCalls = []providers.ToolCall{{ID: "call-domain", Name: tool.Name()}}
		exec.assistantToolCallsPersisted = true
		manager := &fakeToolSuspensionManager{
			disposition: ToolSuspensionDisposition{InteractionID: "interaction-domain", Durable: true},
		}
		pipeline := &Pipeline{Interaction: PipelineInteractionServices{
			Hooks: NewHookManager(nil), Suspension: manager,
		}}
		if outcome := pipeline.ExecuteTools(
			t.Context(),
			t.Context(),
			ts,
			exec,
			1,
		); outcome.Control != ToolControlSuspend {
			t.Fatalf(
				"control = %v, want suspend; requests=%#v messages=%#v",
				outcome.Control,
				manager.requests,
				exec.messages,
			)
		}
		if len(manager.requests) != 1 || manager.requests[0].Resolution == nil {
			t.Fatalf("suspension requests = %#v", manager.requests)
		}
		select {
		case outcome := <-called:
			t.Fatalf("hook clone resolved suspension before durable answer: %q", outcome)
		default:
		}
		if err := manager.requests[0].Resolution(t.Context(), interactions.OutcomeAnswered); err != nil {
			t.Fatal(err)
		}
		if got := <-called; got != interactions.OutcomeAnswered {
			t.Fatalf("resolution outcome = %q", got)
		}
	})
	t.Run("fallback", func(t *testing.T) {
		called := make(chan interactions.Outcome, 1)
		tool := newTool(called)
		registry := tools.NewToolRegistry()
		registry.Register(tool)
		agent := &AgentInstance{ID: "browser", Tools: registry, Sessions: session.NewSessionManager("")}
		ts := &turnState{
			agent: agent, agentID: agent.ID, turnID: "turn-domain-fallback", sessionKey: "session-domain-fallback",
			opts: processOptions{Dispatch: DispatchRequest{SessionKey: "session-domain-fallback"}},
		}
		exec := newTurnExecution(agent, ts.opts, nil, "", nil)
		exec.normalizedToolCalls = []providers.ToolCall{{ID: "call-domain", Name: tool.Name()}}
		pipeline := &Pipeline{}
		if outcome := pipeline.ExecuteTools(
			t.Context(),
			t.Context(),
			ts,
			exec,
			1,
		); outcome.Control != ToolControlContinue {
			t.Fatalf("control = %v, want continue", outcome.Control)
		}
		if got := <-called; got != interactions.OutcomeCanceled {
			t.Fatalf("fallback resolution outcome = %q", got)
		}
	})
}

func TestPipelineBindsToolOriginatedApprovalSuspensionToTrustedArguments(t *testing.T) {
	registry := tools.NewToolRegistry()
	tool := &boundApprovalSuspensionTool{}
	registry.Register(tool)
	workspace := t.TempDir()
	agent := &AgentInstance{
		ID: "browser", Tools: registry, Sessions: session.NewSessionManager(""),
	}
	ts := &turnState{
		agent: agent, agentID: agent.ID, turnID: "turn-bound-approval",
		sessionKey: "session-bound-approval", workspace: workspace,
		opts: processOptions{Dispatch: DispatchRequest{SessionKey: "session-bound-approval"}},
	}
	exec := newTurnExecution(agent, ts.opts, nil, "", nil)
	exec.normalizedToolCalls = []providers.ToolCall{{
		ID: "call-bound-approval", Name: tool.Name(), Arguments: map[string]any{"value": "model-authored"},
	}}
	exec.assistantToolCallsPersisted = true
	manager := &fakeToolSuspensionManager{
		disposition: ToolSuspensionDisposition{InteractionID: "interaction-bound", Durable: true},
	}
	pipeline := &Pipeline{Interaction: PipelineInteractionServices{Suspension: manager}}

	outcome := pipeline.ExecuteTools(t.Context(), t.Context(), ts, exec, 1)
	if outcome.Control != ToolControlSuspend || outcome.SuspendedInteractionID != "interaction-bound" {
		t.Fatalf("outcome = %+v, want bound approval suspension", outcome)
	}
	if tool.executions != 1 || tool.preparationCalls != 1 || len(manager.requests) != 1 {
		t.Fatalf(
			"executions = %d, preparations = %d, requests = %d",
			tool.executions,
			tool.preparationCalls,
			len(manager.requests),
		)
	}
	wantHash, err := interactions.HashArguments(workspace, map[string]any{
		"prepared_action_id": "prepared_1", "action_hash": "trusted_hash",
	})
	if err != nil {
		t.Fatal(err)
	}
	request := manager.requests[0]
	if request.Prompt.Kind != interactions.KindApproval ||
		request.Origin.ArgumentHash != wantHash ||
		request.ApprovalAction != "Publish the prepared browser action" {
		t.Fatalf("bound suspension request = %#v", request)
	}

	resumeState := &turnState{
		agent: agent, agentID: agent.ID, turnID: "turn-bound-approval-resume",
		sessionKey: "session-bound-approval", workspace: workspace,
		opts: processOptions{
			Dispatch: DispatchRequest{SessionKey: "session-bound-approval"},
			ApprovalGrant: &ToolApprovalGrant{
				InteractionID: "interaction-bound", Revision: 2,
				OriginExecutionID: "execution-original", OriginArgumentHash: wantHash,
			},
		},
	}
	resumeExec := newTurnExecution(agent, resumeState.opts, nil, "", nil)
	resumeExec.normalizedToolCalls = []providers.ToolCall{{
		ID: "call-bound-approval", Name: tool.Name(), Arguments: map[string]any{"value": "model-authored"},
	}}
	hooks := NewHookManager(nil)
	defer hooks.Close()
	if err = hooks.Mount(NamedHook("approval", &durableApprovalHook{
		actionSummary: "Publish the prepared browser action",
	})); err != nil {
		t.Fatal(err)
	}
	pipeline.Interaction.Hooks = hooks
	resumeOutcome := pipeline.ExecuteTools(t.Context(), t.Context(), resumeState, resumeExec, 1)
	if resumeOutcome.Control != ToolControlContinue || !tool.continued || len(manager.consumptions) != 1 {
		t.Fatalf(
			"resume outcome = %+v, continued = %t, consumptions = %#v, messages = %#v",
			resumeOutcome,
			tool.continued,
			manager.consumptions,
			resumeExec.messages,
		)
	}
	if manager.consumptions[0].Origin.ArgumentHash != wantHash || resumeState.opts.ApprovalGrant != nil {
		t.Fatalf(
			"consumed hash = %q, retained grant = %#v",
			manager.consumptions[0].Origin.ArgumentHash,
			resumeState.opts.ApprovalGrant,
		)
	}
}

func TestPipelineSuspensionFailureBecomesPairedToolError(t *testing.T) {
	registry := tools.NewToolRegistry()
	requestTool, err := tools.NewRequestUserInputTool(tools.RequestUserInputToolOptions{})
	if err != nil {
		t.Fatalf("NewRequestUserInputTool() error = %v", err)
	}
	registry.Register(requestTool)
	store := session.NewSessionManager("")
	agent := &AgentInstance{ID: "main", Tools: registry, Sessions: store}
	ts := &turnState{
		agent: agent, agentID: agent.ID, turnID: "turn-no-persist", sessionKey: "session-no-persist",
		opts: processOptions{Dispatch: DispatchRequest{SessionKey: "session-no-persist"}},
	}
	exec := newTurnExecution(agent, ts.opts, nil, "", nil)
	exec.normalizedToolCalls = []providers.ToolCall{{
		ID: "call-question", Name: requestTool.Name(), Arguments: map[string]any{
			"questions": []any{map[string]any{"id": "mode", "question": "Which mode?"}},
		},
	}}
	manager := &fakeToolSuspensionManager{
		disposition: ToolSuspensionDisposition{InteractionID: "must-not-run", Durable: true},
	}
	pipeline := &Pipeline{Interaction: PipelineInteractionServices{Suspension: manager}}

	if control := pipeline.ExecuteTools(t.Context(), t.Context(), ts, exec, 1); control.Control != ToolControlContinue {
		t.Fatalf("control = %v, want continue with tool error", control.Control)
	}
	if len(manager.requests) != 0 {
		t.Fatal("suspension manager called before assistant persistence")
	}
	if len(exec.messages) != 1 || exec.messages[0].ToolCallID != "call-question" ||
		exec.messages[0].ToolResultStatus != providers.ToolResultStatusError ||
		!strings.Contains(exec.messages[0].Content, "originating assistant tool call was not persisted") {
		t.Fatalf("messages = %#v, want paired persistence error", exec.messages)
	}
}

func TestPipelineSteeringWinsBeforeSuspensionCommit(t *testing.T) {
	registry := tools.NewToolRegistry()
	requestTool, err := tools.NewRequestUserInputTool(tools.RequestUserInputToolOptions{})
	if err != nil {
		t.Fatalf("NewRequestUserInputTool() error = %v", err)
	}
	registry.Register(requestTool)
	agent := &AgentInstance{ID: "main", Tools: registry, Sessions: session.NewSessionManager("")}
	ts := &turnState{
		agent: agent, agentID: agent.ID, turnID: "turn-steer-suspend", sessionKey: "session-steer-suspend",
		opts: processOptions{Dispatch: DispatchRequest{SessionKey: "session-steer-suspend"}},
	}
	exec := newTurnExecution(agent, ts.opts, nil, "", nil)
	exec.normalizedToolCalls = []providers.ToolCall{{
		ID: "call-question", Name: requestTool.Name(), Arguments: map[string]any{
			"questions": []any{map[string]any{"id": "mode", "question": "Which mode?"}},
		},
	}}
	exec.assistantToolCallsPersisted = true
	manager := &fakeToolSuspensionManager{
		disposition: ToolSuspensionDisposition{InteractionID: "must-not-run", Durable: true},
	}
	pipeline := &Pipeline{
		Context: PipelineContextServices{Steering: &delayedSteering{
			messages: []providers.Message{{Role: "user", Content: "Use canary instead"}},
		}},
		Interaction: PipelineInteractionServices{Suspension: manager},
	}

	if control := pipeline.ExecuteTools(t.Context(), t.Context(), ts, exec, 1); control.Control != ToolControlContinue {
		t.Fatalf("control = %v, want continue", control.Control)
	}
	if len(manager.requests) != 0 || len(exec.pendingMessages) != 1 {
		t.Fatalf("requests = %d, pending = %#v", len(manager.requests), exec.pendingMessages)
	}
	if len(exec.messages) != 1 || exec.messages[0].ToolCallID != "call-question" {
		t.Fatalf("messages = %#v, want paired deferred result", exec.messages)
	}
}

type pipelineLoopGuardReadTool struct {
	executions int
}

type steeringSafetyTestTool struct {
	name       string
	safety     tools.SteeringSafety
	executions int
}

func (t *steeringSafetyTestTool) Name() string        { return t.name }
func (t *steeringSafetyTestTool) Description() string { return "steering safety test" }
func (t *steeringSafetyTestTool) Parameters() map[string]any {
	return map[string]any{"type": "object"}
}

func (t *steeringSafetyTestTool) ToolSteeringSafety(map[string]any) tools.SteeringSafety {
	return t.safety
}

func (t *steeringSafetyTestTool) Execute(context.Context, map[string]any) *tools.ToolResult {
	t.executions++
	return tools.SilentResult(t.name + " complete")
}

type unknownSteeringSafetyTestTool struct {
	executions int
}

func (*unknownSteeringSafetyTestTool) Name() string        { return "unknown" }
func (*unknownSteeringSafetyTestTool) Description() string { return "unknown steering safety test" }
func (*unknownSteeringSafetyTestTool) Parameters() map[string]any {
	return map[string]any{"type": "object"}
}

func (t *unknownSteeringSafetyTestTool) Execute(context.Context, map[string]any) *tools.ToolResult {
	t.executions++
	return tools.SilentResult("unknown complete")
}

func (t *pipelineLoopGuardReadTool) Name() string        { return "loop_hook_test" }
func (t *pipelineLoopGuardReadTool) Description() string { return "hook loop test" }
func (t *pipelineLoopGuardReadTool) Parameters() map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": map[string]any{"text": map[string]any{"type": "string"}},
	}
}

func (t *pipelineLoopGuardReadTool) ToolLoopSemantics() loopguard.Semantics {
	return loopguard.SemanticsReadOnlyIdempotent
}

func (t *pipelineLoopGuardReadTool) Execute(_ context.Context, args map[string]any) *tools.ToolResult {
	t.executions++
	text, _ := args["text"].(string)
	return tools.SilentResult(text)
}

type capturedRuntimeEvent struct {
	kind    runtimeevents.Kind
	payload any
}

type captureRuntimeEmitter struct {
	events []capturedRuntimeEvent
}

type oneShotLoopGuardSteering struct {
	messages []providers.Message
}

type delayedSteering struct {
	polls    int
	messages []providers.Message
}

func (s *delayedSteering) dequeueSteeringMessagesForTurn(runtimeSessionScope, string) []providers.Message {
	s.polls++
	if s.polls < 2 {
		return nil
	}
	messages := s.messages
	s.messages = nil
	return messages
}

func (s *oneShotLoopGuardSteering) dequeueSteeringMessagesForTurn(runtimeSessionScope, string) []providers.Message {
	messages := s.messages
	s.messages = nil
	return messages
}

func (e *captureRuntimeEmitter) emitEvent(kind runtimeevents.Kind, _ HookMeta, payload any) {
	e.events = append(e.events, capturedRuntimeEvent{kind: kind, payload: payload})
}

func (t *pipelineLoopGuardTool) Name() string        { return "pipeline_loop_test" }
func (t *pipelineLoopGuardTool) Description() string { return "pipeline loop test" }
func (t *pipelineLoopGuardTool) Parameters() map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": map[string]any{"value": map[string]any{"type": "string"}},
	}
}

func (t *pipelineLoopGuardTool) ToolLoopSemantics() loopguard.Semantics {
	return loopguard.SemanticsReadOnlyIdempotent
}

func (t *pipelineLoopGuardTool) Execute(context.Context, map[string]any) *tools.ToolResult {
	t.executions++
	return tools.ErrorResult("stable pipeline failure")
}

func TestPipelineLoopGuardBlocksAndPreservesToolCallResults(t *testing.T) {
	registry := tools.NewToolRegistry()
	tool := &pipelineLoopGuardTool{}
	registry.Register(tool)
	guardConfig := loopguard.DefaultConfig()
	guardConfig.HardStopsEnabled = true
	guardConfig.ExactFailureWarn = 1
	guardConfig.ExactFailureBlock = 2
	guardConfig.SameToolFailureHalt = 99
	agent := &AgentInstance{
		ID: "main", Tools: registry, Sessions: session.NewSessionManager(""),
		ToolLoopDetection: guardConfig,
	}
	ts := &turnState{
		agent: agent, agentID: "main", turnID: "turn-loop-guard",
		sessionKey: "session-loop-guard", opts: processOptions{NoHistory: true},
	}
	exec := newTurnExecution(agent, ts.opts, nil, "", nil)
	emitter := &captureRuntimeEmitter{}
	pipeline := &Pipeline{Runtime: PipelineRuntimeServices{Events: emitter}}

	for i := 1; i <= 3; i++ {
		exec.normalizedToolCalls = []providers.ToolCall{{
			ID: fmt.Sprintf("call-%d", i), Name: tool.Name(),
			Arguments: map[string]any{"value": "same"},
		}}
		if i == 3 {
			exec.normalizedToolCalls = append(exec.normalizedToolCalls, providers.ToolCall{
				ID: "call-3-skipped", Name: tool.Name(), Arguments: map[string]any{"value": "other"},
			})
			pipeline.Context.Steering = &delayedSteering{
				messages: []providers.Message{{Role: "user", Content: "change course"}},
			}
		}
		exec.allResponsesHandled = true
		if got := pipeline.ExecuteTools(
			context.Background(),
			context.Background(),
			ts,
			exec,
			i,
		); got.Control != ToolControlContinue {
			t.Fatalf("iteration %d control = %v", i, got.Control)
		}
	}

	if tool.executions != 2 {
		t.Fatalf("tool executions = %d, want 2", tool.executions)
	}
	if len(exec.messages) != 4 {
		t.Fatalf("tool messages = %d, want 4", len(exec.messages))
	}
	for i, message := range exec.messages[:3] {
		wantID := fmt.Sprintf("call-%d", i+1)
		if message.Role != "tool" || message.ToolCallID != wantID {
			t.Fatalf("message %d = %#v, want tool result for %s", i, message, wantID)
		}
	}
	if exec.messages[0].ToolResultStatus != providers.ToolResultStatusError ||
		exec.messages[1].ToolResultStatus != providers.ToolResultStatusError {
		t.Fatalf(
			"executed error statuses = %q, %q",
			exec.messages[0].ToolResultStatus,
			exec.messages[1].ToolResultStatus,
		)
	}
	if exec.messages[2].ToolResultStatus != "" || exec.messages[3].ToolResultStatus != "" {
		t.Fatalf("non-executed results must remain unknown: %#v", exec.messages[2:])
	}
	if !strings.Contains(exec.messages[2].Content, "repeated_exact_failure_block") {
		t.Fatalf("blocked content = %q", exec.messages[2].Content)
	}
	if exec.messages[3].ToolCallID != "call-3-skipped" ||
		!strings.Contains(exec.messages[3].Content, "newer user message arrived") {
		t.Fatalf("steering-skipped result = %#v", exec.messages[3])
	}
	var decisions []ToolLoopDecisionPayload
	for _, event := range emitter.events {
		if event.kind == runtimeevents.KindAgentToolLoopDecision {
			decisions = append(decisions, event.payload.(ToolLoopDecisionPayload))
		}
	}
	if len(decisions) != 3 || decisions[len(decisions)-1].Action != "block" {
		t.Fatalf("loop decision events = %#v", decisions)
	}
	encoded, err := json.Marshal(decisions)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "same") || strings.Contains(string(encoded), "stable pipeline failure") {
		t.Fatalf("decision events exposed arguments/results: %s", encoded)
	}
}

func TestTurnExecutionsHaveIsolatedLoopGuardState(t *testing.T) {
	config := loopguard.DefaultConfig()
	config.ExactFailureWarn = 1
	agent := &AgentInstance{ToolLoopDetection: config}
	first := newTurnExecution(agent, processOptions{}, nil, "", nil)
	second := newTurnExecution(agent, processOptions{}, nil, "", nil)
	observation := loopguard.Observation{
		Tool: "read_file", Args: map[string]any{"path": "x"}, Failed: true,
	}
	if got := first.loopGuard.After(observation); got.Action != loopguard.ActionWarn {
		t.Fatalf("first decision = %#v", got)
	}
	if got := second.loopGuard.Before(
		"read_file",
		observation.Args,
		loopguard.SemanticsReadOnlyIdempotent,
	); !got.AllowsExecution() ||
		got.Count != 0 {
		t.Fatalf("second turn inherited state: %#v", got)
	}
}

func TestPipelineEmergencyHaltTerminatesUnknownSuccessfulLoop(t *testing.T) {
	registry := tools.NewToolRegistry()
	tool := &unknownSteeringSafetyTestTool{}
	registry.Register(tool)
	config := loopguard.DefaultConfig()
	config.IdenticalCallHalt = 3
	agent := &AgentInstance{
		ID: "main", Tools: registry, Sessions: session.NewSessionManager(""),
		ToolLoopDetection: config,
	}
	ts := &turnState{
		agent: agent, agentID: "main", turnID: "turn-emergency-loop-guard",
		sessionKey: "session-emergency-loop-guard", opts: processOptions{NoHistory: true},
	}
	exec := newTurnExecution(agent, ts.opts, nil, "", nil)
	emitter := &captureRuntimeEmitter{}
	pipeline := &Pipeline{Runtime: PipelineRuntimeServices{Events: emitter}}

	for i := 1; i <= config.IdenticalCallHalt; i++ {
		exec.normalizedToolCalls = []providers.ToolCall{{
			ID: fmt.Sprintf("call-%d", i), Name: tool.Name(), Arguments: map[string]any{},
		}}
		exec.allResponsesHandled = false
		outcome := pipeline.ExecuteTools(context.Background(), context.Background(), ts, exec, i)
		if i < config.IdenticalCallHalt {
			if outcome.Control != ToolControlContinue {
				t.Fatalf("iteration %d outcome = %#v", i, outcome)
			}
			continue
		}
		if outcome.Control != ToolControlHalt ||
			!strings.Contains(outcome.FinalContent, "Stopped the turn") {
			t.Fatalf("terminal outcome = %#v", outcome)
		}
	}
	if tool.executions != config.IdenticalCallHalt {
		t.Fatalf("tool executions = %d, want %d", tool.executions, config.IdenticalCallHalt)
	}
	var decisions []ToolLoopDecisionPayload
	for _, event := range emitter.events {
		if event.kind == runtimeevents.KindAgentToolLoopDecision {
			decisions = append(decisions, event.payload.(ToolLoopDecisionPayload))
		}
	}
	if len(decisions) != 1 || decisions[0].Action != "halt" ||
		decisions[0].Code != "identical_call_emergency_halt" {
		t.Fatalf("loop decisions = %#v", decisions)
	}
}

func TestPipelineEmergencyHaltPreservesReasonForResponseHandledTool(t *testing.T) {
	const haltThreshold = 4
	toolCalls := make([]providers.ToolCall, 0, haltThreshold)
	for i := 1; i <= haltThreshold; i++ {
		toolCalls = append(toolCalls, providers.ToolCall{
			ID:        fmt.Sprintf("call-%d", i),
			Name:      "handled-loop",
			Arguments: map[string]any{},
		})
	}
	provider := &sequenceProvider{responses: []*providers.LLMResponse{
		{ToolCalls: toolCalls},
		{Content: "model rewrote the runtime halt reason", FinishReason: "stop"},
	}}
	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()

	tool := &fixedToolResultTool{
		name:   "handled-loop",
		result: tools.SilentResult("same successful result").WithResponseHandled(),
	}
	agent.Tools.Register(tool)
	agent.ToolLoopDetection = loopguard.DefaultConfig()
	agent.ToolLoopDetection.IdenticalCallHalt = haltThreshold

	response, err := al.runAgentLoop(t.Context(), agent, processOptions{
		Dispatch: DispatchRequest{
			SessionKey:  "session-emergency-handled-loop",
			UserMessage: "run the handled tool",
		},
		DefaultResponse: "default response",
		NoHistory:       true,
	})
	if err != nil {
		t.Fatalf("runAgentLoop() error = %v", err)
	}
	want := "Stopped the turn after 4 consecutive identical successful calls to handled-loop because the operation was not making progress."
	if response != want {
		t.Fatalf("response = %q, want exact runtime halt reason %q", response, want)
	}
	if provider.callCount != 1 {
		t.Fatalf("provider calls = %d, want 1 without final rendering", provider.callCount)
	}
	if tool.executions != haltThreshold {
		t.Fatalf("tool executions = %d, want %d", tool.executions, haltThreshold)
	}
}

func TestPipelineLoopGuardUsesHookModifiedArgumentsAndResults(t *testing.T) {
	registry := tools.NewToolRegistry()
	tool := &pipelineLoopGuardReadTool{}
	registry.Register(tool)
	config := loopguard.DefaultConfig()
	config.NoProgressWarn = 2
	agent := &AgentInstance{
		ID:                "main",
		Tools:             registry,
		Sessions:          session.NewSessionManager(""),
		ToolLoopDetection: config,
	}
	ts := &turnState{
		agent:      agent,
		agentID:    "main",
		turnID:     "turn-hook-loop",
		sessionKey: "hook-loop",
		opts:       processOptions{NoHistory: true},
	}
	exec := newTurnExecution(agent, ts.opts, nil, "", nil)
	hooks := NewHookManager(nil)
	defer hooks.Close()
	if err := hooks.Mount(NamedHook("rewrite", &toolRewriteHook{})); err != nil {
		t.Fatal(err)
	}
	pipeline := &Pipeline{Interaction: PipelineInteractionServices{Hooks: hooks}}

	for i, value := range []string{"original-one", "original-two"} {
		exec.normalizedToolCalls = []providers.ToolCall{{
			ID: fmt.Sprintf("hook-%d", i), Name: tool.Name(), Arguments: map[string]any{"text": value},
		}}
		exec.allResponsesHandled = true
		pipeline.ExecuteTools(context.Background(), context.Background(), ts, exec, i+1)
	}
	if tool.executions != 2 {
		t.Fatalf("executions = %d", tool.executions)
	}
	if !strings.Contains(exec.messages[1].Content, "read_only_no_progress_warning") ||
		!strings.Contains(exec.messages[1].Content, "after:modified") {
		t.Fatalf("second hook result = %q", exec.messages[1].Content)
	}
}

func TestPipelineLoopGuardDoesNotCountPolicyDenials(t *testing.T) {
	registry := tools.NewToolRegistry()
	tool := &pipelineLoopGuardTool{}
	registry.Register(tool)
	config := loopguard.DefaultConfig()
	config.HardStopsEnabled = true
	config.ExactFailureWarn = 1
	config.ExactFailureBlock = 1
	config.SameToolFailureHalt = 99
	agent := &AgentInstance{
		ID:                "main",
		Tools:             registry,
		Sessions:          session.NewSessionManager(""),
		ToolLoopDetection: config,
	}
	ts := &turnState{
		agent:      agent,
		agentID:    "main",
		turnID:     "turn-denial-loop",
		sessionKey: "denial-loop",
		opts:       processOptions{NoHistory: true},
	}
	exec := newTurnExecution(agent, ts.opts, nil, "", nil)
	hooks := NewHookManager(nil)
	defer hooks.Close()
	if err := hooks.Mount(NamedHook("deny", &denyToolHook{denyTools: map[string]bool{tool.Name(): true}})); err != nil {
		t.Fatal(err)
	}
	pipeline := &Pipeline{Interaction: PipelineInteractionServices{Hooks: hooks}}
	call := providers.ToolCall{ID: "denied", Name: tool.Name(), Arguments: map[string]any{"value": "same"}}
	exec.normalizedToolCalls = []providers.ToolCall{call}
	pipeline.ExecuteTools(context.Background(), context.Background(), ts, exec, 1)
	hooks.Unmount("deny")
	call.ID = "executed"
	exec.normalizedToolCalls = []providers.ToolCall{call}
	pipeline.ExecuteTools(context.Background(), context.Background(), ts, exec, 2)
	if tool.executions != 1 {
		t.Fatalf("policy denial affected loop state; executions = %d", tool.executions)
	}
	if strings.Contains(exec.messages[1].Content, "_block") {
		t.Fatalf("first executed failure was incorrectly blocked: %q", exec.messages[1].Content)
	}
}

func TestPipelineLoopGuardBlocksBeforeApprovalAuthority(t *testing.T) {
	for _, test := range []struct {
		name        string
		grant       *ToolApprovalGrant
		blockLoop   bool
		wantContent string
	}{
		{name: "loop request", blockLoop: true, wantContent: "repeated_exact_failure_block"},
		{
			name:      "loop resume",
			blockLoop: true,
			grant: &ToolApprovalGrant{
				InteractionID: "approval-1",
				Revision:      2,
			},
			wantContent: "repeated_exact_failure_block",
		},
		{
			name:        "invalid request",
			wantContent: `missing required property "mutable"`,
		},
		{
			name: "invalid resume",
			grant: &ToolApprovalGrant{
				InteractionID: "approval-1",
				Revision:      2,
			},
			wantContent: `missing required property "mutable"`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := tools.NewToolRegistry()
			tool := &approvalBindingTool{}
			registry.Register(tool)
			config := loopguard.DefaultConfig()
			config.HardStopsEnabled = true
			config.ExactFailureWarn = 1
			config.ExactFailureBlock = 1
			config.SameToolFailureHalt = 99
			agent := &AgentInstance{
				ID:                "main",
				Tools:             registry,
				Sessions:          session.NewSessionManager(""),
				ToolLoopDetection: config,
			}
			ts := &turnState{
				agent: agent, agentID: "main", turnID: "turn-approval-loop",
				sessionKey: "approval-loop", workspace: t.TempDir(),
				opts: processOptions{
					NoHistory:     true,
					ApprovalGrant: test.grant,
					Dispatch: DispatchRequest{
						SessionKey: "approval-loop",
					},
				},
			}
			exec := newTurnExecution(agent, ts.opts, nil, "", nil)
			args := map[string]any{}
			if test.blockLoop {
				args["mutable"] = "same"
				exec.loopGuard.After(loopguard.Observation{
					Tool: tool.Name(), Args: args, Failed: true,
				})
			}
			exec.normalizedToolCalls = []providers.ToolCall{{
				ID: "call-blocked-approval", Name: tool.Name(), Arguments: args,
			}}
			exec.assistantToolCallsPersisted = true
			hooks := NewHookManager(nil)
			defer hooks.Close()
			if err := hooks.Mount(NamedHook("approval", &durableApprovalHook{
				actionSummary: "Run protected action",
			})); err != nil {
				t.Fatal(err)
			}
			manager := &fakeToolSuspensionManager{
				disposition: ToolSuspensionDisposition{
					InteractionID: "must-not-run",
					Durable:       true,
				},
			}
			pipeline := &Pipeline{
				Interaction: PipelineInteractionServices{
					Hooks:      hooks,
					Suspension: manager,
				},
			}

			if control := pipeline.ExecuteTools(
				t.Context(),
				t.Context(),
				ts,
				exec,
				1,
			); control.Control != ToolControlContinue {
				t.Fatalf("control = %v, want continue", control.Control)
			}
			if len(tool.bindingCalls) != 0 || len(manager.requests) != 0 ||
				len(manager.consumptions) != 0 || tool.executions != 0 {
				t.Fatalf(
					"blocked approval side effects = bindings:%#v requests:%#v consumptions:%#v executions:%d",
					tool.bindingCalls,
					manager.requests,
					manager.consumptions,
					tool.executions,
				)
			}
			if len(exec.messages) != 1 ||
				!strings.Contains(exec.messages[0].Content, test.wantContent) {
				t.Fatalf("blocked result = %#v", exec.messages)
			}
		})
	}
}

func TestInferSkillNamesFromToolCall_ReadFileSkillMarkdown(t *testing.T) {
	workspace := t.TempDir()
	skillDir := filepath.Join(workspace, "skills", "three-one")
	if err := os.MkdirAll(skillDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(
		filepath.Join(skillDir, "SKILL.md"),
		[]byte("---\nname: three-one\ndescription: test\n---\n# Three One\n"),
		0o644,
	); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cb := NewContextBuilder(workspace)
	ts := &turnState{
		workspace: workspace,
		agent: &AgentInstance{
			Workspace:      workspace,
			ContextBuilder: cb,
		},
	}

	got := inferSkillNamesFromToolCall(ts, "read_file", map[string]any{
		"path": filepath.Join(workspace, "skills", "three-one", "SKILL.md"),
	})
	if len(got) != 1 || got[0] != "three-one" {
		t.Fatalf("inferSkillNamesFromToolCall = %v, want [three-one]", got)
	}
}

func TestInferSkillNamesFromToolCall_NonSkillFileIgnored(t *testing.T) {
	workspace := t.TempDir()
	ts := &turnState{workspace: workspace}

	got := inferSkillNamesFromToolCall(ts, "read_file", map[string]any{
		"path": filepath.Join(workspace, "README.md"),
	})
	if len(got) != 0 {
		t.Fatalf("inferSkillNamesFromToolCall = %v, want empty", got)
	}
}

func TestIsFatalMCPTransportErrorSummary(t *testing.T) {
	if !isFatalMCPTransportErrorSummary(
		`MCP tool execution failed: failed to call tool: connection closed: calling "tools/call": client is closing: invalid character 'ð' looking for beginning of value`,
	) {
		t.Fatal("expected fatal MCP transport error to match")
	}
	if isFatalMCPTransportErrorSummary("MCP tool returned error: rate limited, retry later") {
		t.Fatal("expected normal MCP server error not to match fatal transport classifier")
	}
}

func TestPipelineAppendToolMessage_PersistsWithoutIngest(t *testing.T) {
	sessionStore := session.NewSessionManager("")
	cm := &trackingContextManager{}
	pipeline := &Pipeline{Context: PipelineContextServices{Runtime: cm}}
	ts := &turnState{
		agent:      &AgentInstance{Sessions: sessionStore},
		sessionKey: "session-tool-message",
	}
	runner := &toolLoopRunner{
		p:       pipeline,
		turnCtx: context.Background(),
		ts:      ts,
	}
	msg := providers.Message{
		Role:       "tool",
		Content:    "skipped",
		ToolCallID: "call-1",
	}

	runner.appendToolMessage(msg, toolMessagePersistOnly)
	if len(runner.messages) != 1 || runner.messages[0].Content != "skipped" {
		t.Fatalf("messages = %#v, want appended message", runner.messages)
	}
	history := sessionStore.GetHistory(ts.sessionKey)
	if len(history) != 1 || history[0].Content != "skipped" {
		t.Fatalf("session history = %#v, want persisted message", history)
	}
	if got := cm.ingestCalls.Load(); got != 0 {
		t.Fatalf("ingest calls = %d, want 0", got)
	}
}

func TestPipelineAppendToolMessage_PersistsAndIngests(t *testing.T) {
	sessionStore := session.NewSessionManager("")
	cm := &trackingContextManager{}
	pipeline := &Pipeline{Context: PipelineContextServices{Runtime: cm}}
	ts := &turnState{
		agent:      &AgentInstance{Sessions: sessionStore},
		sessionKey: "session-tool-result",
	}
	runner := &toolLoopRunner{
		p:       pipeline,
		turnCtx: context.Background(),
		ts:      ts,
	}
	msg := providers.Message{
		Role:       "tool",
		Content:    "result",
		ToolCallID: "call-2",
	}

	runner.appendToolMessage(msg, toolMessagePersistAndIngest)
	if len(runner.messages) != 1 || runner.messages[0].Content != "result" {
		t.Fatalf("messages = %#v, want appended message", runner.messages)
	}
	history := sessionStore.GetHistory(ts.sessionKey)
	if len(history) != 1 || history[0].Content != "result" {
		t.Fatalf("session history = %#v, want persisted message", history)
	}
	if got := cm.ingestCalls.Load(); got != 1 {
		t.Fatalf("ingest calls = %d, want 1", got)
	}
	if cm.lastIngest == nil || cm.lastIngest.Message.Content != "result" {
		t.Fatalf("last ingest = %#v, want result message", cm.lastIngest)
	}
}

func TestPipelineAppendSkippedToolMessages_PersistsRemainingWithoutIngest(t *testing.T) {
	sessionStore := session.NewSessionManager("")
	cm := &trackingContextManager{}
	pipeline := &Pipeline{Context: PipelineContextServices{Runtime: cm}}
	ts := &turnState{
		agent:      &AgentInstance{Sessions: sessionStore},
		sessionKey: "session-skipped-tool",
	}
	toolCalls := []providers.ToolCall{
		{ID: "call-complete", Name: "done_tool"},
		{ID: "call-skip-1", Name: "expensive_tool"},
		{ID: "call-skip-2", Name: "slow_tool"},
	}
	runner := &toolLoopRunner{
		p:         pipeline,
		turnCtx:   context.Background(),
		ts:        ts,
		toolCalls: toolCalls,
	}

	runner.appendSkippedToolMessages(
		1,
		"queued user steering message",
		queuedSteeringDeferredToolResult,
	)
	if len(runner.messages) != 2 {
		t.Fatalf("messages len = %d, want 2", len(runner.messages))
	}
	if runner.messages[0].ToolCallID != "call-skip-1" ||
		runner.messages[1].ToolCallID != "call-skip-2" ||
		runner.messages[0].Content != queuedSteeringDeferredToolResult {
		t.Fatalf("messages = %#v, want skipped tool messages", runner.messages)
	}
	history := sessionStore.GetHistory(ts.sessionKey)
	if len(history) != 2 ||
		history[0].ToolCallID != "call-skip-1" ||
		history[1].ToolCallID != "call-skip-2" {
		t.Fatalf("session history = %#v, want skipped messages persisted", history)
	}
	if got := cm.ingestCalls.Load(); got != 0 {
		t.Fatalf("ingest calls = %d, want 0", got)
	}
}

func TestPipelineSteeringClassifiesEveryPendingToolAndPreservesPairing(t *testing.T) {
	registry := tools.NewToolRegistry()
	readOnly := &steeringSafetyTestTool{name: "read", safety: tools.SteeringSafetyReadOnly}
	cancellable := &steeringSafetyTestTool{name: "write", safety: tools.SteeringSafetyCancellable}
	nonCancellable := &steeringSafetyTestTool{name: "commit", safety: tools.SteeringSafetyNonCancellable}
	unknown := &unknownSteeringSafetyTestTool{}
	for _, tool := range []tools.Tool{readOnly, cancellable, nonCancellable, unknown} {
		registry.Register(tool)
	}
	agent := &AgentInstance{ID: "main", Tools: registry, Sessions: session.NewSessionManager("")}
	ts := &turnState{
		agent: agent, agentID: "main", turnID: "turn-steering-safety",
		sessionKey: "session-steering-safety", opts: processOptions{NoHistory: true},
	}
	exec := newTurnExecution(agent, ts.opts, nil, "", nil)
	exec.normalizedToolCalls = []providers.ToolCall{
		{ID: "call-read", Name: "read"},
		{ID: "call-write", Name: "write"},
		{ID: "call-commit", Name: "commit"},
		{ID: "call-unknown", Name: "unknown"},
	}
	emitter := &captureRuntimeEmitter{}
	pipeline := &Pipeline{
		Context: PipelineContextServices{Steering: &oneShotLoopGuardSteering{
			messages: []providers.Message{{Role: "user", Content: "change course"}},
		}},
		Runtime: PipelineRuntimeServices{Events: emitter},
	}

	if got := pipeline.ExecuteTools(
		context.Background(),
		context.Background(),
		ts,
		exec,
		1,
	); got.Control != ToolControlContinue {
		t.Fatalf("control = %v, want continue", got.Control)
	}
	if readOnly.executions != 1 || nonCancellable.executions != 1 {
		t.Fatalf("safe executions = read:%d commit:%d, want 1 each", readOnly.executions, nonCancellable.executions)
	}
	if cancellable.executions != 0 || unknown.executions != 0 {
		t.Fatalf("unsafe executions = write:%d unknown:%d, want 0", cancellable.executions, unknown.executions)
	}
	if len(exec.messages) != 4 {
		t.Fatalf("tool results = %d, want one per call", len(exec.messages))
	}
	for i, call := range exec.normalizedToolCalls {
		if exec.messages[i].Role != "tool" || exec.messages[i].ToolCallID != call.ID {
			t.Fatalf("result[%d] = %#v, want source-ordered result for %s", i, exec.messages[i], call.ID)
		}
	}
	if exec.messages[0].ToolResultStatus != providers.ToolResultStatusSuccess ||
		exec.messages[1].ToolResultStatus != "" ||
		exec.messages[2].ToolResultStatus != providers.ToolResultStatusSuccess ||
		exec.messages[3].ToolResultStatus != "" {
		t.Fatalf("steering result statuses = %#v", exec.messages)
	}
	if !strings.Contains(exec.messages[1].Content, "reissue it if it is still requested") ||
		!strings.Contains(exec.messages[3].Content, "omit it only if the user canceled or replaced it") {
		t.Fatalf("deferred results do not explain reconciliation: %#v", exec.messages)
	}

	decisions := make(map[string]ToolSteeringDecisionPayload)
	for _, event := range emitter.events {
		if event.kind != runtimeevents.KindAgentToolSteeringDecision {
			continue
		}
		payload := event.payload.(ToolSteeringDecisionPayload)
		decisions[payload.ToolCallID] = payload
	}
	if len(decisions) != 4 || decisions["call-read"].Decision != "finish" ||
		decisions["call-write"].Decision != "skip" || decisions["call-commit"].Decision != "finish" ||
		decisions["call-unknown"].Classification != string(tools.SteeringSafetyUnknown) {
		t.Fatalf("decisions = %#v", decisions)
	}
}

func TestPipelineSteeringArrivingDuringBatchDoesNotCancelCompletedCall(t *testing.T) {
	registry := tools.NewToolRegistry()
	first := &steeringSafetyTestTool{name: "first-write", safety: tools.SteeringSafetyCancellable}
	second := &steeringSafetyTestTool{name: "second-write", safety: tools.SteeringSafetyCancellable}
	registry.Register(first)
	registry.Register(second)
	agent := &AgentInstance{ID: "main", Tools: registry, Sessions: session.NewSessionManager("")}
	ts := &turnState{
		agent: agent, agentID: "main", turnID: "turn-delayed-steering",
		sessionKey: "session-delayed-steering", opts: processOptions{NoHistory: true},
	}
	exec := newTurnExecution(agent, ts.opts, nil, "", nil)
	exec.normalizedToolCalls = []providers.ToolCall{
		{ID: "call-first", Name: first.Name()},
		{ID: "call-second", Name: second.Name()},
	}
	pipeline := &Pipeline{Context: PipelineContextServices{Steering: &delayedSteering{
		messages: []providers.Message{{Role: "user", Content: "stop the second write"}},
	}}}

	pipeline.ExecuteTools(context.Background(), context.Background(), ts, exec, 1)
	if first.executions != 1 || second.executions != 0 {
		t.Fatalf("executions = first:%d second:%d, want 1 and 0", first.executions, second.executions)
	}
	if len(exec.messages) != 2 || exec.messages[0].ToolCallID != "call-first" ||
		exec.messages[1].ToolCallID != "call-second" {
		t.Fatalf("tool results = %#v, want one source-ordered result per call", exec.messages)
	}
}

func TestToolLoopRunnerAppendPendingSubTurnResult_PersistsAndIngests(t *testing.T) {
	sessionStore := session.NewSessionManager("")
	cm := &trackingContextManager{}
	pipeline := &Pipeline{Context: PipelineContextServices{Runtime: cm}}
	ts := &turnState{
		agent: &AgentInstance{
			Sessions: sessionStore,
		},
		sessionKey:     "session-subturn-result",
		pendingResults: make(chan *tools.ToolResult, 1),
	}
	ts.pendingResults <- &tools.ToolResult{ForLLM: "child result"}
	runner := &toolLoopRunner{
		p:       pipeline,
		turnCtx: context.Background(),
		ts:      ts,
	}

	runner.appendPendingSubTurnResult()
	if len(runner.messages) != 1 ||
		!strings.Contains(runner.messages[0].Content, "child result") {
		t.Fatalf("messages = %#v, want subturn result message", runner.messages)
	}
	history := sessionStore.GetHistory(ts.sessionKey)
	if len(history) != 1 || !strings.Contains(history[0].Content, "child result") {
		t.Fatalf("session history = %#v, want persisted subturn result", history)
	}
	persisted := ts.persistedMessagesSnapshot()
	if len(persisted) != 1 || !strings.Contains(persisted[0].Content, "child result") {
		t.Fatalf("persisted messages = %#v, want subturn result", persisted)
	}
	if got := cm.ingestCalls.Load(); got != 1 {
		t.Fatalf("ingest calls = %d, want 1", got)
	}
	if cm.lastIngest == nil || !strings.Contains(cm.lastIngest.Message.Content, "child result") {
		t.Fatalf("last ingest = %#v, want subturn result", cm.lastIngest)
	}
}

type repeatingFatalToolProvider struct {
	calls int
}

func (p *repeatingFatalToolProvider) Chat(
	ctx context.Context,
	messages []providers.Message,
	toolDefs []providers.ToolDefinition,
	model string,
	opts map[string]any,
) (*providers.LLMResponse, error) {
	p.calls++
	return &providers.LLMResponse{
		ToolCalls: []providers.ToolCall{{
			ID:        "call-fatal",
			Name:      "mcp_gpt_researcher_quick_search",
			Arguments: map[string]any{"query": "modere refresh"},
		}},
	}, nil
}

func (p *repeatingFatalToolProvider) GetDefaultModel() string {
	return "fatal-loop-model"
}

type repeatingFatalTool struct{}

func (t *repeatingFatalTool) Name() string { return "mcp_gpt_researcher_quick_search" }
func (t *repeatingFatalTool) Description() string {
	return "Always fails with a fatal MCP transport error"
}

func (t *repeatingFatalTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query": map[string]any{
				"type": "string",
			},
		},
	}
}

func (t *repeatingFatalTool) Execute(ctx context.Context, args map[string]any) *tools.ToolResult {
	err := `MCP tool execution failed: failed to call tool: connection closed: calling "tools/call": client is closing: invalid character 'ð' looking for beginning of value`
	return tools.ErrorResult(err)
}

func TestRunAgentLoop_AbortsRepeatedFatalToolTransportErrors(t *testing.T) {
	workspace := t.TempDir()
	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         workspace,
				ModelName:         "test-model",
				MaxTokens:         2048,
				MaxToolIterations: 20,
			},
		},
	}

	msgBus := bus.NewMessageBus()
	provider := &repeatingFatalToolProvider{}
	al := NewAgentLoop(cfg, msgBus, provider)
	al.RegisterTool(&repeatingFatalTool{})

	defaultAgent := al.registry.GetDefaultAgent()
	if defaultAgent == nil {
		t.Fatal("expected default agent")
	}

	response, err := al.runAgentLoop(context.Background(), defaultAgent, processOptions{
		SessionKey:      "session-fatal-tool-loop",
		Channel:         "cli",
		ChatID:          "direct",
		UserMessage:     "run research",
		DefaultResponse: defaultResponse,
		EnableSummary:   false,
		SendResponse:    false,
		InboundContext: &bus.InboundContext{
			Channel:  "cli",
			ChatID:   "direct",
			ChatType: "direct",
			SenderID: "tester",
		},
		RouteResult: &routing.ResolvedRoute{
			AgentID:   "main",
			Channel:   "cli",
			AccountID: routing.DefaultAccountID,
			SessionPolicy: routing.SessionPolicy{
				Dimensions: []string{"sender"},
			},
			MatchedBy: "default",
		},
		SessionScope: &session.SessionScope{
			Version:    session.ScopeVersionV1,
			AgentID:    "main",
			Channel:    "cli",
			Account:    routing.DefaultAccountID,
			Dimensions: []string{"sender"},
			Values: map[string]string{
				"sender": "tester",
			},
		},
	})
	if err != nil {
		t.Fatalf("runAgentLoop() error = %v", err)
	}
	if got, want := response, "I hit repeated backend tool transport errors while using `mcp_gpt_researcher_quick_search` and stopped instead of retrying indefinitely. Please try again."; got != want {
		t.Fatalf("runAgentLoop() response = %q, want %q", got, want)
	}
	if provider.calls != repeatedFatalToolErrorStreakLimit {
		t.Fatalf("provider calls = %d, want %d", provider.calls, repeatedFatalToolErrorStreakLimit)
	}
}

type fatalMCPServerTool struct{}

func (t *fatalMCPServerTool) Name() string { return "mcp_gpt_researcher_quick_search" }
func (t *fatalMCPServerTool) Description() string {
	return "Fails with a fatal MCP server transport error"
}

func (t *fatalMCPServerTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query": map[string]any{
				"type": "string",
			},
		},
	}
}
func (t *fatalMCPServerTool) MCPServerName() string { return "gpt_researcher" }
func (t *fatalMCPServerTool) Execute(ctx context.Context, args map[string]any) *tools.ToolResult {
	err := `MCP tool execution failed: failed to call tool: connection closed: calling "tools/call": client is closing: invalid character 'ð' looking for beginning of value`
	return tools.ErrorResult(err)
}

func TestRunAgentLoop_AbortsFatalMCPServerTransportErrorImmediately(t *testing.T) {
	workspace := t.TempDir()
	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         workspace,
				ModelName:         "test-model",
				MaxTokens:         2048,
				MaxToolIterations: 20,
			},
		},
	}

	msgBus := bus.NewMessageBus()
	provider := &repeatingFatalToolProvider{}
	al := NewAgentLoop(cfg, msgBus, provider)
	al.RegisterTool(&fatalMCPServerTool{})

	defaultAgent := al.registry.GetDefaultAgent()
	if defaultAgent == nil {
		t.Fatal("expected default agent")
	}

	response, err := al.runAgentLoop(context.Background(), defaultAgent, processOptions{
		SessionKey:      "session-fatal-mcp-server",
		Channel:         "cli",
		ChatID:          "direct",
		UserMessage:     "run research",
		DefaultResponse: defaultResponse,
		EnableSummary:   false,
		SendResponse:    false,
		InboundContext: &bus.InboundContext{
			Channel:  "cli",
			ChatID:   "direct",
			ChatType: "direct",
			SenderID: "tester",
		},
		RouteResult: &routing.ResolvedRoute{
			AgentID:   "main",
			Channel:   "cli",
			AccountID: routing.DefaultAccountID,
			SessionPolicy: routing.SessionPolicy{
				Dimensions: []string{"sender"},
			},
			MatchedBy: "default",
		},
		SessionScope: &session.SessionScope{
			Version:    session.ScopeVersionV1,
			AgentID:    "main",
			Channel:    "cli",
			Account:    routing.DefaultAccountID,
			Dimensions: []string{"sender"},
			Values: map[string]string{
				"sender": "tester",
			},
		},
	})
	if err != nil {
		t.Fatalf("runAgentLoop() error = %v", err)
	}
	want := "I hit a backend MCP transport error while using the `gpt_researcher` server and stopped instead of trying workarounds. Please restart or fix that MCP server, then try again."
	if response != want {
		t.Fatalf("runAgentLoop() response = %q, want %q", response, want)
	}
	if provider.calls != 1 {
		t.Fatalf("provider calls = %d, want 1", provider.calls)
	}
}
