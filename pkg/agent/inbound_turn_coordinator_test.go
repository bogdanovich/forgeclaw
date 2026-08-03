// MintClaw - Ultra-lightweight personal AI agent

package agent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/commands"
	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/media"
	"github.com/bogdanovich/mintclaw/pkg/outbox"
	"github.com/bogdanovich/mintclaw/pkg/providers"
	"github.com/bogdanovich/mintclaw/pkg/session"
)

type finalResponseAdmissionTestBus struct {
	*bus.MessageBus
	publishErr     error
	publishResults []error
	publishCalls   int
	beforePublish  func(bus.OutboundMessage) error

	mu           sync.Mutex
	acked        []string
	released     []string
	releaseCause error
}

type failingRootTurnJournal struct {
	session.SessionStore
	err error
}

func (s *failingRootTurnJournal) AppendTurnMessage(
	_ context.Context,
	_ string,
	msg providers.Message,
) error {
	if msg.Role == "user" {
		return s.err
	}
	return errors.New("unexpected non-root journal append")
}

type countingAdmissionProvider struct {
	calls int
}

func (p *countingAdmissionProvider) Chat(
	context.Context,
	[]providers.Message,
	[]providers.ToolDefinition,
	string,
	map[string]any,
) (*providers.LLMResponse, error) {
	p.calls++
	return &providers.LLMResponse{Content: "must not run"}, nil
}

func (p *countingAdmissionProvider) GetDefaultModel() string { return "counting" }

func (b *finalResponseAdmissionTestBus) PublishOutbound(
	ctx context.Context,
	msg bus.OutboundMessage,
) error {
	if b.beforePublish != nil {
		if err := b.beforePublish(msg); err != nil {
			return err
		}
	}
	b.mu.Lock()
	if b.publishCalls < len(b.publishResults) {
		err := b.publishResults[b.publishCalls]
		b.publishCalls++
		b.mu.Unlock()
		if err != nil {
			return err
		}
		return b.MessageBus.PublishOutbound(ctx, msg)
	}
	b.mu.Unlock()
	if b.publishErr != nil {
		return b.publishErr
	}
	return b.MessageBus.PublishOutbound(ctx, msg)
}

func TestFinalResponseAdmissionPersistsOutboxBeforeBusPublish(t *testing.T) {
	al, _, msgBus, _, cleanup := newTestAgentLoop(t)
	defer cleanup()
	workspace := al.registry.GetDefaultAgent().Workspace
	coordinator := outbox.NewCoordinator()
	al.SetOutboundOutbox(coordinator)

	trackingBus := &finalResponseAdmissionTestBus{MessageBus: msgBus}
	trackingBus.beforePublish = func(msg bus.OutboundMessage) error {
		store, err := outbox.Open(workspace)
		if err != nil {
			return err
		}
		intent, err := store.Get(msg.DeliveryID)
		if err != nil {
			return err
		}
		if intent.Status != outbox.StatusPending {
			return errors.New("outbox intent was not pending before bus publish")
		}
		return nil
	}
	al.bus = trackingBus

	admission := al.publishResponseWithMetadataAndScopes(
		withOutboundSource(t.Context(), "spool-durable-final"),
		workspace,
		"main",
		"telegram",
		"chat-1",
		"session-1",
		"final reply",
		&bus.InboundContext{Channel: "telegram", ChatID: "chat-1"},
		finalResponseAlwaysPublish,
		bus.OutboundMetadata{},
		nil,
	)
	if !admission.permitsInboundAck() || admission.err != nil {
		t.Fatalf("admission = %+v, want durable acceptance", admission)
	}
	select {
	case msg := <-msgBus.OutboundChan():
		if msg.DeliveryID == "" {
			t.Fatal("published message has no delivery ID")
		}
	default:
		t.Fatal("durable final response was not published")
	}
}

func TestProcessMessageSyncAdmitsSystemCompletionOnceOnOriginRoute(t *testing.T) {
	tests := []struct {
		name    string
		sender  string
		content string
		raw     map[string]string
	}{
		{
			name:    "system completion",
			sender:  "subagent:worker",
			content: "Task 'worker' completed.\n\nResult:\nfinished",
		},
		{
			name:    "async completion",
			sender:  "async:spawn",
			content: asyncCompletionPrompt("spawn", "finished"),
			raw: systemFollowUpAsyncCompletionRaw(
				&bus.InboundContext{Channel: "telegram", ChatID: "chat-1", ChatType: "direct"},
				"telegram",
				"chat-1",
				"completion-durable",
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			al, _, msgBus, _, cleanup := newTestAgentLoop(t)
			defer cleanup()
			coordinator := outbox.NewCoordinator()
			al.SetOutboundOutbox(coordinator)
			agent := al.registry.GetDefaultAgent()

			msg := bus.InboundMessage{
				Channel:  "system",
				ChatID:   "telegram:chat-1",
				SenderID: tt.sender,
				SpoolID:  "spool-" + strings.ReplaceAll(tt.name, " ", "-"),
				Content:  tt.content,
				Context: bus.InboundContext{
					Channel:  "system",
					ChatID:   "telegram:chat-1",
					ChatType: "direct",
					SenderID: tt.sender,
					Raw:      tt.raw,
				},
			}
			admission := al.processMessageSync(withOutboundSource(t.Context(), msg.SpoolID), msg)
			if !admission.permitsInboundAck() || admission.err != nil {
				t.Fatalf("processMessageSync() admission = %+v", admission)
			}

			select {
			case outbound := <-msgBus.OutboundChan():
				if outbound.Context.Channel != "telegram" || outbound.Context.ChatID != "chat-1" ||
					outbound.DeliveryID == "" || outbound.SessionKey != session.BuildMainSessionKey(agent.ID) {
					t.Fatalf("outbound = %+v", outbound)
				}
				store, err := outbox.Open(agent.Workspace)
				if err != nil {
					t.Fatalf("Open(outbox) error = %v", err)
				}
				intent, err := store.Get(outbound.DeliveryID)
				if err != nil || intent.Message == nil || intent.Message.Content != outbound.Content {
					t.Fatalf("durable intent = %+v, %v", intent, err)
				}
			default:
				t.Fatal("system completion did not publish durable origin response")
			}
			select {
			case duplicate := <-msgBus.OutboundChan():
				t.Fatalf("system completion published twice: %+v", duplicate)
			default:
			}

			replay := al.processMessageSync(withOutboundSource(t.Context(), msg.SpoolID), msg)
			if !replay.permitsInboundAck() || replay.err != nil {
				t.Fatalf("processMessageSync(replay) admission = %+v", replay)
			}
			select {
			case duplicate := <-msgBus.OutboundChan():
				t.Fatalf("system completion replay published duplicate: %+v", duplicate)
			default:
			}
		})
	}
}

func TestControlReplyFailureReleasesInbound(t *testing.T) {
	al, _, msgBus, _, cleanup := newTestAgentLoop(t)
	defer cleanup()
	al.SetOutboundOutbox(outbox.NewCoordinator())
	trackingBus := &finalResponseAdmissionTestBus{
		MessageBus: msgBus,
		publishErr: errors.New("bus unavailable"),
	}
	al.bus = trackingBus
	msg := finalResponseAdmissionInboundMessage("spool-control")
	ctx := withOutboundSource(t.Context(), msg.SpoolID)
	agent := al.registry.GetDefaultAgent()
	admission := al.publishStopReply(
		ctx,
		msg,
		newRuntimeSessionScope(agent.Workspace, msg.SessionKey),
		agent.ID,
		commands.StopResult{Stopped: true},
		nil,
	)
	al.settleInboundAdmission(ctx, msg, admission)

	acked, released, cause := trackingBus.ownership()
	if len(acked) != 0 || len(released) != 1 || released[0] != msg.SpoolID || cause == nil {
		t.Fatalf("ownership = acked %v released %v cause %v", acked, released, cause)
	}
}

func TestInteractionNoticeFailureReleasesInbound(t *testing.T) {
	al, _, msgBus, _, cleanup := newTestAgentLoop(t)
	defer cleanup()
	al.SetOutboundOutbox(outbox.NewCoordinator())
	trackingBus := &finalResponseAdmissionTestBus{
		MessageBus: msgBus,
		publishErr: errors.New("bus unavailable"),
	}
	al.bus = trackingBus
	msg := finalResponseAdmissionInboundMessage("spool-interaction-notice")
	ctx := withOutboundSource(t.Context(), msg.SpoolID)
	admission := al.publishInteractionNoticeAdmission(
		ctx,
		msg,
		msg.SessionKey,
		"The pending interaction could not be canceled; please retry.",
	)
	al.settleInboundAdmission(ctx, msg, admission)

	acked, released, cause := trackingBus.ownership()
	if len(acked) != 0 || len(released) != 1 || released[0] != msg.SpoolID || cause == nil {
		t.Fatalf("ownership = acked %v released %v cause %v", acked, released, cause)
	}
}

func (b *finalResponseAdmissionTestBus) AckInbound(
	ctx context.Context,
	msg bus.InboundMessage,
) error {
	b.mu.Lock()
	b.acked = append(b.acked, msg.SpoolID)
	b.mu.Unlock()
	return b.MessageBus.AckInbound(ctx, msg)
}

func (b *finalResponseAdmissionTestBus) ReleaseInbound(
	ctx context.Context,
	msg bus.InboundMessage,
	cause error,
) error {
	b.mu.Lock()
	b.released = append(b.released, msg.SpoolID)
	b.releaseCause = cause
	b.mu.Unlock()
	return b.MessageBus.ReleaseInbound(ctx, msg, cause)
}

func (b *finalResponseAdmissionTestBus) ownership() (acked, released []string, cause error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.acked...), append([]string(nil), b.released...), b.releaseCause
}

func TestFinalResponseAdmissionRejectsClosedBus(t *testing.T) {
	al, _, msgBus, _, cleanup := newTestAgentLoop(t)
	defer cleanup()
	msgBus.Close()

	admission := al.publishResponseWithMetadataAndScopes(
		t.Context(),
		al.registry.GetDefaultAgent().Workspace,
		"main",
		"telegram",
		"chat-1",
		"session-1",
		"final reply",
		&bus.InboundContext{Channel: "telegram", ChatID: "chat-1"},
		finalResponseAlwaysPublish,
		bus.OutboundMetadata{},
		nil,
	)

	if admission.permitsInboundAck() {
		t.Fatalf("closed bus admission = %+v, want rejection", admission)
	}
	if !errors.Is(admission.err, bus.ErrBusClosed) {
		t.Fatalf("closed bus admission error = %v, want %v", admission.err, bus.ErrBusClosed)
	}
}

func TestInboundTurnCoordinatorAcknowledgesAcceptedFinalResponse(t *testing.T) {
	al, _, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
	defer cleanup()
	trackingBus := &finalResponseAdmissionTestBus{MessageBus: al.bus.(*bus.MessageBus)}
	al.bus = trackingBus
	msg := finalResponseAdmissionInboundMessage("spool-accepted")

	runFinalResponseAdmissionTurn(t, al, msg)

	acked, released, cause := trackingBus.ownership()
	if len(acked) != 1 || acked[0] != msg.SpoolID || len(released) != 0 || cause != nil {
		t.Fatalf("accepted ownership = acked:%v released:%v cause:%v", acked, released, cause)
	}
	select {
	case outbound := <-trackingBus.OutboundChan():
		if outbound.Content != "Hello! How can I help you today?" {
			t.Fatalf("outbound content = %q", outbound.Content)
		}
	default:
		t.Fatal("accepted final response was not queued")
	}
}

func TestInboundTurnCoordinatorReleasesRootJournalFailuresBeforeLLM(t *testing.T) {
	for _, stage := range []string{"append", "flush", "rename", "fsync"} {
		t.Run(stage, func(t *testing.T) {
			provider := &countingAdmissionProvider{}
			al, agent, cleanup := newTurnCoordTestLoop(t, provider)
			defer cleanup()
			events := al.SubscribeEvents(8)
			defer al.UnsubscribeEvents(events.ID)
			journalErr := errors.New("injected " + stage + " failure")
			agent.Sessions = &failingRootTurnJournal{
				SessionStore: session.NewSessionManager(""),
				err:          journalErr,
			}
			trackingBus := &finalResponseAdmissionTestBus{MessageBus: al.bus.(*bus.MessageBus)}
			al.bus = trackingBus
			msg := finalResponseAdmissionInboundMessage("spool-root-" + stage)

			runFinalResponseAdmissionTurn(t, al, msg)

			acked, released, cause := trackingBus.ownership()
			if len(acked) != 0 || len(released) != 1 || released[0] != msg.SpoolID {
				t.Fatalf("journal failure ownership = acked:%v released:%v", acked, released)
			}
			if !errors.Is(cause, journalErr) {
				t.Fatalf("release cause = %v, want %v", cause, journalErr)
			}
			if provider.calls != 0 {
				t.Fatalf("failure executed provider %d times", provider.calls)
			}
			for {
				select {
				case event := <-events.C:
					if event.Kind != EventKindTurnEnd {
						continue
					}
					payload, ok := event.Payload.(TurnEndPayload)
					if !ok || payload.Status != TurnEndStatusError {
						t.Fatalf("turn end = %#v, want error", event.Payload)
					}
					return
				case <-time.After(time.Second):
					t.Fatal("timed out waiting for error turn end")
				}
			}
		})
	}
}

func TestInboundTurnCoordinatorReleasesRejectedFinalResponse(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "queue rejection", err: errors.New("outbound queue rejected")},
		{name: "canceled admission", err: context.Canceled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			al, _, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
			defer cleanup()
			trackingBus := &finalResponseAdmissionTestBus{
				MessageBus: al.bus.(*bus.MessageBus),
				publishErr: tt.err,
			}
			al.bus = trackingBus
			msg := finalResponseAdmissionInboundMessage("spool-rejected")

			runFinalResponseAdmissionTurn(t, al, msg)

			acked, released, cause := trackingBus.ownership()
			if len(acked) != 0 || len(released) != 1 || released[0] != msg.SpoolID {
				t.Fatalf("rejected ownership = acked:%v released:%v", acked, released)
			}
			if !errors.Is(cause, tt.err) {
				t.Fatalf("release cause = %v, want %v", cause, tt.err)
			}
		})
	}
}

func TestInboundTurnCoordinatorReleasesOriginalAndSteeringAfterAggregateRejection(t *testing.T) {
	rejection := errors.New("aggregate outbound rejected")
	al, _, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
	defer cleanup()
	trackingBus := &finalResponseAdmissionTestBus{
		MessageBus: al.bus.(*bus.MessageBus),
		publishErr: rejection,
	}
	al.bus = trackingBus
	msg := finalResponseAdmissionInboundMessage("spool-original")
	coordinator, target, claim := prepareFinalResponseAdmissionTurn(t, al, msg, "spool-steering")

	coordinator.runWorker(t.Context(), msg, target, claim)

	acked, released, cause := trackingBus.ownership()
	if len(acked) != 0 || !containsExactly(released, "spool-original", "spool-steering") {
		t.Fatalf("rejected aggregate ownership = acked:%v released:%v", acked, released)
	}
	if !errors.Is(cause, rejection) {
		t.Fatalf("release cause = %v, want %v", cause, rejection)
	}
}

func TestInboundTurnCoordinatorReleasesActiveTurnSteeringAfterAggregateRejection(t *testing.T) {
	rejection := errors.New("aggregate outbound rejected")
	al, _, cleanup := newTurnCoordTestLoop(t, &simpleConvProvider{})
	defer cleanup()
	trackingBus := &finalResponseAdmissionTestBus{
		MessageBus: al.bus.(*bus.MessageBus),
		publishErr: rejection,
	}
	al.bus = trackingBus
	msg := finalResponseAdmissionInboundMessage("spool-original")
	coordinator, target, claim := prepareFinalResponseAdmissionTurnForSender(
		t,
		al,
		msg,
		"spool-steering",
		msg.SenderID,
	)

	coordinator.runWorker(t.Context(), msg, target, claim)

	acked, released, cause := trackingBus.ownership()
	if len(acked) != 0 || !containsExactly(released, "spool-original", "spool-steering") {
		t.Fatalf("rejected active-turn aggregate ownership = acked:%v released:%v", acked, released)
	}
	if !errors.Is(cause, rejection) {
		t.Fatalf("release cause = %v, want %v", cause, rejection)
	}
}

func TestInboundTurnCoordinatorSettlesOriginalAndSteeringAdmissionsIndependently(t *testing.T) {
	rejection := errors.New("outbound rejected")
	tests := []struct {
		name           string
		publishResults []error
		wantAcked      []string
		wantReleased   []string
	}{
		{
			name:           "rejected original error and accepted continuation",
			publishResults: []error{rejection, nil},
			wantAcked:      []string{"spool-steering"},
			wantReleased:   []string{"spool-original"},
		},
		{
			name:           "accepted original error and rejected continuation",
			publishResults: []error{nil, rejection},
			wantAcked:      []string{"spool-original"},
			wantReleased:   []string{"spool-steering"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := &sequenceProvider{
				errors: []error{errors.New("provider unavailable")},
				responses: []*providers.LLMResponse{
					nil,
					{Content: "continuation response", FinishReason: "stop"},
				},
			}
			al, _, cleanup := newTurnCoordTestLoop(t, provider)
			defer cleanup()
			trackingBus := &finalResponseAdmissionTestBus{
				MessageBus:     al.bus.(*bus.MessageBus),
				publishResults: append([]error(nil), tt.publishResults...),
			}
			al.bus = trackingBus
			msg := finalResponseAdmissionInboundMessage("spool-original")
			coordinator, target, claim := prepareFinalResponseAdmissionTurn(t, al, msg, "spool-steering")

			coordinator.runWorker(t.Context(), msg, target, claim)

			acked, released, _ := trackingBus.ownership()
			if !containsExactly(acked, tt.wantAcked...) || !containsExactly(released, tt.wantReleased...) {
				t.Fatalf("ownership = acked:%v released:%v", acked, released)
			}
		})
	}
}

func TestInboundTurnCoordinatorSettlesHandledNoOutputIndependently(t *testing.T) {
	rejection := errors.New("aggregate outbound rejected")
	handledResponse := &providers.LLMResponse{
		Content: "Delivering the result now.",
		ToolCalls: []providers.ToolCall{{
			ID:        "call-handled-user",
			Type:      "function",
			Name:      "handled_user_tool",
			Arguments: map[string]any{},
		}},
	}
	textResponse := &providers.LLMResponse{Content: "aggregate text", FinishReason: "stop"}
	handledTerminalResponse := &providers.LLMResponse{}
	tests := []struct {
		name         string
		responses    []*providers.LLMResponse
		steeringID   string
		wantAcked    []string
		wantReleased []string
	}{
		{
			name:         "handled original does not depend on continuation aggregate",
			responses:    []*providers.LLMResponse{handledResponse, handledTerminalResponse, textResponse},
			steeringID:   "user-2",
			wantAcked:    []string{"spool-original"},
			wantReleased: []string{"spool-steering"},
		},
		{
			name:         "handled steering does not depend on original aggregate",
			responses:    []*providers.LLMResponse{textResponse, handledResponse, handledTerminalResponse},
			steeringID:   "user-2",
			wantAcked:    []string{"spool-steering"},
			wantReleased: []string{"spool-original"},
		},
		{
			name:       "handled active-turn steering does not wait for aggregate admission",
			responses:  []*providers.LLMResponse{handledResponse, handledTerminalResponse},
			steeringID: "user-1",
			wantAcked:  []string{"spool-original", "spool-steering"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := &sequenceProvider{responses: tt.responses}
			al, agent, cleanup := newTurnCoordTestLoop(t, provider)
			defer cleanup()
			agent.Tools.Register(&handledUserTool{})
			trackingBus := &finalResponseAdmissionTestBus{
				MessageBus:     al.bus.(*bus.MessageBus),
				publishResults: []error{nil, rejection},
			}
			al.bus = trackingBus
			store := media.NewFileMediaStore()
			al.SetMediaStore(store)
			al.SetChannelManager(newStartedTestChannelManager(
				t,
				trackingBus.MessageBus,
				store,
				"telegram",
				&fakeMediaChannel{fakeChannel: fakeChannel{id: "rid-telegram"}},
			))
			msg := finalResponseAdmissionInboundMessage("spool-original")
			coordinator, target, claim := prepareFinalResponseAdmissionTurnForSender(
				t,
				al,
				msg,
				"spool-steering",
				tt.steeringID,
			)

			coordinator.runWorker(t.Context(), msg, target, claim)

			acked, released, _ := trackingBus.ownership()
			if !containsExactly(acked, tt.wantAcked...) || !containsExactly(released, tt.wantReleased...) {
				t.Fatalf("ownership = acked:%v released:%v", acked, released)
			}
		})
	}
}

func finalResponseAdmissionInboundMessage(spoolID string) bus.InboundMessage {
	return testInboundMessage(bus.InboundMessage{
		SpoolID:  spoolID,
		Channel:  "telegram",
		ChatID:   "chat-1",
		SenderID: "user-1",
		Content:  "hello",
	})
}

func runFinalResponseAdmissionTurn(t *testing.T, al *AgentLoop, msg bus.InboundMessage) {
	t.Helper()
	coordinator, target, claim := prepareFinalResponseAdmissionTurn(t, al, msg, "")
	coordinator.runWorker(t.Context(), msg, target, claim)
}

func prepareFinalResponseAdmissionTurn(
	t *testing.T,
	al *AgentLoop,
	msg bus.InboundMessage,
	steeringSpoolID string,
) (*inboundTurnCoordinator, *inboundDispatchTarget, *runtimeSessionClaim) {
	return prepareFinalResponseAdmissionTurnForSender(t, al, msg, steeringSpoolID, "user-2")
}

func prepareFinalResponseAdmissionTurnForSender(
	t *testing.T,
	al *AgentLoop,
	msg bus.InboundMessage,
	steeringSpoolID string,
	steeringSenderID string,
) (*inboundTurnCoordinator, *inboundDispatchTarget, *runtimeSessionClaim) {
	t.Helper()
	coordinator := newInboundTurnCoordinator(al)
	target, ok := al.resolveSteeringTarget(msg)
	if !ok {
		t.Fatal("resolveSteeringTarget() rejected test inbound")
	}
	if steeringSpoolID != "" {
		err := al.enqueueSteeringMessageWithSender(
			target.runtimeSessionScope(),
			target.Agent.ID,
			steeringSenderID,
			providers.Message{
				Role:           "user",
				Content:        "queued steering",
				InboundSpoolID: steeringSpoolID,
			},
		)
		if err != nil {
			t.Fatalf("enqueueSteeringMessageWithSender() error = %v", err)
		}
	}
	claim, active, claimed := coordinator.claimSession(target)
	if !claimed {
		t.Fatalf("claimSession() failed with active target %+v", active)
	}
	return coordinator, target, claim
}

func containsExactly(values []string, wants ...string) bool {
	if len(values) != len(wants) {
		return false
	}
	remaining := make(map[string]int, len(wants))
	for _, want := range wants {
		remaining[want]++
	}
	for _, value := range values {
		if remaining[value] == 0 {
			return false
		}
		remaining[value]--
	}
	return true
}

func TestAcquireTurnCapacityDoesNotHoldAdmissionWhileWaitingForWorker(t *testing.T) {
	al := &AgentLoop{
		workerSem: make(chan struct{}, 1),
		agentTurnAdmissions: &agentTurnAdmissionController{
			limits:  map[string]int{"agent-a": 1},
			active:  make(map[string]int),
			changed: make(chan struct{}),
		},
	}
	al.workerSem <- struct{}{}
	coordinator := newInboundTurnCoordinator(al)
	al.agentTurnAdmissions.mu.Lock()
	admissionReleased := al.agentTurnAdmissions.changed
	al.agentTurnAdmissions.mu.Unlock()

	capacityDone := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() {
		_, release, err := coordinator.acquireTurnCapacity(ctx, "agent-a")
		if err == nil {
			release()
		}
		capacityDone <- err
	}()
	select {
	case <-admissionReleased:
	case <-time.After(time.Second):
		t.Fatal("queued turn retained agent admission while waiting for worker")
	}

	// The queued inbound turn must release agent-a while the only worker is
	// occupied, allowing the running worker to delegate to agent-a.
	delegateCtx, delegateCancel := context.WithTimeout(context.Background(), time.Second)
	_, releaseDelegate, err := al.acquireAgentTurn(delegateCtx, "agent-a")
	delegateCancel()
	if err != nil {
		t.Fatalf("delegate acquireAgentTurn() error = %v", err)
	}
	releaseDelegate()

	<-al.workerSem
	select {
	case err = <-capacityDone:
		if err != nil {
			t.Fatalf("acquireTurnCapacity() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for capacity acquisition")
	}
}

func coordinatorTestTarget(routeScopeKey, sessionKey string) *inboundDispatchTarget {
	return &inboundDispatchTarget{
		Agent:         &AgentInstance{ID: "main", Workspace: "/test/main"},
		RouteClaimKey: runtimeRouteClaimKey(routeScopeKey, ""),
		Allocation: session.Allocation{
			RouteScopeKey: routeScopeKey,
		},
		SessionKey: sessionKey,
	}
}

func TestInboundTurnCoordinatorClaimSessionSerializesSession(t *testing.T) {
	al := &AgentLoop{}
	coord := newInboundTurnCoordinator(al)

	firstTarget := coordinatorTestTarget("route-1", "session-1")
	claim, _, claimed := coord.claimSession(firstTarget)
	if !claimed {
		t.Fatal("expected first claim to succeed")
	}
	if claim == nil || claim.placeholder == nil {
		t.Fatal("expected claim with placeholder")
	}
	if claim.scope.sessionKey != "session-1" {
		t.Fatalf("claim session key = %q, want session-1", claim.scope.sessionKey)
	}
	if !isPendingTurnState(claim.placeholder) {
		t.Fatalf("placeholder turn id = %q, want pending turn", claim.placeholder.turnID)
	}
	if got := al.getActiveTurnState(firstTarget.runtimeSessionScope()); got != claim.placeholder {
		t.Fatalf("active turn = %p, want placeholder %p", got, claim.placeholder)
	}

	second, activeTarget, claimed := coord.claimSession(coordinatorTestTarget("route-1", "session-2"))
	if claimed {
		t.Fatalf("expected second claim to fail, got placeholder %p", second)
	}
	if activeTarget.SessionKey != "session-1" {
		t.Fatalf("active session key = %q, want session-1", activeTarget.SessionKey)
	}
	if activeTarget != firstTarget {
		t.Fatal("route claim did not retain the original dispatch target")
	}
	if got := al.getActiveTurnState(firstTarget.runtimeSessionScope()); got != claim.placeholder {
		t.Fatalf("active turn changed after rejected claim: got %p, want %p", got, claim.placeholder)
	}
}

func TestInboundTurnCoordinatorCleanupOnlyClearsOwnedPlaceholder(t *testing.T) {
	al := &AgentLoop{}
	coord := newInboundTurnCoordinator(al)

	first, _, claimed := coord.claimSession(coordinatorTestTarget("route-1", "session-1"))
	if !claimed {
		t.Fatal("expected first claim")
	}

	replacement := &turnState{
		turnID:     makePendingTurnID("session-1", al.turnSeq.Add(1)),
		workspace:  first.scope.workspace,
		sessionKey: first.scope.sessionKey,
		phase:      TurnPhaseSetup,
	}
	al.activeTurnStates.Store(first.scope, replacement)

	first.releaseIfOwned()
	if got := al.getActiveTurnState(first.scope); got != replacement {
		t.Fatalf("cleanup removed unowned placeholder: got %p, want replacement %p", got, replacement)
	}

	replacementClaim := &runtimeSessionClaim{
		al:          al,
		scope:       first.scope,
		placeholder: replacement,
	}
	replacementClaim.releaseIfOwned()
	if got := al.getActiveTurnState(first.scope); got != nil {
		t.Fatalf("cleanup left owned placeholder active: got %p", got)
	}
}

func TestInboundTurnCoordinatorPinsFollowUpAcrossCalendarBoundary(t *testing.T) {
	al, cfg, _, _, cleanup := newTestAgentLoop(t)
	defer cleanup()
	cfg.Session.Lifecycle = &config.SessionLifecycleConfig{
		Strategy: "calendar",
		Period:   "day",
		Timezone: "UTC",
	}
	now := time.Date(2026, 7, 17, 23, 59, 0, 0, time.UTC)
	al.sessionNow = func() time.Time { return now }
	msg := bus.NormalizeInboundMessage(bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  "telegram",
			ChatID:   "chat-1",
			ChatType: "direct",
			SenderID: "telegram:42",
		},
		Content: "first",
	})

	initial, ok := al.resolveSteeringTarget(msg)
	if !ok {
		t.Fatal("resolveSteeringTarget() failed for initial message")
	}
	coord := newInboundTurnCoordinator(al)
	claim, _, claimed := coord.claimSession(initial)
	if !claimed {
		t.Fatal("initial route claim failed")
	}
	defer claim.releaseIfOwned()

	now = now.Add(2 * time.Minute)
	followUp, ok := al.resolveSteeringTarget(msg)
	if !ok {
		t.Fatal("resolveSteeringTarget() failed for follow-up")
	}
	if followUp != initial || followUp.SessionKey != initial.SessionKey {
		t.Fatal("follow-up escaped the active epoch at calendar boundary")
	}
}

func TestInboundTurnCoordinatorFollowUpExtendsIdleEpoch(t *testing.T) {
	al, cfg, _, _, cleanup := newTestAgentLoop(t)
	defer cleanup()
	cfg.Session.Lifecycle = &config.SessionLifecycleConfig{
		Strategy:           "idle",
		IdleTimeoutMinutes: 30,
	}
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	al.sessionNow = func() time.Time { return now }
	msg := bus.NormalizeInboundMessage(bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  "telegram",
			ChatID:   "chat-1",
			ChatType: "direct",
			SenderID: "telegram:42",
		},
		Content: "first",
	})

	initial, ok := al.resolveSteeringTarget(msg)
	if !ok {
		t.Fatal("resolveSteeringTarget() failed for initial message")
	}
	coord := newInboundTurnCoordinator(al)
	claim, _, claimed := coord.claimSession(initial)
	if !claimed {
		t.Fatal("initial route claim failed")
	}

	now = now.Add(20 * time.Minute)
	followUp, ok := al.resolveSteeringTarget(msg)
	if !ok || followUp.SessionKey != initial.SessionKey {
		t.Fatal("follow-up did not remain in the active idle epoch")
	}
	claim.releaseIfOwned()

	now = now.Add(20 * time.Minute)
	next, ok := al.resolveSteeringTarget(msg)
	if !ok {
		t.Fatal("resolveSteeringTarget() failed after active turn")
	}
	if next.SessionKey != initial.SessionKey {
		t.Fatal("idle epoch rotated relative to initial activity instead of follow-up activity")
	}
}
