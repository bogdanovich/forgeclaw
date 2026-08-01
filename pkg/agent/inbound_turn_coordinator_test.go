// MintClaw - Ultra-lightweight personal AI agent

package agent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/session"
)

type finalResponseAdmissionTestBus struct {
	*bus.MessageBus
	publishErr error

	mu           sync.Mutex
	acked        []string
	released     []string
	releaseCause error
}

func (b *finalResponseAdmissionTestBus) PublishOutbound(
	ctx context.Context,
	msg bus.OutboundMessage,
) error {
	if b.publishErr != nil {
		return b.publishErr
	}
	return b.MessageBus.PublishOutbound(ctx, msg)
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
	coordinator := newInboundTurnCoordinator(al)
	target, ok := al.resolveSteeringTarget(msg)
	if !ok {
		t.Fatal("resolveSteeringTarget() rejected test inbound")
	}
	claim, active, claimed := coordinator.claimSession(target)
	if !claimed {
		t.Fatalf("claimSession() failed with active target %+v", active)
	}
	coordinator.runWorker(t.Context(), msg, target, claim)
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
