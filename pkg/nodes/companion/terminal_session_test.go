package companion

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

func TestTerminalCoordinatorBindsOwnerAndAllowsOneAttachment(t *testing.T) {
	coordinator, broker := testTerminalCoordinator(t)
	plan := testTerminalOpenPlan(t, coordinator, "open_test")
	metadata, err := coordinator.Open(t.Context(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.State != TerminalSessionPendingAttach || broker.openCount() != 1 {
		t.Fatalf("opened terminal = (%#v, %d)", metadata, broker.openCount())
	}
	for name, mutate := range map[string]func(*nodes.TerminalOwner){
		"actor":     func(owner *nodes.TerminalOwner) { owner.ActorID = "actor_other" },
		"agent":     func(owner *nodes.TerminalOwner) { owner.AgentID = "agent_other" },
		"route":     func(owner *nodes.TerminalOwner) { owner.RouteID = "route_other" },
		"session":   func(owner *nodes.TerminalOwner) { owner.SessionID = "session_other" },
		"workspace": func(owner *nodes.TerminalOwner) { owner.WorkspaceID = "workspace_other" },
		"target":    func(owner *nodes.TerminalOwner) { owner.Target = "target_other" },
		"profile":   func(owner *nodes.TerminalOwner) { owner.Profile = "profile_other" },
	} {
		t.Run(name, func(t *testing.T) {
			owner := plan.Owner
			mutate(&owner)
			_, statusErr := coordinator.Status(nodes.TerminalSessionRequest{
				TerminalID: metadata.TerminalID,
				Owner:      owner,
			})
			if !errors.Is(statusErr, ErrTerminalOwnerMismatch) {
				t.Fatalf("Status() error = %v", statusErr)
			}
		})
	}
	request := nodes.TerminalSessionRequest{
		TerminalID: metadata.TerminalID,
		Owner:      plan.Owner,
	}
	attachment, err := coordinator.Attach(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Attach(request); !errors.Is(err, ErrTerminalAlreadyAttached) {
		t.Fatalf("second Attach() error = %v", err)
	}
	if err := attachment.Send(t.Context(), nodes.TerminalControlRequest{
		TerminalSessionRequest: request,
		Sequence:               1,
		IdempotencyKey:         "input-1",
		InputBase64:            base64.StdEncoding.EncodeToString([]byte("x")),
	}); err != nil {
		t.Fatal(err)
	}
	broker.session.events <- TerminalBrokerEvent{
		Version: AuthorityBrokerProtocolVersion, Type: TerminalEventAck,
		TerminalID: metadata.TerminalID, AcceptedSequence: 1, State: "live",
	}
	event := <-attachment.Events()
	if event.Type != TerminalEventAck || event.AcceptedSequence != 1 {
		t.Fatalf("attachment event = %#v", event)
	}
	if err := attachment.Close(); err != nil {
		t.Fatal(err)
	}
	closed, err := coordinator.Status(request)
	if err != nil || closed.State != TerminalSessionClosed ||
		closed.Reason != TerminalCloseDisconnected ||
		!closed.TerminationConfirmed {
		t.Fatalf("closed terminal = (%#v, %v)", closed, err)
	}
	if _, err := coordinator.Attach(request); !errors.Is(err, ErrTerminalAlreadyAttached) {
		t.Fatalf("reconnect Attach() error = %v", err)
	}
}

func TestTerminalCoordinatorOpenIsIdempotentWithoutDuplicatePTY(t *testing.T) {
	coordinator, broker := testTerminalCoordinator(t)
	plan := testTerminalOpenPlan(t, coordinator, "open_test")
	first, err := coordinator.Open(t.Context(), plan)
	if err != nil {
		t.Fatal(err)
	}
	second, err := coordinator.Open(t.Context(), plan)
	if err != nil || second.TerminalID != first.TerminalID || broker.openCount() != 1 {
		t.Fatalf("idempotent open = (%#v, %v, count %d)", second, err, broker.openCount())
	}
	changed := plan
	changed.IdempotencyKey = "terminal-open-other"
	changed, err = nodes.PrepareTerminalOpenPlan(
		changed,
		time.Unix(plan.PreparedAt, 0),
		time.Duration(plan.ExpiresAt-plan.PreparedAt)*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Open(t.Context(), changed); !errors.Is(err, ErrTerminalOpenConflict) {
		t.Fatalf("conflicting Open() error = %v", err)
	}
	if broker.openCount() != 1 {
		t.Fatalf("conflicting open allocated %d PTYs", broker.openCount())
	}
}

func TestTerminalCoordinatorDoesNotReplayAmbiguousOpen(t *testing.T) {
	coordinator, broker := testTerminalCoordinator(t)
	broker.openErr = ErrTerminalOutcomeUnknown
	plan := testTerminalOpenPlan(t, coordinator, "open_unknown")
	if _, err := coordinator.Open(t.Context(), plan); !errors.Is(err, ErrTerminalOutcomeUnknown) {
		t.Fatalf("first Open() error = %v", err)
	}
	broker.openErr = nil
	if _, err := coordinator.Open(t.Context(), plan); !errors.Is(err, ErrTerminalOutcomeUnknown) {
		t.Fatalf("repeated Open() error = %v", err)
	}
	if broker.openCount() != 1 {
		t.Fatalf("ambiguous open dispatched %d times", broker.openCount())
	}
}

func TestTerminalCoordinatorBoundsRetainedFailedOpenMetadata(t *testing.T) {
	coordinator, broker := testTerminalCoordinator(t)
	for index := 0; index < MaxTerminalMetadataRecords; index++ {
		coordinator.failedOpens[fmt.Sprintf("open_failed_%d", index)] = failedTerminalOpen{
			planHash:       strings.Repeat("a", 64),
			idempotencyKey: fmt.Sprintf("terminal-failed-%d", index),
			expiresAt:      coordinator.now().Add(time.Minute).Unix(),
		}
	}
	plan := testTerminalOpenPlan(t, coordinator, "open_new")
	if _, err := coordinator.Open(t.Context(), plan); err == nil ||
		!strings.Contains(err.Error(), "metadata limit") {
		t.Fatalf("metadata-bounded Open() error = %v", err)
	}
	if broker.openCount() != 0 || len(coordinator.failedOpens) != MaxTerminalMetadataRecords {
		t.Fatalf(
			"bounded state = (broker opens %d, failed opens %d)",
			broker.openCount(),
			len(coordinator.failedOpens),
		)
	}
	existingPlan := testTerminalOpenPlan(t, coordinator, "open_failed_0")
	coordinator.failedOpens[existingPlan.OpenID] = failedTerminalOpen{
		planHash:       existingPlan.PlanHash,
		idempotencyKey: existingPlan.IdempotencyKey,
		expiresAt:      existingPlan.ExpiresAt,
	}
	if _, err := coordinator.Open(t.Context(), existingPlan); !errors.Is(err, ErrTerminalOutcomeUnknown) {
		t.Fatalf("exact failed-open retry error = %v", err)
	}
}

func TestTerminalCoordinatorSingleFlightsOpenAndOwnsSessionContext(t *testing.T) {
	coordinator, broker := testTerminalCoordinator(t)
	broker.openGate = make(chan struct{})
	broker.openStarted = make(chan struct{}, 1)
	plan := testTerminalOpenPlan(t, coordinator, "open_singleflight")
	openCtx, cancelOpen := context.WithCancel(t.Context())
	firstDone := make(chan struct {
		metadata nodes.TerminalMetadata
		err      error
	}, 1)
	go func() {
		metadata, err := coordinator.Open(openCtx, plan)
		firstDone <- struct {
			metadata nodes.TerminalMetadata
			err      error
		}{metadata: metadata, err: err}
	}()
	<-broker.openStarted
	secondDone := make(chan struct {
		metadata nodes.TerminalMetadata
		err      error
	}, 1)
	go func() {
		metadata, err := coordinator.Open(t.Context(), plan)
		secondDone <- struct {
			metadata nodes.TerminalMetadata
			err      error
		}{metadata: metadata, err: err}
	}()
	close(broker.openGate)
	first := <-firstDone
	second := <-secondDone
	if first.err != nil || second.err != nil ||
		first.metadata.TerminalID != second.metadata.TerminalID ||
		broker.openCount() != 1 {
		t.Fatalf(
			"single-flight open = (%#v, %v), (%#v, %v), count %d",
			first.metadata,
			first.err,
			second.metadata,
			second.err,
			broker.openCount(),
		)
	}
	cancelOpen()
	if err := broker.openContextError(); err != nil {
		t.Fatalf("request context still owned live terminal: %v", err)
	}
}

func TestTerminalCoordinatorDeniesStaleAuthorityBeforeBroker(t *testing.T) {
	coordinator, broker := testTerminalCoordinator(t)
	base := testTerminalOpenPlan(t, coordinator, "open_test")
	for name, mutate := range map[string]func(*nodes.TerminalOpenPlan){
		"node":      func(plan *nodes.TerminalOpenPlan) { plan.NodeID = nodes.ID("node_other") },
		"catalog":   func(plan *nodes.TerminalOpenPlan) { plan.CatalogHash = strings.Repeat("c", 64) },
		"authority": func(plan *nodes.TerminalOpenPlan) { plan.AuthorityDigest = strings.Repeat("d", 64) },
		"profile":   func(plan *nodes.TerminalOpenPlan) { plan.Owner.Profile = "owner-other" },
		"scope":     func(plan *nodes.TerminalOpenPlan) { plan.WorkingScope = "other" },
		"expired": func(plan *nodes.TerminalOpenPlan) {
			coordinator.now = func() time.Time { return time.Unix(plan.ExpiresAt, 0) }
		},
	} {
		t.Run(name, func(t *testing.T) {
			plan := base
			mutate(&plan)
			if name != "expired" {
				var err error
				plan, err = nodes.PrepareTerminalOpenPlan(
					plan,
					time.Unix(base.PreparedAt, 0),
					time.Duration(base.ExpiresAt-base.PreparedAt)*time.Second,
				)
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := coordinator.Open(t.Context(), plan); !errors.Is(err, nodes.ErrCommandDenied) {
				t.Fatalf("Open() error = %v", err)
			}
			coordinator.now = func() time.Time { return time.Unix(1_700_000_000, 0) }
		})
	}
	if broker.openCount() != 0 {
		t.Fatalf("denied plans allocated %d PTYs", broker.openCount())
	}
}

func TestTerminalCoordinatorExpiresUnattachedSession(t *testing.T) {
	coordinator, broker := testTerminalCoordinator(t)
	coordinator.attachTimeout = 10 * time.Millisecond
	plan := testTerminalOpenPlan(t, coordinator, "open_test")
	metadata, err := coordinator.Open(t.Context(), plan)
	if err != nil {
		t.Fatal(err)
	}
	request := nodes.TerminalSessionRequest{TerminalID: metadata.TerminalID, Owner: plan.Owner}
	deadline := time.After(time.Second)
	for {
		status, statusErr := coordinator.Status(request)
		if statusErr != nil {
			t.Fatal(statusErr)
		}
		if status.State == TerminalSessionClosed {
			if status.Reason != TerminalCloseAttachTimeout || !status.TerminationConfirmed {
				t.Fatalf("expired terminal = %#v", status)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("unattached terminal did not expire")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if broker.session.closeControls() != 1 {
		t.Fatalf("attach expiry close controls = %d", broker.session.closeControls())
	}
	if _, err := coordinator.Attach(request); !errors.Is(err, ErrTerminalAlreadyAttached) {
		t.Fatalf("Attach() after expiry error = %v", err)
	}
}

func TestTerminalCoordinatorSerializesInputBeforeDisconnectClose(t *testing.T) {
	coordinator, broker := testTerminalCoordinator(t)
	plan := testTerminalOpenPlan(t, coordinator, "open_order")
	metadata, err := coordinator.Open(t.Context(), plan)
	if err != nil {
		t.Fatal(err)
	}
	request := nodes.TerminalSessionRequest{TerminalID: metadata.TerminalID, Owner: plan.Owner}
	attachment, err := coordinator.Attach(request)
	if err != nil {
		t.Fatal(err)
	}
	broker.session.sendGate = make(chan struct{})
	broker.session.sendStarted = make(chan struct{}, 1)
	sendDone := make(chan error, 1)
	go func() {
		sendDone <- attachment.Send(t.Context(), nodes.TerminalControlRequest{
			TerminalSessionRequest: request,
			Sequence:               1,
			IdempotencyKey:         "input-1",
			InputBase64:            base64.StdEncoding.EncodeToString([]byte("x")),
		})
	}()
	<-broker.session.sendStarted
	closeDone := make(chan error, 1)
	go func() { closeDone <- attachment.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("disconnect close overtook blocked input: %v", err)
	default:
	}
	close(broker.session.sendGate)
	if err := <-sendDone; err != nil {
		t.Fatal(err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	if got := broker.session.sequences(); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("broker control order = %v", got)
	}
}

func TestRuntimeExposesNoTerminalWithoutExplicitOwnerAuthority(t *testing.T) {
	policy := nodes.LocalCommandPolicy{
		Revision:          "test",
		AllowedCommands:   []string{},
		MaximumRisk:       nodes.RiskRead,
		MaxTimeoutSeconds: 30,
		MaxOutputBytes:    64 * 1024,
	}
	withoutBroker, err := NewRuntime(
		nodes.ID("node_test"),
		"test",
		policy,
		newMemoryInvocationLedger(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if withoutBroker.terminals != nil {
		t.Fatal("runtime without owner broker exposed terminal authority")
	}
	broker := &fakeTerminalBroker{
		session: &fakeTerminalBrokerSession{events: make(chan TerminalBrokerEvent, 8)},
	}
	deniedByPolicy, err := NewRuntime(
		nodes.ID("node_test"),
		"test",
		policy,
		newMemoryInvocationLedger(),
		WithShellBroker(validShellBrokerSnapshot(), broker),
	)
	if err != nil {
		t.Fatal(err)
	}
	if deniedByPolicy.terminals != nil {
		t.Fatal("owner broker bypassed deny-by-default local policy")
	}
}

type fakeTerminalBroker struct {
	mu          sync.Mutex
	opens       int
	session     *fakeTerminalBrokerSession
	openErr     error
	openGate    chan struct{}
	openStarted chan struct{}
	openContext context.Context
}

func (broker *fakeTerminalBroker) Execute(
	context.Context,
	ShellBrokerRequest,
) (ShellBrokerResult, error) {
	return ShellBrokerResult{}, errors.New("unexpected shell execution")
}

func (broker *fakeTerminalBroker) openTerminal(
	ctx context.Context,
	request TerminalBrokerOpenRequest,
) (terminalBrokerSession, TerminalBrokerEvent, error) {
	broker.mu.Lock()
	broker.opens++
	broker.openContext = ctx
	gate := broker.openGate
	started := broker.openStarted
	openErr := broker.openErr
	broker.mu.Unlock()
	if openErr != nil {
		return nil, TerminalBrokerEvent{}, openErr
	}
	if gate != nil {
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, TerminalBrokerEvent{}, ctx.Err()
		}
	}
	broker.mu.Lock()
	broker.session.id = "terminal_test"
	broker.mu.Unlock()
	return broker.session, TerminalBrokerEvent{
		Version:    AuthorityBrokerProtocolVersion,
		Type:       TerminalEventOpened,
		TerminalID: broker.session.id,
		State:      "live",
		StartedAt:  1_700_000_000,
	}, nil
}

func (broker *fakeTerminalBroker) openContextError() error {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	return broker.openContext.Err()
}

func (broker *fakeTerminalBroker) openCount() int {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	return broker.opens
}

type fakeTerminalBrokerSession struct {
	mu               sync.Mutex
	id               string
	events           chan TerminalBrokerEvent
	closeRequests    int
	closed           bool
	controlSequences []uint64
	sendGate         chan struct{}
	sendStarted      chan struct{}
}

func (session *fakeTerminalBrokerSession) ID() string { return session.id }

func (session *fakeTerminalBrokerSession) Send(
	_ context.Context,
	control TerminalBrokerControl,
) error {
	if _, err := control.validate(); err != nil {
		return err
	}
	session.mu.Lock()
	session.controlSequences = append(session.controlSequences, control.Sequence)
	gate := session.sendGate
	started := session.sendStarted
	session.mu.Unlock()
	if !control.Close && gate != nil {
		select {
		case started <- struct{}{}:
		default:
		}
		<-gate
	}
	if !control.Close {
		return nil
	}
	session.mu.Lock()
	session.closeRequests++
	session.mu.Unlock()
	go func() {
		session.events <- TerminalBrokerEvent{
			Version: AuthorityBrokerProtocolVersion, Type: TerminalEventAck,
			TerminalID: session.id, AcceptedSequence: control.Sequence, State: "live",
		}
		session.events <- TerminalBrokerEvent{
			Version: AuthorityBrokerProtocolVersion, Type: TerminalEventClosed,
			TerminalID: session.id, State: "closed", Reason: TerminalCloseRequested,
			StartedAt: 1_700_000_000, CompletedAt: 1_700_000_001,
			TerminationConfirmed: true,
		}
	}()
	return nil
}

func (session *fakeTerminalBrokerSession) Receive(
	ctx context.Context,
) (TerminalBrokerEvent, error) {
	select {
	case event := <-session.events:
		return event, nil
	case <-ctx.Done():
		return TerminalBrokerEvent{}, ctx.Err()
	}
}

func (session *fakeTerminalBrokerSession) Close() error {
	session.mu.Lock()
	session.closed = true
	session.mu.Unlock()
	return nil
}

func (session *fakeTerminalBrokerSession) closeControls() int {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.closeRequests
}

func (session *fakeTerminalBrokerSession) sequences() []uint64 {
	session.mu.Lock()
	defer session.mu.Unlock()
	return append([]uint64(nil), session.controlSequences...)
}

func testTerminalCoordinator(t *testing.T) (*TerminalCoordinator, *fakeTerminalBroker) {
	t.Helper()
	broker := &fakeTerminalBroker{
		session: &fakeTerminalBrokerSession{events: make(chan TerminalBrokerEvent, 8)},
	}
	policy := nodes.LocalCommandPolicy{
		Revision: "test",
		AllowedCommands: []string{
			"shell.exec.v1",
		},
		MaximumRisk:       nodes.RiskPrivileged,
		MaxTimeoutSeconds: 30,
		MaxOutputBytes:    64 * 1024,
	}
	runtime, err := NewRuntime(
		nodes.ID("node_test"),
		"test",
		policy,
		newMemoryInvocationLedger(),
		WithShellBroker(validShellBrokerSnapshot(), broker),
	)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.terminals == nil {
		t.Fatal("terminal coordinator is disabled")
	}
	runtime.terminals.now = func() time.Time { return time.Unix(1_700_000_000, 0) }
	return runtime.terminals, broker
}

func testTerminalOpenPlan(
	t *testing.T,
	coordinator *TerminalCoordinator,
	openID string,
) nodes.TerminalOpenPlan {
	t.Helper()
	owner := testCompanionTerminalOwner()
	owner.Profile = coordinator.profile.Alias
	plan, err := nodes.PrepareTerminalOpenPlan(nodes.TerminalOpenPlan{
		OpenID:          openID,
		IdempotencyKey:  "terminal-" + openID,
		NodeID:          coordinator.nodeID,
		Owner:           owner,
		CatalogHash:     coordinator.catalogHash,
		AuthorityDigest: coordinator.authorityHash,
		WorkingScope:    coordinator.profile.WorkingScopes[0],
		Columns:         80,
		Rows:            24,
		ApprovalMode:    "session_start",
	}, coordinator.now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func testCompanionTerminalOwner() nodes.TerminalOwner {
	return nodes.TerminalOwner{
		ActorID:     "actor_test",
		AgentID:     "agent_test",
		RouteID:     "route_test",
		SessionID:   "session_test",
		WorkspaceID: "workspace_test",
		Target:      "target_test",
		Profile:     "owner-root",
	}
}
