package gateway

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/fileutil"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
	nodews "github.com/bogdanovich/mintclaw/pkg/nodes/ws"
	"github.com/bogdanovich/mintclaw/pkg/tools"
)

type fakeNodeAdmissionHandler struct {
	beforeCommit  *sync.WaitGroup
	invocation    nodes.InvocationRecord
	invocationErr error
	invokeCalls   atomic.Int32
	writeCalls    atomic.Int32
	queryCalls    atomic.Int32
	cancelCalls   atomic.Int32
}

func (*fakeNodeAdmissionHandler) ServeHTTP(http.ResponseWriter, *http.Request) {}

func (*fakeNodeAdmissionHandler) Close(context.Context) error { return nil }

func (*fakeNodeAdmissionHandler) WithPreparationAuthority(
	_ nodes.ID,
	_ string,
	_ string,
	operation func(nodes.Registration, nodes.CommandApproval) error,
) (nodes.CommandApproval, error) {
	approval := nodes.CommandApproval{}
	return approval, operation(nodes.Registration{}, approval)
}

func (handler *fakeNodeAdmissionHandler) Invoke(
	_ context.Context,
	_ nodes.ID,
	_ nodes.ExecutionPlan,
	commit func() error,
) (json.RawMessage, bool, error) {
	handler.invokeCalls.Add(1)
	if handler.beforeCommit != nil {
		handler.beforeCommit.Done()
		handler.beforeCommit.Wait()
	}
	if err := commit(); err != nil {
		return nil, false, err
	}
	handler.writeCalls.Add(1)
	return json.RawMessage(`{"value":"ok"}`), true, nil
}

func (handler *fakeNodeAdmissionHandler) Invocation(
	context.Context,
	nodes.ID,
	string,
) (nodes.InvocationRecord, error) {
	handler.queryCalls.Add(1)
	if handler.invocationErr != nil {
		return nodes.InvocationRecord{}, handler.invocationErr
	}
	return handler.invocation, nil
}

func (handler *fakeNodeAdmissionHandler) CancelInvocation(
	context.Context,
	nodes.ID,
	string,
) (nodes.InvocationRecord, error) {
	handler.cancelCalls.Add(1)
	return handler.invocation, nil
}

func TestNodeInvocationSourceGrantsOneDispatchWinner(t *testing.T) {
	var beforeCommit sync.WaitGroup
	beforeCommit.Add(2)
	handler := &fakeNodeAdmissionHandler{beforeCommit: &beforeCommit}
	source := newTestNodeInvocationSource(t, handler)
	descriptor, plan, owner := testGatewayInvocation(t)
	if _, _, err := source.PrepareInvocation(
		"builder-node",
		"build",
		owner.ToolCallID,
		nodes.GatewayInvocationPrincipal{
			AgentID: plan.AgentID, SessionID: plan.SessionID, ActorID: plan.ActorID,
		},
		plan,
		descriptor,
		true,
		func(tools.NodeDiscoveryRecord) error { return nil },
	); err != nil {
		t.Fatal(err)
	}

	type result struct {
		dispatched bool
		err        error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			_, dispatched, err := source.DispatchInvocation(
				t.Context(),
				owner,
				plan.InvocationID,
				plan.PlanHash,
			)
			results <- result{dispatched: dispatched, err: err}
		}()
	}

	var successes, duplicates int
	for range 2 {
		got := <-results
		switch {
		case got.err == nil && got.dispatched:
			successes++
		case errors.Is(got.err, nodes.ErrGatewayInvocationDispatched) && !got.dispatched:
			duplicates++
		default:
			t.Fatalf("unexpected dispatch result = %#v", got)
		}
	}
	if successes != 1 || duplicates != 1 ||
		handler.invokeCalls.Load() != 2 || handler.writeCalls.Load() != 1 {
		t.Fatalf(
			"dispatches = successes %d, duplicates %d, invokes %d, writes %d",
			successes,
			duplicates,
			handler.invokeCalls.Load(),
			handler.writeCalls.Load(),
		)
	}
	record, found, err := source.LookupInvocation(
		nodes.GatewayInvocationPrincipal{
			AgentID: plan.AgentID, SessionID: plan.SessionID, ActorID: plan.ActorID,
		},
		plan.InvocationID,
	)
	if err != nil || !found || record.State != nodes.GatewayInvocationDispatched {
		t.Fatalf("durable dispatch record = %#v, found %v, error %v", record, found, err)
	}
}

func TestNodeInvocationSourceSendsOneDurableCancellation(t *testing.T) {
	handler := &fakeNodeAdmissionHandler{}
	source := newTestNodeInvocationSource(t, handler)
	descriptor, plan, owner := testGatewayInvocation(t)
	principal := nodes.GatewayInvocationPrincipal{
		AgentID:     owner.AgentID,
		SessionID:   owner.SessionID,
		ActorID:     owner.ActorID,
		WorkspaceID: "workspace_1",
		ExecutionID: "execution_1",
	}
	owner.WorkspaceID = principal.WorkspaceID
	owner.ExecutionID = principal.ExecutionID
	if _, _, err := source.PrepareInvocation(
		"builder-node",
		"build",
		owner.ToolCallID,
		principal,
		plan,
		descriptor,
		true,
		func(tools.NodeDiscoveryRecord) error { return nil },
	); err != nil {
		t.Fatal(err)
	}
	if _, _, err := source.store.MarkDispatched(
		owner,
		plan.InvocationID,
		plan.PlanHash,
	); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixNano()
	handler.invocation = nodes.InvocationRecord{
		InvocationID: plan.InvocationID, IdempotencyKey: plan.IdempotencyKey,
		PlanHash: plan.PlanHash, NodeID: plan.NodeID, CatalogHash: plan.CatalogHash,
		Command: plan.Command, Risk: plan.Risk, State: nodes.InvocationRunning,
		AcceptedAt: now, UpdatedAt: now, ExpiresAt: plan.ExpiresAt,
		Cancellation: &nodes.InvocationCancellation{RequestedAt: now},
	}
	for index := range 2 {
		remote, transitioned, err := source.CancelInvocation(
			t.Context(),
			principal,
			"build",
			plan.NodeID,
			plan.InvocationID,
		)
		if err != nil || remote.Cancellation == nil || transitioned != (index == 0) {
			t.Fatalf("cancellation %d = (%#v, %v, %v)", index, remote, transitioned, err)
		}
	}
	if handler.cancelCalls.Load() != 1 || handler.queryCalls.Load() != 1 {
		t.Fatalf(
			"remote calls = cancel %d, query %d",
			handler.cancelCalls.Load(),
			handler.queryCalls.Load(),
		)
	}
	wrongExecution := principal
	wrongExecution.ExecutionID = "execution_2"
	if _, _, err := source.CancelInvocation(
		t.Context(),
		wrongExecution,
		"build",
		plan.NodeID,
		plan.InvocationID,
	); !errors.Is(err, nodes.ErrGatewayInvocationConflict) {
		t.Fatalf("wrong execution cancellation error = %v", err)
	}
}

func TestNodeInvocationSourceDeliversCancellationAfterCommittedWriteWarning(t *testing.T) {
	handler := &fakeNodeAdmissionHandler{}
	source := newTestNodeInvocationSource(t, handler)
	descriptor, plan, owner := testGatewayInvocation(t)
	principal := nodes.GatewayInvocationPrincipal{
		AgentID:     owner.AgentID,
		SessionID:   owner.SessionID,
		ActorID:     owner.ActorID,
		WorkspaceID: "workspace_1",
		ExecutionID: "execution_1",
	}
	owner.WorkspaceID = principal.WorkspaceID
	owner.ExecutionID = principal.ExecutionID
	if _, _, err := source.PrepareInvocation(
		"builder-node",
		"build",
		owner.ToolCallID,
		principal,
		plan,
		descriptor,
		true,
		func(tools.NodeDiscoveryRecord) error { return nil },
	); err != nil {
		t.Fatal(err)
	}
	if _, _, err := source.store.MarkDispatched(
		owner,
		plan.InvocationID,
		plan.PlanHash,
	); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixNano()
	handler.invocation = nodes.InvocationRecord{
		InvocationID: plan.InvocationID, IdempotencyKey: plan.IdempotencyKey,
		PlanHash: plan.PlanHash, NodeID: plan.NodeID, CatalogHash: plan.CatalogHash,
		Command: plan.Command, Risk: plan.Risk, State: nodes.InvocationRunning,
		AcceptedAt: now, UpdatedAt: now, ExpiresAt: plan.ExpiresAt,
		Cancellation: &nodes.InvocationCancellation{RequestedAt: now},
	}
	source.requestCancellation = func(
		requestPrincipal nodes.GatewayInvocationPrincipal,
		invocationID string,
	) (nodes.GatewayInvocationRecord, bool, error) {
		record, transitioned, err := source.store.RequestCancellation(
			requestPrincipal,
			invocationID,
		)
		if err == nil && transitioned {
			err = &fileutil.CommittedWriteError{Err: errors.New("sync invocation directory")}
		}
		return record, transitioned, err
	}
	remote, transitioned, err := source.CancelInvocation(
		t.Context(),
		principal,
		"build",
		plan.NodeID,
		plan.InvocationID,
	)
	if !transitioned || !fileutil.IsCommittedWriteError(err) ||
		remote.Cancellation == nil || handler.cancelCalls.Load() != 1 {
		t.Fatalf(
			"committed cancellation = (%#v, %v, %v); cancel calls = %d",
			remote,
			transitioned,
			err,
			handler.cancelCalls.Load(),
		)
	}
	source.requestCancellation = nil
	if _, transitioned, err := source.CancelInvocation(
		t.Context(),
		principal,
		"build",
		plan.NodeID,
		plan.InvocationID,
	); err != nil || transitioned {
		t.Fatalf("repeated cancellation = transitioned %v, error %v", transitioned, err)
	}
	if handler.cancelCalls.Load() != 1 || handler.queryCalls.Load() != 1 {
		t.Fatalf(
			"remote calls = cancel %d, query %d",
			handler.cancelCalls.Load(),
			handler.queryCalls.Load(),
		)
	}
}

func TestNodeInvocationSourceRecoversOnlyBoundDispatchedResult(t *testing.T) {
	handler := &fakeNodeAdmissionHandler{}
	source := newTestNodeInvocationSource(t, handler)
	descriptor, plan, owner := testGatewayInvocation(t)
	if _, _, err := source.PrepareInvocation(
		"builder-node",
		"build",
		owner.ToolCallID,
		nodes.GatewayInvocationPrincipal{
			AgentID: plan.AgentID, SessionID: plan.SessionID, ActorID: plan.ActorID,
		},
		plan,
		descriptor,
		true,
		func(tools.NodeDiscoveryRecord) error { return nil },
	); err != nil {
		t.Fatal(err)
	}
	principal := nodes.GatewayInvocationPrincipal{
		AgentID: plan.AgentID, SessionID: plan.SessionID, ActorID: plan.ActorID,
	}
	if _, err := source.QueryInvocation(
		t.Context(),
		principal,
		owner.Target,
		plan.NodeID,
		plan.InvocationID,
	); !errors.Is(err, nodes.ErrGatewayInvocationConflict) {
		t.Fatalf("prepared recovery error = %v", err)
	}
	if handler.queryCalls.Load() != 0 {
		t.Fatal("prepared invocation queried the companion")
	}
	if _, transitioned, err := source.store.MarkDispatched(
		owner,
		plan.InvocationID,
		plan.PlanHash,
	); err != nil || !transitioned {
		t.Fatalf("mark dispatched = transitioned %v, error %v", transitioned, err)
	}

	now := time.Now()
	handler.invocation = nodes.InvocationRecord{
		InvocationID:   plan.InvocationID,
		IdempotencyKey: plan.IdempotencyKey,
		PlanHash:       plan.PlanHash,
		NodeID:         plan.NodeID,
		CatalogHash:    plan.CatalogHash,
		Command:        plan.Command,
		Risk:           plan.Risk,
		State:          nodes.InvocationSucceeded,
		AcceptedAt:     now.Add(-time.Second).UnixNano(),
		UpdatedAt:      now.UnixNano(),
		ExpiresAt:      now.Add(time.Minute).Unix(),
		CompletedAt:    now.UnixNano(),
		Result:         json.RawMessage(`{ "value": "ok" }`),
	}
	handler.invocation.CompletedAt = handler.invocation.UpdatedAt
	recovered, err := source.QueryInvocation(
		t.Context(),
		principal,
		owner.Target,
		plan.NodeID,
		plan.InvocationID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(recovered.Result) != `{"value":"ok"}` {
		t.Fatalf("recovered canonical result = %s", recovered.Result)
	}

	handler.invocation.Result = json.RawMessage(`{"other":"wrong schema"}`)
	if _, err := source.QueryInvocation(
		t.Context(),
		principal,
		owner.Target,
		plan.NodeID,
		plan.InvocationID,
	); !errors.Is(err, nodes.ErrGatewayInvocationConflict) {
		t.Fatalf("schema-invalid recovery error = %v", err)
	} else if code, classified := nodes.InvocationQueryErrorCode(err); !classified || code != nodes.InvocationQueryRejected {
		t.Fatalf("schema-invalid recovery classification = %q, %v", code, classified)
	}

	handler.invocationErr = nodes.NewInvocationQueryError(nodes.InvocationQueryNotFound, nil)
	if _, err := source.QueryInvocation(
		t.Context(),
		principal,
		owner.Target,
		plan.NodeID,
		plan.InvocationID,
	); err == nil {
		t.Fatal("missing invocation query unexpectedly succeeded")
	} else if code, classified := nodes.InvocationQueryErrorCode(err); !classified || code != nodes.InvocationQueryNotFound {
		t.Fatalf("missing invocation query error = %v, code = %q", err, code)
	}
}

func TestNodeInvocationSourceRejectsStaleRuntimeGeneration(t *testing.T) {
	handler := &fakeNodeAdmissionHandler{}
	source := newTestNodeInvocationSource(t, handler)
	source.runtime.registryMu.Lock()
	source.runtime.generation++
	source.runtime.registryMu.Unlock()
	descriptor, plan, owner := testGatewayInvocation(t)

	if _, _, err := source.PrepareInvocation(
		"builder-node",
		"build",
		owner.ToolCallID,
		nodes.GatewayInvocationPrincipal{
			AgentID: plan.AgentID, SessionID: plan.SessionID, ActorID: plan.ActorID,
		},
		plan,
		descriptor,
		true,
		func(tools.NodeDiscoveryRecord) error { return nil },
	); !errors.Is(err, errNodeDiscoveryAuthorityUnavailable) {
		t.Fatalf("stale prepare error = %v", err)
	}
	if _, dispatched, err := source.DispatchInvocation(
		t.Context(),
		owner,
		plan.InvocationID,
		plan.PlanHash,
	); !errors.Is(err, errNodeDiscoveryAuthorityUnavailable) || dispatched {
		t.Fatalf("stale dispatch = dispatched %v, error %v", dispatched, err)
	}
}

func TestNodeInvocationPreparationLeasesRealSessionThroughPersistence(t *testing.T) {
	fixture := newRealNodePreparationFixture(t)
	validationStarted := make(chan struct{})
	allowValidation := make(chan struct{})
	type prepareResult struct {
		record  nodes.GatewayInvocationRecord
		created bool
		err     error
	}
	prepared := make(chan prepareResult, 1)
	go func() {
		record, created, err := fixture.source.PrepareInvocation(
			string(fixture.nodeID),
			"build",
			fixture.owner.ToolCallID,
			fixture.principal(),
			fixture.plan,
			fixture.descriptor,
			true,
			func(tools.NodeDiscoveryRecord) error {
				close(validationStarted)
				<-allowValidation
				return nil
			},
		)
		prepared <- prepareResult{record: record, created: created, err: err}
	}()
	<-validationStarted

	released := make(chan error, 1)
	go func() {
		_, err := fixture.release()
		released <- err
	}()
	select {
	case err := <-released:
		t.Fatalf("disconnect completed before durable preparation: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	if !fixture.sessions.Connected(fixture.nodeID) {
		t.Fatal("session became unavailable while preparation held its generation lease")
	}

	close(allowValidation)
	result := <-prepared
	if result.err != nil || !result.created {
		t.Fatalf("PrepareInvocation() = created %v, error %v", result.created, result.err)
	}
	if err := <-released; err != nil {
		t.Fatal(err)
	}
	if fixture.sessions.Connected(fixture.nodeID) {
		t.Fatal("released session remains connected")
	}
	record, found, err := fixture.source.store.Lookup(
		fixture.principal(),
		fixture.plan.InvocationID,
	)
	if err != nil || !found || record.Plan.PlanHash != fixture.plan.PlanHash {
		t.Fatalf("durable preparation = %#v, found %v, error %v", record, found, err)
	}
}

func TestNodeInvocationPreparationAndDispatchLockOrderSurvivesReloadWriter(t *testing.T) {
	fixture := newRealNodePreparationFixture(t)
	fixture.runtime.registryMu.RLock()

	registryHeld := make(chan struct{})
	allowDispatchRuntime := make(chan struct{})
	dispatchDone := make(chan error, 1)
	go func() {
		_, err := fixture.handler.WithResolvedApprovedCommand(
			string(fixture.nodeID),
			fixture.descriptor.Name,
			func(nodes.Registration, nodes.CommandApproval) error {
				close(registryHeld)
				<-allowDispatchRuntime
				return fixture.runtime.withInvocationHandler(
					fixture.runtime.registryPath,
					fixture.runtime.generation,
					func(nodeAdmissionHandler) error { return nil },
				)
			},
		)
		dispatchDone <- err
	}()
	<-registryHeld

	prepareDone := make(chan error, 1)
	go func() {
		_, _, err := fixture.source.PrepareInvocation(
			string(fixture.nodeID),
			"build",
			fixture.owner.ToolCallID,
			fixture.principal(),
			fixture.plan,
			fixture.descriptor,
			true,
			func(tools.NodeDiscoveryRecord) error { return nil },
		)
		prepareDone <- err
	}()
	time.Sleep(25 * time.Millisecond)

	writerStarted := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		close(writerStarted)
		fixture.runtime.registryMu.Lock()
		fixture.runtime.registryMu.Unlock()
		close(writerDone)
	}()
	<-writerStarted
	time.Sleep(25 * time.Millisecond)
	close(allowDispatchRuntime)
	fixture.runtime.registryMu.RUnlock()

	for name, done := range map[string]<-chan struct{}{
		"reload writer": writerDone,
	} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("%s deadlocked", name)
		}
	}
	select {
	case err := <-dispatchDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatch-side registry lease deadlocked")
	}
	select {
	case err := <-prepareDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("preparation deadlocked behind dispatch and reload")
	}
}

func TestNodeDiscoveryAndPreparationLockOrderSurvivesReloadWriter(t *testing.T) {
	fixture := newRealNodePreparationFixture(t)
	registryHeld := make(chan struct{})
	allowPreparationRuntime := make(chan struct{})
	preparationDone := make(chan error, 1)
	go func() {
		_, err := fixture.handler.WithPreparationAuthority(
			fixture.nodeID,
			string(fixture.nodeID),
			fixture.descriptor.Name,
			func(nodes.Registration, nodes.CommandApproval) error {
				close(registryHeld)
				<-allowPreparationRuntime
				return fixture.runtime.withInvocationHandler(
					fixture.runtime.registryPath,
					fixture.runtime.generation,
					func(nodeAdmissionHandler) error { return nil },
				)
			},
		)
		preparationDone <- err
	}()
	<-registryHeld

	type discoveryResult struct {
		found bool
		err   error
	}
	discoveryStarted := make(chan struct{})
	discoveryDone := make(chan discoveryResult, 1)
	go func() {
		close(discoveryStarted)
		_, found, err := fixture.runtime.lookup(
			fixture.runtime.registryPath,
			string(fixture.nodeID),
		)
		discoveryDone <- discoveryResult{found: found, err: err}
	}()
	<-discoveryStarted
	time.Sleep(25 * time.Millisecond)

	writerStarted := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		close(writerStarted)
		fixture.runtime.registryMu.Lock()
		fixture.runtime.registryMu.Unlock()
		close(writerDone)
	}()
	<-writerStarted
	time.Sleep(25 * time.Millisecond)
	close(allowPreparationRuntime)

	select {
	case <-writerDone:
	case <-time.After(time.Second):
		t.Fatal("reload writer deadlocked behind discovery and preparation")
	}
	select {
	case err := <-preparationDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("preparation deadlocked behind discovery and reload")
	}
	select {
	case result := <-discoveryDone:
		if result.err != nil || !result.found {
			t.Fatalf("lookup = found %v, error %v", result.found, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("discovery deadlocked behind preparation and reload")
	}
}

type realNodePreparationFixture struct {
	source     *nodeInvocationSource
	runtime    *nodeAdmissionRuntime
	handler    *nodews.AdmissionHandler
	sessions   *nodews.SessionHub
	nodeID     nodes.ID
	descriptor nodes.CommandDescriptor
	plan       nodes.ExecutionPlan
	owner      nodes.GatewayInvocationOwner
	release    func() (bool, error)
}

func newRealNodePreparationFixture(t *testing.T) *realNodePreparationFixture {
	t.Helper()
	descriptor, plan, owner := testGatewayInvocation(t)
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	nodeID, err := nodes.DeriveID(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	catalog := nodes.CapabilityCatalog{Commands: []nodes.CommandDescriptor{descriptor}}
	catalogHash, err := catalog.Hash()
	if err != nil {
		t.Fatal(err)
	}
	registryPath := filepath.Join(t.TempDir(), "registry.json")
	registry, err := nodes.NewFileRegistry(registryPath, 4)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := nodes.Snapshot{
		ID:              nodeID,
		State:           nodes.StatePendingPairing,
		ProtocolVersion: nodes.ProtocolV1,
		Platform:        "linux",
		Architecture:    "amd64",
		SoftwareVersion: "v0.1.0",
		CatalogHash:     catalogHash,
		Catalog:         catalog,
		LastSeenAt:      1,
	}
	if err := registry.UpsertPending(nodes.PendingPairing{
		Node:          snapshot,
		PublicKey:     publicKey,
		RequestedRole: "companion",
		RequestedAt:   1,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Approve(nodeID, nodes.PairingApproval{
		AllowedCommands: []string{descriptor.Name},
		At:              2,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot.State = nodes.StateConnected
	snapshot.LastSeenAt = 2
	if err := registry.Upsert(snapshot); err != nil {
		t.Fatal(err)
	}
	authenticator, err := nodes.NewAuthenticator(registry, nodes.AdmissionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	sessions := nodews.NewSessionHub()
	handler, err := nodews.NewAdmissionHandler(
		authenticator,
		nodews.AdmissionConfig{Sessions: sessions},
	)
	if err != nil {
		t.Fatal(err)
	}
	release, err := sessions.Claim(
		nodeID,
		&testNodeConnection{},
		nil,
		func() error {
			return authenticator.Disconnect(nodeID, "test session released")
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	plan.NodeID = nodeID
	plan.CatalogHash = catalogHash
	plan, err = nodes.PrepareExecutionPlan(
		plan.InvocationRequest,
		descriptor,
		plan.Executor,
		plan.PolicyRevision,
		time.Unix(plan.PreparedAt, 0),
		time.Duration(plan.ExpiresAt-plan.PreparedAt)*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &nodeAdmissionRuntime{
		registry:     registry,
		registryPath: registryPath,
		handler:      handler,
		sessions:     sessions,
		generation:   1,
		mounted:      true,
	}
	source := newTestNodeInvocationSourceWithRuntime(t, runtime)
	fixture := &realNodePreparationFixture{
		source: source, runtime: runtime, handler: handler, sessions: sessions,
		nodeID: nodeID, descriptor: descriptor, plan: plan, owner: owner, release: release,
	}
	t.Cleanup(func() {
		_, _ = release()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = handler.Close(ctx)
	})
	return fixture
}

func (fixture *realNodePreparationFixture) principal() nodes.GatewayInvocationPrincipal {
	return nodes.GatewayInvocationPrincipal{
		AgentID: fixture.plan.AgentID, SessionID: fixture.plan.SessionID,
		ActorID: fixture.plan.ActorID,
	}
}

func newTestNodeInvocationSource(
	t *testing.T,
	handler nodeAdmissionHandler,
) *nodeInvocationSource {
	t.Helper()
	registryPath := filepath.Join(t.TempDir(), "registry.json")
	runtime := &nodeAdmissionRuntime{
		registryPath: registryPath,
		handler:      handler,
		generation:   1,
		mounted:      true,
	}
	return newTestNodeInvocationSourceWithRuntime(t, runtime)
}

func newTestNodeInvocationSourceWithRuntime(
	t *testing.T,
	runtime *nodeAdmissionRuntime,
) *nodeInvocationSource {
	t.Helper()
	store, err := nodes.NewGatewayInvocationStore(
		nodes.GatewayInvocationStorePath(t.TempDir()),
		8,
		1024*1024,
	)
	if err != nil {
		t.Fatal(err)
	}
	return &nodeInvocationSource{
		nodeDiscoverySource: nodeDiscoverySource{
			runtime: runtime, registryPath: runtime.registryPath,
		},
		store:      store,
		generation: runtime.generation,
	}
}

func testGatewayInvocation(
	t *testing.T,
) (nodes.CommandDescriptor, nodes.ExecutionPlan, nodes.GatewayInvocationOwner) {
	t.Helper()
	descriptor := nodes.CommandDescriptor{
		Name:        "node.info.v1",
		InputSchema: json.RawMessage(`{"type":"object","additionalProperties":false}`),
		OutputSchema: json.RawMessage(
			`{"type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}`,
		),
		Risk: nodes.RiskRead,
	}
	catalogHash, err := (nodes.CapabilityCatalog{
		Commands: []nodes.CommandDescriptor{descriptor},
	}).Hash()
	if err != nil {
		t.Fatal(err)
	}
	request := nodes.InvocationRequest{
		InvocationID:     "inv_1",
		IdempotencyKey:   "idem_1",
		NodeID:           "node_test",
		CatalogHash:      catalogHash,
		Command:          descriptor.Name,
		Input:            json.RawMessage(`{}`),
		AgentID:          "agent_1",
		SessionID:        "session_1",
		ActorID:          "actor_1",
		TimeoutSeconds:   30,
		OutputLimitBytes: 1024,
	}
	plan, err := nodes.PrepareExecutionPlan(
		request,
		descriptor,
		"builtin",
		"policy_1",
		time.Now(),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	return descriptor, plan, nodes.GatewayInvocationOwner{
		Target:     "build",
		AgentID:    plan.AgentID,
		SessionID:  plan.SessionID,
		ActorID:    plan.ActorID,
		ToolCallID: "call_1",
	}
}
