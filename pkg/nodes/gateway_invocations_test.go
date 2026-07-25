package nodes

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/fileutil"
)

func TestGatewayInvocationStorePersistsPreparedBinding(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "node_invocations.json")
	store, err := NewGatewayInvocationStore(path, 8, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	plan := gatewayTestPlan(t, "inv_persist", "idem_persist", time.Now())
	record, err := store.Prepare("vpn_box", "call-1", plan, gatewayTestDescriptor(plan))
	if err != nil {
		t.Fatal(err)
	}
	if record.ExpectedPlanHash != plan.PlanHash ||
		record.State != GatewayInvocationPrepared {
		t.Fatalf("prepared record = %#v", record)
	}

	reloaded, err := NewGatewayInvocationStore(path, 8, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := reloaded.ByToolCall(gatewayTestPrincipal(plan), "call-1")
	if err != nil || !found || got.ExpectedPlanHash != plan.PlanHash ||
		got.Plan.InvocationID != plan.InvocationID ||
		!sameCommandDescriptor(got.Descriptor, gatewayTestDescriptor(plan)) {
		t.Fatalf("reloaded record = (%#v, %v, %v)", got, found, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("store mode = %o", info.Mode().Perm())
	}
}

func TestGatewayInvocationStoreReloadsAcrossInstancesBeforeMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "node_invocations.json")
	first, err := NewGatewayInvocationStore(path, 8, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewGatewayInvocationStore(path, 8, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	firstPlan := gatewayTestPlan(t, "inv_first", "idem_first", time.Now())
	if _, err = first.Prepare("vpn", "call-1", firstPlan, gatewayTestDescriptor(firstPlan)); err != nil {
		t.Fatal(err)
	}
	secondPlan := gatewayTestPlan(t, "inv_second", "idem_second", time.Now())
	if _, err = second.Prepare("vpn", "call-2", secondPlan, gatewayTestDescriptor(secondPlan)); err != nil {
		t.Fatal(err)
	}
	for _, plan := range []ExecutionPlan{firstPlan, secondPlan} {
		if _, found, lookupErr := first.Lookup(
			gatewayTestPrincipal(plan),
			plan.InvocationID,
		); lookupErr != nil || !found {
			t.Fatalf("canonical record %q = (%v, %v)", plan.InvocationID, found, lookupErr)
		}
	}
	conflict := gatewayTestPlan(t, "inv_conflict", firstPlan.IdempotencyKey, time.Now())
	if _, err = second.Prepare("vpn", "call-3", conflict, gatewayTestDescriptor(conflict)); !errors.Is(
		err,
		ErrGatewayInvocationConflict,
	) {
		t.Fatalf("cross-instance idempotency conflict = %v", err)
	}
}

func TestGatewayInvocationStoreRejectsToolCallRebinding(t *testing.T) {
	store := newGatewayInvocationStore("", 8, 1024*1024, time.Now)
	first := gatewayTestPlan(t, "inv_first", "idem_first", time.Now())
	if _, err := store.Prepare("vpn", "call-1", first, gatewayTestDescriptor(first)); err != nil {
		t.Fatal(err)
	}
	second := gatewayTestPlan(t, "inv_second", "idem_second", time.Now())
	if _, err := store.Prepare("vpn", "call-1", second, gatewayTestDescriptor(second)); !errors.Is(
		err,
		ErrGatewayInvocationConflict,
	) {
		t.Fatalf("Prepare() error = %v", err)
	}
	if _, err := store.Prepare("other", "call-2", first, gatewayTestDescriptor(first)); !errors.Is(
		err,
		ErrGatewayInvocationConflict,
	) {
		t.Fatalf("invocation retry error = %v", err)
	}
	reusedKey := gatewayTestPlan(t, "inv_other", first.IdempotencyKey, time.Now())
	if _, err := store.Prepare("vpn", "call-2", reusedKey, gatewayTestDescriptor(reusedKey)); !errors.Is(
		err,
		ErrGatewayInvocationConflict,
	) {
		t.Fatalf("idempotency retry error = %v", err)
	}
}

func TestGatewayInvocationStoreMarksDispatchAgainstRetainedHash(t *testing.T) {
	store := newGatewayInvocationStore("", 8, 1024*1024, time.Now)
	plan := gatewayTestPlan(t, "inv_dispatch", "idem_dispatch", time.Now())
	if _, err := store.Prepare("vpn", "call-1", plan, gatewayTestDescriptor(plan)); err != nil {
		t.Fatal(err)
	}
	owner := GatewayInvocationOwner{
		Target: "vpn", AgentID: plan.AgentID, SessionID: plan.SessionID,
		ActorID: plan.ActorID, ToolCallID: "call-1",
	}
	wrongOwner := owner
	wrongOwner.ToolCallID = "call-other"
	if _, _, err := store.MarkDispatched(
		wrongOwner,
		plan.InvocationID,
		plan.PlanHash,
	); !errors.Is(err, ErrGatewayInvocationConflict) {
		t.Fatalf("wrong owner error = %v", err)
	}
	if _, _, err := store.MarkDispatched(owner, plan.InvocationID, "wrong"); !errors.Is(
		err,
		ErrGatewayInvocationConflict,
	) {
		t.Fatalf("wrong hash error = %v", err)
	}
	dispatched, transitioned, err := store.MarkDispatched(
		owner,
		plan.InvocationID,
		plan.PlanHash,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !transitioned ||
		dispatched.State != GatewayInvocationDispatched ||
		dispatched.DispatchedAt == 0 {
		t.Fatalf("dispatched record = %#v", dispatched)
	}
	if _, transitioned, err := store.MarkDispatched(
		owner,
		plan.InvocationID,
		plan.PlanHash,
	); err != nil || transitioned {
		t.Fatalf("second dispatch transition = (%v, %v)", transitioned, err)
	}
	principal := gatewayTestPrincipal(plan)
	principal.AgentID = "other"
	if _, found, err := store.Lookup(principal, plan.InvocationID); err != nil || found {
		t.Fatal("different agent accessed invocation")
	}
	principal = gatewayTestPrincipal(plan)
	principal.SessionID = "other"
	if _, found, err := store.Lookup(principal, plan.InvocationID); err != nil || found {
		t.Fatal("different session accessed invocation")
	}
	principal = gatewayTestPrincipal(plan)
	principal.ActorID = "other"
	if _, found, err := store.Lookup(principal, plan.InvocationID); err != nil || found {
		t.Fatal("different actor accessed invocation")
	}
	if _, found, err := store.Lookup(
		gatewayTestPrincipal(plan),
		plan.InvocationID,
	); err != nil || !found {
		t.Fatal("invocation owner could not access record")
	}
}

func TestGatewayInvocationStoreAllowsOneConcurrentDispatchWinner(t *testing.T) {
	store := newGatewayInvocationStore("", 8, 1024*1024, time.Now)
	plan := gatewayTestPlan(t, "inv_concurrent", "idem_concurrent", time.Now())
	if _, err := store.Prepare("vpn", "call-1", plan, gatewayTestDescriptor(plan)); err != nil {
		t.Fatal(err)
	}
	owner := GatewayInvocationOwner{
		Target: "vpn", AgentID: plan.AgentID, SessionID: plan.SessionID,
		ActorID: plan.ActorID, ToolCallID: "call-1",
	}
	start := make(chan struct{})
	results := make(chan bool, 2)
	errs := make(chan error, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	for range 2 {
		go func() {
			ready.Done()
			<-start
			_, transitioned, err := store.MarkDispatched(
				owner,
				plan.InvocationID,
				plan.PlanHash,
			)
			results <- transitioned
			errs <- err
		}()
	}
	ready.Wait()
	close(start)

	winners := 0
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		if <-results {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("dispatch winners = %d, want 1", winners)
	}
}

func TestGatewayInvocationStoreRejectsExpiredPreparedAuthority(t *testing.T) {
	now := time.Now()
	store := newGatewayInvocationStore("", 8, 1024*1024, func() time.Time { return now })
	plan := gatewayTestPlan(t, "inv_expired", "idem_expired", now)
	if _, err := store.Prepare("vpn", "call-1", plan, gatewayTestDescriptor(plan)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	if _, found, err := store.ByToolCall(
		gatewayTestPrincipal(plan),
		"call-1",
	); err != nil || found {
		t.Fatalf("expired ByToolCall() = (%v, %v)", found, err)
	}
	owner := GatewayInvocationOwner{
		Target: "vpn", AgentID: plan.AgentID, SessionID: plan.SessionID,
		ActorID: plan.ActorID, ToolCallID: "call-1",
	}
	if _, _, err := store.MarkDispatched(
		owner,
		plan.InvocationID,
		plan.PlanHash,
	); !errors.Is(err, ErrGatewayInvocationNotFound) {
		t.Fatalf("expired MarkDispatched() error = %v", err)
	}
}

func TestGatewayInvocationStoreKeepsCommittedMutationInMemory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node_invocations.json")
	store := newGatewayInvocationStore(path, 8, 1024*1024, time.Now)
	store.writeFile = func(path string, data []byte, mode os.FileMode) error {
		if err := os.WriteFile(path, data, mode); err != nil {
			return err
		}
		return &fileutil.CommittedWriteError{Err: errors.New("sync directory")}
	}
	plan := gatewayTestPlan(t, "inv_committed", "idem_committed", time.Now())
	if _, err := store.Prepare("vpn", "call-1", plan, gatewayTestDescriptor(plan)); err == nil ||
		!fileutil.IsCommittedWriteError(err) {
		t.Fatalf("Prepare() error = %v", err)
	}
	got, found, err := store.ByToolCall(gatewayTestPrincipal(plan), "call-1")
	if err != nil || !found || got.Plan.InvocationID != plan.InvocationID {
		t.Fatalf("committed record = (%#v, %v, %v)", got, found, err)
	}
	owner := GatewayInvocationOwner{
		Target: "vpn", AgentID: plan.AgentID, SessionID: plan.SessionID,
		ActorID: plan.ActorID, ToolCallID: "call-1",
	}
	_, transitioned, dispatchErr := store.MarkDispatched(
		owner,
		plan.InvocationID,
		plan.PlanHash,
	)
	if !transitioned || dispatchErr == nil || !fileutil.IsCommittedWriteError(dispatchErr) {
		t.Fatalf("MarkDispatched() = (transitioned %v, error %v)", transitioned, dispatchErr)
	}
	got, found, err = store.Lookup(gatewayTestPrincipal(plan), plan.InvocationID)
	if err != nil || !found || got.State != GatewayInvocationDispatched {
		t.Fatalf("committed dispatch = (%#v, %v, %v)", got, found, err)
	}
}

func TestGatewayInvocationStorePersistsDefinitiveRejection(t *testing.T) {
	store := newGatewayInvocationStore("", 8, 1024*1024, time.Now)
	plan := gatewayTestPlan(t, "inv_rejected", "idem_rejected", time.Now())
	if _, err := store.Prepare("vpn", "call-1", plan, gatewayTestDescriptor(plan)); err != nil {
		t.Fatal(err)
	}
	owner := GatewayInvocationOwner{
		Target: "vpn", AgentID: plan.AgentID, SessionID: plan.SessionID,
		ActorID: plan.ActorID, ToolCallID: "call-1",
	}
	if _, transitioned, err := store.MarkDispatched(
		owner,
		plan.InvocationID,
		plan.PlanHash,
	); err != nil || !transitioned {
		t.Fatalf("dispatch transition = (%v, %v)", transitioned, err)
	}
	rejection := InvocationFailure{Code: "NODE_BUSY", Message: "node is busy"}
	record, transitioned, err := store.MarkRejected(
		owner,
		plan.InvocationID,
		plan.PlanHash,
		rejection,
	)
	if err != nil || !transitioned ||
		record.State != GatewayInvocationRejected ||
		record.Rejection == nil ||
		*record.Rejection != rejection {
		t.Fatalf("rejection transition = (%#v, %v, %v)", record, transitioned, err)
	}
	if _, transitioned, err := store.MarkRejected(
		owner,
		plan.InvocationID,
		plan.PlanHash,
		rejection,
	); err != nil || transitioned {
		t.Fatalf("second rejection transition = (%v, %v)", transitioned, err)
	}
}

func TestGatewayInvocationStoreReloadsAfterBackwardClockTransitions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "node_invocations.json")
	now := time.Now()
	store := newGatewayInvocationStore(path, 8, 1024*1024, func() time.Time {
		return now
	})
	plan := gatewayTestPlan(t, "inv_clock", "idem_clock", now)
	prepared, err := store.Prepare("vpn", "call-1", plan, gatewayTestDescriptor(plan))
	if err != nil {
		t.Fatal(err)
	}
	owner := GatewayInvocationOwner{
		Target: "vpn", AgentID: plan.AgentID, SessionID: plan.SessionID,
		ActorID: plan.ActorID, ToolCallID: "call-1",
	}

	now = now.Add(-time.Hour)
	dispatched, transitioned, err := store.MarkDispatched(
		owner,
		plan.InvocationID,
		plan.PlanHash,
	)
	if err != nil || !transitioned ||
		dispatched.UpdatedAt != prepared.UpdatedAt ||
		dispatched.DispatchedAt != prepared.UpdatedAt {
		t.Fatalf("backward-clock dispatch = (%#v, %v, %v)", dispatched, transitioned, err)
	}
	rejected, transitioned, err := store.MarkRejected(
		owner,
		plan.InvocationID,
		plan.PlanHash,
		InvocationFailure{Code: "NODE_BUSY", Message: "node is busy"},
	)
	if err != nil || !transitioned || rejected.UpdatedAt != dispatched.UpdatedAt {
		t.Fatalf("backward-clock rejection = (%#v, %v, %v)", rejected, transitioned, err)
	}

	reloaded, err := NewGatewayInvocationStore(path, 8, 1024*1024)
	if err != nil {
		t.Fatalf("reload backward-clock transitions: %v", err)
	}
	got, found, err := reloaded.Lookup(gatewayTestPrincipal(plan), plan.InvocationID)
	if err != nil || !found || got.State != GatewayInvocationRejected {
		t.Fatalf("reloaded invocation = (%#v, %v, %v)", got, found, err)
	}
}

func TestGatewayInvocationStoreLoadRejectsMutatedPlan(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node_invocations.json")
	plan := gatewayTestPlan(t, "inv_mutated", "idem_mutated", time.Now())
	record := GatewayInvocationRecord{
		Target:           "vpn",
		ToolCallID:       "call-1",
		Plan:             plan,
		Descriptor:       gatewayTestDescriptor(plan),
		ExpectedPlanHash: plan.PlanHash,
		State:            GatewayInvocationPrepared,
		CreatedAt:        time.Now().UnixNano(),
		UpdatedAt:        time.Now().UnixNano(),
	}
	record.Plan.Input = json.RawMessage(`{"argv":["different"]}`)
	data, err := json.Marshal(gatewayInvocationDocument{
		Version: gatewayInvocationStoreVersion,
		Records: map[string]GatewayInvocationRecord{plan.InvocationID: record},
	})
	if err != nil {
		t.Fatal(err)
	}
	if writeErr := os.WriteFile(path, data, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	if _, loadErr := NewGatewayInvocationStore(path, 8, 1024*1024); loadErr == nil {
		t.Fatal("mutated persisted plan was accepted")
	}
}

func TestGatewayInvocationStoreLoadRejectsDescriptorOutsidePlan(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node_invocations.json")
	plan := gatewayTestPlan(t, "inv_descriptor", "idem_descriptor", time.Now())
	record := GatewayInvocationRecord{
		Target:           "vpn",
		ToolCallID:       "call-1",
		Plan:             plan,
		Descriptor:       gatewayTestDescriptor(plan),
		ExpectedPlanHash: plan.PlanHash,
		State:            GatewayInvocationPrepared,
		CreatedAt:        time.Now().UnixNano(),
		UpdatedAt:        time.Now().UnixNano(),
	}
	record.Descriptor.Name = "system.other.v1"
	data, err := json.Marshal(gatewayInvocationDocument{
		Version: gatewayInvocationStoreVersion,
		Records: map[string]GatewayInvocationRecord{plan.InvocationID: record},
	})
	if err != nil {
		t.Fatal(err)
	}
	if writeErr := os.WriteFile(path, data, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	if _, loadErr := NewGatewayInvocationStore(path, 8, 1024*1024); loadErr == nil {
		t.Fatal("descriptor outside the approved plan was accepted")
	}
}

func TestGatewayInvocationStoreLoadPrunesExpiredPreparedAuthority(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node_invocations.json")
	preparedAt := time.Now().Add(-2 * time.Minute)
	plan := gatewayTestPlan(t, "inv_stale", "idem_stale", preparedAt)
	now := time.Now().UnixNano()
	data, err := json.Marshal(gatewayInvocationDocument{
		Version: gatewayInvocationStoreVersion,
		Records: map[string]GatewayInvocationRecord{plan.InvocationID: {
			Target:           "vpn",
			ToolCallID:       "call-1",
			Plan:             plan,
			Descriptor:       gatewayTestDescriptor(plan),
			ExpectedPlanHash: plan.PlanHash,
			State:            GatewayInvocationPrepared,
			CreatedAt:        now,
			UpdatedAt:        now,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if writeErr := os.WriteFile(path, data, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	store, err := NewGatewayInvocationStore(path, 8, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	_, found, lookupErr := store.ByToolCall(gatewayTestPrincipal(plan), "call-1")
	if lookupErr != nil || found {
		t.Fatalf("expired loaded record = (%v, %v)", found, lookupErr)
	}
	var document gatewayInvocationDocument
	persisted, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if decodeErr := json.Unmarshal(persisted, &document); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if len(document.Records) != 0 {
		t.Fatalf("persisted stale records = %#v", document.Records)
	}
}

func gatewayTestPlan(
	t *testing.T,
	invocationID string,
	idempotencyKey string,
	preparedAt time.Time,
) ExecutionPlan {
	t.Helper()
	request := invocationRequest(json.RawMessage(`{"argv":["git","status"]}`))
	request.InvocationID = invocationID
	request.IdempotencyKey = idempotencyKey
	plan, err := PrepareExecutionPlan(
		request,
		invocationDescriptor(RiskWrite),
		"local",
		"policy-1",
		preparedAt,
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func gatewayTestPrincipal(plan ExecutionPlan) GatewayInvocationPrincipal {
	return GatewayInvocationPrincipal{
		AgentID:   plan.AgentID,
		SessionID: plan.SessionID,
		ActorID:   plan.ActorID,
	}
}

func gatewayTestDescriptor(plan ExecutionPlan) CommandDescriptor {
	return invocationDescriptor(plan.Risk)
}
