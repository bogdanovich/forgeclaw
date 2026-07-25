package nodes

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGatewayInvocationStorePersistsPreparedBinding(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "node_invocations.json")
	store, err := NewGatewayInvocationStore(path, 8, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	plan := gatewayTestPlan(t, "inv_persist", "idem_persist", time.Now())
	record, err := store.Prepare("vpn_box", "call-1", plan)
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
	got, found := reloaded.ByToolCall(plan.AgentID, plan.SessionID, "call-1")
	if !found || got.ExpectedPlanHash != plan.PlanHash ||
		got.Plan.InvocationID != plan.InvocationID {
		t.Fatalf("reloaded record = (%#v, %v)", got, found)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("store mode = %o", info.Mode().Perm())
	}
}

func TestGatewayInvocationStoreRejectsToolCallRebinding(t *testing.T) {
	store := newGatewayInvocationStore("", 8, 1024*1024, time.Now)
	first := gatewayTestPlan(t, "inv_first", "idem_first", time.Now())
	if _, err := store.Prepare("vpn", "call-1", first); err != nil {
		t.Fatal(err)
	}
	second := gatewayTestPlan(t, "inv_second", "idem_second", time.Now())
	if _, err := store.Prepare("vpn", "call-1", second); !errors.Is(
		err,
		ErrGatewayInvocationConflict,
	) {
		t.Fatalf("Prepare() error = %v", err)
	}
}

func TestGatewayInvocationStoreMarksDispatchAgainstRetainedHash(t *testing.T) {
	store := newGatewayInvocationStore("", 8, 1024*1024, time.Now)
	plan := gatewayTestPlan(t, "inv_dispatch", "idem_dispatch", time.Now())
	if _, err := store.Prepare("vpn", "call-1", plan); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkDispatched(plan.InvocationID, "wrong"); !errors.Is(
		err,
		ErrGatewayInvocationConflict,
	) {
		t.Fatalf("wrong hash error = %v", err)
	}
	dispatched, err := store.MarkDispatched(plan.InvocationID, plan.PlanHash)
	if err != nil {
		t.Fatal(err)
	}
	if dispatched.State != GatewayInvocationDispatched || dispatched.DispatchedAt == 0 {
		t.Fatalf("dispatched record = %#v", dispatched)
	}
	if _, found := store.Lookup("other", plan.SessionID, plan.InvocationID); found {
		t.Fatal("different agent accessed invocation")
	}
	if _, found := store.Lookup(plan.AgentID, "other", plan.InvocationID); found {
		t.Fatal("different session accessed invocation")
	}
	if _, found := store.Lookup(plan.AgentID, plan.SessionID, plan.InvocationID); !found {
		t.Fatal("invocation owner could not access record")
	}
}

func TestGatewayInvocationStoreLoadRejectsMutatedPlan(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node_invocations.json")
	plan := gatewayTestPlan(t, "inv_mutated", "idem_mutated", time.Now())
	record := GatewayInvocationRecord{
		Target:           "vpn",
		ToolCallID:       "call-1",
		Plan:             plan,
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
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewGatewayInvocationStore(path, 8, 1024*1024); err == nil {
		t.Fatal("mutated persisted plan was accepted")
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
