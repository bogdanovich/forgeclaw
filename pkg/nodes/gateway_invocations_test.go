package nodes

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/fileutil"
)

func TestGatewayInvocationStorePersistsPreparedBinding(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "node_invocations.json")
	store, err := NewGatewayInvocationStore(path, 8, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	plan := gatewayTestPlan(t, "inv_persist", "idem_persist", time.Now())
	record, created, err := store.Prepare(
		"vpn_box",
		"call-1",
		plan,
		gatewayTestDescriptor(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("first prepare did not report record creation")
	}
	retained, created, err := store.Prepare(
		"vpn_box",
		"call-1",
		plan,
		gatewayTestDescriptor(),
	)
	if err != nil || created || retained.CreatedAt != record.CreatedAt {
		t.Fatalf("repeated prepare = (%#v, %v, %v)", retained, created, err)
	}
	if record.ExpectedPlanHash != plan.PlanHash ||
		record.State != GatewayInvocationPrepared {
		t.Fatalf("prepared record = %#v", record)
	}
	descriptorHash, err := record.Descriptor.Hash()
	if err != nil || descriptorHash != plan.DescriptorHash {
		t.Fatalf("prepared descriptor hash = %q, error %v", descriptorHash, err)
	}

	reloaded, err := NewGatewayInvocationStore(path, 8, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := reloaded.ByToolCall(gatewayTestPrincipal(plan), "call-1")
	if err != nil || !found || got.ExpectedPlanHash != plan.PlanHash ||
		got.Plan.InvocationID != plan.InvocationID {
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
	if _, _, err = first.Prepare(
		"vpn",
		"call-1",
		firstPlan,
		gatewayTestDescriptor(),
	); err != nil {
		t.Fatal(err)
	}
	secondPlan := gatewayTestPlan(t, "inv_second", "idem_second", time.Now())
	if _, _, err = second.Prepare(
		"vpn",
		"call-2",
		secondPlan,
		gatewayTestDescriptor(),
	); err != nil {
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
	if _, _, err = second.Prepare(
		"vpn",
		"call-3",
		conflict,
		gatewayTestDescriptor(),
	); !errors.Is(
		err,
		ErrGatewayInvocationConflict,
	) {
		t.Fatalf("cross-instance idempotency conflict = %v", err)
	}
}

func TestGatewayInvocationStoreRejectsToolCallRebinding(t *testing.T) {
	store := newGatewayInvocationStore("", 8, 1024*1024, time.Now)
	first := gatewayTestPlan(t, "inv_first", "idem_first", time.Now())
	if _, _, err := store.Prepare(
		"vpn",
		"call-1",
		first,
		gatewayTestDescriptor(),
	); err != nil {
		t.Fatal(err)
	}
	second := gatewayTestPlan(t, "inv_second", "idem_second", time.Now())
	if _, _, err := store.Prepare(
		"vpn",
		"call-1",
		second,
		gatewayTestDescriptor(),
	); !errors.Is(
		err,
		ErrGatewayInvocationConflict,
	) {
		t.Fatalf("Prepare() error = %v", err)
	}
	if _, _, err := store.Prepare(
		"other",
		"call-2",
		first,
		gatewayTestDescriptor(),
	); !errors.Is(
		err,
		ErrGatewayInvocationConflict,
	) {
		t.Fatalf("invocation retry error = %v", err)
	}
	reusedKey := gatewayTestPlan(t, "inv_other", first.IdempotencyKey, time.Now())
	if _, _, err := store.Prepare(
		"vpn",
		"call-2",
		reusedKey,
		gatewayTestDescriptor(),
	); !errors.Is(
		err,
		ErrGatewayInvocationConflict,
	) {
		t.Fatalf("idempotency retry error = %v", err)
	}
}

func TestGatewayInvocationStoreMarksDispatchAgainstRetainedHash(t *testing.T) {
	store := newGatewayInvocationStore("", 8, 1024*1024, time.Now)
	plan := gatewayTestPlan(t, "inv_dispatch", "idem_dispatch", time.Now())
	if _, _, err := store.Prepare(
		"vpn",
		"call-1",
		plan,
		gatewayTestDescriptor(),
	); err != nil {
		t.Fatal(err)
	}
	owner := gatewayTestOwner("vpn", "call-1", plan)
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
	if !transitioned || dispatched.State != GatewayInvocationDispatched ||
		dispatched.DispatchedAt == 0 {
		t.Fatalf("dispatched record = %#v", dispatched)
	}
	principal := gatewayTestPrincipal(plan)
	principal.AgentID = "other"
	if _, found, lookupErr := store.Lookup(
		principal,
		plan.InvocationID,
	); lookupErr != nil || found {
		t.Fatal("different agent accessed invocation")
	}
	principal = gatewayTestPrincipal(plan)
	principal.SessionID = "other"
	if _, found, lookupErr := store.Lookup(
		principal,
		plan.InvocationID,
	); lookupErr != nil || found {
		t.Fatal("different session accessed invocation")
	}
	principal = gatewayTestPrincipal(plan)
	principal.ActorID = "other"
	if _, found, lookupErr := store.Lookup(
		principal,
		plan.InvocationID,
	); lookupErr != nil || found {
		t.Fatal("different actor accessed invocation")
	}
	if _, found, lookupErr := store.Lookup(
		gatewayTestPrincipal(plan),
		plan.InvocationID,
	); lookupErr != nil || !found {
		t.Fatal("invocation owner could not access record")
	}
	repeated, transitioned, err := store.MarkDispatched(
		owner,
		plan.InvocationID,
		plan.PlanHash,
	)
	if err != nil || transitioned || repeated.DispatchedAt != dispatched.DispatchedAt {
		t.Fatalf("repeated dispatch = (%#v, %v, %v)", repeated, transitioned, err)
	}
}

func TestGatewayInvocationStoreAllowsOneDispatchWinnerAcrossInstances(t *testing.T) {
	now := time.Now()
	path := filepath.Join(t.TempDir(), "node_invocations.json")
	first := newGatewayInvocationStore(path, 8, 1024*1024, func() time.Time { return now })
	second, err := NewGatewayInvocationStore(path, 8, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	second.now = func() time.Time { return now }
	plan := gatewayTestPlan(t, "inv_race", "idem_race", now)
	prepared, _, err := first.Prepare("vpn", "call-1", plan, gatewayTestDescriptor())
	if err != nil {
		t.Fatal(err)
	}
	owner := gatewayTestOwner("vpn", "call-1", plan)
	type dispatchResult struct {
		record       GatewayInvocationRecord
		transitioned bool
		err          error
	}
	start := make(chan struct{})
	results := make(chan dispatchResult, 2)
	for _, store := range []*GatewayInvocationStore{first, second} {
		go func() {
			<-start
			record, transitioned, dispatchErr := store.MarkDispatched(
				owner,
				plan.InvocationID,
				plan.PlanHash,
			)
			results <- dispatchResult{
				record: record, transitioned: transitioned, err: dispatchErr,
			}
		}()
	}
	close(start)
	winners := 0
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.transitioned {
			winners++
		}
		if result.record.UpdatedAt <= prepared.UpdatedAt {
			t.Fatalf("non-monotonic transition = %#v after %#v", result.record, prepared)
		}
	}
	if winners != 1 {
		t.Fatalf("dispatch transition winners = %d", winners)
	}
}

func TestGatewayInvocationStorePinsDescriptor(t *testing.T) {
	store := newGatewayInvocationStore("", 8, 1024*1024, time.Now)
	plan := gatewayTestPlan(t, "inv_descriptor", "idem_descriptor", time.Now())
	descriptor := gatewayTestDescriptor()
	record, _, err := store.Prepare("vpn", "call-1", plan, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	descriptor.OutputSchema[0] = 'x'
	record.Descriptor.OutputSchema[0] = 'y'
	got, found, err := store.Lookup(gatewayTestPrincipal(plan), plan.InvocationID)
	if err != nil || !found {
		t.Fatalf("Lookup() = (%#v, %v, %v)", got, found, err)
	}
	hash, err := got.Descriptor.Hash()
	if err != nil || hash != plan.DescriptorHash {
		t.Fatalf("retained descriptor hash = %q, error %v", hash, err)
	}
	wrong := gatewayTestDescriptor()
	wrong.Risk = RiskRead
	other := gatewayTestPlan(t, "inv_wrong_descriptor", "idem_wrong_descriptor", time.Now())
	if _, _, err := store.Prepare("vpn", "call-2", other, wrong); !errors.Is(
		err,
		ErrInvalidInvocation,
	) {
		t.Fatalf("mismatched descriptor error = %v", err)
	}
}

func TestGatewayInvocationStoreRejectsExpiredPreparedAuthority(t *testing.T) {
	now := time.Now()
	store := newGatewayInvocationStore("", 8, 1024*1024, func() time.Time { return now })
	plan := gatewayTestPlan(t, "inv_expired", "idem_expired", now)
	if _, _, err := store.Prepare(
		"vpn",
		"call-1",
		plan,
		gatewayTestDescriptor(),
	); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	if _, found, err := store.ByToolCall(
		gatewayTestPrincipal(plan),
		"call-1",
	); err != nil || found {
		t.Fatalf("expired ByToolCall() = (%v, %v)", found, err)
	}
	owner := gatewayTestOwner("vpn", "call-1", plan)
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
	if _, _, err := store.Prepare(
		"vpn",
		"call-1",
		plan,
		gatewayTestDescriptor(),
	); err == nil ||
		!fileutil.IsCommittedWriteError(err) {
		t.Fatalf("Prepare() error = %v", err)
	}
	got, found, err := store.ByToolCall(gatewayTestPrincipal(plan), "call-1")
	if err != nil || !found || got.Plan.InvocationID != plan.InvocationID {
		t.Fatalf("committed record = (%#v, %v, %v)", got, found, err)
	}
	owner := gatewayTestOwner("vpn", "call-1", plan)
	_, transitioned, dispatchErr := store.MarkDispatched(
		owner,
		plan.InvocationID,
		plan.PlanHash,
	)
	if !transitioned || dispatchErr == nil ||
		!fileutil.IsCommittedWriteError(dispatchErr) {
		t.Fatalf("MarkDispatched() error = %v", dispatchErr)
	}
	got, found, err = store.Lookup(gatewayTestPrincipal(plan), plan.InvocationID)
	if err != nil || !found || got.State != GatewayInvocationDispatched {
		t.Fatalf("committed dispatch = (%#v, %v, %v)", got, found, err)
	}
}

func TestGatewayInvocationStoreDoesNotGrantRolledBackDispatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node_invocations.json")
	store, err := NewGatewayInvocationStore(path, 8, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	plan := gatewayTestPlan(t, "inv_rollback", "idem_rollback", time.Now())
	if _, _, err = store.Prepare(
		"vpn",
		"call-1",
		plan,
		gatewayTestDescriptor(),
	); err != nil {
		t.Fatal(err)
	}
	writeErr := errors.New("write invocation store")
	store.writeFile = func(string, []byte, os.FileMode) error { return writeErr }
	owner := gatewayTestOwner("vpn", "call-1", plan)
	_, transitioned, err := store.MarkDispatched(
		owner,
		plan.InvocationID,
		plan.PlanHash,
	)
	if !errors.Is(err, writeErr) || transitioned {
		t.Fatalf("failed dispatch = (transitioned %v, error %v)", transitioned, err)
	}
	record, found, err := store.Lookup(gatewayTestPrincipal(plan), plan.InvocationID)
	if err != nil || !found || record.State != GatewayInvocationPrepared {
		t.Fatalf("rolled-back record = (%#v, %v, %v)", record, found, err)
	}

	store.writeFile = fileutil.WriteFileAtomic
	_, transitioned, err = store.MarkDispatched(
		owner,
		plan.InvocationID,
		plan.PlanHash,
	)
	if err != nil || !transitioned {
		t.Fatalf("retry dispatch = (transitioned %v, error %v)", transitioned, err)
	}
}

func TestGatewayInvocationStoreLoadRejectsMutatedPlan(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node_invocations.json")
	plan := gatewayTestPlan(t, "inv_mutated", "idem_mutated", time.Now())
	record := GatewayInvocationRecord{
		Target:           "vpn",
		ToolCallID:       "call-1",
		Plan:             plan,
		Descriptor:       gatewayTestDescriptor(),
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
			Descriptor:       gatewayTestDescriptor(),
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

func gatewayTestDescriptor() CommandDescriptor {
	return invocationDescriptor(RiskWrite)
}

func gatewayTestPrincipal(plan ExecutionPlan) GatewayInvocationPrincipal {
	return GatewayInvocationPrincipal{
		AgentID:   plan.AgentID,
		SessionID: plan.SessionID,
		ActorID:   plan.ActorID,
	}
}

func gatewayTestOwner(
	target string,
	toolCallID string,
	plan ExecutionPlan,
) GatewayInvocationOwner {
	return GatewayInvocationOwner{
		Target:     target,
		AgentID:    plan.AgentID,
		SessionID:  plan.SessionID,
		ActorID:    plan.ActorID,
		ToolCallID: toolCallID,
	}
}
