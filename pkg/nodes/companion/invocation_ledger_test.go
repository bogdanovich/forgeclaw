package companion

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/fileutil"
	"github.com/sipeed/picoclaw/pkg/nodes"
)

func TestInvocationLedgerPersistsTerminalResultAndDeduplicates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invocations.json")
	ledger, newErr := NewFileInvocationLedger(path, 4, 1024*1024)
	if newErr != nil {
		t.Fatal(newErr)
	}
	plan := testLedgerPlan(t, "one")
	record, existing, err := ledger.Accept(plan)
	if err != nil || existing || record.State != nodes.InvocationAccepted {
		t.Fatalf("Accept() = %#v, existing %v, error %v", record, existing, err)
	}
	if _, markErr := ledger.MarkRunning(plan.InvocationID); markErr != nil {
		t.Fatal(markErr)
	}
	result := json.RawMessage(`{"value":"durable"}`)
	if _, completeErr := ledger.CompleteSuccess(plan.InvocationID, result); completeErr != nil {
		t.Fatal(completeErr)
	}

	reloaded, err := NewFileInvocationLedger(path, 4, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, existing, err := reloaded.Accept(plan)
	if err != nil || !existing || duplicate.State != nodes.InvocationSucceeded ||
		string(duplicate.Result) != string(result) {
		t.Fatalf("duplicate Accept() = %#v, existing %v, error %v", duplicate, existing, err)
	}
	conflict := testLedgerPlanIdentity(t, plan.InvocationID, "idem_conflict")
	if _, _, err := reloaded.Accept(conflict); !errors.Is(err, ErrInvocationConflict) {
		t.Fatalf("conflicting Accept() error = %v", err)
	}
}

func TestInvocationLedgerRecoversRunningInvocationAsUnknown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invocations.json")
	ledger, newErr := NewFileInvocationLedger(path, 4, 1024*1024)
	if newErr != nil {
		t.Fatal(newErr)
	}
	plan := testLedgerPlan(t, "unfinished")
	if _, _, acceptErr := ledger.Accept(plan); acceptErr != nil {
		t.Fatal(acceptErr)
	}
	if _, markErr := ledger.MarkRunning(plan.InvocationID); markErr != nil {
		t.Fatal(markErr)
	}

	recovered, err := NewFileInvocationLedger(path, 4, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	record, found := recovered.Get(plan.InvocationID)
	if !found || record.State != nodes.InvocationUnknown || record.CompletedAt != 0 {
		t.Fatalf("recovered record = %#v, found %v", record, found)
	}
	if _, existing, err := recovered.Accept(plan); !existing || err != nil {
		t.Fatalf("unknown duplicate existing = %v, error = %v", existing, err)
	}
}

func TestInvocationLedgerPreservesAcceptedInvocationForResume(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invocations.json")
	ledger, newErr := NewFileInvocationLedger(path, 4, 1024*1024)
	if newErr != nil {
		t.Fatal(newErr)
	}
	plan := testLedgerPlan(t, "accepted")
	if _, _, acceptErr := ledger.Accept(plan); acceptErr != nil {
		t.Fatal(acceptErr)
	}

	recovered, err := NewFileInvocationLedger(path, 4, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	record, found := recovered.Get(plan.InvocationID)
	if !found || record.State != nodes.InvocationAccepted {
		t.Fatalf("recovered accepted record = %#v, found %v", record, found)
	}
}

func TestInvocationLedgerCancelsAcceptedInvocationAfterExpiry(t *testing.T) {
	clock := time.Now()
	ledger := newInvocationLedger("", 4, 1024*1024, func() time.Time { return clock })
	plan := testLedgerPlanAt(t, "expired-accepted", clock)
	if _, _, err := ledger.Accept(plan); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(2 * time.Minute)
	if err := ledger.recoverUnfinished(); err != nil {
		t.Fatal(err)
	}
	record, found := ledger.Get(plan.InvocationID)
	if !found || record.State != nodes.InvocationCanceled || record.Failure == nil ||
		record.Failure.Code != "PLAN_EXPIRED" {
		t.Fatalf("expired accepted record = %#v, found %v", record, found)
	}
}

func TestInvocationLedgerRetainsExecutableIdempotencyProof(t *testing.T) {
	clock := time.Now()
	ledger := newInvocationLedger("", 2, 1024*1024, func() time.Time { return clock })
	first := testLedgerPlan(t, "first")
	second := testLedgerPlan(t, "second")
	third := testLedgerPlan(t, "third")
	for _, plan := range []nodes.ExecutionPlan{first, second} {
		if _, _, err := ledger.Accept(plan); err != nil {
			t.Fatal(err)
		}
		if _, err := ledger.MarkRunning(plan.InvocationID); err != nil {
			t.Fatal(err)
		}
		if _, err := ledger.CompleteSuccess(plan.InvocationID, json.RawMessage(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := ledger.Accept(third); !errors.Is(err, ErrInvocationLedgerFull) {
		t.Fatalf("unexpired capacity error = %v", err)
	}
	if duplicate, existing, err := ledger.Accept(first); err != nil || !existing ||
		duplicate.State != nodes.InvocationSucceeded {
		t.Fatalf("retained duplicate = %#v, existing %v, error %v", duplicate, existing, err)
	}

	clock = clock.Add(2 * time.Minute)
	fresh := testLedgerPlanAt(t, "fresh", clock)
	if _, _, err := ledger.Accept(fresh); err != nil {
		t.Fatal(err)
	}
	if _, found := ledger.Get(first.InvocationID); found {
		t.Fatal("oldest expired terminal record was not pruned")
	}
	if _, found := ledger.Get(second.InvocationID); !found {
		t.Fatal("second expired terminal record was pruned instead of the oldest")
	}
	if _, _, err := ledger.Accept(first); !errors.Is(err, nodes.ErrInvalidInvocation) {
		t.Fatalf("pruned expired plan became executable: %v", err)
	}

	protected := newInvocationLedger("", 1, 1024*1024, time.Now)
	if _, _, err := protected.Accept(first); err != nil {
		t.Fatal(err)
	}
	if _, _, err := protected.Accept(second); !errors.Is(err, ErrInvocationLedgerFull) {
		t.Fatalf("nonterminal capacity error = %v", err)
	}
}

func TestInvocationLedgerRetainsExecutableProofUnderBytePressure(t *testing.T) {
	clock := time.Now()
	ledger := newInvocationLedger(
		filepath.Join(t.TempDir(), "invocations.json"),
		4,
		1024*1024,
		func() time.Time { return clock },
	)
	first := testLedgerPlan(t, "byte-first")
	if _, _, err := ledger.Accept(first); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.MarkRunning(first.InvocationID); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.CompleteSuccess(first.InvocationID, json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	current, err := json.Marshal(invocationLedgerDocument{
		Version: invocationLedgerVersion,
		Records: ledger.records,
	})
	if err != nil {
		t.Fatal(err)
	}
	ledger.maxBytes = len(current) + 1

	second := testLedgerPlan(t, "byte-second")
	if _, _, err := ledger.Accept(second); !errors.Is(err, ErrInvocationLedgerFull) {
		t.Fatalf("byte capacity error = %v", err)
	}
	if duplicate, existing, err := ledger.Accept(first); err != nil || !existing ||
		duplicate.State != nodes.InvocationSucceeded {
		t.Fatalf("retained duplicate = %#v, existing %v, error %v", duplicate, existing, err)
	}
}

func TestInvocationLedgerDoesNotExecuteAfterUnconfirmedAcceptance(t *testing.T) {
	ledger := newInvocationLedger(
		filepath.Join(t.TempDir(), "invocations.json"),
		4,
		1024*1024,
		time.Now,
	)
	ledger.writeFile = func(string, []byte, os.FileMode) error {
		return errors.New("storage unavailable")
	}
	plan := testLedgerPlan(t, "write-failure")
	if _, _, err := ledger.Accept(plan); err == nil {
		t.Fatal("Accept() succeeded without durable storage")
	}
	if _, found := ledger.Get(plan.InvocationID); found {
		t.Fatal("uncommitted acceptance remained in memory")
	}

	ledger.writeFile = func(string, []byte, os.FileMode) error {
		return &fileutil.CommittedWriteError{Err: errors.New("directory sync failed")}
	}
	if _, _, err := ledger.Accept(plan); err == nil {
		t.Fatal("Accept() treated unconfirmed durability as executable")
	}
	if record, found := ledger.Get(plan.InvocationID); !found || record.State != nodes.InvocationAccepted {
		t.Fatalf("committed acceptance = %#v, found %v", record, found)
	}
}

func TestInvocationLedgerFileIsPrivateAndVersioned(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "invocations.json")
	ledger, newErr := NewFileInvocationLedger(path, 4, 1024*1024)
	if newErr != nil {
		t.Fatal(newErr)
	}
	if _, _, acceptErr := ledger.Accept(testLedgerPlan(t, "private")); acceptErr != nil {
		t.Fatal(acceptErr)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("ledger mode = %o", info.Mode().Perm())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	document, err := decodeLedgerDocument(data)
	if err != nil {
		t.Fatal(err)
	}
	if document.Version != invocationLedgerVersion || len(document.Records) != 1 {
		t.Fatalf("ledger document = %#v", document)
	}
}

func testLedgerPlan(t *testing.T, suffix string) nodes.ExecutionPlan {
	return testLedgerPlanAt(t, suffix, time.Now())
}

func testLedgerPlanAt(
	t *testing.T,
	suffix string,
	preparedAt time.Time,
) nodes.ExecutionPlan {
	return testLedgerPlanIdentityAt(
		t,
		"inv_"+suffix,
		"idem_"+suffix,
		preparedAt,
	)
}

func testLedgerPlanIdentity(
	t *testing.T,
	invocationID string,
	idempotencyKey string,
) nodes.ExecutionPlan {
	return testLedgerPlanIdentityAt(t, invocationID, idempotencyKey, time.Now())
}

func testLedgerPlanIdentityAt(
	t *testing.T,
	invocationID string,
	idempotencyKey string,
	preparedAt time.Time,
) nodes.ExecutionPlan {
	t.Helper()
	descriptor := nodes.CommandDescriptor{
		Name:         "node.info.v1",
		InputSchema:  json.RawMessage(`{"type":"object","additionalProperties":false}`),
		OutputSchema: json.RawMessage(`{"type":"object"}`),
		Risk:         nodes.RiskRead,
	}
	catalog := nodes.CapabilityCatalog{Commands: []nodes.CommandDescriptor{descriptor}}
	catalogHash, err := catalog.Hash()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := nodes.PrepareExecutionPlan(nodes.InvocationRequest{
		InvocationID:     invocationID,
		IdempotencyKey:   idempotencyKey,
		NodeID:           nodes.ID("node_test"),
		CatalogHash:      catalogHash,
		Command:          descriptor.Name,
		Input:            json.RawMessage(`{}`),
		AgentID:          "agent_test",
		SessionID:        "session_test",
		ActorID:          "actor_test",
		TimeoutSeconds:   5,
		OutputLimitBytes: 4096,
	}, descriptor, LocalExecutor, "policy-test", preparedAt, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}
