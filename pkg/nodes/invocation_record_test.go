package nodes

import (
	"encoding/json"
	"testing"
)

func TestInvocationRecordValidation(t *testing.T) {
	record := validInvocationRecord()
	if err := record.Validate(); err != nil {
		t.Fatal(err)
	}
	record.State = InvocationSucceeded
	record.CompletedAt = 3
	record.UpdatedAt = 3
	record.Result = json.RawMessage(`{"ok":true}`)
	if err := record.Validate(); err != nil {
		t.Fatal(err)
	}
	record.CompletedAt = 2
	if err := record.Validate(); err == nil {
		t.Fatal("terminal record accepted a completion timestamp before its update")
	}
	record.CompletedAt = 3
	record.Failure = &InvocationFailure{Code: "EXECUTION_FAILED", Message: "failed"}
	if err := record.Validate(); err == nil {
		t.Fatal("successful record accepted a failure")
	}
}

func TestInvocationStateTerminal(t *testing.T) {
	for _, state := range []InvocationState{
		InvocationSucceeded,
		InvocationFailed,
		InvocationCanceled,
	} {
		if !state.Terminal() {
			t.Fatalf("state %q is not terminal", state)
		}
	}
	for _, state := range []InvocationState{
		InvocationAccepted,
		InvocationRunning,
		InvocationUnknown,
	} {
		if state.Terminal() {
			t.Fatalf("state %q is terminal", state)
		}
	}
}

func validInvocationRecord() InvocationRecord {
	return InvocationRecord{
		InvocationID:   "inv_test",
		IdempotencyKey: "idem_test",
		PlanHash:       "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		NodeID:         ID("node_test"),
		CatalogHash:    "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
		Command:        "node.info.v1",
		Risk:           RiskRead,
		State:          InvocationAccepted,
		AcceptedAt:     1,
		UpdatedAt:      1,
	}
}
