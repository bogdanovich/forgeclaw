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

func TestInvocationRecordValidatesCancellationMetadata(t *testing.T) {
	record := validInvocationRecord()
	record.State = InvocationRunning
	record.UpdatedAt = 2
	record.Cancellation = &InvocationCancellation{RequestedAt: 2}
	if err := record.Validate(); err != nil {
		t.Fatal(err)
	}
	record.Cancellation.TerminationConfirmed = true
	if err := record.Validate(); err == nil {
		t.Fatal("running invocation confirmed termination")
	}
	record.State = InvocationCanceled
	record.CompletedAt = 2
	record.Failure = &InvocationFailure{Code: "CANCELED", Message: "canceled"}
	if err := record.Validate(); err != nil {
		t.Fatal(err)
	}
	record.Cancellation = nil
	if err := record.Validate(); err == nil {
		t.Fatal("explicit cancellation validated without termination proof")
	}
	record.Failure = &InvocationFailure{Code: "PLAN_EXPIRED", Message: "expired"}
	if err := record.Validate(); err != nil {
		t.Fatal(err)
	}
	record.State = InvocationSucceeded
	record.Result = json.RawMessage(`{"ok":true}`)
	record.Failure = nil
	record.Cancellation = &InvocationCancellation{RequestedAt: 2}
	if err := record.Validate(); err == nil {
		t.Fatal("successful invocation retained cancellation metadata")
	}
}

func TestInvocationCancelRequestValidation(t *testing.T) {
	if err := (InvocationCancelRequest{InvocationID: "inv_test"}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (InvocationCancelRequest{}).Validate(); err == nil {
		t.Fatal("empty cancellation request validated")
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
		ExpiresAt:      2,
	}
}
