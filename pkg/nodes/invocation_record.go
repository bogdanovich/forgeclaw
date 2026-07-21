package nodes

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

const MaxInvocationFailureMessage = 512

var (
	ErrInvalidInvocationRecord = errors.New("invalid node invocation record")
	failureCodePattern         = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)
)

type InvocationState string

const (
	InvocationAccepted  InvocationState = "accepted"
	InvocationRunning   InvocationState = "running"
	InvocationSucceeded InvocationState = "succeeded"
	InvocationFailed    InvocationState = "failed"
	InvocationCanceled  InvocationState = "canceled"
	InvocationUnknown   InvocationState = "unknown"
)

func (state InvocationState) Valid() bool {
	switch state {
	case InvocationAccepted, InvocationRunning, InvocationSucceeded,
		InvocationFailed, InvocationCanceled, InvocationUnknown:
		return true
	default:
		return false
	}
}

func (state InvocationState) Terminal() bool {
	return state == InvocationSucceeded || state == InvocationFailed ||
		state == InvocationCanceled
}

type InvocationFailure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type InvocationQuery struct {
	InvocationID string `json:"invocation_id"`
}

func (query InvocationQuery) Validate() error {
	if !validInvocationIdentifier(query.InvocationID) {
		return fmt.Errorf("%w: malformed invocation query", ErrInvalidInvocation)
	}
	return nil
}

type InvocationCancelRequest struct {
	InvocationID string `json:"invocation_id"`
}

func (request InvocationCancelRequest) Validate() error {
	if !validInvocationIdentifier(request.InvocationID) {
		return fmt.Errorf("%w: malformed cancellation request", ErrInvalidInvocation)
	}
	return nil
}

// InvocationCancellation records a durable cancellation request separately
// from its outcome. TerminationConfirmed becomes true only after the node can
// prove the cancel-capable command handler has stopped.
type InvocationCancellation struct {
	RequestedAt          int64 `json:"requested_at"`
	TerminationConfirmed bool  `json:"termination_confirmed"`
}

func (failure InvocationFailure) Validate() error {
	if !failureCodePattern.MatchString(failure.Code) || failure.Message == "" ||
		len(failure.Message) > MaxInvocationFailureMessage {
		return fmt.Errorf("%w: malformed failure", ErrInvalidInvocationRecord)
	}
	return nil
}

// InvocationRecord is the durable companion-owned proof of one accepted
// logical invocation. Result bytes remain bounded by the execution plan.
type InvocationRecord struct {
	InvocationID   string                  `json:"invocation_id"`
	IdempotencyKey string                  `json:"idempotency_key"`
	PlanHash       string                  `json:"plan_hash"`
	NodeID         ID                      `json:"node_id"`
	CatalogHash    string                  `json:"catalog_hash"`
	Command        string                  `json:"command"`
	Risk           Risk                    `json:"risk"`
	State          InvocationState         `json:"state"`
	AcceptedAt     int64                   `json:"accepted_at"`
	UpdatedAt      int64                   `json:"updated_at"`
	ExpiresAt      int64                   `json:"expires_at"`
	CompletedAt    int64                   `json:"completed_at,omitempty"`
	Result         json.RawMessage         `json:"result,omitempty"`
	Failure        *InvocationFailure      `json:"failure,omitempty"`
	Cancellation   *InvocationCancellation `json:"cancellation,omitempty"`
}

func (record InvocationRecord) Validate() error {
	if !validInvocationIdentifier(record.InvocationID) ||
		!validInvocationIdentifier(record.IdempotencyKey) ||
		!validSHA256Digest(record.PlanHash) || !validSHA256Digest(record.CatalogHash) ||
		len(record.Command) == 0 || len(record.Command) > MaxCommandNameLen ||
		!commandPattern.MatchString(record.Command) || !record.Risk.Valid() ||
		!record.State.Valid() {
		return fmt.Errorf("%w: malformed identity or command", ErrInvalidInvocationRecord)
	}
	if err := record.NodeID.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidInvocationRecord, err)
	}
	if record.AcceptedAt <= 0 || record.UpdatedAt < record.AcceptedAt ||
		record.ExpiresAt <= record.AcceptedAt/int64(time.Second) {
		return fmt.Errorf("%w: malformed timestamps", ErrInvalidInvocationRecord)
	}
	if record.Cancellation != nil &&
		(record.Cancellation.RequestedAt < record.AcceptedAt ||
			record.Cancellation.RequestedAt > record.UpdatedAt ||
			(record.Cancellation.TerminationConfirmed &&
				record.State != InvocationCanceled)) {
		return fmt.Errorf("%w: malformed cancellation metadata", ErrInvalidInvocationRecord)
	}
	switch record.State {
	case InvocationSucceeded:
		if record.CompletedAt != record.UpdatedAt || len(record.Result) == 0 ||
			len(record.Result) > MaxInvocationOutput || !json.Valid(record.Result) ||
			record.Failure != nil || record.Cancellation != nil {
			return fmt.Errorf("%w: malformed successful result", ErrInvalidInvocationRecord)
		}
	case InvocationFailed:
		if record.CompletedAt != record.UpdatedAt || len(record.Result) != 0 ||
			record.Failure == nil || record.Cancellation != nil {
			return fmt.Errorf("%w: malformed terminal failure", ErrInvalidInvocationRecord)
		}
		if err := record.Failure.Validate(); err != nil {
			return err
		}
	case InvocationCanceled:
		if record.CompletedAt != record.UpdatedAt || len(record.Result) != 0 ||
			record.Failure == nil {
			return fmt.Errorf("%w: malformed terminal cancellation", ErrInvalidInvocationRecord)
		}
		if err := record.Failure.Validate(); err != nil {
			return err
		}
		if record.Cancellation == nil {
			if record.Failure.Code != "PLAN_EXPIRED" {
				return fmt.Errorf("%w: unproven terminal cancellation", ErrInvalidInvocationRecord)
			}
		} else if record.Failure.Code != "CANCELED" ||
			!record.Cancellation.TerminationConfirmed {
			return fmt.Errorf("%w: unconfirmed terminal cancellation", ErrInvalidInvocationRecord)
		}
	default:
		if record.CompletedAt != 0 || len(record.Result) != 0 || record.Failure != nil ||
			(record.State == InvocationAccepted && record.Cancellation != nil) {
			return fmt.Errorf("%w: nonterminal record contains a result", ErrInvalidInvocationRecord)
		}
	}
	return nil
}
