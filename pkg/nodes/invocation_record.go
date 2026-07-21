package nodes

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
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
	InvocationID   string             `json:"invocation_id"`
	IdempotencyKey string             `json:"idempotency_key"`
	PlanHash       string             `json:"plan_hash"`
	NodeID         ID                 `json:"node_id"`
	CatalogHash    string             `json:"catalog_hash"`
	Command        string             `json:"command"`
	Risk           Risk               `json:"risk"`
	State          InvocationState    `json:"state"`
	AcceptedAt     int64              `json:"accepted_at"`
	UpdatedAt      int64              `json:"updated_at"`
	CompletedAt    int64              `json:"completed_at,omitempty"`
	Result         json.RawMessage    `json:"result,omitempty"`
	Failure        *InvocationFailure `json:"failure,omitempty"`
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
	if record.AcceptedAt <= 0 || record.UpdatedAt < record.AcceptedAt {
		return fmt.Errorf("%w: malformed timestamps", ErrInvalidInvocationRecord)
	}
	switch record.State {
	case InvocationSucceeded:
		if record.CompletedAt != record.UpdatedAt || len(record.Result) == 0 ||
			len(record.Result) > MaxInvocationOutput || !json.Valid(record.Result) ||
			record.Failure != nil {
			return fmt.Errorf("%w: malformed successful result", ErrInvalidInvocationRecord)
		}
	case InvocationFailed, InvocationCanceled:
		if record.CompletedAt != record.UpdatedAt || len(record.Result) != 0 ||
			record.Failure == nil {
			return fmt.Errorf("%w: malformed terminal failure", ErrInvalidInvocationRecord)
		}
		if err := record.Failure.Validate(); err != nil {
			return err
		}
	default:
		if record.CompletedAt != 0 || len(record.Result) != 0 || record.Failure != nil {
			return fmt.Errorf("%w: nonterminal record contains a result", ErrInvalidInvocationRecord)
		}
	}
	return nil
}
