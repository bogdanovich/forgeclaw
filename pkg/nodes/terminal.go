package nodes

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/bogdanovich/mintclaw/pkg/nodes/internal/jsonstrict"
)

const (
	MaxTerminalOpenPlanTTL          = 5 * time.Minute
	MaxTerminalTransportFrameBytes  = 32 * 1024
	MaxTerminalTransportBuffer      = 1024 * 1024
	MinTerminalTransportEventCharge = 256
	TerminalProtocolVersion         = 1
)

var ErrInvalidTerminal = errors.New("invalid node terminal request")

// TerminalOwner is the complete authority tuple retained on every terminal
// operation. Terminal IDs are correlation values and grant no authority.
type TerminalOwner struct {
	ActorID     string `json:"actor_id"`
	AgentID     string `json:"agent_id"`
	RouteID     string `json:"route_id"`
	SessionID   string `json:"session_id"`
	WorkspaceID string `json:"workspace_id"`
	Target      string `json:"target"`
	Profile     string `json:"profile"`
}

func (owner TerminalOwner) Validate() error {
	if !validInvocationIdentifier(owner.ActorID) ||
		!validInvocationIdentifier(owner.AgentID) ||
		!validInvocationIdentifier(owner.RouteID) ||
		!validInvocationIdentifier(owner.SessionID) ||
		!validInvocationIdentifier(owner.WorkspaceID) ||
		!validInvocationIdentifier(owner.Target) ||
		!validInvocationIdentifier(owner.Profile) {
		return fmt.Errorf("%w: malformed terminal owner", ErrInvalidTerminal)
	}
	return nil
}

// TerminalOpenPlan is the immutable authority reviewed before a live PTY is
// allocated. AuthorityDigest is a safe binding to the node-local profile; it
// does not reveal that profile's identity, shell, paths, or environment.
type TerminalOpenPlan struct {
	OpenID          string        `json:"open_id"`
	IdempotencyKey  string        `json:"idempotency_key"`
	NodeID          ID            `json:"node_id"`
	Owner           TerminalOwner `json:"owner"`
	CatalogHash     string        `json:"catalog_hash"`
	AuthorityDigest string        `json:"authority_digest"`
	WorkingScope    string        `json:"working_scope"`
	Columns         int           `json:"columns"`
	Rows            int           `json:"rows"`
	ApprovalMode    string        `json:"approval_mode"`
	PreparedAt      int64         `json:"prepared_at"`
	ExpiresAt       int64         `json:"expires_at"`
	PlanHash        string        `json:"plan_hash"`
}

func PrepareTerminalOpenPlan(
	plan TerminalOpenPlan,
	preparedAt time.Time,
	ttl time.Duration,
) (TerminalOpenPlan, error) {
	plan.PreparedAt = preparedAt.Unix()
	plan.ExpiresAt = preparedAt.Add(ttl).Unix()
	plan.PlanHash = ""
	if err := plan.validateFields(); err != nil {
		return TerminalOpenPlan{}, err
	}
	hash, err := plan.computeHash()
	if err != nil {
		return TerminalOpenPlan{}, err
	}
	plan.PlanHash = hash
	return plan, nil
}

func (plan TerminalOpenPlan) Validate() error {
	if err := plan.validateFields(); err != nil {
		return err
	}
	want, err := plan.computeHash()
	if err != nil {
		return err
	}
	if !validSHA256Digest(plan.PlanHash) ||
		subtle.ConstantTimeCompare([]byte(plan.PlanHash), []byte(want)) != 1 {
		return fmt.Errorf("%w: terminal plan hash mismatch", ErrInvalidTerminal)
	}
	return nil
}

func (plan TerminalOpenPlan) ValidateAgainstHash(expected string) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	if !validSHA256Digest(expected) ||
		subtle.ConstantTimeCompare([]byte(plan.PlanHash), []byte(expected)) != 1 {
		return fmt.Errorf("%w: terminal plan does not match retained hash", ErrCommandDenied)
	}
	return nil
}

func (plan TerminalOpenPlan) validateFields() error {
	if !validInvocationIdentifier(plan.OpenID) ||
		!validInvocationIdentifier(plan.IdempotencyKey) {
		return fmt.Errorf("%w: malformed terminal open identity", ErrInvalidTerminal)
	}
	if err := plan.NodeID.Validate(); err != nil {
		return fmt.Errorf("%w: malformed terminal node", ErrInvalidTerminal)
	}
	if err := plan.Owner.Validate(); err != nil {
		return err
	}
	if !validSHA256Digest(plan.CatalogHash) ||
		!validSHA256Digest(plan.AuthorityDigest) ||
		!validInvocationIdentifier(plan.WorkingScope) ||
		plan.Columns < 20 || plan.Columns > 400 ||
		plan.Rows < 5 || plan.Rows > 200 ||
		plan.ApprovalMode != "session_start" {
		return fmt.Errorf("%w: malformed terminal authority", ErrInvalidTerminal)
	}
	if plan.PreparedAt <= 0 || plan.ExpiresAt <= plan.PreparedAt ||
		plan.ExpiresAt-plan.PreparedAt > int64(MaxTerminalOpenPlanTTL/time.Second) {
		return fmt.Errorf("%w: terminal plan lifetime is outside bounds", ErrInvalidTerminal)
	}
	return nil
}

func (plan TerminalOpenPlan) computeHash() (string, error) {
	plan.PlanHash = ""
	data, err := json.Marshal(plan)
	if err != nil {
		return "", fmt.Errorf("%w: encode terminal plan", ErrInvalidTerminal)
	}
	canonical, err := jsonstrict.Canonical(data)
	if err != nil {
		return "", fmt.Errorf("%w: canonicalize terminal plan", ErrInvalidTerminal)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

type TerminalSessionRequest struct {
	TerminalID string        `json:"terminal_id"`
	Owner      TerminalOwner `json:"owner"`
}

func (request TerminalSessionRequest) Validate() error {
	if !validInvocationIdentifier(request.TerminalID) {
		return fmt.Errorf("%w: malformed terminal identity", ErrInvalidTerminal)
	}
	return request.Owner.Validate()
}

type TerminalControlRequest struct {
	TerminalSessionRequest
	Sequence       uint64 `json:"sequence"`
	IdempotencyKey string `json:"idempotency_key"`
	InputBase64    string `json:"input_base64,omitempty"`
	Columns        int    `json:"columns,omitempty"`
	Rows           int    `json:"rows,omitempty"`
	Signal         string `json:"signal,omitempty"`
	Close          bool   `json:"close,omitempty"`
}

func (request TerminalControlRequest) Validate() error {
	if err := request.TerminalSessionRequest.Validate(); err != nil {
		return err
	}
	if request.Sequence == 0 || !validInvocationIdentifier(request.IdempotencyKey) {
		return fmt.Errorf("%w: malformed terminal control identity", ErrInvalidTerminal)
	}
	operations := 0
	if request.InputBase64 != "" {
		operations++
	}
	if request.Columns != 0 || request.Rows != 0 {
		operations++
		if request.Columns < 20 || request.Columns > 400 ||
			request.Rows < 5 || request.Rows > 200 {
			return fmt.Errorf("%w: terminal resize is outside bounds", ErrInvalidTerminal)
		}
	}
	if request.Signal != "" {
		operations++
		switch request.Signal {
		case "INT", "TERM", "HUP":
		default:
			return fmt.Errorf("%w: terminal signal is unsupported", ErrInvalidTerminal)
		}
	}
	if request.Close {
		operations++
	}
	if operations != 1 {
		return fmt.Errorf("%w: terminal control requires exactly one operation", ErrInvalidTerminal)
	}
	if request.InputBase64 != "" {
		data, err := base64.StdEncoding.Strict().DecodeString(request.InputBase64)
		if err != nil || len(data) == 0 || len(data) > MaxTerminalTransportFrameBytes {
			return fmt.Errorf("%w: terminal input frame is invalid", ErrInvalidTerminal)
		}
	}
	return nil
}

type TerminalMetadata struct {
	TerminalID           string        `json:"terminal_id"`
	Owner                TerminalOwner `json:"owner"`
	State                string        `json:"state"`
	Reason               string        `json:"reason,omitempty"`
	StartedAt            int64         `json:"started_at"`
	CompletedAt          int64         `json:"completed_at,omitempty"`
	ExitCode             int           `json:"exit_code,omitempty"`
	Signal               string        `json:"signal,omitempty"`
	TerminationConfirmed bool          `json:"termination_confirmed,omitempty"`
}

// TerminalEvent is the live transport shape. DataBase64 is present only for
// output events and must never be copied into passive records or logs.
type TerminalEvent struct {
	Version              int    `json:"version"`
	Type                 string `json:"type"`
	TerminalID           string `json:"terminal_id"`
	AcceptedSequence     uint64 `json:"accepted_sequence,omitempty"`
	Cursor               uint64 `json:"cursor,omitempty"`
	DataBase64           string `json:"data_base64,omitempty"`
	State                string `json:"state,omitempty"`
	Reason               string `json:"reason,omitempty"`
	ExitCode             int    `json:"exit_code,omitempty"`
	Signal               string `json:"signal,omitempty"`
	StartedAt            int64  `json:"started_at,omitempty"`
	CompletedAt          int64  `json:"completed_at,omitempty"`
	TerminationConfirmed bool   `json:"termination_confirmed,omitempty"`
}

// Validate enforces the shared live-event contract at both sides of the
// authenticated transport. The returned size is the decoded output byte count
// and is zero for non-output events.
func (event TerminalEvent) Validate() (int, error) {
	if event.Version != TerminalProtocolVersion ||
		!validInvocationIdentifier(event.TerminalID) {
		return 0, fmt.Errorf("%w: malformed terminal event identity", ErrInvalidTerminal)
	}
	noBytesOrCursor := func() bool {
		return event.Cursor == 0 && event.DataBase64 == ""
	}
	noLifecycle := func() bool {
		return event.Reason == "" &&
			event.ExitCode == 0 &&
			event.Signal == "" &&
			event.StartedAt == 0 &&
			event.CompletedAt == 0 &&
			!event.TerminationConfirmed
	}
	switch event.Type {
	case "output":
		data, err := base64.StdEncoding.Strict().DecodeString(event.DataBase64)
		if err != nil || len(data) == 0 || len(data) > MaxTerminalTransportFrameBytes ||
			event.Cursor < uint64(len(data)) ||
			event.AcceptedSequence != 0 ||
			event.State != "" ||
			!noLifecycle() {
			return 0, fmt.Errorf("%w: malformed terminal output event", ErrInvalidTerminal)
		}
		return len(data), nil
	case "ack":
		if event.AcceptedSequence == 0 ||
			event.State != "live" ||
			!noBytesOrCursor() ||
			!noLifecycle() {
			return 0, fmt.Errorf("%w: malformed terminal acknowledgement", ErrInvalidTerminal)
		}
	case "denied":
		if event.AcceptedSequence != 0 ||
			event.State != "live" ||
			!validInvocationIdentifier(event.Reason) ||
			!noBytesOrCursor() ||
			event.ExitCode != 0 ||
			event.Signal != "" ||
			event.StartedAt != 0 ||
			event.CompletedAt != 0 ||
			event.TerminationConfirmed {
			return 0, fmt.Errorf("%w: malformed terminal denial", ErrInvalidTerminal)
		}
	case "closed":
		if event.AcceptedSequence != 0 ||
			event.State != "closed" ||
			!validInvocationIdentifier(event.Reason) ||
			!noBytesOrCursor() ||
			event.StartedAt <= 0 ||
			event.CompletedAt < event.StartedAt ||
			!event.TerminationConfirmed ||
			!validTerminalSignalMetadata(event.Signal) {
			return 0, fmt.Errorf("%w: malformed terminal close event", ErrInvalidTerminal)
		}
	case "unknown":
		if event.AcceptedSequence != 0 ||
			event.State != "unknown" ||
			!validInvocationIdentifier(event.Reason) ||
			!noBytesOrCursor() ||
			event.ExitCode != 0 ||
			event.StartedAt <= 0 ||
			event.CompletedAt != 0 ||
			event.TerminationConfirmed ||
			event.Signal != "" {
			return 0, fmt.Errorf("%w: malformed terminal unknown event", ErrInvalidTerminal)
		}
	default:
		return 0, fmt.Errorf("%w: unsupported terminal event type", ErrInvalidTerminal)
	}
	return 0, nil
}

func validTerminalSignalMetadata(signal string) bool {
	return len(signal) <= 32 &&
		strings.IndexFunc(signal, unicode.IsControl) < 0
}
