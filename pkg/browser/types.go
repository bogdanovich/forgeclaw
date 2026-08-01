package browser

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const (
	MaxIdentifierBytes  = 128
	MaxSafeFailureBytes = 256
	MaxTerminalBytes    = 320 * 1024
)

var (
	ErrBusy              = errors.New("browser profile is busy")
	ErrConflict          = errors.New("browser state conflicts with durable state")
	ErrDenied            = errors.New("browser authority denied")
	ErrInvalid           = errors.New("invalid browser state")
	ErrNotFound          = errors.New("browser state not found")
	ErrStale             = errors.New("browser state revision is stale")
	ErrWorkerUnavailable = errors.New("browser worker is unavailable")
	identifierRegexp     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)
	safeFailureRegexp    = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
)

type SessionState string

const (
	SessionOpening SessionState = "opening"
	SessionReady   SessionState = "ready"
	SessionClosing SessionState = "closing"
	SessionClosed  SessionState = "closed"
	SessionExpired SessionState = "expired"
	SessionLost    SessionState = "lost"
)

func (state SessionState) Valid() bool {
	switch state {
	case SessionOpening, SessionReady, SessionClosing, SessionClosed, SessionExpired, SessionLost:
		return true
	default:
		return false
	}
}

func (state SessionState) Terminal() bool {
	return state == SessionClosed || state == SessionExpired || state == SessionLost
}

func validSessionTransition(from, to SessionState) bool {
	switch from {
	case SessionOpening:
		return to == SessionReady || to == SessionClosing || to == SessionLost
	case SessionReady:
		return to == SessionClosing || to == SessionExpired || to == SessionLost
	case SessionClosing:
		return to == SessionClosed || to == SessionExpired || to == SessionLost
	default:
		return false
	}
}

type InvocationState string

const (
	InvocationPrepared  InvocationState = "prepared"
	InvocationAccepted  InvocationState = "accepted"
	InvocationSucceeded InvocationState = "succeeded"
	InvocationFailed    InvocationState = "failed"
	InvocationUnknown   InvocationState = "unknown"
	InvocationCanceled  InvocationState = "canceled"
)

func (state InvocationState) Valid() bool {
	switch state {
	case InvocationPrepared, InvocationAccepted, InvocationSucceeded, InvocationFailed,
		InvocationUnknown, InvocationCanceled:
		return true
	default:
		return false
	}
}

func (state InvocationState) Terminal() bool {
	return state == InvocationSucceeded || state == InvocationFailed ||
		state == InvocationUnknown || state == InvocationCanceled
}

func validInvocationTransition(from, to InvocationState) bool {
	switch from {
	case InvocationPrepared:
		return to == InvocationAccepted || to == InvocationCanceled
	case InvocationAccepted:
		return to == InvocationSucceeded || to == InvocationFailed ||
			to == InvocationUnknown || to == InvocationCanceled
	default:
		return false
	}
}

type Effect string

const (
	EffectRead           Effect = "read"
	EffectNavigation     Effect = "navigation"
	EffectLocalEdit      Effect = "local_edit"
	EffectExternalCommit Effect = "external_commit"
	EffectUnknown        Effect = "unknown"
)

func (effect Effect) Valid() bool {
	switch effect {
	case EffectRead, EffectNavigation, EffectLocalEdit, EffectExternalCommit, EffectUnknown:
		return true
	default:
		return false
	}
}

type Owner struct {
	ActorID     string `json:"actor_id"`
	AgentID     string `json:"agent_id"`
	SessionKey  string `json:"session_key"`
	ExecutionID string `json:"execution_id"`
}

func (owner Owner) Validate() error {
	values := []string{owner.ActorID, owner.AgentID, owner.SessionKey, owner.ExecutionID}
	for _, value := range values {
		if value == "" || strings.TrimSpace(value) != value || len(value) > MaxIdentifierBytes {
			return fmt.Errorf("%w: malformed owner", ErrInvalid)
		}
	}
	return nil
}

func (owner Owner) Equal(other Owner) bool {
	return owner == other
}

type Session struct {
	ID                   string       `json:"id"`
	Owner                Owner        `json:"owner"`
	Target               string       `json:"target"`
	Profile              string       `json:"profile"`
	State                SessionState `json:"state"`
	DryRun               bool         `json:"dry_run"`
	PolicyRevision       string       `json:"policy_revision"`
	ControllerGeneration uint64       `json:"controller_generation"`
	Revision             uint64       `json:"revision"`
	CreatedAt            int64        `json:"created_at"`
	UpdatedAt            int64        `json:"updated_at"`
	LastActivityAt       int64        `json:"last_activity_at"`
	ExpiresAt            int64        `json:"expires_at"`
	SafeFailure          string       `json:"safe_failure,omitempty"`
}

func (session Session) Validate() error {
	if !validIdentifier(session.ID) || !validIdentifier(session.Target) ||
		!validIdentifier(session.Profile) || !validIdentifier(session.PolicyRevision) ||
		!session.State.Valid() || session.Owner.Validate() != nil ||
		session.ControllerGeneration == 0 || session.Revision == 0 ||
		session.CreatedAt <= 0 || session.UpdatedAt < session.CreatedAt ||
		session.LastActivityAt < session.CreatedAt || session.LastActivityAt > session.UpdatedAt ||
		session.ExpiresAt <= session.CreatedAt || len(session.SafeFailure) > MaxSafeFailureBytes {
		return fmt.Errorf("%w: malformed session", ErrInvalid)
	}
	if session.State == SessionLost && session.SafeFailure == "" {
		return fmt.Errorf("%w: lost session requires a safe failure", ErrInvalid)
	}
	if session.State != SessionLost && session.SafeFailure != "" {
		return fmt.Errorf("%w: non-lost session contains a failure", ErrInvalid)
	}
	if session.SafeFailure != "" && !safeFailureRegexp.MatchString(session.SafeFailure) {
		return fmt.Errorf("%w: malformed safe failure", ErrInvalid)
	}
	return nil
}

type Invocation struct {
	ID             string          `json:"id"`
	SessionID      string          `json:"session_id"`
	Owner          Owner           `json:"owner"`
	ActionHash     string          `json:"action_hash"`
	Effect         Effect          `json:"effect"`
	State          InvocationState `json:"state"`
	Revision       uint64          `json:"revision"`
	CreatedAt      int64           `json:"created_at"`
	UpdatedAt      int64           `json:"updated_at"`
	ExpiresAt      int64           `json:"expires_at"`
	AcceptedAt     int64           `json:"accepted_at,omitempty"`
	CompletedAt    int64           `json:"completed_at,omitempty"`
	TerminalResult json.RawMessage `json:"terminal_result,omitempty"`
	SafeFailure    string          `json:"safe_failure,omitempty"`
}

func (invocation Invocation) Validate() error {
	if !validIdentifier(invocation.ID) || !validIdentifier(invocation.SessionID) ||
		!validDigest(invocation.ActionHash) || invocation.Owner.Validate() != nil ||
		!invocation.Effect.Valid() || !invocation.State.Valid() || invocation.Revision == 0 ||
		invocation.CreatedAt <= 0 || invocation.UpdatedAt < invocation.CreatedAt ||
		invocation.ExpiresAt <= invocation.CreatedAt || len(invocation.SafeFailure) > MaxSafeFailureBytes ||
		len(invocation.TerminalResult) > MaxTerminalBytes {
		return fmt.Errorf("%w: malformed invocation", ErrInvalid)
	}
	if invocation.SafeFailure != "" && !safeFailureRegexp.MatchString(invocation.SafeFailure) {
		return fmt.Errorf("%w: malformed safe failure", ErrInvalid)
	}
	if invocation.State == InvocationPrepared {
		if invocation.AcceptedAt != 0 || invocation.CompletedAt != 0 ||
			len(invocation.TerminalResult) != 0 || invocation.SafeFailure != "" {
			return fmt.Errorf("%w: prepared invocation contains outcome data", ErrInvalid)
		}
		return nil
	}
	if invocation.State == InvocationCanceled && invocation.AcceptedAt == 0 {
		if invocation.CompletedAt != invocation.UpdatedAt ||
			invocation.CompletedAt < invocation.CreatedAt ||
			len(invocation.TerminalResult) != 0 || invocation.SafeFailure == "" {
			return fmt.Errorf("%w: malformed pre-acceptance cancellation", ErrInvalid)
		}
		return nil
	}
	if invocation.AcceptedAt < invocation.CreatedAt || invocation.AcceptedAt > invocation.UpdatedAt {
		return fmt.Errorf("%w: malformed invocation acceptance", ErrInvalid)
	}
	if !invocation.State.Terminal() {
		if invocation.CompletedAt != 0 || len(invocation.TerminalResult) != 0 || invocation.SafeFailure != "" {
			return fmt.Errorf("%w: accepted invocation contains terminal data", ErrInvalid)
		}
		return nil
	}
	if invocation.CompletedAt != invocation.UpdatedAt || invocation.CompletedAt < invocation.AcceptedAt {
		return fmt.Errorf("%w: malformed invocation completion", ErrInvalid)
	}
	if invocation.State == InvocationSucceeded {
		if len(invocation.TerminalResult) == 0 || !json.Valid(invocation.TerminalResult) ||
			invocation.SafeFailure != "" {
			return fmt.Errorf("%w: malformed successful invocation", ErrInvalid)
		}
	} else if len(invocation.TerminalResult) != 0 || invocation.SafeFailure == "" {
		return fmt.Errorf("%w: malformed terminal invocation", ErrInvalid)
	}
	return nil
}

func validIdentifier(value string) bool {
	return len(value) <= MaxIdentifierBytes && identifierRegexp.MatchString(value)
}

func validDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}
