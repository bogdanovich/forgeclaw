package browser

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/bogdanovich/mintclaw/pkg/config"
)

const (
	MaxIdentifierBytes  = 128
	MaxSafeFailureBytes = 256
	MaxTerminalBytes    = 320 * 1024
	MaxURLBytes         = 2048
	MaxElementNameBytes = 512
	MaxScrollAmount     = 5
)

var (
	ErrBusy               = errors.New("browser profile is busy")
	ErrConflict           = errors.New("browser state conflicts with durable state")
	ErrDenied             = errors.New("browser authority denied")
	ErrDriverIncompatible = errors.New("browser driver is incompatible")
	ErrDriverRejected     = errors.New("browser driver rejected the operation")
	ErrInvalid            = errors.New("invalid browser state")
	ErrNotFound           = errors.New("browser state not found")
	ErrApprovalRequired   = errors.New("browser action requires approval")
	ErrStale              = errors.New("browser state revision is stale")
	ErrWorkerUnavailable  = errors.New("browser worker is unavailable")
	identifierRegexp      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)
	safeFailureRegexp     = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
	elementRoleRegexp     = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
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
		return to == SessionReady || to == SessionClosing || to == SessionExpired || to == SessionLost
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
			to == InvocationUnknown
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

type ActionKind string

const (
	ActionNavigate ActionKind = "navigate"
	ActionClick    ActionKind = "click"
	ActionFill     ActionKind = "fill"
	ActionSelect   ActionKind = "select"
	ActionPress    ActionKind = "press"
	ActionScroll   ActionKind = "scroll"
)

func (kind ActionKind) Valid() bool {
	switch kind {
	case ActionNavigate, ActionClick, ActionFill, ActionSelect, ActionPress, ActionScroll:
		return true
	default:
		return false
	}
}

type Action struct {
	Kind      ActionKind `json:"kind"`
	URL       string     `json:"url,omitempty"`
	Ref       string     `json:"ref,omitempty"`
	Value     string     `json:"value,omitempty"`
	Key       string     `json:"key,omitempty"`
	Direction string     `json:"direction,omitempty"`
	Amount    int        `json:"amount,omitempty"`
}

func (action Action) Validate(maxTextBytes int) error {
	if !action.Kind.Valid() || len(action.URL) > MaxURLBytes || len(action.Value) > maxTextBytes {
		return fmt.Errorf("%w: malformed browser action", ErrInvalid)
	}
	switch action.Kind {
	case ActionNavigate:
		if action.URL == "" || action.Ref != "" || action.Value != "" || action.Key != "" || action.Direction != "" ||
			action.Amount != 0 {
			return fmt.Errorf("%w: malformed navigate action", ErrInvalid)
		}
	case ActionClick:
		if !validIdentifier(action.Ref) || action.URL != "" || action.Value != "" || action.Key != "" ||
			action.Direction != "" ||
			action.Amount != 0 {
			return fmt.Errorf("%w: malformed click action", ErrInvalid)
		}
	case ActionFill:
		if !validIdentifier(action.Ref) || action.URL != "" || action.Key != "" || action.Direction != "" ||
			action.Amount != 0 {
			return fmt.Errorf("%w: malformed fill action", ErrInvalid)
		}
	case ActionSelect:
		if !validIdentifier(action.Ref) || action.URL != "" || action.Key != "" || action.Direction != "" ||
			action.Amount != 0 {
			return fmt.Errorf("%w: malformed select action", ErrInvalid)
		}
	case ActionPress:
		if action.URL != "" || action.Ref != "" || action.Value != "" || !validBrowserKey(action.Key) ||
			action.Direction != "" ||
			action.Amount != 0 {
			return fmt.Errorf("%w: malformed press action", ErrInvalid)
		}
	case ActionScroll:
		if action.URL != "" || action.Ref != "" || action.Value != "" || action.Key != "" ||
			(action.Direction != "up" && action.Direction != "down") || action.Amount < 1 || action.Amount > MaxScrollAmount {
			return fmt.Errorf("%w: malformed scroll action", ErrInvalid)
		}
	}
	return nil
}

func validBrowserKey(key string) bool {
	switch key {
	case "Escape", "Tab", "Shift+Tab", "ArrowUp", "ArrowDown", "ArrowLeft", "ArrowRight",
		"Home", "End", "PageUp", "PageDown", "Backspace", "Delete", "Enter":
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
	TabID                string       `json:"tab_id"`
	SnapshotID           string       `json:"snapshot_id,omitempty"`
	SnapshotGeneration   uint64       `json:"snapshot_generation"`
	SnapshotOrigin       string       `json:"snapshot_origin,omitempty"`
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
		session.ControllerGeneration == 0 || !validIdentifier(session.TabID) || session.Revision == 0 ||
		session.CreatedAt <= 0 || session.UpdatedAt < session.CreatedAt ||
		session.LastActivityAt < session.CreatedAt || session.LastActivityAt > session.UpdatedAt ||
		session.ExpiresAt <= session.CreatedAt || len(session.SafeFailure) > MaxSafeFailureBytes {
		return fmt.Errorf("%w: malformed session", ErrInvalid)
	}
	if (session.SnapshotID == "") != (session.SnapshotOrigin == "") ||
		(session.SnapshotID != "" &&
			(!validIdentifier(session.SnapshotID) || session.SnapshotGeneration == 0)) ||
		len(session.SnapshotOrigin) > MaxURLBytes {
		return fmt.Errorf("%w: malformed session snapshot", ErrInvalid)
	}
	if session.SnapshotOrigin != "" {
		normalized, err := config.NormalizeBrowserOrigin(session.SnapshotOrigin)
		if err != nil || normalized != session.SnapshotOrigin {
			return fmt.Errorf("%w: malformed session snapshot origin", ErrInvalid)
		}
	}
	if session.State.Terminal() && session.SnapshotID != "" {
		return fmt.Errorf("%w: terminal session retains snapshot authority", ErrInvalid)
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

// PreparedAction is the immutable durable authority that approval binds. It
// deliberately omits the driver-local element target; that target remains in
// the live worker slot and is usable only while the bound snapshot is fresh.
type PreparedAction struct {
	ID                   string `json:"id"`
	RequestID            string `json:"request_id"`
	SessionID            string `json:"session_id"`
	Owner                Owner  `json:"owner"`
	Target               string `json:"target"`
	Profile              string `json:"profile"`
	ControllerGeneration uint64 `json:"controller_generation"`
	TabID                string `json:"tab_id"`
	SnapshotID           string `json:"snapshot_id"`
	SnapshotGeneration   uint64 `json:"snapshot_generation"`
	CurrentOrigin        string `json:"current_origin"`
	DestinationOrigin    string `json:"destination_origin,omitempty"`
	Action               Action `json:"action"`
	InputDigest          string `json:"input_digest,omitempty"`
	InputBytes           int    `json:"input_bytes,omitempty"`
	ElementRole          string `json:"element_role,omitempty"`
	ElementName          string `json:"element_name,omitempty"`
	Effect               Effect `json:"effect"`
	DryRun               bool   `json:"dry_run"`
	PolicyRevision       string `json:"policy_revision"`
	CatalogRevision      string `json:"catalog_revision"`
	ActionHash           string `json:"action_hash"`
	CreatedAt            int64  `json:"created_at"`
	ExpiresAt            int64  `json:"expires_at"`
}

func (prepared PreparedAction) Validate(maxTextBytes int) error {
	if !validIdentifier(prepared.ID) || !validIdentifier(prepared.RequestID) ||
		!validIdentifier(prepared.SessionID) || prepared.Owner.Validate() != nil ||
		!validIdentifier(prepared.Target) || !validIdentifier(prepared.Profile) ||
		prepared.ControllerGeneration == 0 || !validIdentifier(prepared.TabID) ||
		!validIdentifier(prepared.SnapshotID) || prepared.SnapshotGeneration == 0 ||
		prepared.CurrentOrigin == "" || len(prepared.CurrentOrigin) > MaxURLBytes ||
		len(prepared.DestinationOrigin) > MaxURLBytes || len(prepared.ElementRole) > 64 ||
		len(prepared.ElementName) > MaxElementNameBytes || !prepared.Effect.Valid() ||
		!validIdentifier(prepared.PolicyRevision) || !validDigest(prepared.CatalogRevision) ||
		!validDigest(prepared.ActionHash) || prepared.CreatedAt <= 0 || prepared.ExpiresAt <= prepared.CreatedAt ||
		prepared.Action.Validate(maxTextBytes) != nil {
		return fmt.Errorf("%w: malformed prepared action", ErrInvalid)
	}
	currentOrigin, err := config.NormalizeBrowserOrigin(prepared.CurrentOrigin)
	if err != nil || currentOrigin != prepared.CurrentOrigin {
		return fmt.Errorf("%w: malformed prepared action origin", ErrInvalid)
	}
	switch prepared.Action.Kind {
	case ActionNavigate:
		normalizedURL, normalizeErr := normalizeDriverNavigationURL(prepared.Action.URL)
		destination, destinationErr := originFromURL(prepared.Action.URL)
		if normalizeErr != nil || normalizedURL != prepared.Action.URL || destinationErr != nil ||
			destination != prepared.DestinationOrigin || prepared.Effect != EffectNavigation ||
			prepared.ElementRole != "" || prepared.ElementName != "" ||
			prepared.InputDigest != "" || prepared.InputBytes != 0 {
			return fmt.Errorf("%w: malformed prepared navigation", ErrInvalid)
		}
	case ActionFill, ActionSelect:
		if prepared.DestinationOrigin != "" || !editableElementRole(prepared.ElementRole) ||
			prepared.Effect != EffectLocalEdit || prepared.Action.Value != "" ||
			!validDigest(prepared.InputDigest) || prepared.InputBytes < 0 ||
			prepared.InputBytes > maxTextBytes {
			return fmt.Errorf("%w: malformed prepared local edit", ErrInvalid)
		}
		if prepared.Action.Kind == ActionSelect && prepared.ElementRole != "combobox" {
			return fmt.Errorf("%w: malformed prepared selection", ErrInvalid)
		}
	case ActionClick:
		if prepared.DestinationOrigin != "" || !elementRoleRegexp.MatchString(prepared.ElementRole) ||
			prepared.Effect != classifyClickEffect(DriverElement{Role: prepared.ElementRole}) ||
			prepared.InputDigest != "" || prepared.InputBytes != 0 {
			return fmt.Errorf("%w: malformed prepared click", ErrInvalid)
		}
	case ActionPress:
		if prepared.DestinationOrigin != "" || prepared.ElementRole != "" || prepared.ElementName != "" ||
			prepared.Effect != classifyPressEffect(
				prepared.Action.Key,
			) || prepared.InputDigest != "" || prepared.InputBytes != 0 {
			return fmt.Errorf("%w: malformed prepared key press", ErrInvalid)
		}
	case ActionScroll:
		if prepared.DestinationOrigin != "" || prepared.ElementRole != "" || prepared.ElementName != "" ||
			prepared.Effect != EffectRead || prepared.InputDigest != "" || prepared.InputBytes != 0 {
			return fmt.Errorf("%w: malformed prepared scroll", ErrInvalid)
		}
	}
	expectedID := derivedIdentifier("prepared", prepared.Owner, prepared.SessionID, prepared.RequestID)
	expectedHash, hashErr := hashPreparedAction(prepared)
	if expectedID != prepared.ID || hashErr != nil || expectedHash != prepared.ActionHash {
		return fmt.Errorf("%w: malformed prepared action binding", ErrInvalid)
	}
	return nil
}

type ApprovalBinding struct {
	PreparedActionID string `json:"prepared_action_id"`
	ActionHash       string `json:"action_hash"`
	PolicyRevision   string `json:"policy_revision"`
	ExpiresAt        int64  `json:"expires_at"`
}

type Invocation struct {
	ID               string          `json:"id"`
	PreparedActionID string          `json:"prepared_action_id,omitempty"`
	SessionID        string          `json:"session_id"`
	Owner            Owner           `json:"owner"`
	ActionHash       string          `json:"action_hash"`
	Effect           Effect          `json:"effect"`
	State            InvocationState `json:"state"`
	Revision         uint64          `json:"revision"`
	CreatedAt        int64           `json:"created_at"`
	UpdatedAt        int64           `json:"updated_at"`
	ExpiresAt        int64           `json:"expires_at"`
	AcceptedAt       int64           `json:"accepted_at,omitempty"`
	CompletedAt      int64           `json:"completed_at,omitempty"`
	TerminalResult   json.RawMessage `json:"terminal_result,omitempty"`
	SafeFailure      string          `json:"safe_failure,omitempty"`
}

func (invocation Invocation) Validate() error {
	if !validIdentifier(invocation.ID) ||
		(invocation.PreparedActionID != "" && !validIdentifier(invocation.PreparedActionID)) ||
		!validIdentifier(invocation.SessionID) ||
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
	if invocation.State == InvocationCanceled {
		return fmt.Errorf("%w: accepted invocation cannot become canceled", ErrInvalid)
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
