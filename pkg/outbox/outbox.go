// Package outbox defines the durable ownership state for outbound delivery.
package outbox

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/fileutil"
)

const recordVersion = 2

const interruptedAttemptError = "process stopped before the delivery outcome was persisted"

// Status records what is known about remote acceptance of an outbound intent.
type Status string

const (
	StatusPending          Status = "pending"
	StatusAttempting       Status = "attempting"
	StatusDelivered        Status = "delivered"
	StatusDefinitelyFailed Status = "definitely_failed"
	// StatusAmbiguous is terminal and never retried automatically. It covers an
	// uncertain remote outcome and an irrecoverable pre-dispatch prerequisite;
	// LastError preserves which condition occurred.
	StatusAmbiguous Status = "ambiguous"
)

// Kind identifies the transport payload stored by an intent.
type Kind string

const (
	KindMessage Kind = "message"
	KindMedia   Kind = "media"
)

// Identity names one logical outbound message independently of a transport attempt.
type Identity struct {
	SourceID   string `json:"source_id"`
	Ordinal    int    `json:"ordinal"`
	Kind       Kind   `json:"kind"`
	Channel    string `json:"channel"`
	ChatID     string `json:"chat_id"`
	SessionKey string `json:"session_key,omitempty"`
}

// Intent is the durable record for one logical outbound delivery.
type Intent struct {
	Version            int                       `json:"version"`
	ID                 string                    `json:"id"`
	OwnerWorkspace     string                    `json:"owner_workspace"`
	Identity           Identity                  `json:"identity"`
	Status             Status                    `json:"status"`
	Message            *bus.OutboundMessage      `json:"message,omitempty"`
	Media              *bus.OutboundMediaMessage `json:"media,omitempty"`
	Attempts           int                       `json:"attempts,omitempty"`
	PlatformMessageIDs []string                  `json:"platform_message_ids,omitempty"`
	RetryAfter         time.Time                 `json:"retry_after,omitempty"`
	LastError          string                    `json:"last_error,omitempty"`
	CreatedAt          time.Time                 `json:"created_at"`
	UpdatedAt          time.Time                 `json:"updated_at"`
}

// Outcome supplies terminal metadata captured from a channel adapter.
type Outcome struct {
	PlatformMessageIDs []string
	RetryAfter         time.Time
	Error              string
}

// Store persists outbound intents for one MintClaw instance.
type Store struct {
	dir         string
	mu          sync.Mutex
	now         func() time.Time
	writeAtomic func(string, []byte, os.FileMode) error
}

type mkdirDurableFunc func(string, string, os.FileMode) error

// Path returns the instance-wide directory containing durable outbound intents.
func Path(instanceRoot string) string {
	return filepath.Join(instanceRoot, "state", "outbox")
}

// Open creates an instance-scoped outbox store.
func Open(instanceRoot string) (*Store, error) {
	return open(instanceRoot, fileutil.MkdirAllDurable)
}

func open(instanceRoot string, mkdirDurable mkdirDurableFunc) (*Store, error) {
	instanceRoot = strings.TrimSpace(instanceRoot)
	dir := Path(instanceRoot)
	if instanceRoot == "" {
		return nil, errors.New("outbox instance root is required")
	}
	if err := mkdirDurable(instanceRoot, filepath.Join("state", "outbox"), 0o700); err != nil {
		return nil, fmt.Errorf("create outbox directory: %w", err)
	}
	return &Store{
		dir:         dir,
		now:         time.Now,
		writeAtomic: fileutil.WriteFileAtomic,
	}, nil
}

// DeliveryID deterministically identifies a logical outbound message.
func DeliveryID(identity Identity) (string, error) {
	identity = normalizeIdentity(identity)
	if err := validateIdentity(identity); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(struct {
		Version  int    `json:"version"`
		SourceID string `json:"source_id"`
		Ordinal  int    `json:"ordinal"`
		Kind     Kind   `json:"kind"`
	}{
		Version:  recordVersion,
		SourceID: identity.SourceID,
		Ordinal:  identity.Ordinal,
		Kind:     identity.Kind,
	})
	if err != nil {
		return "", fmt.Errorf("encode outbox identity: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return "out_" + hex.EncodeToString(sum[:16]), nil
}

// NewMessageIntent creates a pending text-message intent.
func NewMessageIntent(
	ownerWorkspace string,
	identity Identity,
	msg bus.OutboundMessage,
	now time.Time,
) (Intent, error) {
	ownerWorkspace = strings.TrimSpace(ownerWorkspace)
	if ownerWorkspace == "" {
		return Intent{}, errors.New("outbox owner workspace is required")
	}
	identity.Kind = KindMessage
	identity = normalizeIdentity(identity)
	id, err := DeliveryID(identity)
	if err != nil {
		return Intent{}, err
	}
	msg.Channel = identity.Channel
	msg.ChatID = identity.ChatID
	msg.Context.Channel = identity.Channel
	msg.Context.ChatID = identity.ChatID
	msg.SessionKey = identity.SessionKey
	if msg.Scope != nil {
		msg.Scope.Channel = identity.Channel
	}
	msg.DeliveryID = id
	msg, err = bus.NormalizeOutboundMessage(msg)
	if err != nil {
		return Intent{}, fmt.Errorf("normalize outbox message: %w", err)
	}
	now = now.UTC()
	return Intent{
		Version:        recordVersion,
		ID:             id,
		OwnerWorkspace: ownerWorkspace,
		Identity:       normalizeIdentity(identity),
		Status:         StatusPending,
		Message:        &msg,
		CreatedAt:      now,
		UpdatedAt:      now,
	}, nil
}

// NewMediaIntent creates a pending media-message intent.
func NewMediaIntent(
	ownerWorkspace string,
	identity Identity,
	msg bus.OutboundMediaMessage,
	now time.Time,
) (Intent, error) {
	ownerWorkspace = strings.TrimSpace(ownerWorkspace)
	if ownerWorkspace == "" {
		return Intent{}, errors.New("outbox owner workspace is required")
	}
	identity.Kind = KindMedia
	identity = normalizeIdentity(identity)
	id, err := DeliveryID(identity)
	if err != nil {
		return Intent{}, err
	}
	msg.Channel = identity.Channel
	msg.ChatID = identity.ChatID
	msg.Context.Channel = identity.Channel
	msg.Context.ChatID = identity.ChatID
	msg.SessionKey = identity.SessionKey
	if msg.Scope != nil {
		msg.Scope.Channel = identity.Channel
	}
	msg.DeliveryID = id
	msg, err = bus.NormalizeOutboundMediaMessage(msg)
	if err != nil {
		return Intent{}, fmt.Errorf("normalize outbox media: %w", err)
	}
	now = now.UTC()
	return Intent{
		Version:        recordVersion,
		ID:             id,
		OwnerWorkspace: ownerWorkspace,
		Identity:       normalizeIdentity(identity),
		Status:         StatusPending,
		Media:          &msg,
		CreatedAt:      now,
		UpdatedAt:      now,
	}, nil
}

// Create persists a pending intent before ownership is transferred to delivery.
func (s *Store) Create(intent Intent) (Intent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := validateNewIntent(intent); err != nil {
		return Intent{}, err
	}
	existing, err := s.read(intent.ID)
	if err == nil {
		if !sameLogicalIdentity(existing, intent) {
			return Intent{}, fmt.Errorf("outbox intent %q conflicts with existing record", intent.ID)
		}
		if existing.Status != StatusPending {
			return existing, nil
		}
		// Rewrite duplicate pending records so a prior uncertain directory sync
		// must pass durability confirmation before the caller admits ingress.
		if writeErr := s.write(existing); writeErr != nil {
			return Intent{}, writeErr
		}
		return existing, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return Intent{}, err
	}
	if err := s.write(intent); err != nil {
		return Intent{}, err
	}
	return intent, nil
}

// BeginAttempt persists the crash boundary immediately before a transport call.
func (s *Store) BeginAttempt(id string) (Intent, error) {
	return s.transition(id, StatusAttempting, Outcome{}, false, StatusPending, StatusAttempting, StatusDefinitelyFailed)
}

// MarkDispatchRejected records a failure before any transport call was made.
func (s *Store) MarkDispatchRejected(id string, outcome Outcome) (Intent, error) {
	return s.transition(id, StatusDefinitelyFailed, outcome, true, StatusPending, StatusDefinitelyFailed)
}

// MarkUnrecoverable records a terminal pre-dispatch failure whose prerequisite
// cannot be reconstructed. The existing non-retryable status preserves record
// compatibility with older binaries while LastError distinguishes this from an
// uncertain transport outcome.
func (s *Store) MarkUnrecoverable(id string, outcome Outcome) (Intent, error) {
	return s.transition(
		id,
		StatusAmbiguous,
		outcome,
		false,
		StatusPending,
		StatusDefinitelyFailed,
	)
}

// MarkDelivered records confirmed remote acceptance and platform message IDs.
func (s *Store) MarkDelivered(id string, outcome Outcome) (Intent, error) {
	return s.transition(id, StatusDelivered, outcome, false, StatusAttempting)
}

// MarkDefinitelyFailed records a failure known to precede remote acceptance.
func (s *Store) MarkDefinitelyFailed(id string, outcome Outcome) (Intent, error) {
	return s.transition(id, StatusDefinitelyFailed, outcome, false, StatusAttempting)
}

// MarkAmbiguous records an outcome that may have been accepted remotely.
func (s *Store) MarkAmbiguous(id string, outcome Outcome) (Intent, error) {
	return s.transition(id, StatusAmbiguous, outcome, false, StatusAttempting)
}

// Recover converts interrupted attempts to ambiguous and returns records that
// are known safe to dispatch again.
func (s *Store) Recover() ([]Intent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	records, err := s.list()
	if err != nil {
		return nil, err
	}
	dispatchable := make([]Intent, 0, len(records))
	for _, intent := range records {
		switch intent.Status {
		case StatusPending, StatusDefinitelyFailed:
			dispatchable = append(dispatchable, intent)
		case StatusAttempting:
			intent.Status = StatusAmbiguous
			intent.LastError = interruptedAttemptError
			intent.UpdatedAt = s.now().UTC()
			if err := s.write(intent); err != nil {
				return nil, fmt.Errorf("mark interrupted intent %q ambiguous: %w", intent.ID, err)
			}
		case StatusAmbiguous:
			if intent.LastError == interruptedAttemptError {
				if err := s.write(intent); err != nil {
					return nil, fmt.Errorf("reconfirm interrupted intent %q: %w", intent.ID, err)
				}
			}
		}
	}
	return dispatchable, nil
}

// Get loads one intent by delivery ID.
func (s *Store) Get(id string) (Intent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.read(id)
}

func (s *Store) transition(
	id string,
	next Status,
	outcome Outcome,
	refreshSame bool,
	allowed ...Status,
) (Intent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	intent, err := s.read(id)
	if err != nil {
		return Intent{}, err
	}
	if intent.Status == next && next != StatusAttempting && !refreshSame {
		if err := s.write(intent); err != nil {
			return Intent{}, err
		}
		return intent, nil
	}
	if !statusAllowed(intent.Status, allowed) {
		return Intent{}, fmt.Errorf("outbox intent %q cannot transition from %q to %q", id, intent.Status, next)
	}
	intent.Status = next
	if next == StatusAttempting {
		intent.Attempts++
	}
	intent.PlatformMessageIDs = append([]string(nil), outcome.PlatformMessageIDs...)
	intent.RetryAfter = outcome.RetryAfter.UTC()
	intent.LastError = strings.TrimSpace(outcome.Error)
	intent.UpdatedAt = s.now().UTC()
	if err := s.write(intent); err != nil {
		return Intent{}, err
	}
	return intent, nil
}

func (s *Store) list() ([]Intent, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("read outbox directory: %w", err)
	}
	records := make([]Intent, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		intent, err := s.read(strings.TrimSuffix(entry.Name(), ".json"))
		if err != nil {
			return nil, err
		}
		records = append(records, intent)
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].CreatedAt.Equal(records[j].CreatedAt) {
			return records[i].ID < records[j].ID
		}
		return records[i].CreatedAt.Before(records[j].CreatedAt)
	})
	return records, nil
}

func (s *Store) read(id string) (Intent, error) {
	if err := validateID(id); err != nil {
		return Intent{}, err
	}
	data, err := os.ReadFile(s.recordPath(id))
	if err != nil {
		return Intent{}, err
	}
	var intent Intent
	if err := json.Unmarshal(data, &intent); err != nil {
		return Intent{}, fmt.Errorf("decode outbox intent %q: %w", id, err)
	}
	if err := validateIntent(intent); err != nil {
		return Intent{}, fmt.Errorf("validate outbox intent %q: %w", id, err)
	}
	if intent.ID != id {
		return Intent{}, fmt.Errorf("outbox filename %q contains intent %q", id, intent.ID)
	}
	return intent, nil
}

func (s *Store) write(intent Intent) error {
	data, err := json.MarshalIndent(intent, "", "  ")
	if err != nil {
		return fmt.Errorf("encode outbox intent %q: %w", intent.ID, err)
	}
	data = append(data, '\n')
	if err := s.writeAtomic(s.recordPath(intent.ID), data, 0o600); err != nil {
		return fmt.Errorf("persist outbox intent %q: %w", intent.ID, err)
	}
	return nil
}

func (s *Store) recordPath(id string) string {
	return filepath.Join(s.dir, id+".json")
}

func normalizeIdentity(identity Identity) Identity {
	identity.SourceID = strings.TrimSpace(identity.SourceID)
	identity.Channel = strings.TrimSpace(identity.Channel)
	identity.ChatID = strings.TrimSpace(identity.ChatID)
	identity.SessionKey = strings.TrimSpace(identity.SessionKey)
	return identity
}

func validateIdentity(identity Identity) error {
	if identity.SourceID == "" {
		return errors.New("outbox source identity is required")
	}
	if identity.Ordinal < 0 {
		return errors.New("outbox message ordinal cannot be negative")
	}
	if identity.Kind != KindMessage && identity.Kind != KindMedia {
		return fmt.Errorf("unsupported outbox kind %q", identity.Kind)
	}
	if identity.Channel == "" || identity.ChatID == "" {
		return errors.New("outbox channel and chat identity are required")
	}
	return nil
}

func validateIntent(intent Intent) error {
	if intent.Version != recordVersion {
		return fmt.Errorf("unsupported outbox record version %d", intent.Version)
	}
	if err := validateID(intent.ID); err != nil {
		return err
	}
	if strings.TrimSpace(intent.OwnerWorkspace) == "" {
		return errors.New("outbox owner workspace is required")
	}
	if err := validateIdentity(normalizeIdentity(intent.Identity)); err != nil {
		return err
	}
	wantID, err := DeliveryID(intent.Identity)
	if err != nil {
		return err
	}
	if intent.ID != wantID {
		return fmt.Errorf("outbox intent ID %q does not match identity", intent.ID)
	}
	if !validStatus(intent.Status) {
		return fmt.Errorf("unsupported outbox status %q", intent.Status)
	}
	if intent.CreatedAt.IsZero() || intent.UpdatedAt.IsZero() {
		return errors.New("outbox timestamps are required")
	}
	switch intent.Identity.Kind {
	case KindMessage:
		if intent.Message == nil || intent.Media != nil || intent.Message.DeliveryID != intent.ID {
			return errors.New("outbox message payload does not match its identity")
		}
		if err := validateMessageRoute(intent.Identity, *intent.Message); err != nil {
			return err
		}
	case KindMedia:
		if intent.Media == nil || intent.Message != nil || intent.Media.DeliveryID != intent.ID {
			return errors.New("outbox media payload does not match its identity")
		}
		if err := validateMediaRoute(intent.Identity, *intent.Media); err != nil {
			return err
		}
	}
	return nil
}

func validateNewIntent(intent Intent) error {
	if err := validateIntent(intent); err != nil {
		return err
	}
	if intent.Status != StatusPending {
		return fmt.Errorf("new outbox intent must be %q, got %q", StatusPending, intent.Status)
	}
	if intent.Attempts != 0 || len(intent.PlatformMessageIDs) != 0 ||
		!intent.RetryAfter.IsZero() || strings.TrimSpace(intent.LastError) != "" {
		return errors.New("new outbox intent cannot contain delivery outcome state")
	}
	return nil
}

func validateMessageRoute(identity Identity, msg bus.OutboundMessage) error {
	if strings.TrimSpace(msg.Channel) != identity.Channel ||
		strings.TrimSpace(msg.Context.Channel) != identity.Channel ||
		strings.TrimSpace(msg.ChatID) != identity.ChatID ||
		strings.TrimSpace(msg.Context.ChatID) != identity.ChatID ||
		strings.TrimSpace(msg.SessionKey) != identity.SessionKey ||
		(msg.Scope != nil && strings.TrimSpace(msg.Scope.Channel) != identity.Channel) {
		return errors.New("outbox message route does not match its identity")
	}
	return nil
}

func validateMediaRoute(identity Identity, msg bus.OutboundMediaMessage) error {
	if strings.TrimSpace(msg.Channel) != identity.Channel ||
		strings.TrimSpace(msg.Context.Channel) != identity.Channel ||
		strings.TrimSpace(msg.ChatID) != identity.ChatID ||
		strings.TrimSpace(msg.Context.ChatID) != identity.ChatID ||
		strings.TrimSpace(msg.SessionKey) != identity.SessionKey ||
		(msg.Scope != nil && strings.TrimSpace(msg.Scope.Channel) != identity.Channel) {
		return errors.New("outbox media route does not match its identity")
	}
	return nil
}

func validateID(id string) error {
	if len(id) != len("out_")+32 || !strings.HasPrefix(id, "out_") {
		return fmt.Errorf("invalid outbox delivery ID %q", id)
	}
	if _, err := hex.DecodeString(strings.TrimPrefix(id, "out_")); err != nil {
		return fmt.Errorf("invalid outbox delivery ID %q", id)
	}
	return nil
}

func validStatus(status Status) bool {
	switch status {
	case StatusPending, StatusAttempting, StatusDelivered, StatusDefinitelyFailed, StatusAmbiguous:
		return true
	default:
		return false
	}
}

func statusAllowed(status Status, allowed []Status) bool {
	for _, candidate := range allowed {
		if status == candidate {
			return true
		}
	}
	return false
}

func sameLogicalIdentity(left, right Intent) bool {
	return left.ID == right.ID &&
		left.Identity.SourceID == right.Identity.SourceID &&
		left.Identity.Ordinal == right.Identity.Ordinal &&
		left.Identity.Kind == right.Identity.Kind
}
