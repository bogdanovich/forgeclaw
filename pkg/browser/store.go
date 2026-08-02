package browser

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/bogdanovich/mintclaw/pkg/config"
)

// Store is the broker's durable compare-and-swap boundary. The memory
// implementation supports foundation tests; a file-backed store lands with
// lifecycle and restart recovery.
type Store interface {
	CreateSession(context.Context, Session) error
	GetSession(context.Context, string) (Session, error)
	ListSessions(context.Context) ([]Session, error)
	UpdateSession(context.Context, uint64, Session) error
	CreatePreparation(context.Context, PreparedAction, Invocation) error
	GetPreparedAction(context.Context, string) (PreparedAction, error)
	CreateInvocation(context.Context, Invocation) error
	GetInvocation(context.Context, string) (Invocation, error)
	ListInvocations(context.Context, string) ([]Invocation, error)
	UpdateInvocation(context.Context, uint64, Invocation) error
	PruneInvocations(context.Context, int64) error
	PrunePreparedActions(context.Context, int64) error
}

type MemoryStore struct {
	mu          sync.Mutex
	sessions    map[string]Session
	prepared    map[string]PreparedAction
	invocations map[string]Invocation
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		sessions:    make(map[string]Session),
		prepared:    make(map[string]PreparedAction),
		invocations: make(map[string]Invocation),
	}
}

func (store *MemoryStore) CreateSession(_ context.Context, session Session) error {
	if err := session.Validate(); err != nil {
		return err
	}
	if session.State != SessionOpening || session.Revision != 1 {
		return fmt.Errorf("%w: session must enter as opening revision 1", ErrConflict)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, exists := store.sessions[session.ID]; exists {
		return ErrConflict
	}
	for _, existing := range store.sessions {
		if !existing.State.Terminal() && existing.Target == session.Target &&
			existing.Profile == session.Profile {
			return ErrBusy
		}
	}
	store.sessions[session.ID] = session
	return nil
}

func (store *MemoryStore) GetSession(_ context.Context, id string) (Session, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	session, ok := store.sessions[id]
	if !ok {
		return Session{}, ErrNotFound
	}
	return session, nil
}

func (store *MemoryStore) ListSessions(_ context.Context) ([]Session, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	sessions := make([]Session, 0, len(store.sessions))
	for _, session := range store.sessions {
		sessions = append(sessions, session)
	}
	return sessions, nil
}

func (store *MemoryStore) UpdateSession(_ context.Context, expected uint64, next Session) error {
	if err := next.Validate(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	current, ok := store.sessions[next.ID]
	if !ok {
		return ErrNotFound
	}
	if current.Revision != expected || next.Revision != expected+1 {
		return ErrStale
	}
	if current.Owner != next.Owner || current.Target != next.Target ||
		current.Profile != next.Profile || current.CreatedAt != next.CreatedAt ||
		current.DryRun != next.DryRun || current.PolicyRevision != next.PolicyRevision ||
		current.ControllerGeneration != next.ControllerGeneration || current.TabID != next.TabID ||
		current.ExpiresAt != next.ExpiresAt ||
		!validSnapshotTransition(current, next) ||
		!validSessionTransition(current.State, next.State) {
		return ErrConflict
	}
	store.sessions[next.ID] = next
	return nil
}

func validSnapshotTransition(current, next Session) bool {
	if next.SnapshotGeneration < current.SnapshotGeneration {
		return false
	}
	if next.SnapshotID == current.SnapshotID {
		return next.SnapshotGeneration == current.SnapshotGeneration &&
			next.SnapshotOrigin == current.SnapshotOrigin
	}
	if next.SnapshotID == "" {
		return next.SnapshotGeneration == current.SnapshotGeneration && next.SnapshotOrigin == ""
	}
	return next.SnapshotGeneration == current.SnapshotGeneration+1 && next.SnapshotOrigin != ""
}

func (store *MemoryStore) GetPreparedAction(_ context.Context, id string) (PreparedAction, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	prepared, ok := store.prepared[id]
	if !ok {
		return PreparedAction{}, ErrNotFound
	}
	return prepared, nil
}

func (store *MemoryStore) CreatePreparation(
	_ context.Context,
	prepared PreparedAction,
	invocation Invocation,
) error {
	if err := validatePreparationPair(prepared, invocation); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if existingPrepared, exists := store.prepared[prepared.ID]; exists {
		existingInvocation, invocationExists := store.invocations[invocation.ID]
		if invocationExists && existingPrepared == prepared && invocationsEqual(existingInvocation, invocation) {
			return nil
		}
		return ErrConflict
	}
	if _, exists := store.invocations[invocation.ID]; exists {
		return ErrConflict
	}
	session, ok := store.sessions[prepared.SessionID]
	if !ok || !session.Owner.Equal(prepared.Owner) || session.State != SessionReady {
		return ErrDenied
	}
	store.prepared[prepared.ID] = prepared
	invocation.TerminalResult = cloneBytes(invocation.TerminalResult)
	store.invocations[invocation.ID] = invocation
	return nil
}

func (store *MemoryStore) CreateInvocation(_ context.Context, invocation Invocation) error {
	if err := invocation.Validate(); err != nil {
		return err
	}
	if invocation.State != InvocationPrepared || invocation.Revision != 1 {
		return fmt.Errorf("%w: invocation must enter as prepared revision 1", ErrConflict)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, exists := store.invocations[invocation.ID]; exists {
		return ErrConflict
	}
	if invocation.PreparedActionID != "" {
		if _, exists := store.prepared[invocation.PreparedActionID]; !exists {
			return ErrDenied
		}
	}
	session, ok := store.sessions[invocation.SessionID]
	if !ok || !session.Owner.Equal(invocation.Owner) || session.State != SessionReady {
		return ErrDenied
	}
	invocation.TerminalResult = cloneBytes(invocation.TerminalResult)
	store.invocations[invocation.ID] = invocation
	return nil
}

func (store *MemoryStore) GetInvocation(_ context.Context, id string) (Invocation, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	invocation, ok := store.invocations[id]
	if !ok {
		return Invocation{}, ErrNotFound
	}
	invocation.TerminalResult = cloneBytes(invocation.TerminalResult)
	return invocation, nil
}

func (store *MemoryStore) ListInvocations(_ context.Context, sessionID string) ([]Invocation, error) {
	if !validIdentifier(sessionID) {
		return nil, fmt.Errorf("%w: malformed session ID", ErrInvalid)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	invocations := make([]Invocation, 0)
	for _, invocation := range store.invocations {
		if invocation.SessionID == sessionID {
			invocation.TerminalResult = cloneBytes(invocation.TerminalResult)
			invocations = append(invocations, invocation)
		}
	}
	return invocations, nil
}

func (store *MemoryStore) UpdateInvocation(_ context.Context, expected uint64, next Invocation) error {
	if err := next.Validate(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	current, ok := store.invocations[next.ID]
	if !ok {
		return ErrNotFound
	}
	if current.Revision != expected || next.Revision != expected+1 {
		return ErrStale
	}
	if current.Owner != next.Owner || current.PreparedActionID != next.PreparedActionID ||
		current.SessionID != next.SessionID ||
		current.ActionHash != next.ActionHash || current.Effect != next.Effect ||
		current.CreatedAt != next.CreatedAt || current.ExpiresAt != next.ExpiresAt ||
		!validInvocationTransition(current.State, next.State) {
		return ErrConflict
	}
	next.TerminalResult = cloneBytes(next.TerminalResult)
	store.invocations[next.ID] = next
	return nil
}

func (store *MemoryStore) PruneInvocations(_ context.Context, completedBefore int64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	for id, invocation := range store.invocations {
		if invocation.State.Terminal() && invocation.CompletedAt > 0 && invocation.CompletedAt < completedBefore {
			delete(store.invocations, id)
		}
	}
	return nil
}

func (store *MemoryStore) PrunePreparedActions(_ context.Context, expiredBefore int64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	for id, prepared := range store.prepared {
		if prepared.ExpiresAt < expiredBefore && !preparedReferenced(store.invocations, id) {
			delete(store.prepared, id)
		}
	}
	return nil
}

func validatePreparationPair(prepared PreparedAction, invocation Invocation) error {
	if err := prepared.Validate(config.BrowserMaxTextInputBytes); err != nil {
		return err
	}
	if err := invocation.Validate(); err != nil {
		return err
	}
	if invocation.State != InvocationPrepared || invocation.Revision != 1 ||
		invocation.ID != derivedIdentifier("invocation", prepared.Owner, prepared.SessionID, prepared.RequestID) ||
		invocation.PreparedActionID != prepared.ID || invocation.SessionID != prepared.SessionID ||
		invocation.Owner != prepared.Owner || invocation.ActionHash != prepared.ActionHash ||
		invocation.Effect != prepared.Effect || invocation.CreatedAt != prepared.CreatedAt ||
		invocation.UpdatedAt != prepared.CreatedAt || invocation.ExpiresAt != prepared.ExpiresAt {
		return fmt.Errorf("%w: malformed prepared action invocation", ErrConflict)
	}
	return nil
}

func preparedReferenced(invocations map[string]Invocation, preparedID string) bool {
	for _, invocation := range invocations {
		if invocation.PreparedActionID == preparedID {
			return true
		}
	}
	return false
}

func invocationsEqual(left, right Invocation) bool {
	return reflect.DeepEqual(left, right)
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value...)
}
