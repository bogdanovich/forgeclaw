package browser

import (
	"context"
	"fmt"
	"sync"
)

// Store is the broker's durable compare-and-swap boundary. The memory
// implementation supports foundation tests; a file-backed store lands with
// lifecycle and restart recovery.
type Store interface {
	CreateSession(context.Context, Session) error
	GetSession(context.Context, string) (Session, error)
	UpdateSession(context.Context, uint64, Session) error
	CreateInvocation(context.Context, Invocation) error
	GetInvocation(context.Context, string) (Invocation, error)
	UpdateInvocation(context.Context, uint64, Invocation) error
}

type MemoryStore struct {
	mu          sync.Mutex
	sessions    map[string]Session
	invocations map[string]Invocation
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		sessions:    make(map[string]Session),
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
		!validSessionTransition(current.State, next.State) {
		return ErrConflict
	}
	store.sessions[next.ID] = next
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
	if current.Owner != next.Owner || current.SessionID != next.SessionID ||
		current.ActionHash != next.ActionHash || current.Effect != next.Effect ||
		current.CreatedAt != next.CreatedAt || current.ExpiresAt != next.ExpiresAt ||
		!validInvocationTransition(current.State, next.State) {
		return ErrConflict
	}
	next.TerminalResult = cloneBytes(next.TerminalResult)
	store.invocations[next.ID] = next
	return nil
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value...)
}
