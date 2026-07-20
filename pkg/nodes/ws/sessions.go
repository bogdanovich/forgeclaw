package ws

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/sipeed/picoclaw/pkg/nodes"
)

var (
	ErrSessionHubClosed  = errors.New("node session hub is closed")
	ErrSessionSuperseded = errors.New("node session was superseded")
)

type sessionEntry struct {
	connection io.Closer
}

type sessionSlot struct {
	lifecycle sync.Mutex
	current   *sessionEntry // guarded by SessionHub.mu
}

// SessionHub owns the single live transport connection for each paired node.
// A newly authenticated connection replaces an older connection for the same
// cryptographic identity so stale half-open sessions cannot retain ownership.
type SessionHub struct {
	mu       sync.Mutex
	sessions map[nodes.ID]*sessionSlot
	closed   bool
	active   sync.WaitGroup
	errors   map[nodes.ID]error
}

func NewSessionHub() *SessionHub {
	return &SessionHub{
		sessions: make(map[nodes.ID]*sessionSlot),
		errors:   make(map[nodes.ID]error),
	}
}

// Claim returns a release function that reports whether this claim still
// owned the node when released. Only the current owner may persist disconnect.
func (hub *SessionHub) Claim(
	id nodes.ID,
	connection io.Closer,
	activate func() error,
	deactivate func() error,
) (func() (bool, error), error) {
	entry := &sessionEntry{connection: connection}
	hub.mu.Lock()
	if hub.closed {
		hub.mu.Unlock()
		_ = connection.Close()
		return nil, ErrSessionHubClosed
	}
	slot := hub.sessions[id]
	if slot == nil {
		slot = &sessionSlot{}
		hub.sessions[id] = slot
	}
	previous := slot.current
	slot.current = entry
	hub.active.Add(1)
	hub.mu.Unlock()
	if previous != nil {
		_ = previous.connection.Close()
	}

	slot.lifecycle.Lock()
	hub.mu.Lock()
	current := slot.current == entry
	closed := hub.closed
	hub.mu.Unlock()
	if !current || closed {
		slot.lifecycle.Unlock()
		hub.active.Done()
		if closed {
			return nil, ErrSessionHubClosed
		}
		return nil, ErrSessionSuperseded
	}
	activationErr := error(nil)
	if activate != nil {
		activationErr = activate()
	}
	hub.mu.Lock()
	current = slot.current == entry
	closed = hub.closed
	if (activationErr != nil || !current || closed) && current {
		slot.current = nil
	}
	hub.mu.Unlock()
	if activationErr != nil || !current || closed {
		deactivationErr := error(nil)
		if activate != nil && deactivate != nil {
			deactivationErr = deactivate()
			hub.recordDeactivation(id, deactivationErr)
		}
		slot.lifecycle.Unlock()
		hub.active.Done()
		switch {
		case activationErr != nil:
			return nil, errors.Join(activationErr, deactivationErr)
		case closed:
			return nil, errors.Join(ErrSessionHubClosed, deactivationErr)
		default:
			return nil, errors.Join(ErrSessionSuperseded, deactivationErr)
		}
	}
	slot.lifecycle.Unlock()

	var once sync.Once
	var owned bool
	var deactivateErr error
	return func() (bool, error) {
		once.Do(func() {
			slot.lifecycle.Lock()
			hub.mu.Lock()
			if slot.current == entry {
				owned = true
				slot.current = nil
			}
			hub.mu.Unlock()
			if owned && deactivate != nil {
				deactivateErr = deactivate()
				hub.recordDeactivation(id, deactivateErr)
			}
			slot.lifecycle.Unlock()
			hub.active.Done()
		})
		return owned, deactivateErr
	}, nil
}

func (hub *SessionHub) Connected(id nodes.ID) bool {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	slot := hub.sessions[id]
	return slot != nil && slot.current != nil
}

func (hub *SessionHub) Close(ctx context.Context) error {
	hub.mu.Lock()
	entries := make([]*sessionEntry, 0)
	if !hub.closed {
		hub.closed = true
		entries = make([]*sessionEntry, 0, len(hub.sessions))
		for _, slot := range hub.sessions {
			if slot.current != nil {
				entries = append(entries, slot.current)
			}
		}
	}
	hub.mu.Unlock()
	for _, entry := range entries {
		_ = entry.connection.Close()
	}
	done := make(chan struct{})
	go func() {
		hub.active.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return errors.Join(ctx.Err(), hub.deactivationError())
	case <-done:
		return hub.deactivationError()
	}
}

func (hub *SessionHub) recordDeactivation(id nodes.ID, err error) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if err == nil {
		delete(hub.errors, id)
		return
	}
	hub.errors[id] = err
}

func (hub *SessionHub) deactivationError() error {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	errs := make([]error, 0, len(hub.errors))
	for id, err := range hub.errors {
		errs = append(errs, fmt.Errorf("disconnect node %q: %w", id, err))
	}
	return errors.Join(errs...)
}
