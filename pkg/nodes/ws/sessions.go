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

type transportEntry struct {
	connection io.Closer
}

type sessionSlot struct {
	lifecycle sync.Mutex
	current   *sessionEntry // guarded by SessionHub.mu
}

type pendingDeactivation struct {
	callback func() error
	err      error
}

// SessionHub owns the single live transport connection for each paired node.
// A newly authenticated connection replaces an older connection for the same
// cryptographic identity so stale half-open sessions cannot retain ownership.
type SessionHub struct {
	mu       sync.Mutex
	sessions map[nodes.ID]*sessionSlot
	tracked  map[*transportEntry]struct{}
	closed   bool
	active   sync.WaitGroup
	pending  map[nodes.ID]*pendingDeactivation
}

func NewSessionHub() *SessionHub {
	return &SessionHub{
		sessions: make(map[nodes.ID]*sessionSlot),
		tracked:  make(map[*transportEntry]struct{}),
		pending:  make(map[nodes.ID]*pendingDeactivation),
	}
}

// TrackTransport registers an upgraded connection before authentication so
// shutdown can close and drain handshakes as well as authenticated sessions.
func (hub *SessionHub) TrackTransport(connection io.Closer) (func(), error) {
	entry := &transportEntry{connection: connection}
	hub.mu.Lock()
	if hub.closed {
		hub.mu.Unlock()
		_ = connection.Close()
		return nil, ErrSessionHubClosed
	}
	hub.tracked[entry] = struct{}{}
	hub.active.Add(1)
	hub.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			hub.mu.Lock()
			delete(hub.tracked, entry)
			hub.mu.Unlock()
			hub.active.Done()
		})
	}, nil
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
			hub.recordDeactivation(id, deactivate, deactivationErr)
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
	hub.recordDeactivation(id, nil, nil)
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
				hub.recordDeactivation(id, deactivate, deactivateErr)
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
	connections := make([]io.Closer, 0)
	if !hub.closed {
		hub.closed = true
		connections = make([]io.Closer, 0, len(hub.tracked)+len(hub.sessions))
		for entry := range hub.tracked {
			connections = append(connections, entry.connection)
		}
		for _, slot := range hub.sessions {
			if slot.current != nil {
				connections = append(connections, slot.current.connection)
			}
		}
	}
	hub.mu.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
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
		return hub.retryDeactivations()
	}
}

func (hub *SessionHub) recordDeactivation(id nodes.ID, callback func() error, err error) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if err == nil {
		delete(hub.pending, id)
		return
	}
	hub.pending[id] = &pendingDeactivation{callback: callback, err: err}
}

func (hub *SessionHub) deactivationError() error {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	return joinDeactivationErrors(hub.pending)
}

func (hub *SessionHub) retryDeactivations() error {
	hub.mu.Lock()
	pending := make(map[nodes.ID]*pendingDeactivation, len(hub.pending))
	for id, deactivation := range hub.pending {
		pending[id] = deactivation
	}
	hub.mu.Unlock()

	for id, deactivation := range pending {
		if deactivation.callback == nil {
			continue
		}
		err := deactivation.callback()
		hub.recordDeactivation(id, deactivation.callback, err)
	}
	return hub.deactivationError()
}

func joinDeactivationErrors(pending map[nodes.ID]*pendingDeactivation) error {
	errs := make([]error, 0, len(pending))
	for id, deactivation := range pending {
		errs = append(errs, fmt.Errorf("disconnect node %q: %w", id, deactivation.err))
	}
	return errors.Join(errs...)
}
