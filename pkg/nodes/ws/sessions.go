package ws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/nodes"
	"github.com/sipeed/picoclaw/pkg/nodes/protocol"
)

var defaultDeactivationRetryDelays = []time.Duration{
	250 * time.Millisecond,
	time.Second,
	5 * time.Second,
}

var (
	ErrSessionHubClosed  = errors.New("node session hub is closed")
	ErrSessionSuperseded = errors.New("node session was superseded")
)

type sessionEntry struct {
	connection io.Closer
	peer       *peer
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
	attempt  int
	timer    *time.Timer
	running  bool
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
	retries  []time.Duration
}

func NewSessionHub() *SessionHub {
	return &SessionHub{
		sessions: make(map[nodes.ID]*sessionSlot),
		tracked:  make(map[*transportEntry]struct{}),
		pending:  make(map[nodes.ID]*pendingDeactivation),
		retries:  append([]time.Duration(nil), defaultDeactivationRetryDelays...),
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
	if livePeer, ok := connection.(*peer); ok {
		entry.peer = livePeer
	}
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

// Request sends one correlated request to the current authenticated generation.
func (hub *SessionHub) Request(
	ctx context.Context,
	id nodes.ID,
	method string,
	params json.RawMessage,
) (protocol.Envelope, error) {
	return hub.RequestWithIdempotencyKey(ctx, id, method, params, "")
}

// RequestWithIdempotencyKey sends one correlated logical invocation to the
// current authenticated generation.
func (hub *SessionHub) RequestWithIdempotencyKey(
	ctx context.Context,
	id nodes.ID,
	method string,
	params json.RawMessage,
	idempotencyKey string,
) (protocol.Envelope, error) {
	hub.mu.Lock()
	slot := hub.sessions[id]
	if hub.closed || slot == nil || slot.current == nil || slot.current.peer == nil {
		hub.mu.Unlock()
		return protocol.Envelope{}, ErrNodeDisconnected
	}
	session := slot.current.peer
	hub.mu.Unlock()
	return session.request(ctx, method, params, idempotencyKey)
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
	for id, deactivation := range hub.pending {
		hub.cancelRetryLocked(deactivation)
		if deactivation.timer == nil && !deactivation.running {
			hub.scheduleRetryLocked(id, deactivation, 0, false)
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
		return hub.deactivationError()
	}
}

func (hub *SessionHub) recordDeactivation(id nodes.ID, callback func() error, err error) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if previous := hub.pending[id]; previous != nil {
		hub.cancelRetryLocked(previous)
	}
	if err == nil {
		delete(hub.pending, id)
		return
	}
	deactivation := &pendingDeactivation{callback: callback, err: err}
	hub.pending[id] = deactivation
	if !hub.closed {
		hub.scheduleNextRetryLocked(id, deactivation)
	}
}

func (hub *SessionHub) deactivationError() error {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	return joinDeactivationErrors(hub.pending)
}

func (hub *SessionHub) scheduleNextRetryLocked(id nodes.ID, deactivation *pendingDeactivation) {
	if deactivation.attempt >= len(hub.retries) {
		return
	}
	delay := hub.retries[deactivation.attempt]
	deactivation.attempt++
	hub.scheduleRetryLocked(id, deactivation, delay, true)
}

func (hub *SessionHub) scheduleRetryLocked(
	id nodes.ID,
	deactivation *pendingDeactivation,
	delay time.Duration,
	continueRetries bool,
) {
	hub.active.Add(1)
	deactivation.timer = time.AfterFunc(delay, func() {
		hub.runDeactivationRetry(id, deactivation, continueRetries)
	})
}

func (hub *SessionHub) runDeactivationRetry(
	id nodes.ID,
	deactivation *pendingDeactivation,
	continueRetries bool,
) {
	defer hub.active.Done()
	hub.mu.Lock()
	if hub.pending[id] != deactivation {
		hub.mu.Unlock()
		return
	}
	deactivation.timer = nil
	deactivation.running = true
	slot := hub.sessions[id]
	hub.mu.Unlock()

	if slot != nil {
		slot.lifecycle.Lock()
		defer slot.lifecycle.Unlock()
	}
	hub.mu.Lock()
	if hub.pending[id] != deactivation {
		hub.mu.Unlock()
		return
	}
	hub.mu.Unlock()
	err := deactivation.callback()

	hub.mu.Lock()
	deactivation.running = false
	if hub.pending[id] == deactivation {
		if err == nil {
			delete(hub.pending, id)
		} else {
			deactivation.err = err
			if continueRetries && !hub.closed {
				hub.scheduleNextRetryLocked(id, deactivation)
			}
		}
	}
	hub.mu.Unlock()
}

func (hub *SessionHub) cancelRetryLocked(deactivation *pendingDeactivation) {
	if deactivation.timer != nil && deactivation.timer.Stop() {
		deactivation.timer = nil
		hub.active.Done()
	}
}

func joinDeactivationErrors(pending map[nodes.ID]*pendingDeactivation) error {
	errs := make([]error, 0, len(pending))
	for id, deactivation := range pending {
		errs = append(errs, fmt.Errorf("disconnect node %q: %w", id, deactivation.err))
	}
	return errors.Join(errs...)
}
