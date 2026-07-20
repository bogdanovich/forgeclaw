package ws

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/sipeed/picoclaw/pkg/nodes"
)

var ErrSessionHubClosed = errors.New("node session hub is closed")

type sessionEntry struct {
	connection io.Closer
}

// SessionHub owns the single live transport connection for each paired node.
// A newly authenticated connection replaces an older connection for the same
// cryptographic identity so stale half-open sessions cannot retain ownership.
type SessionHub struct {
	mu       sync.Mutex
	sessions map[nodes.ID]*sessionEntry
	closed   bool
	active   sync.WaitGroup
}

func NewSessionHub() *SessionHub {
	return &SessionHub{sessions: make(map[nodes.ID]*sessionEntry)}
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
	previous := hub.sessions[id]
	hub.sessions[id] = entry
	if activate != nil {
		if err := activate(); err != nil {
			if previous == nil {
				delete(hub.sessions, id)
			} else {
				hub.sessions[id] = previous
			}
			hub.mu.Unlock()
			return nil, err
		}
	}
	hub.active.Add(1)
	hub.mu.Unlock()
	if previous != nil {
		_ = previous.connection.Close()
	}
	var once sync.Once
	var owned bool
	var deactivateErr error
	return func() (bool, error) {
		once.Do(func() {
			hub.mu.Lock()
			if hub.sessions[id] == entry {
				owned = true
				if deactivate != nil {
					deactivateErr = deactivate()
				}
				delete(hub.sessions, id)
			}
			hub.mu.Unlock()
			hub.active.Done()
		})
		return owned, deactivateErr
	}, nil
}

func (hub *SessionHub) Connected(id nodes.ID) bool {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	_, exists := hub.sessions[id]
	return exists
}

func (hub *SessionHub) Close(ctx context.Context) error {
	hub.mu.Lock()
	entries := make([]*sessionEntry, 0)
	if !hub.closed {
		hub.closed = true
		entries = make([]*sessionEntry, 0, len(hub.sessions))
		for _, entry := range hub.sessions {
			entries = append(entries, entry)
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
		return ctx.Err()
	case <-done:
		return nil
	}
}
