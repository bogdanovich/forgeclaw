package ws

import (
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
) (func() bool, error) {
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
	hub.mu.Unlock()
	if previous != nil {
		_ = previous.connection.Close()
	}
	return func() bool {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		if hub.sessions[id] != entry {
			return false
		}
		delete(hub.sessions, id)
		return true
	}, nil
}

func (hub *SessionHub) Connected(id nodes.ID) bool {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	_, exists := hub.sessions[id]
	return exists
}

func (hub *SessionHub) Close() {
	hub.mu.Lock()
	if hub.closed {
		hub.mu.Unlock()
		return
	}
	hub.closed = true
	entries := make([]*sessionEntry, 0, len(hub.sessions))
	for _, entry := range hub.sessions {
		entries = append(entries, entry)
	}
	hub.mu.Unlock()
	for _, entry := range entries {
		_ = entry.connection.Close()
	}
}
