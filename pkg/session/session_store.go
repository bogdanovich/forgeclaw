package session

import (
	"github.com/sipeed/picoclaw/pkg/memory"
	"github.com/sipeed/picoclaw/pkg/providers"
)

// SessionStore defines the persistence operations used by the agent loop.
// Both SessionManager (legacy JSON backend) and JSONLBackend satisfy this
// interface, allowing the storage layer to be swapped without touching the
// agent loop code.
//
// Write methods (Add*, Set*, Truncate*) are fire-and-forget: they do not
// return errors. Implementations should log failures internally. This
// matches the original SessionManager contract that the agent loop relies on.
type SessionStore interface {
	// AddMessage appends a simple role/content message to the session.
	AddMessage(sessionKey, role, content string)
	// AddFullMessage appends a complete message including tool calls.
	AddFullMessage(sessionKey string, msg providers.Message)
	// GetHistory returns the full message history for the session.
	GetHistory(key string) []providers.Message
	// GetSummary returns the conversation summary, or "" if none.
	GetSummary(key string) string
	// SetSummary replaces the conversation summary.
	SetSummary(key, summary string)
	// SetHistory replaces the full message history.
	SetHistory(key string, history []providers.Message)
	// TruncateHistory keeps only the last keepLast messages.
	TruncateHistory(key string, keepLast int)
	// Save persists any pending state to durable storage.
	Save(key string) error
	// ListSessions returns all known session keys.
	ListSessions() []string
	// Close releases resources held by the store.
	Close() error
}

// HistoryRevisionProvider exposes a cheap identity for the canonical history.
// Context caches use it to avoid rereading unchanged histories at startup.
type HistoryRevisionProvider interface {
	GetHistoryRevision(sessionKey string) (memory.HistoryRevision, error)
}

// ErrorAwareSessionWriter exposes canonical write failures to callers that
// must coordinate a derived store. SessionStore keeps its legacy fire-and-
// forget methods for compatibility.
type ErrorAwareSessionWriter interface {
	AddMessageWithError(sessionKey, role, content string) error
	AddFullMessageWithError(sessionKey string, msg providers.Message) error
}
