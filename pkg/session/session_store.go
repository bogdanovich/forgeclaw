package session

import (
	"context"

	"github.com/bogdanovich/mintclaw/pkg/memory"
	"github.com/bogdanovich/mintclaw/pkg/providers"
)

// SessionStore defines the persistence operations used by the agent loop.
// Both SessionManager (legacy JSON backend) and JSONLBackend satisfy this
// interface, allowing the storage layer to be swapped without touching the
// agent loop code.
//
// Compatibility writes (Add*, Set*, Truncate*) remain fire-and-forget for
// passive and administrative callers. Turn-critical writes must use the
// embedded TurnJournal contract.
type SessionStore interface {
	TurnJournal

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

// TurnJournal is the durability boundary for messages that determine whether
// a turn may start, execute an external side effect, or report success. A nil
// result means the complete message is durably owned by the canonical store.
type TurnJournal interface {
	AppendTurnMessage(ctx context.Context, sessionKey string, msg providers.Message) error
}

// HistoryRevisionProvider exposes a cheap identity for the canonical history.
// Context caches use it to avoid rereading unchanged histories at startup.
type HistoryRevisionProvider interface {
	GetHistoryRevision(sessionKey string) (memory.HistoryRevision, error)
}

// ErrorAwareHistoryReader allows recovery paths to distinguish a failed write
// from a write that became durable before reporting an error.
type ErrorAwareHistoryReader interface {
	GetHistoryWithError(sessionKey string) ([]providers.Message, error)
}
