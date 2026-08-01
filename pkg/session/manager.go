package session

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/fileutil"
	"github.com/bogdanovich/mintclaw/pkg/memory"
	"github.com/bogdanovich/mintclaw/pkg/providers"
	"github.com/bogdanovich/mintclaw/pkg/providers/messageutil"
)

type Session struct {
	Key             string              `json:"key"`
	Messages        []providers.Message `json:"messages"`
	Summary         string              `json:"summary,omitempty"`
	Created         time.Time           `json:"created"`
	Updated         time.Time           `json:"updated"`
	HistoryRevision uint64              `json:"history_revision,omitempty"`
}

func advanceHistoryRevision(session *Session) {
	session.HistoryRevision++
	if session.HistoryRevision == 0 {
		session.HistoryRevision = 1
	}
}

// GetHistoryRevision returns the monotonic logical history revision persisted
// with legacy sessions. Wall-clock timestamps are not mutation identities:
// multiple writes may observe the same clock tick.
func (sm *SessionManager) GetHistoryRevision(key string) (memory.HistoryRevision, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	session, ok := sm.sessions[key]
	if !ok {
		return memory.HistoryRevision{}, nil
	}
	return memory.HistoryRevision{
		Revision: session.HistoryRevision,
		Count:    len(session.Messages),
	}, nil
}

type SessionManager struct {
	sessions map[string]*Session
	mu       sync.RWMutex
	storage  string
}

func NewSessionManager(storage string) *SessionManager {
	sm := &SessionManager{
		sessions: make(map[string]*Session),
		storage:  storage,
	}

	if storage != "" {
		os.MkdirAll(storage, 0o700)
		sm.loadSessions()
	}

	return sm
}

func (sm *SessionManager) GetOrCreate(key string) *Session {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	session, ok := sm.sessions[key]
	if ok {
		return session
	}

	session = &Session{
		Key:      key,
		Messages: []providers.Message{},
		Created:  time.Now(),
		Updated:  time.Now(),
	}
	sm.sessions[key] = session

	return session
}

func ensureMessageCreatedAt(msg *providers.Message, fallback time.Time) {
	if msg.CreatedAt != nil && !msg.CreatedAt.IsZero() {
		return
	}
	ts := fallback
	msg.CreatedAt = &ts
}

func normalizeHistoryCreatedAt(history []providers.Message) {
	now := time.Now()
	for i := range history {
		ensureMessageCreatedAt(&history[i], now)
	}
}

func (sm *SessionManager) AddMessage(sessionKey, role, content string) {
	sm.AddFullMessage(sessionKey, providers.Message{
		Role:    role,
		Content: content,
	})
}

func (sm *SessionManager) AddMessageWithError(sessionKey, role, content string) error {
	sm.AddMessage(sessionKey, role, content)
	return nil
}

// AddFullMessage adds a complete message with tool calls and tool call ID to the session.
// This is used to save the full conversation flow including tool calls and tool results.
func (sm *SessionManager) AddFullMessage(sessionKey string, msg providers.Message) {
	if messageutil.IsTransientAssistantThoughtMessage(msg) {
		return
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	session, ok := sm.sessions[sessionKey]
	if !ok {
		session = &Session{
			Key:      sessionKey,
			Messages: []providers.Message{},
			Created:  time.Now(),
		}
		sm.sessions[sessionKey] = session
	}

	now := time.Now()
	ensureMessageCreatedAt(&msg, now)

	session.Messages = append(session.Messages, msg)
	advanceHistoryRevision(session)
	session.Updated = now
}

func (sm *SessionManager) AddFullMessageWithError(sessionKey string, msg providers.Message) error {
	sm.AddFullMessage(sessionKey, msg)
	return nil
}

func (sm *SessionManager) AppendTurnMessage(
	ctx context.Context,
	sessionKey string,
	msg providers.Message,
) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if messageutil.IsTransientAssistantThoughtMessage(msg) {
		return nil
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	if err := context.Cause(ctx); err != nil {
		return err
	}

	now := time.Now()
	ensureMessageCreatedAt(&msg, now)
	next := Session{Key: sessionKey, Messages: []providers.Message{}, Created: now, Updated: now}
	if current := sm.sessions[sessionKey]; current != nil {
		next = cloneSession(*current)
	}
	next.Messages = append(next.Messages, msg)
	advanceHistoryRevision(&next)
	next.Updated = now

	writeErr := sm.writeSessionSnapshot(sessionKey, next)
	if writeErr == nil || fileutil.IsCommittedWriteError(writeErr) {
		sm.sessions[sessionKey] = &next
	}
	return writeErr
}

func (sm *SessionManager) GetHistory(key string) []providers.Message {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	session, ok := sm.sessions[key]
	if !ok {
		return []providers.Message{}
	}

	history := make([]providers.Message, len(session.Messages))
	copy(history, session.Messages)
	return history
}

func (sm *SessionManager) GetHistoryWithError(key string) ([]providers.Message, error) {
	return sm.GetHistory(key), nil
}

func (sm *SessionManager) GetSummary(key string) string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	session, ok := sm.sessions[key]
	if !ok {
		return ""
	}
	return session.Summary
}

func (sm *SessionManager) SetSummary(key string, summary string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	session, ok := sm.sessions[key]
	if ok {
		session.Summary = summary
		session.Updated = time.Now()
	}
}

func (sm *SessionManager) TruncateHistory(key string, keepLast int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	session, ok := sm.sessions[key]
	if !ok {
		return
	}

	if keepLast <= 0 {
		if len(session.Messages) == 0 {
			return
		}
		session.Messages = []providers.Message{}
		advanceHistoryRevision(session)
		session.Updated = time.Now()
		return
	}

	if len(session.Messages) <= keepLast {
		return
	}

	session.Messages = session.Messages[len(session.Messages)-keepLast:]
	advanceHistoryRevision(session)
	session.Updated = time.Now()
}

func (sm *SessionManager) ListSessions() []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	keys := make([]string, 0, len(sm.sessions))
	for k := range sm.sessions {
		keys = append(keys, k)
	}
	return keys
}

// sanitizeFilename converts a session key into a cross-platform safe filename.
// Replaces ':' with '_' (session key separator) and '/' and '\' with '_' so
// composite IDs (e.g. Telegram forum "chatID/threadID") do not create
// subdirectories or break on Windows. The original key is preserved inside
// the JSON file, so loadSessions still maps back to the right in-memory key.
func sanitizeFilename(key string) string {
	s := strings.ReplaceAll(key, ":", "_")
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, "\\", "_")
	return s
}

func (sm *SessionManager) Save(key string) error {
	sm.mu.RLock()
	stored, ok := sm.sessions[key]
	if !ok {
		sm.mu.RUnlock()
		return nil
	}
	snapshot := cloneSession(*stored)
	sm.mu.RUnlock()
	return sm.writeSessionSnapshot(key, snapshot)
}

func cloneSession(stored Session) Session {
	snapshot := stored
	snapshot.Messages = append([]providers.Message(nil), stored.Messages...)
	return snapshot
}

func (sm *SessionManager) writeSessionSnapshot(key string, snapshot Session) error {
	if sm.storage == "" {
		return nil
	}
	filename := sanitizeFilename(key)
	if filename == "." || !filepath.IsLocal(filename) {
		return os.ErrInvalid
	}
	snapshot.Messages = messageutil.FilterInvalidHistoryMessages(snapshot.Messages)
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	sessionPath := filepath.Join(sm.storage, filename+".json")
	return fileutil.WriteFileAtomic(sessionPath, data, 0o600)
}

func (sm *SessionManager) loadSessions() error {
	files, err := os.ReadDir(sm.storage)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		if filepath.Ext(file.Name()) != ".json" {
			continue
		}

		sessionPath := filepath.Join(sm.storage, file.Name())
		data, err := os.ReadFile(sessionPath)
		if err != nil {
			continue
		}

		var session Session
		if err := json.Unmarshal(data, &session); err != nil {
			continue
		}
		session.Messages = messageutil.FilterInvalidHistoryMessages(session.Messages)
		normalizeHistoryCreatedAt(session.Messages)

		sm.sessions[session.Key] = &session
	}

	return nil
}

// Close is a no-op for the in-memory SessionManager; it satisfies the
// SessionStore interface so callers can release resources uniformly.
func (sm *SessionManager) Close() error {
	return nil
}

// SetHistory updates the messages of a session.
func (sm *SessionManager) SetHistory(key string, history []providers.Message) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	session, ok := sm.sessions[key]
	if ok {
		history = messageutil.FilterInvalidHistoryMessages(history)
		// Create a deep copy to strictly isolate internal state
		// from the caller's slice.
		msgs := make([]providers.Message, len(history))
		copy(msgs, history)
		normalizeHistoryCreatedAt(msgs)
		session.Messages = msgs
		advanceHistoryRevision(session)
		session.Updated = time.Now()
	}
}
