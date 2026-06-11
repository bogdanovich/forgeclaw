package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/fileutil"
	"github.com/sipeed/picoclaw/pkg/logger"
)

// State represents the persistent state for a workspace.
// It includes information about the last active channel/chat.
type State struct {
	// LastChannel is the last channel used for communication
	LastChannel string `json:"last_channel,omitempty"`

	// LastChatID is the last chat ID used for communication
	LastChatID string `json:"last_chat_id,omitempty"`

	// SessionOverrides maps the default routed session key for a conversation
	// onto an explicit replacement session key created by a soft reset.
	SessionOverrides map[string]string `json:"session_overrides,omitempty"`

	// ToolFeedbackOverrides stores per-routed-session enable/disable overrides
	// for inline tool feedback such as working_summary.
	ToolFeedbackOverrides map[string]bool `json:"tool_feedback_overrides,omitempty"`

	// SessionModelOverrides stores manual model selections scoped to a routed
	// conversation. The stored value is the canonical configured model alias.
	SessionModelOverrides map[string]SessionModelOverride `json:"session_model_overrides,omitempty"`

	// AutoModelSelections stores temporary session-scoped auto-fallback routing
	// state. SelectedModel identifies the user's intended model; ActiveModel is
	// the temporary fallback model currently pinned for the conversation.
	AutoModelSelections map[string]AutoModelSelection `json:"auto_model_selections,omitempty"`

	// Timestamp is the last time this state was updated
	Timestamp time.Time `json:"timestamp"`
}

type AutoModelSelection struct {
	SelectedProvider string    `json:"selected_provider,omitempty"`
	SelectedModel    string    `json:"selected_model,omitempty"`
	ActiveProvider   string    `json:"active_provider,omitempty"`
	ActiveModel      string    `json:"active_model,omitempty"`
	Reason           string    `json:"reason,omitempty"`
	ExpiresAt        time.Time `json:"expires_at,omitempty"`
	UpdatedAt        time.Time `json:"updated_at,omitempty"`
}

type SessionModelOverride struct {
	Model     string    `json:"model,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

// Manager manages persistent state with atomic saves.
type Manager struct {
	workspace string
	state     *State
	mu        sync.RWMutex
	stateFile string
}

// NewManager creates a new state manager for the given workspace.
func NewManager(workspace string) *Manager {
	stateDir := filepath.Join(workspace, "state")
	stateFile := filepath.Join(stateDir, "state.json")
	oldStateFile := filepath.Join(workspace, "state.json")

	// Create state directory if it doesn't exist
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		logger.WarnCF("state", "failed to create state directory", map[string]any{
			"dir":   stateDir,
			"error": err.Error(),
		})
	}

	sm := &Manager{
		workspace: workspace,
		stateFile: stateFile,
		state:     &State{},
	}

	// Try to load from new location first
	if _, err := os.Stat(stateFile); os.IsNotExist(err) {
		// New file doesn't exist, try migrating from old location
		if data, err := os.ReadFile(oldStateFile); err == nil {
			if err := json.Unmarshal(data, sm.state); err == nil {
				// Migrate to new location
				if err := sm.saveAtomic(); err != nil {
					logger.WarnCF("state", "failed to save state", map[string]any{
						"error": err.Error(),
					})
				}
				logger.InfoCF("state", "migrated state", map[string]any{
					"from": oldStateFile,
					"to":   stateFile,
				})
			}
		}
	} else {
		// Load from new location
		if err := sm.load(); err != nil {
			logger.WarnCF("state", "failed to load state", map[string]any{
				"error": err.Error(),
			})
		}
	}

	return sm
}

// SetLastChannel atomically updates the last channel and saves the state.
// This method uses a temp file + rename pattern for atomic writes,
// ensuring that the state file is never corrupted even if the process crashes.
func (sm *Manager) SetLastChannel(channel string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Update state
	sm.state.LastChannel = channel
	sm.state.Timestamp = time.Now()

	// Atomic save using temp file + rename
	if err := sm.saveAtomic(); err != nil {
		return fmt.Errorf("failed to save state atomically: %w", err)
	}

	return nil
}

// SetLastChatID atomically updates the last chat ID and saves the state.
func (sm *Manager) SetLastChatID(chatID string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Update state
	sm.state.LastChatID = chatID
	sm.state.Timestamp = time.Now()

	// Atomic save using temp file + rename
	if err := sm.saveAtomic(); err != nil {
		return fmt.Errorf("failed to save state atomically: %w", err)
	}

	return nil
}

// SetSessionOverride persists a replacement session key for a routed session.
func (sm *Manager) SetSessionOverride(routeSessionKey, sessionKey string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	routeSessionKey = strings.TrimSpace(routeSessionKey)
	sessionKey = strings.TrimSpace(sessionKey)
	if routeSessionKey == "" || sessionKey == "" {
		return fmt.Errorf("route session key and session key are required")
	}

	if sm.state.SessionOverrides == nil {
		sm.state.SessionOverrides = make(map[string]string)
	}
	sm.state.SessionOverrides[routeSessionKey] = sessionKey
	sm.state.Timestamp = time.Now()

	if err := sm.saveAtomic(); err != nil {
		return fmt.Errorf("failed to save state atomically: %w", err)
	}

	return nil
}

// ClearSessionOverride removes a previously persisted replacement session key.
func (sm *Manager) ClearSessionOverride(routeSessionKey string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	routeSessionKey = strings.TrimSpace(routeSessionKey)
	if routeSessionKey == "" {
		return fmt.Errorf("route session key is required")
	}
	if len(sm.state.SessionOverrides) == 0 {
		return nil
	}

	delete(sm.state.SessionOverrides, routeSessionKey)
	if len(sm.state.SessionOverrides) == 0 {
		sm.state.SessionOverrides = nil
	}
	sm.state.Timestamp = time.Now()

	if err := sm.saveAtomic(); err != nil {
		return fmt.Errorf("failed to save state atomically: %w", err)
	}

	return nil
}

// GetSessionOverride returns the replacement session key for a routed session.
func (sm *Manager) GetSessionOverride(routeSessionKey string) string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if len(sm.state.SessionOverrides) == 0 {
		return ""
	}
	return sm.state.SessionOverrides[strings.TrimSpace(routeSessionKey)]
}

// SetToolFeedbackOverride persists a tool feedback enable/disable override for
// a routed session.
func (sm *Manager) SetToolFeedbackOverride(routeSessionKey string, enabled bool) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	routeSessionKey = strings.TrimSpace(routeSessionKey)
	if routeSessionKey == "" {
		return fmt.Errorf("route session key is required")
	}

	if sm.state.ToolFeedbackOverrides == nil {
		sm.state.ToolFeedbackOverrides = make(map[string]bool)
	}
	sm.state.ToolFeedbackOverrides[routeSessionKey] = enabled
	sm.state.Timestamp = time.Now()

	if err := sm.saveAtomic(); err != nil {
		return fmt.Errorf("failed to save state atomically: %w", err)
	}

	return nil
}

// ClearToolFeedbackOverride removes a persisted tool feedback override for a
// routed session, causing config defaults to apply again.
func (sm *Manager) ClearToolFeedbackOverride(routeSessionKey string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	routeSessionKey = strings.TrimSpace(routeSessionKey)
	if routeSessionKey == "" {
		return fmt.Errorf("route session key is required")
	}
	if len(sm.state.ToolFeedbackOverrides) == 0 {
		return nil
	}

	delete(sm.state.ToolFeedbackOverrides, routeSessionKey)
	if len(sm.state.ToolFeedbackOverrides) == 0 {
		sm.state.ToolFeedbackOverrides = nil
	}
	sm.state.Timestamp = time.Now()

	if err := sm.saveAtomic(); err != nil {
		return fmt.Errorf("failed to save state atomically: %w", err)
	}

	return nil
}

// GetToolFeedbackOverride returns the persisted tool feedback override for a
// routed session and whether an override is present.
func (sm *Manager) GetToolFeedbackOverride(routeSessionKey string) (bool, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if len(sm.state.ToolFeedbackOverrides) == 0 {
		return false, false
	}
	value, ok := sm.state.ToolFeedbackOverrides[strings.TrimSpace(routeSessionKey)]
	return value, ok
}

// SetSessionModelOverride persists a conversation-scoped manual model
// selection for a routed session.
func (sm *Manager) SetSessionModelOverride(routeSessionKey, model string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	routeSessionKey = strings.TrimSpace(routeSessionKey)
	model = strings.TrimSpace(model)
	if routeSessionKey == "" || model == "" {
		return fmt.Errorf("route session key and model are required")
	}

	if sm.state.SessionModelOverrides == nil {
		sm.state.SessionModelOverrides = make(map[string]SessionModelOverride)
	}
	sm.state.SessionModelOverrides[routeSessionKey] = SessionModelOverride{
		Model:     model,
		UpdatedAt: time.Now(),
	}
	sm.state.Timestamp = time.Now()

	if err := sm.saveAtomic(); err != nil {
		return fmt.Errorf("failed to save state atomically: %w", err)
	}

	return nil
}

// ClearSessionModelOverride removes a previously persisted manual model
// selection for a routed session.
func (sm *Manager) ClearSessionModelOverride(routeSessionKey string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	routeSessionKey = strings.TrimSpace(routeSessionKey)
	if routeSessionKey == "" {
		return fmt.Errorf("route session key is required")
	}
	if len(sm.state.SessionModelOverrides) == 0 {
		return nil
	}

	delete(sm.state.SessionModelOverrides, routeSessionKey)
	if len(sm.state.SessionModelOverrides) == 0 {
		sm.state.SessionModelOverrides = nil
	}
	sm.state.Timestamp = time.Now()

	if err := sm.saveAtomic(); err != nil {
		return fmt.Errorf("failed to save state atomically: %w", err)
	}

	return nil
}

// GetSessionModelOverride returns a persisted manual model selection for a
// routed session.
func (sm *Manager) GetSessionModelOverride(routeSessionKey string) (SessionModelOverride, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if len(sm.state.SessionModelOverrides) == 0 {
		return SessionModelOverride{}, false
	}
	value, ok := sm.state.SessionModelOverrides[strings.TrimSpace(routeSessionKey)]
	return value, ok
}

// SetAutoModelSelection persists a temporary auto-fallback model selection for
// a routed session.
func (sm *Manager) SetAutoModelSelection(routeSessionKey string, selection AutoModelSelection) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	routeSessionKey = strings.TrimSpace(routeSessionKey)
	if routeSessionKey == "" {
		return fmt.Errorf("route session key is required")
	}

	selection.SelectedProvider = strings.TrimSpace(selection.SelectedProvider)
	selection.SelectedModel = strings.TrimSpace(selection.SelectedModel)
	selection.ActiveProvider = strings.TrimSpace(selection.ActiveProvider)
	selection.ActiveModel = strings.TrimSpace(selection.ActiveModel)
	selection.Reason = strings.TrimSpace(selection.Reason)
	selection.UpdatedAt = time.Now()

	if sm.state.AutoModelSelections == nil {
		sm.state.AutoModelSelections = make(map[string]AutoModelSelection)
	}
	sm.state.AutoModelSelections[routeSessionKey] = selection
	sm.state.Timestamp = selection.UpdatedAt

	if err := sm.saveAtomic(); err != nil {
		return fmt.Errorf("failed to save state atomically: %w", err)
	}

	return nil
}

// ClearAutoModelSelection removes a persisted auto-fallback model selection.
func (sm *Manager) ClearAutoModelSelection(routeSessionKey string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	routeSessionKey = strings.TrimSpace(routeSessionKey)
	if routeSessionKey == "" {
		return fmt.Errorf("route session key is required")
	}
	if len(sm.state.AutoModelSelections) == 0 {
		return nil
	}

	delete(sm.state.AutoModelSelections, routeSessionKey)
	if len(sm.state.AutoModelSelections) == 0 {
		sm.state.AutoModelSelections = nil
	}
	sm.state.Timestamp = time.Now()

	if err := sm.saveAtomic(); err != nil {
		return fmt.Errorf("failed to save state atomically: %w", err)
	}

	return nil
}

// GetAutoModelSelection returns the persisted auto-fallback model selection
// for a routed session and whether one is present.
func (sm *Manager) GetAutoModelSelection(routeSessionKey string) (AutoModelSelection, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if len(sm.state.AutoModelSelections) == 0 {
		return AutoModelSelection{}, false
	}
	value, ok := sm.state.AutoModelSelections[strings.TrimSpace(routeSessionKey)]
	return value, ok
}

// GetLastChannel returns the last channel from the state.
func (sm *Manager) GetLastChannel() string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.state.LastChannel
}

// GetLastChatID returns the last chat ID from the state.
func (sm *Manager) GetLastChatID() string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.state.LastChatID
}

// GetTimestamp returns the timestamp of the last state update.
func (sm *Manager) GetTimestamp() time.Time {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.state.Timestamp
}

// saveAtomic performs an atomic save using temp file + rename.
// This ensures that the state file is never corrupted:
// 1. Write to a temp file
// 2. Sync to disk (critical for SD cards/flash storage)
// 3. Rename temp file to target (atomic on POSIX systems)
// 4. If rename fails, cleanup the temp file
//
// Must be called with the lock held.
func (sm *Manager) saveAtomic() error {
	// Use unified atomic write utility with explicit sync for flash storage reliability.
	// Using 0o600 (owner read/write only) for secure default permissions.
	data, err := json.MarshalIndent(sm.state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	return fileutil.WriteFileAtomic(sm.stateFile, data, 0o600)
}

// load loads the state from disk.
func (sm *Manager) load() error {
	data, err := os.ReadFile(sm.stateFile)
	if err != nil {
		// File doesn't exist yet, that's OK
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to read state file: %w", err)
	}

	if err := json.Unmarshal(data, sm.state); err != nil {
		return fmt.Errorf("failed to unmarshal state: %w", err)
	}

	return nil
}
