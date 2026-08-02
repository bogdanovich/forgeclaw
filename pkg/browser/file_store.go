package browser

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/bogdanovich/mintclaw/pkg/fileutil"
)

const (
	fileStoreVersion        = 1
	DefaultFileStoreRecords = 512
	DefaultFileStoreBytes   = 8 * 1024 * 1024
)

var (
	ErrStoreFull   = errors.New("browser state store is full")
	ErrStoreOwned  = errors.New("browser state store is owned by another process")
	ErrStoreClosed = errors.New("browser state store is closed")
)

type fileStoreDocument struct {
	Version     int                   `json:"version"`
	Sessions    map[string]Session    `json:"sessions"`
	Invocations map[string]Invocation `json:"invocations"`
}

// FileStore is the gateway's bounded durable browser state boundary. One
// gateway process owns the document at a time; every mutation is an atomic
// owner-only replacement.
type FileStore struct {
	path        string
	maxRecords  int
	maxBytes    int
	writeFile   func(string, []byte, os.FileMode) error
	releaseLock func()

	mu          sync.Mutex
	closed      bool
	sessions    map[string]Session
	invocations map[string]Invocation
}

func NewFileStore(path string, maxRecords, maxBytes int) (*FileStore, error) {
	path = filepath.Clean(path)
	if path == "." || path == string(filepath.Separator) {
		return nil, errors.New("browser state store path is required")
	}
	if maxRecords <= 0 {
		maxRecords = DefaultFileStoreRecords
	}
	if maxBytes <= 0 {
		maxBytes = DefaultFileStoreBytes
	}
	if err := prepareStorePath(path); err != nil {
		return nil, err
	}
	release, err := acquireStoreLock(path + ".lock")
	if err != nil {
		return nil, err
	}
	store := &FileStore{
		path: path, maxRecords: maxRecords, maxBytes: maxBytes,
		writeFile: writeSecureStoreFile, releaseLock: release,
		sessions: make(map[string]Session), invocations: make(map[string]Invocation),
	}
	if err := store.load(); err != nil {
		store.Close()
		return nil, err
	}
	return store, nil
}

func (store *FileStore) Close() {
	if store == nil {
		return
	}
	store.mu.Lock()
	if store.closed {
		store.mu.Unlock()
		return
	}
	store.closed = true
	release := store.releaseLock
	store.releaseLock = nil
	store.mu.Unlock()
	if release != nil {
		release()
	}
}

func (store *FileStore) load() error {
	file, err := os.Open(store.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open browser state store: %w", err)
	}
	defer file.Close()
	limited := io.LimitReader(file, int64(store.maxBytes)+1)
	raw, err := io.ReadAll(limited)
	if err != nil {
		return fmt.Errorf("read browser state store: %w", err)
	}
	if len(raw) > store.maxBytes {
		return ErrStoreFull
	}
	if err = rejectDuplicateJSONMembers(raw); err != nil {
		return fmt.Errorf("decode browser state store: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var document fileStoreDocument
	if err = decoder.Decode(&document); err != nil {
		return fmt.Errorf("decode browser state store: %w", err)
	}
	if err = ensureJSONEOF(decoder); err != nil {
		return fmt.Errorf("decode browser state store: %w", err)
	}
	if document.Version != fileStoreVersion || document.Sessions == nil || document.Invocations == nil ||
		len(document.Sessions)+len(document.Invocations) > store.maxRecords {
		return fmt.Errorf("%w: invalid browser state document", ErrInvalid)
	}
	for id, session := range document.Sessions {
		if id != session.ID || session.Validate() != nil {
			return fmt.Errorf("%w: invalid persisted browser session", ErrInvalid)
		}
	}
	for id, invocation := range document.Invocations {
		if id != invocation.ID || invocation.Validate() != nil {
			return fmt.Errorf("%w: invalid persisted browser invocation", ErrInvalid)
		}
		if _, ok := document.Sessions[invocation.SessionID]; !ok {
			return fmt.Errorf("%w: invocation references a missing session", ErrInvalid)
		}
		invocation.TerminalResult = cloneBytes(invocation.TerminalResult)
		document.Invocations[id] = invocation
	}
	store.sessions = document.Sessions
	store.invocations = document.Invocations
	return nil
}

func rejectDuplicateJSONMembers(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	var scan func() error
	scan = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delimiter {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				nameToken, tokenErr := decoder.Token()
				if tokenErr != nil {
					return tokenErr
				}
				name, ok := nameToken.(string)
				if !ok {
					return errors.New("object member name is not a string")
				}
				if _, exists := seen[name]; exists {
					return fmt.Errorf("duplicate object member %q", name)
				}
				seen[name] = struct{}{}
				if scanErr := scan(); scanErr != nil {
					return scanErr
				}
			}
		case '[':
			for decoder.More() {
				if scanErr := scan(); scanErr != nil {
					return scanErr
				}
			}
		default:
			return errors.New("invalid JSON delimiter")
		}
		_, err = decoder.Token()
		return err
	}
	if err := scan(); err != nil {
		return err
	}
	return ensureJSONEOF(decoder)
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func (store *FileStore) persistLocked(
	previousSessions map[string]Session,
	previousInvocations map[string]Invocation,
) error {
	document := fileStoreDocument{Version: fileStoreVersion, Sessions: store.sessions, Invocations: store.invocations}
	raw, err := json.Marshal(document)
	if err != nil {
		store.sessions = previousSessions
		store.invocations = previousInvocations
		return err
	}
	if len(raw) > store.maxBytes {
		store.sessions = previousSessions
		store.invocations = previousInvocations
		return ErrStoreFull
	}
	if err = store.writeFile(store.path, raw, 0o600); err != nil {
		if !fileutil.IsCommittedWriteError(err) {
			store.sessions = previousSessions
			store.invocations = previousInvocations
		}
		return err
	}
	return nil
}

func (store *FileStore) ensureOpenLocked() error {
	if store.closed || store.releaseLock == nil {
		return ErrStoreClosed
	}
	return nil
}

func (store *FileStore) CreateSession(_ context.Context, session Session) error {
	if err := session.Validate(); err != nil {
		return err
	}
	if session.State != SessionOpening || session.Revision != 1 {
		return fmt.Errorf("%w: session must enter as opening revision 1", ErrConflict)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ensureOpenLocked(); err != nil {
		return err
	}
	if _, exists := store.sessions[session.ID]; exists {
		return ErrConflict
	}
	for _, existing := range store.sessions {
		if !existing.State.Terminal() && existing.Target == session.Target && existing.Profile == session.Profile {
			return ErrBusy
		}
	}
	if len(store.sessions)+len(store.invocations) >= store.maxRecords {
		return ErrStoreFull
	}
	previousSessions, previousInvocations := store.cloneLocked()
	store.sessions[session.ID] = session
	return store.persistLocked(previousSessions, previousInvocations)
}

func (store *FileStore) GetSession(_ context.Context, id string) (Session, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ensureOpenLocked(); err != nil {
		return Session{}, err
	}
	session, ok := store.sessions[id]
	if !ok {
		return Session{}, ErrNotFound
	}
	return session, nil
}

func (store *FileStore) ListSessions(_ context.Context) ([]Session, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ensureOpenLocked(); err != nil {
		return nil, err
	}
	result := make([]Session, 0, len(store.sessions))
	for _, session := range store.sessions {
		result = append(result, session)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (store *FileStore) UpdateSession(_ context.Context, expected uint64, next Session) error {
	if err := next.Validate(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ensureOpenLocked(); err != nil {
		return err
	}
	current, ok := store.sessions[next.ID]
	if !ok {
		return ErrNotFound
	}
	if current.Revision != expected || next.Revision != expected+1 {
		return ErrStale
	}
	if current.Owner != next.Owner || current.Target != next.Target || current.Profile != next.Profile ||
		current.CreatedAt != next.CreatedAt || current.DryRun != next.DryRun ||
		current.PolicyRevision != next.PolicyRevision || current.ControllerGeneration != next.ControllerGeneration ||
		current.ExpiresAt != next.ExpiresAt || !validSessionTransition(current.State, next.State) {
		return ErrConflict
	}
	previousSessions, previousInvocations := store.cloneLocked()
	store.sessions[next.ID] = next
	return store.persistLocked(previousSessions, previousInvocations)
}

func (store *FileStore) CreateInvocation(_ context.Context, invocation Invocation) error {
	if err := invocation.Validate(); err != nil {
		return err
	}
	if invocation.State != InvocationPrepared || invocation.Revision != 1 {
		return fmt.Errorf("%w: invocation must enter as prepared revision 1", ErrConflict)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ensureOpenLocked(); err != nil {
		return err
	}
	if _, exists := store.invocations[invocation.ID]; exists {
		return ErrConflict
	}
	session, ok := store.sessions[invocation.SessionID]
	if !ok || !session.Owner.Equal(invocation.Owner) || session.State != SessionReady {
		return ErrDenied
	}
	if len(store.sessions)+len(store.invocations) >= store.maxRecords {
		return ErrStoreFull
	}
	previousSessions, previousInvocations := store.cloneLocked()
	invocation.TerminalResult = cloneBytes(invocation.TerminalResult)
	store.invocations[invocation.ID] = invocation
	return store.persistLocked(previousSessions, previousInvocations)
}

func (store *FileStore) GetInvocation(_ context.Context, id string) (Invocation, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ensureOpenLocked(); err != nil {
		return Invocation{}, err
	}
	invocation, ok := store.invocations[id]
	if !ok {
		return Invocation{}, ErrNotFound
	}
	invocation.TerminalResult = cloneBytes(invocation.TerminalResult)
	return invocation, nil
}

func (store *FileStore) ListInvocations(_ context.Context, sessionID string) ([]Invocation, error) {
	if !validIdentifier(sessionID) {
		return nil, fmt.Errorf("%w: malformed session ID", ErrInvalid)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ensureOpenLocked(); err != nil {
		return nil, err
	}
	result := make([]Invocation, 0)
	for _, invocation := range store.invocations {
		if invocation.SessionID == sessionID {
			invocation.TerminalResult = cloneBytes(invocation.TerminalResult)
			result = append(result, invocation)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (store *FileStore) UpdateInvocation(_ context.Context, expected uint64, next Invocation) error {
	if err := next.Validate(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ensureOpenLocked(); err != nil {
		return err
	}
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
	previousSessions, previousInvocations := store.cloneLocked()
	next.TerminalResult = cloneBytes(next.TerminalResult)
	store.invocations[next.ID] = next
	return store.persistLocked(previousSessions, previousInvocations)
}

func (store *FileStore) PruneInvocations(_ context.Context, completedBefore int64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ensureOpenLocked(); err != nil {
		return err
	}
	previousSessions, previousInvocations := store.cloneLocked()
	changed := false
	for id, invocation := range store.invocations {
		if invocation.State.Terminal() && invocation.CompletedAt > 0 && invocation.CompletedAt < completedBefore {
			delete(store.invocations, id)
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return store.persistLocked(previousSessions, previousInvocations)
}

func (store *FileStore) cloneLocked() (map[string]Session, map[string]Invocation) {
	sessions := make(map[string]Session, len(store.sessions))
	for id, session := range store.sessions {
		sessions[id] = session
	}
	invocations := make(map[string]Invocation, len(store.invocations))
	for id, invocation := range store.invocations {
		invocation.TerminalResult = cloneBytes(invocation.TerminalResult)
		invocations[id] = invocation
	}
	return sessions, invocations
}
