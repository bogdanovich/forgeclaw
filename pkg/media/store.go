package media

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/sipeed/picoclaw/pkg/logger"
)

// CleanupPolicy controls how the MediaStore treats the underlying file when
// a ref is released or expires.
type CleanupPolicy string

const (
	// CleanupPolicyDeleteOnCleanup means the file is store-managed and may be
	// deleted once the final ref for that path is gone.
	CleanupPolicyDeleteOnCleanup CleanupPolicy = "delete_on_cleanup"
	// CleanupPolicyForgetOnly means the store should only drop ref mappings and
	// must never delete the underlying file.
	CleanupPolicyForgetOnly CleanupPolicy = "forget_only"
)

// MediaMeta holds metadata about a stored media file.
type MediaMeta struct {
	Filename      string
	ContentType   string
	Source        string        // "telegram", "discord", "tool:image-gen", etc.
	CleanupPolicy CleanupPolicy // defaults to CleanupPolicyDeleteOnCleanup
}

// MediaStore manages the lifecycle of media files associated with processing scopes.
type MediaStore interface {
	// Store registers an existing local file under the given scope.
	// Returns a ref identifier (e.g. "media://<id>").
	// Store does not move or copy the file; it only records the mapping.
	// If meta.CleanupPolicy is empty, CleanupPolicyDeleteOnCleanup is assumed.
	Store(localPath string, meta MediaMeta, scope string) (ref string, err error)

	// Resolve returns the local file path for a given ref.
	Resolve(ref string) (localPath string, err error)

	// ResolveWithMeta returns the local file path and metadata for a given ref.
	ResolveWithMeta(ref string) (localPath string, meta MediaMeta, err error)

	// ReleaseAll deletes all files registered under the given scope
	// and removes the mapping entries. File-not-exist errors are ignored.
	ReleaseAll(scope string) error
}

// mediaEntry holds the path and metadata for a stored media file.
type mediaEntry struct {
	path     string
	meta     MediaMeta
	storedAt time.Time
}

type pathRefState struct {
	refCount       int
	deleteEligible bool
}

// MediaCleanerConfig configures the background TTL cleanup.
type MediaCleanerConfig struct {
	Enabled  bool
	MaxAge   time.Duration
	Interval time.Duration
}

// FileMediaStore manages local media refs. When constructed with a persistent
// index, refs survive process restarts as long as their underlying files remain.
type FileMediaStore struct {
	mu          sync.RWMutex
	refs        map[string]mediaEntry
	scopeToRefs map[string]map[string]struct{}
	refToScope  map[string]string
	refToPath   map[string]string
	pathStates  map[string]pathRefState

	cleanerCfg MediaCleanerConfig
	stop       chan struct{}
	startOnce  sync.Once
	stopOnce   sync.Once
	nowFunc    func() time.Time // for testing
	index      *mediaIndex
}

// NewFileMediaStore creates a new FileMediaStore without background cleanup.
func NewFileMediaStore() *FileMediaStore {
	return newFileMediaStore(MediaCleanerConfig{}, nil)
}

// NewFileMediaStoreWithCleanup creates a FileMediaStore with TTL-based background cleanup.
func NewFileMediaStoreWithCleanup(cfg MediaCleanerConfig) *FileMediaStore {
	return newFileMediaStore(cfg, nil)
}

// NewFileMediaStoreWithPersistentIndex creates a MediaStore backed by an
// atomic workspace-local index. Missing files from an earlier process are
// discarded during recovery and are never exposed as valid refs.
func NewFileMediaStoreWithPersistentIndex(indexPath string, cfg MediaCleanerConfig) (*FileMediaStore, error) {
	index := &mediaIndex{path: indexPath}
	store := newFileMediaStore(cfg, index)
	entries, err := loadMediaIndex(indexPath)
	if err != nil {
		return nil, err
	}

	missingEntries := false
	for _, entry := range entries {
		if _, err := os.Stat(entry.Path); err != nil {
			missingEntries = true
			continue
		}
		entry.Meta.CleanupPolicy = normalizeCleanupPolicy(entry.Meta.CleanupPolicy)
		store.addEntryLocked(entry.Ref, mediaEntry{path: entry.Path, meta: entry.Meta, storedAt: entry.StoredAt}, entry.Scope)
	}
	if missingEntries {
		if err := store.persistLocked(nil, nil); err != nil {
			return nil, err
		}
	}
	return store, nil
}

func newFileMediaStore(cfg MediaCleanerConfig, index *mediaIndex) *FileMediaStore {
	store := &FileMediaStore{
		refs:        make(map[string]mediaEntry),
		scopeToRefs: make(map[string]map[string]struct{}),
		refToScope:  make(map[string]string),
		refToPath:   make(map[string]string),
		pathStates:  make(map[string]pathRefState),
		cleanerCfg:  cfg,
		nowFunc:     time.Now,
		index:       index,
	}
	if cfg.Enabled {
		store.stop = make(chan struct{})
	}
	return store
}

// Store registers a local file under the given scope. The file must exist.
func (s *FileMediaStore) Store(localPath string, meta MediaMeta, scope string) (string, error) {
	absPath, err := filepath.Abs(localPath)
	if err != nil {
		return "", fmt.Errorf("media store: resolve path %q: %w", localPath, err)
	}
	localPath = absPath
	if _, err := os.Stat(localPath); err != nil {
		return "", fmt.Errorf("media store: %s: %w", localPath, err)
	}

	ref := "media://" + uuid.New().String()
	meta.CleanupPolicy = normalizeCleanupPolicy(meta.CleanupPolicy)

	entry := mediaEntry{path: localPath, meta: meta, storedAt: s.nowFunc()}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.persistLocked([]persistentMediaEntry{{
		Ref: ref, Path: entry.path, Meta: entry.meta, Scope: scope, StoredAt: entry.storedAt,
	}}, nil); err != nil {
		return "", err
	}
	s.addEntryLocked(ref, entry, scope)
	return ref, nil
}

func (s *FileMediaStore) addEntryLocked(ref string, entry mediaEntry, scope string) {
	s.refs[ref] = entry
	if s.scopeToRefs[scope] == nil {
		s.scopeToRefs[scope] = make(map[string]struct{})
	}
	s.scopeToRefs[scope][ref] = struct{}{}
	s.refToScope[ref] = scope
	s.refToPath[ref] = entry.path

	pathState := s.pathStates[entry.path]
	if pathState.refCount == 0 {
		pathState.deleteEligible = entry.meta.CleanupPolicy == CleanupPolicyDeleteOnCleanup
	} else if entry.meta.CleanupPolicy == CleanupPolicyForgetOnly {
		// Be conservative: once a path is borrowed externally, never let this
		// lifecycle auto-delete it even if store-managed refs also exist.
		pathState.deleteEligible = false
	}
	pathState.refCount++
	s.pathStates[entry.path] = pathState
}

// Resolve returns the local path for the given ref.
func (s *FileMediaStore) Resolve(ref string) (string, error) {
	path, _, err := s.resolve(ref)
	return path, err
}

// ResolveWithMeta returns the local path and metadata for the given ref.
func (s *FileMediaStore) ResolveWithMeta(ref string) (string, MediaMeta, error) {
	return s.resolve(ref)
}

func (s *FileMediaStore) resolve(ref string) (string, MediaMeta, error) {
	s.mu.RLock()
	entry, ok := s.refs[ref]
	s.mu.RUnlock()
	if !ok {
		return "", MediaMeta{}, fmt.Errorf("media store: unknown ref: %s", ref)
	}
	if _, err := os.Stat(entry.path); err == nil {
		return entry.path, entry.meta, nil
	}

	// Persist removal before dropping the in-memory entry. This may leave an
	// orphaned file after a crash, but can never revive a released ref.
	s.mu.Lock()
	if current, exists := s.refs[ref]; exists && current.path == entry.path {
		if err := s.persistLocked(nil, map[string]struct{}{ref: {}}); err != nil {
			s.mu.Unlock()
			return "", MediaMeta{}, fmt.Errorf("media store: unavailable ref: %s (persist removal: %w)", ref, err)
		}
		if scope, exists := s.refToScope[ref]; exists {
			delete(s.scopeToRefs[scope], ref)
			if len(s.scopeToRefs[scope]) == 0 {
				delete(s.scopeToRefs, scope)
			}
		}
		s.releaseRefLocked(ref, entry.path)
	}
	s.mu.Unlock()
	return "", MediaMeta{}, fmt.Errorf("media store: unavailable ref: %s", ref)
}

// ReleaseAll removes all files under the given scope and cleans up mappings.
// Phase 1 (under lock): remove entries from maps.
// Phase 2 (no lock): delete store-managed files from disk once their final
// path ref is gone.
func (s *FileMediaStore) ReleaseAll(scope string) error {
	// Phase 1: collect paths and remove from maps under lock
	var paths []string

	s.mu.Lock()
	refs, ok := s.scopeToRefs[scope]
	if !ok {
		s.mu.Unlock()
		return nil
	}

	removedRefs := make(map[string]struct{}, len(refs))
	for ref := range refs {
		removedRefs[ref] = struct{}{}
	}
	if err := s.persistLocked(nil, removedRefs); err != nil {
		s.mu.Unlock()
		return err
	}
	for ref := range refs {
		fallbackPath := ""
		if entry, exists := s.refs[ref]; exists {
			fallbackPath = entry.path
		}
		if removablePath, shouldDelete := s.releaseRefLocked(ref, fallbackPath); shouldDelete {
			paths = append(paths, removablePath)
		}
	}
	delete(s.scopeToRefs, scope)
	s.mu.Unlock()

	// Phase 2: delete files without holding the lock
	for _, p := range paths {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			logger.WarnCF("media", "release: failed to remove file", map[string]any{
				"path":  p,
				"error": err.Error(),
			})
		}
	}

	return nil
}

// CleanExpired removes all entries older than MaxAge.
// Phase 1 (under lock): identify expired entries and remove from maps.
// Phase 2 (no lock): delete store-managed files from disk to minimize lock contention.
func (s *FileMediaStore) CleanExpired() int {
	if s.cleanerCfg.MaxAge <= 0 {
		return 0
	}

	// Phase 1: collect expired entries under lock
	type expiredEntry struct {
		ref        string
		deletePath string
	}

	s.mu.Lock()
	cutoff := s.nowFunc().Add(-s.cleanerCfg.MaxAge)
	var expired []expiredEntry

	for ref, entry := range s.refs {
		if entry.storedAt.Before(cutoff) {
			if expired == nil {
				expired = make([]expiredEntry, 0)
			}
			expired = append(expired, expiredEntry{ref: ref})
		}
	}
	if len(expired) == 0 {
		s.mu.Unlock()
		return 0
	}
	removedRefs := make(map[string]struct{}, len(expired))
	for _, item := range expired {
		removedRefs[item.ref] = struct{}{}
	}
	if err := s.persistLocked(nil, removedRefs); err != nil {
		s.mu.Unlock()
		logger.WarnCF("media", "cleanup: failed to persist index", map[string]any{"error": err.Error()})
		return 0
	}
	for idx := range expired {
		ref := expired[idx].ref
		entry := s.refs[ref]
		if entry.storedAt.Before(cutoff) {
			if scope, ok := s.refToScope[ref]; ok {
				if scopeRefs, ok := s.scopeToRefs[scope]; ok {
					delete(scopeRefs, ref)
					if len(scopeRefs) == 0 {
						delete(s.scopeToRefs, scope)
					}
				}
			}

			if deletePath, shouldDelete := s.releaseRefLocked(ref, entry.path); shouldDelete {
				expired[idx].deletePath = deletePath
			}
		}
	}
	s.mu.Unlock()

	// Phase 2: delete files without holding the lock
	for _, e := range expired {
		if e.deletePath == "" {
			continue
		}
		if err := os.Remove(e.deletePath); err != nil && !os.IsNotExist(err) {
			logger.WarnCF("media", "cleanup: failed to remove file", map[string]any{
				"path":  e.deletePath,
				"error": err.Error(),
			})
		}
	}

	return len(expired)
}

// persistLocked writes a complete bounded snapshot. additions are entries not
// yet present in memory; removed refs are omitted from the snapshot.
func (s *FileMediaStore) persistLocked(additions []persistentMediaEntry, removed map[string]struct{}) error {
	if s.index == nil {
		return nil
	}
	entries := make([]persistentMediaEntry, 0, len(s.refs)+len(additions))
	for ref, entry := range s.refs {
		if _, remove := removed[ref]; remove {
			continue
		}
		entries = append(entries, persistentMediaEntry{Ref: ref, Path: entry.path, Meta: entry.meta, Scope: s.refToScope[ref], StoredAt: entry.storedAt})
	}
	entries = append(entries, additions...)
	return s.index.save(entries)
}

func normalizeCleanupPolicy(policy CleanupPolicy) CleanupPolicy {
	switch policy {
	case "", CleanupPolicyDeleteOnCleanup:
		return CleanupPolicyDeleteOnCleanup
	case CleanupPolicyForgetOnly:
		return CleanupPolicyForgetOnly
	default:
		return CleanupPolicyDeleteOnCleanup
	}
}

func (s *FileMediaStore) releaseRefLocked(ref, fallbackPath string) (string, bool) {
	path := fallbackPath
	if storedPath, ok := s.refToPath[ref]; ok {
		path = storedPath
		delete(s.refToPath, ref)
	}

	delete(s.refs, ref)
	delete(s.refToScope, ref)

	if path == "" {
		return "", false
	}

	pathState, ok := s.pathStates[path]
	if !ok {
		return "", false
	}
	if pathState.refCount <= 1 {
		delete(s.pathStates, path)
		return path, pathState.deleteEligible
	}

	pathState.refCount--
	s.pathStates[path] = pathState
	return "", false
}

// Start begins the background cleanup goroutine if cleanup is enabled.
// Safe to call multiple times; only the first call starts the goroutine.
func (s *FileMediaStore) Start() {
	if !s.cleanerCfg.Enabled || s.stop == nil {
		return
	}
	if s.cleanerCfg.Interval <= 0 || s.cleanerCfg.MaxAge <= 0 {
		logger.WarnCF("media", "cleanup: skipped due to invalid config", map[string]any{
			"interval": s.cleanerCfg.Interval.String(),
			"max_age":  s.cleanerCfg.MaxAge.String(),
		})
		return
	}

	s.startOnce.Do(func() {
		logger.InfoCF("media", "cleanup enabled", map[string]any{
			"interval": s.cleanerCfg.Interval.String(),
			"max_age":  s.cleanerCfg.MaxAge.String(),
		})

		go func() {
			ticker := time.NewTicker(s.cleanerCfg.Interval)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					if n := s.CleanExpired(); n > 0 {
						logger.InfoCF("media", "cleanup: removed expired entries", map[string]any{
							"count": n,
						})
					}
				case <-s.stop:
					return
				}
			}
		}()
	})
}

// Stop terminates the background cleanup goroutine.
// Safe to call multiple times; only the first call closes the channel.
func (s *FileMediaStore) Stop() {
	if s.stop == nil {
		return
	}
	s.stopOnce.Do(func() {
		close(s.stop)
	})
}
