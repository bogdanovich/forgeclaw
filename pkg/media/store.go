package media

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/bogdanovich/mintclaw/pkg/fileutil"
	"github.com/bogdanovich/mintclaw/pkg/logger"
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

// MediaOwner is a durable, non-reversible ownership projection for
// authority-sensitive media consumers.
type MediaOwner struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	ActorID     string `json:"actor_id"`
	RouteID     string `json:"route_id"`
	SessionID   string `json:"session_id"`
}

// NewMediaOwner derives an exact owner without retaining raw routing or actor
// identifiers in the media index.
func NewMediaOwner(
	workspace, agentID, actorID, routeSession, channel, chatID, topicID string,
) (MediaOwner, error) {
	workspace = strings.TrimSpace(workspace)
	agentID = strings.TrimSpace(agentID)
	actorID = strings.TrimSpace(actorID)
	routeSession = strings.TrimSpace(routeSession)
	channel = strings.TrimSpace(channel)
	chatID = strings.TrimSpace(chatID)
	topicID = strings.TrimSpace(topicID)
	if workspace == "" || agentID == "" || actorID == "" ||
		routeSession == "" || channel == "" || chatID == "" {
		return MediaOwner{}, errors.New(
			"media owner requires workspace, agent, actor, route, channel, and chat",
		)
	}
	return MediaOwner{
		WorkspaceID: mediaOwnerCorrelation("workspace", workspace),
		AgentID:     mediaOwnerCorrelation("agent", agentID),
		ActorID:     mediaOwnerCorrelation("actor", actorID),
		RouteID: mediaOwnerCorrelation(
			"route",
			channel,
			chatID,
			topicID,
			routeSession,
		),
		SessionID: mediaOwnerCorrelation("session", routeSession),
	}, nil
}

func mediaOwnerCorrelation(prefix string, values ...string) string {
	hash := sha256.New()
	for _, value := range values {
		_, _ = fmt.Fprintf(hash, "%d:", len(value))
		_, _ = hash.Write([]byte(value))
	}
	return prefix + "_" + hex.EncodeToString(hash.Sum(nil))
}

func (owner MediaOwner) validate() error {
	for _, value := range []string{
		owner.WorkspaceID,
		owner.AgentID,
		owner.ActorID,
		owner.RouteID,
		owner.SessionID,
	} {
		if strings.TrimSpace(value) == "" || len(value) > 96 {
			return errors.New("invalid media owner")
		}
	}
	return nil
}

// MediaStore manages the lifecycle of media files associated with processing scopes.
type MediaStore interface {
	// Store registers an existing local file under the given scope.
	// Returns a ref identifier (e.g. "media://<id>").
	// Persistent stores promote delete-on-cleanup files from TempDir into
	// workspace-owned storage and remove the temporary source after commit.
	// Other files are only recorded and are never moved or copied.
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
	owner    *MediaOwner
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
	// lifecycleMu serializes registration with file deletion. Acquire it
	// before mu whenever an operation may add or remove a path-backed ref.
	lifecycleMu sync.Mutex

	cleanerCfg  MediaCleanerConfig
	stop        chan struct{}
	startOnce   sync.Once
	stopOnce    sync.Once
	cleanerWG   sync.WaitGroup
	nowFunc     func() time.Time // for testing
	index       *mediaIndex
	durableRoot string
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
	durableRoot, err := existingDirectoryAncestor(filepath.Dir(indexPath))
	if err != nil {
		return nil, err
	}
	index := &mediaIndex{path: indexPath}
	store := newFileMediaStore(cfg, index)
	store.durableRoot = durableRoot
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
		store.addEntryLocked(
			entry.Ref,
			mediaEntry{
				path: entry.Path, meta: entry.Meta, storedAt: entry.StoredAt,
				owner: cloneMediaOwner(entry.Owner),
			},
			entry.Scope,
		)
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
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	absPath, err := filepath.Abs(localPath)
	if err != nil {
		return "", fmt.Errorf("media store: resolve path %q: %w", localPath, err)
	}
	localPath = absPath
	if _, statErr := os.Stat(localPath); statErr != nil {
		return "", fmt.Errorf("media store: %s: %w", localPath, statErr)
	}

	ref := "media://" + uuid.New().String()
	meta.CleanupPolicy = normalizeCleanupPolicy(meta.CleanupPolicy)
	persistedPath, promotion, err := s.promoteManagedTempFile(localPath, ref, meta.CleanupPolicy)
	if err != nil {
		return "", err
	}
	if promotion != nil {
		defer promotion.close()
	}

	entry := mediaEntry{path: persistedPath, meta: meta, storedAt: s.nowFunc()}
	s.mu.Lock()
	if err := s.persistLocked([]persistentMediaEntry{{
		Ref: ref, Path: entry.path, Meta: entry.meta, Scope: scope, StoredAt: entry.storedAt,
	}}, nil); err != nil {
		s.mu.Unlock()
		if promotion != nil {
			_ = os.Remove(persistedPath)
		}
		return "", err
	}
	s.addEntryLocked(ref, entry, scope)
	s.mu.Unlock()

	if promotion != nil {
		if err := promotion.removeSource(); err != nil && !os.IsNotExist(err) {
			logger.WarnCF("media", "store: failed to remove promoted temporary file", map[string]any{
				"path":  localPath,
				"error": err.Error(),
			})
		}
	}
	return ref, nil
}

// promoteManagedTempFile copies store-managed downloads from the shared OS
// temp directory into the persistent workspace media directory. The copy is
// committed before the source is removed, so a failed index write leaves the
// caller's original file intact.
type mediaPromotion struct {
	sourceRoot *os.Root
	sourceRel  string
	sourceInfo os.FileInfo
}

func (p *mediaPromotion) close() {
	_ = p.sourceRoot.Close()
}

func (p *mediaPromotion) removeSource() error {
	current, err := p.sourceRoot.Stat(p.sourceRel)
	if err != nil {
		return err
	}
	if !os.SameFile(p.sourceInfo, current) {
		return errors.New("temporary source changed during media promotion")
	}
	return p.sourceRoot.Remove(p.sourceRel)
}

func (s *FileMediaStore) promoteManagedTempFile(
	localPath string,
	ref string,
	policy CleanupPolicy,
) (string, *mediaPromotion, error) {
	if s.index == nil || policy != CleanupPolicyDeleteOnCleanup || !isManagedTempPath(localPath) {
		return localPath, nil, nil
	}

	sourceRoot, sourceRel, sourceFile, sourceInfo, err := openManagedTempFile(localPath)
	if err != nil {
		return "", nil, err
	}
	defer sourceFile.Close()
	promotion := &mediaPromotion{
		sourceRoot: sourceRoot,
		sourceRel:  sourceRel,
		sourceInfo: sourceInfo,
	}
	cleanupPromotion := true
	defer func() {
		if cleanupPromotion {
			promotion.close()
		}
	}()

	managedDir := filepath.Join(filepath.Dir(s.index.path), "files")
	if err := ensureDurableDirectory(
		s.durableRoot,
		managedDir,
		fileutil.MkdirAllDurable,
	); err != nil {
		return "", nil, fmt.Errorf("media store: create persistent media directory: %w", err)
	}

	id := strings.TrimPrefix(ref, "media://")
	destination := filepath.Join(managedDir, id+strings.ToLower(filepath.Ext(localPath)))
	if err := copyMediaFile(sourceFile, destination); err != nil {
		return "", nil, fmt.Errorf("media store: persist temporary file: %w", err)
	}
	cleanupPromotion = false
	return destination, promotion, nil
}

func isManagedTempPath(path string) bool {
	rel, err := filepath.Rel(TempDir(), path)
	return err == nil && rel != "." && rel != "" && rel != ".." &&
		!strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

func existingDirectoryAncestor(path string) (string, error) {
	root := filepath.Clean(path)
	for {
		info, err := os.Stat(root)
		if err == nil {
			if !info.IsDir() {
				return "", fmt.Errorf("durable media ancestor is not a directory: %s", root)
			}
			return root, nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(root)
		if parent == root {
			return "", fmt.Errorf("media store: no existing ancestor for %s", path)
		}
		root = parent
	}
}

func ensureDurableDirectory(
	root string,
	path string,
	mkdirAll func(string, string, os.FileMode) error,
) error {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return err
	}
	return mkdirAll(root, relative, 0o700)
}

func openManagedTempFile(path string) (*os.Root, string, *os.File, os.FileInfo, error) {
	rel, err := filepath.Rel(TempDir(), path)
	if err != nil {
		return nil, "", nil, nil, err
	}
	root, err := os.OpenRoot(TempDir())
	if err != nil {
		return nil, "", nil, nil, err
	}
	fail := func(err error) (*os.Root, string, *os.File, os.FileInfo, error) {
		_ = root.Close()
		return nil, "", nil, nil, err
	}

	current := ""
	parts := strings.Split(filepath.Clean(rel), string(os.PathSeparator))
	for idx, part := range parts {
		current = filepath.Join(current, part)
		info, statErr := root.Lstat(current)
		if statErr != nil {
			return fail(statErr)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fail(fmt.Errorf("media store: managed temporary path contains symlink: %s", current))
		}
		if idx < len(parts)-1 && !info.IsDir() {
			return fail(fmt.Errorf("media store: managed temporary path component is not a directory: %s", current))
		}
	}

	file, err := root.Open(rel)
	if err != nil {
		return fail(err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return fail(err)
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return fail(errors.New("media store: managed temporary source is not a regular file"))
	}
	return root, rel, file, info, nil
}

func copyMediaFile(source io.Reader, destination string) (retErr error) {
	out, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	removeOnError := true
	defer func() {
		if removeOnError {
			_ = out.Close()
			_ = os.Remove(destination)
		}
	}()
	if _, err := io.Copy(out, source); err != nil {
		return err
	}
	if err := out.Sync(); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	if err := fileutil.SyncDirectory(filepath.Dir(destination)); err != nil {
		return err
	}
	removeOnError = false
	return nil
}

// StoreIdempotent registers one deterministic media ref for a durable
// delivery key. Repeating the exact handoff returns the same ref; conflicting
// path, metadata, or scope fails closed.
func (s *FileMediaStore) StoreIdempotent(
	localPath string,
	meta MediaMeta,
	scope string,
	key string,
) (string, error) {
	return s.storeIdempotent(localPath, meta, scope, key, nil)
}

// StoreIdempotentOwned registers a deterministic ref that only its exact
// durable owner may resolve through ResolveOwnedWithMeta.
func (s *FileMediaStore) StoreIdempotentOwned(
	localPath string,
	meta MediaMeta,
	scope string,
	key string,
	owner MediaOwner,
) (string, error) {
	if err := owner.validate(); err != nil {
		return "", err
	}
	return s.storeIdempotent(localPath, meta, scope, key, &owner)
}

func (s *FileMediaStore) storeIdempotent(
	localPath string,
	meta MediaMeta,
	scope string,
	key string,
	owner *MediaOwner,
) (string, error) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if strings.TrimSpace(key) == "" || len(key) > 512 {
		return "", fmt.Errorf("media store: invalid idempotency key")
	}
	absPath, err := filepath.Abs(localPath)
	if err != nil {
		return "", fmt.Errorf("media store: resolve path %q: %w", localPath, err)
	}
	if _, err := os.Stat(absPath); err != nil {
		return "", fmt.Errorf("media store: %s: %w", absPath, err)
	}
	sum := sha256.Sum256([]byte(key))
	ref := "media://node-transfer-" + hex.EncodeToString(sum[:16])
	meta.CleanupPolicy = normalizeCleanupPolicy(meta.CleanupPolicy)
	entry := mediaEntry{
		path: absPath, meta: meta, storedAt: s.nowFunc(), owner: cloneMediaOwner(owner),
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, found := s.refs[ref]; found {
		if existing.path != entry.path ||
			existing.meta != entry.meta ||
			s.refToScope[ref] != scope ||
			!equalMediaOwner(existing.owner, entry.owner) {
			return "", fmt.Errorf("media store: idempotent ref conflicts with retained handoff")
		}
		return ref, nil
	}
	if err := s.persistLocked([]persistentMediaEntry{{
		Ref: ref, Path: entry.path, Meta: entry.meta, Scope: scope,
		StoredAt: entry.storedAt, Owner: cloneMediaOwner(entry.owner),
	}}, nil); err != nil {
		return "", err
	}
	s.addEntryLocked(ref, entry, scope)
	return ref, nil
}

// BindOwner durably binds an existing trusted ref. Ownership cannot be
// changed or broadened after the first successful binding.
func (s *FileMediaStore) BindOwner(ref string, owner MediaOwner) error {
	if err := owner.validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, found := s.refs[ref]
	if !found {
		return errors.New("media store: unknown ref")
	}
	if entry.owner != nil {
		if *entry.owner != owner {
			return errors.New("media store: owner conflict")
		}
		return nil
	}
	previous := entry
	entry.owner = cloneMediaOwner(&owner)
	s.refs[ref] = entry
	if err := s.persistLocked(nil, nil); err != nil {
		s.refs[ref] = previous
		return err
	}
	return nil
}

// ResolveOwnedWithMeta resolves a ref only for its immutable durable owner.
func (s *FileMediaStore) ResolveOwnedWithMeta(
	ref string,
	owner MediaOwner,
) (string, MediaMeta, error) {
	if err := owner.validate(); err != nil {
		return "", MediaMeta{}, err
	}
	s.mu.RLock()
	entry, found := s.refs[ref]
	owned := found && entry.owner != nil && *entry.owner == owner
	s.mu.RUnlock()
	if !owned {
		return "", MediaMeta{}, errors.New("media store: ref is not owned by this route")
	}
	return s.resolve(ref)
}

func cloneMediaOwner(owner *MediaOwner) *MediaOwner {
	if owner == nil {
		return nil
	}
	cloned := *owner
	return &cloned
}

func equalMediaOwner(left, right *MediaOwner) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
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
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
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

// CleanExpired removes all entries older than MaxAge and reclaims stale
// promotion files that were never committed to the persistent index.
// Phase 1 (under lock): identify expired entries and remove from maps.
// Phase 2 (no lock): delete store-managed files from disk to minimize lock contention.
func (s *FileMediaStore) CleanExpired() int {
	if s.cleanerCfg.MaxAge <= 0 {
		return 0
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

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
	if len(expired) > 0 {
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
	}
	retainedPaths := make(map[string]struct{}, len(s.refs))
	for _, entry := range s.refs {
		retainedPaths[entry.path] = struct{}{}
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

	orphans := s.cleanOrphanedPromotionFiles(cutoff, retainedPaths)
	return len(expired) + orphans
}

func (s *FileMediaStore) cleanOrphanedPromotionFiles(
	cutoff time.Time,
	retainedPaths map[string]struct{},
) int {
	if s.index == nil {
		return 0
	}
	managedDir := filepath.Join(filepath.Dir(s.index.path), "files")
	root, err := os.OpenRoot(managedDir)
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		logger.WarnCF("media", "cleanup: failed to open persistent media directory", map[string]any{
			"error": err.Error(),
		})
		return 0
	}
	defer root.Close()

	entries, err := os.ReadDir(managedDir)
	if err != nil {
		logger.WarnCF("media", "cleanup: failed to scan persistent media directory", map[string]any{
			"error": err.Error(),
		})
		return 0
	}
	removed := 0
	for _, entry := range entries {
		if !isPromotionFilename(entry.Name()) {
			continue
		}
		path := filepath.Join(managedDir, entry.Name())
		if _, retained := retainedPaths[path]; retained {
			continue
		}
		info, infoErr := root.Lstat(entry.Name())
		if infoErr != nil || !info.Mode().IsRegular() || !info.ModTime().Before(cutoff) {
			continue
		}
		if removeErr := root.Remove(entry.Name()); removeErr != nil {
			logger.WarnCF("media", "cleanup: failed to remove orphaned promotion", map[string]any{
				"path":  path,
				"error": removeErr.Error(),
			})
			continue
		}
		removed++
	}
	return removed
}

func isPromotionFilename(name string) bool {
	id := strings.TrimSuffix(name, filepath.Ext(name))
	_, err := uuid.Parse(id)
	return err == nil
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
		entries = append(entries, persistentMediaEntry{
			Ref: ref, Path: entry.path, Meta: entry.meta,
			Scope: s.refToScope[ref], StoredAt: entry.storedAt,
			Owner: cloneMediaOwner(entry.owner),
		})
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

		s.cleanerWG.Add(1)
		go func() {
			defer s.cleanerWG.Done()
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
	// Wait for an in-flight cleanup before another store opens the same durable
	// index. Otherwise the retired cleaner could overwrite newer state.
	s.cleanerWG.Wait()
}
