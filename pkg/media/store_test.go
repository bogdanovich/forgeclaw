package media

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bogdanovich/mintclaw/pkg/fileutil"
)

func createTempFile(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("test content"), 0o644); err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	return path
}

func TestStoreAndResolve(t *testing.T) {
	dir := t.TempDir()
	store := NewFileMediaStore()

	path := createTempFile(t, dir, "photo.jpg")

	ref, err := store.Store(path, MediaMeta{Filename: "photo.jpg", Source: "telegram"}, "scope1")
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	if !strings.HasPrefix(ref, "media://") {
		t.Errorf("ref should start with media://, got %q", ref)
	}

	resolved, err := store.Resolve(ref)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if resolved != path {
		t.Errorf("Resolve returned %q, want %q", resolved, path)
	}
}

func TestReleaseAll(t *testing.T) {
	dir := t.TempDir()
	store := NewFileMediaStore()

	paths := make([]string, 3)
	refs := make([]string, 3)
	for i := range 3 {
		paths[i] = createTempFile(t, dir, strings.Repeat("a", i+1)+".jpg")
		var err error
		refs[i], err = store.Store(paths[i], MediaMeta{Source: "test"}, "scope1")
		if err != nil {
			t.Fatalf("Store failed: %v", err)
		}
	}

	if err := store.ReleaseAll("scope1"); err != nil {
		t.Fatalf("ReleaseAll failed: %v", err)
	}

	// Files should be deleted
	for _, p := range paths {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("file %q should have been deleted", p)
		}
	}

	// Refs should be unresolvable
	for _, ref := range refs {
		if _, err := store.Resolve(ref); err == nil {
			t.Errorf("Resolve(%q) should fail after ReleaseAll", ref)
		}
	}
}

func TestReleaseAllForgetOnlyKeepsFile(t *testing.T) {
	dir := t.TempDir()
	store := NewFileMediaStore()

	path := createTempFile(t, dir, "workspace.txt")
	ref, err := store.Store(path, MediaMeta{
		Source:        "test",
		CleanupPolicy: CleanupPolicyForgetOnly,
	}, "scope1")
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	if err := store.ReleaseAll("scope1"); err != nil {
		t.Fatalf("ReleaseAll failed: %v", err)
	}

	if _, err := store.Resolve(ref); err == nil {
		t.Error("forget-only ref should be unresolvable after release")
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("forget-only file should remain on disk: %v", err)
	}
}

func TestReleaseAllSharedPathDeletesOnFinalRefOnly(t *testing.T) {
	dir := t.TempDir()
	store := NewFileMediaStore()

	path := createTempFile(t, dir, "shared.jpg")
	refA, err := store.Store(path, MediaMeta{
		Source:        "test",
		CleanupPolicy: CleanupPolicyDeleteOnCleanup,
	}, "scopeA")
	if err != nil {
		t.Fatalf("Store(scopeA) failed: %v", err)
	}
	refB, err := store.Store(path, MediaMeta{
		Source:        "test",
		CleanupPolicy: CleanupPolicyDeleteOnCleanup,
	}, "scopeB")
	if err != nil {
		t.Fatalf("Store(scopeB) failed: %v", err)
	}

	if err := store.ReleaseAll("scopeA"); err != nil {
		t.Fatalf("ReleaseAll(scopeA) failed: %v", err)
	}

	if _, err := store.Resolve(refA); err == nil {
		t.Error("refA should be unresolvable after ReleaseAll(scopeA)")
	}
	if _, err := store.Resolve(refB); err != nil {
		t.Fatalf("refB should still resolve: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("shared file should remain until final ref is released: %v", err)
	}

	if err := store.ReleaseAll("scopeB"); err != nil {
		t.Fatalf("ReleaseAll(scopeB) failed: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("shared file should be deleted after final ref is released")
	}
}

func TestReleaseAllMixedPoliciesKeepsFile(t *testing.T) {
	dir := t.TempDir()
	store := NewFileMediaStore()

	path := createTempFile(t, dir, "shared.txt")
	if _, err := store.Store(path, MediaMeta{
		Source:        "test",
		CleanupPolicy: CleanupPolicyDeleteOnCleanup,
	}, "owned"); err != nil {
		t.Fatalf("Store(owned) failed: %v", err)
	}
	if _, err := store.Store(path, MediaMeta{
		Source:        "test",
		CleanupPolicy: CleanupPolicyForgetOnly,
	}, "borrowed"); err != nil {
		t.Fatalf("Store(borrowed) failed: %v", err)
	}

	if err := store.ReleaseAll("owned"); err != nil {
		t.Fatalf("ReleaseAll(owned) failed: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("mixed-policy file should remain after owned ref release: %v", err)
	}

	if err := store.ReleaseAll("borrowed"); err != nil {
		t.Fatalf("ReleaseAll(borrowed) failed: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("mixed-policy path should not be auto-deleted: %v", err)
	}
}

func TestMultiScopeIsolation(t *testing.T) {
	dir := t.TempDir()
	store := NewFileMediaStore()

	pathA := createTempFile(t, dir, "fileA.jpg")
	pathB := createTempFile(t, dir, "fileB.jpg")

	refA, _ := store.Store(pathA, MediaMeta{Source: "test"}, "scopeA")
	refB, _ := store.Store(pathB, MediaMeta{Source: "test"}, "scopeB")

	// Release only scopeA
	if err := store.ReleaseAll("scopeA"); err != nil {
		t.Fatalf("ReleaseAll(scopeA) failed: %v", err)
	}

	// scopeA file should be gone
	if _, err := os.Stat(pathA); !os.IsNotExist(err) {
		t.Error("file A should have been deleted")
	}
	if _, err := store.Resolve(refA); err == nil {
		t.Error("refA should be unresolvable after release")
	}

	// scopeB file should still exist
	if _, err := os.Stat(pathB); err != nil {
		t.Error("file B should still exist")
	}
	resolved, err := store.Resolve(refB)
	if err != nil {
		t.Fatalf("refB should still resolve: %v", err)
	}
	if resolved != pathB {
		t.Errorf("resolved %q, want %q", resolved, pathB)
	}
}

func TestReleaseAllIdempotent(t *testing.T) {
	store := NewFileMediaStore()

	// ReleaseAll on non-existent scope should not error
	if err := store.ReleaseAll("nonexistent"); err != nil {
		t.Fatalf("ReleaseAll on empty scope should not error: %v", err)
	}

	// Create and release, then release again
	dir := t.TempDir()
	path := createTempFile(t, dir, "file.jpg")
	_, _ = store.Store(path, MediaMeta{Source: "test"}, "scope1")

	if err := store.ReleaseAll("scope1"); err != nil {
		t.Fatalf("first ReleaseAll failed: %v", err)
	}
	if err := store.ReleaseAll("scope1"); err != nil {
		t.Fatalf("second ReleaseAll should not error: %v", err)
	}
}

func TestReleaseAllCleansMappingsIfRefsMissing(t *testing.T) {
	dir := t.TempDir()
	store := NewFileMediaStore()

	path := createTempFile(t, dir, "file.jpg")
	ref, err := store.Store(path, MediaMeta{Source: "test"}, "scope1")
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	// Simulate internal inconsistency: scopeToRefs/refToScope contains ref but refs map doesn't.
	store.mu.Lock()
	delete(store.refs, ref)
	store.mu.Unlock()

	if err := store.ReleaseAll("scope1"); err != nil {
		t.Fatalf("ReleaseAll failed: %v", err)
	}

	// ReleaseAll should still clean mappings (even if it can't delete the file without the path).
	store.mu.RLock()
	defer store.mu.RUnlock()
	if _, ok := store.refToScope[ref]; ok {
		t.Error("refToScope should not contain ref after ReleaseAll")
	}
	if _, ok := store.scopeToRefs["scope1"]; ok {
		t.Error("scopeToRefs should not contain scope1 after ReleaseAll")
	}
}

func TestStoreNonexistentFile(t *testing.T) {
	store := NewFileMediaStore()

	_, err := store.Store("/nonexistent/path/file.jpg", MediaMeta{Source: "test"}, "scope1")
	if err == nil {
		t.Error("Store should fail for nonexistent file")
	}
	// Error message should include the underlying os error, not just "file does not exist"
	if !strings.Contains(err.Error(), "no such file or directory") &&
		!strings.Contains(err.Error(), "cannot find") {
		t.Errorf("Error should contain OS error detail, got: %v", err)
	}
}

func TestResolveWithMeta(t *testing.T) {
	dir := t.TempDir()
	store := NewFileMediaStore()

	path := createTempFile(t, dir, "image.png")
	meta := MediaMeta{
		Filename:    "image.png",
		ContentType: "image/png",
		Source:      "telegram",
	}

	ref, err := store.Store(path, meta, "scope1")
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	resolvedPath, resolvedMeta, err := store.ResolveWithMeta(ref)
	if err != nil {
		t.Fatalf("ResolveWithMeta failed: %v", err)
	}
	if resolvedPath != path {
		t.Errorf("ResolveWithMeta path = %q, want %q", resolvedPath, path)
	}
	if resolvedMeta.Filename != meta.Filename {
		t.Errorf("ResolveWithMeta Filename = %q, want %q", resolvedMeta.Filename, meta.Filename)
	}
	if resolvedMeta.ContentType != meta.ContentType {
		t.Errorf("ResolveWithMeta ContentType = %q, want %q", resolvedMeta.ContentType, meta.ContentType)
	}
	if resolvedMeta.Source != meta.Source {
		t.Errorf("ResolveWithMeta Source = %q, want %q", resolvedMeta.Source, meta.Source)
	}

	// Unknown ref should fail
	_, _, err = store.ResolveWithMeta("media://nonexistent")
	if err == nil {
		t.Error("ResolveWithMeta should fail for unknown ref")
	}
}

func TestConcurrentSafety(t *testing.T) {
	dir := t.TempDir()
	store := NewFileMediaStore()

	const goroutines = 20
	const filesPerGoroutine = 5

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := range goroutines {
		go func(gIdx int) {
			defer wg.Done()
			scope := strings.Repeat("s", gIdx+1)

			for i := range filesPerGoroutine {
				path := createTempFile(t, dir, strings.Repeat("f", gIdx*filesPerGoroutine+i+1)+".tmp")
				ref, err := store.Store(path, MediaMeta{Source: "test"}, scope)
				if err != nil {
					t.Errorf("Store failed: %v", err)
					return
				}

				if _, err := store.Resolve(ref); err != nil {
					t.Errorf("Resolve failed: %v", err)
				}
			}

			if err := store.ReleaseAll(scope); err != nil {
				t.Errorf("ReleaseAll failed: %v", err)
			}
		}(g)
	}

	wg.Wait()
}

// --- TTL cleanup tests ---

func newTestStoreWithCleanup(maxAge time.Duration) *FileMediaStore {
	s := NewFileMediaStoreWithCleanup(MediaCleanerConfig{
		Enabled:  true,
		MaxAge:   maxAge,
		Interval: time.Hour, // won't tick in tests
	})
	return s
}

func TestCleanExpiredRemovesOldEntries(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	store := newTestStoreWithCleanup(10 * time.Minute)
	store.nowFunc = func() time.Time { return now.Add(-20 * time.Minute) }

	path := createTempFile(t, dir, "old.jpg")
	ref, err := store.Store(path, MediaMeta{Source: "test"}, "scope1")
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	// Advance clock to present
	store.nowFunc = func() time.Time { return now }
	removed := store.CleanExpired()

	if removed != 1 {
		t.Errorf("expected 1 removed, got %d", removed)
	}
	if _, err := store.Resolve(ref); err == nil {
		t.Error("expired ref should be unresolvable")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("expired file should be deleted")
	}
}

func TestCleanExpiredForgetOnlyKeepsFile(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	store := newTestStoreWithCleanup(10 * time.Minute)
	store.nowFunc = func() time.Time { return now.Add(-20 * time.Minute) }

	path := createTempFile(t, dir, "workspace.txt")
	ref, err := store.Store(path, MediaMeta{
		Source:        "test",
		CleanupPolicy: CleanupPolicyForgetOnly,
	}, "scope1")
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	store.nowFunc = func() time.Time { return now }
	removed := store.CleanExpired()

	if removed != 1 {
		t.Errorf("expected 1 removed, got %d", removed)
	}
	if _, err := store.Resolve(ref); err == nil {
		t.Error("expired forget-only ref should be unresolvable")
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("forget-only file should remain on disk: %v", err)
	}
}

func TestCleanExpiredKeepsNonExpired(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	store := newTestStoreWithCleanup(10 * time.Minute)
	store.nowFunc = func() time.Time { return now }

	path := createTempFile(t, dir, "fresh.jpg")
	ref, err := store.Store(path, MediaMeta{Source: "test"}, "scope1")
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	removed := store.CleanExpired()
	if removed != 0 {
		t.Errorf("expected 0 removed, got %d", removed)
	}

	if _, err := store.Resolve(ref); err != nil {
		t.Errorf("fresh ref should still resolve: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Error("fresh file should still exist")
	}
}

func TestCleanExpiredMixedAges(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	store := newTestStoreWithCleanup(10 * time.Minute)

	// Store old entry
	store.nowFunc = func() time.Time { return now.Add(-20 * time.Minute) }
	oldPath := createTempFile(t, dir, "old.jpg")
	oldRef, _ := store.Store(oldPath, MediaMeta{Source: "test"}, "scope1")

	// Store fresh entry
	store.nowFunc = func() time.Time { return now }
	freshPath := createTempFile(t, dir, "fresh.jpg")
	freshRef, _ := store.Store(freshPath, MediaMeta{Source: "test"}, "scope1")

	removed := store.CleanExpired()
	if removed != 1 {
		t.Errorf("expected 1 removed, got %d", removed)
	}

	if _, err := store.Resolve(oldRef); err == nil {
		t.Error("old ref should be gone")
	}
	if _, err := store.Resolve(freshRef); err != nil {
		t.Errorf("fresh ref should still resolve: %v", err)
	}
}

func TestCleanExpiredSharedPathDeletesOnFinalRefOnly(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	store := newTestStoreWithCleanup(10 * time.Minute)

	path := createTempFile(t, dir, "shared.jpg")

	store.nowFunc = func() time.Time { return now.Add(-20 * time.Minute) }
	oldRef, err := store.Store(path, MediaMeta{
		Source:        "test",
		CleanupPolicy: CleanupPolicyDeleteOnCleanup,
	}, "scope-old")
	if err != nil {
		t.Fatalf("Store(old) failed: %v", err)
	}

	store.nowFunc = func() time.Time { return now }
	freshRef, err := store.Store(path, MediaMeta{
		Source:        "test",
		CleanupPolicy: CleanupPolicyDeleteOnCleanup,
	}, "scope-fresh")
	if err != nil {
		t.Fatalf("Store(fresh) failed: %v", err)
	}

	removed := store.CleanExpired()
	if removed != 1 {
		t.Errorf("expected 1 removed, got %d", removed)
	}
	if _, err := store.Resolve(oldRef); err == nil {
		t.Error("old ref should be gone after cleanup")
	}
	if _, err := store.Resolve(freshRef); err != nil {
		t.Fatalf("fresh ref should still resolve: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("shared file should remain while fresh ref exists: %v", err)
	}

	if err := store.ReleaseAll("scope-fresh"); err != nil {
		t.Fatalf("ReleaseAll(scope-fresh) failed: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("shared file should be deleted after final ref is released")
	}
}

func TestCleanExpiredCleansEmptyScopes(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	store := newTestStoreWithCleanup(10 * time.Minute)

	// Store old entry as the only one in scope
	store.nowFunc = func() time.Time { return now.Add(-20 * time.Minute) }
	path := createTempFile(t, dir, "only.jpg")
	store.Store(path, MediaMeta{Source: "test"}, "lonely_scope")

	store.nowFunc = func() time.Time { return now }
	store.CleanExpired()

	store.mu.RLock()
	defer store.mu.RUnlock()
	if _, ok := store.scopeToRefs["lonely_scope"]; ok {
		t.Error("empty scope should be cleaned up")
	}
}

func TestStartStopLifecycle(t *testing.T) {
	store := NewFileMediaStoreWithCleanup(MediaCleanerConfig{
		Enabled:  true,
		MaxAge:   time.Minute,
		Interval: 50 * time.Millisecond,
	})

	// Start and stop should not panic
	store.Start()
	// Double start should not spawn a second goroutine
	store.Start()
	time.Sleep(100 * time.Millisecond)
	store.Stop()

	// Double stop should not panic
	store.Stop()
}

func TestStopWaitsForInFlightCleanup(t *testing.T) {
	store := NewFileMediaStoreWithCleanup(MediaCleanerConfig{
		Enabled:  true,
		MaxAge:   time.Minute,
		Interval: time.Millisecond,
	})
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	store.nowFunc = func() time.Time {
		enteredOnce.Do(func() { close(entered) })
		<-release
		return time.Now()
	}
	store.Start()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not start")
	}

	stopped := make(chan struct{})
	go func() {
		store.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("Stop returned before cleanup finished")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not wait for cleanup to finish")
	}
}

func TestCleanExpiredZeroMaxAge(t *testing.T) {
	store := NewFileMediaStoreWithCleanup(MediaCleanerConfig{
		Enabled:  true,
		MaxAge:   0,
		Interval: time.Hour,
	})

	dir := t.TempDir()
	path := createTempFile(t, dir, "file.jpg")
	ref, _ := store.Store(path, MediaMeta{Source: "test"}, "scope1")

	// Zero MaxAge should be a no-op
	removed := store.CleanExpired()
	if removed != 0 {
		t.Errorf("expected 0 removed with zero MaxAge, got %d", removed)
	}
	if _, err := store.Resolve(ref); err != nil {
		t.Errorf("ref should still resolve: %v", err)
	}
}

func TestStartDisabledIsNoop(t *testing.T) {
	store := NewFileMediaStoreWithCleanup(MediaCleanerConfig{
		Enabled:  false,
		MaxAge:   time.Minute,
		Interval: time.Minute,
	})
	// Should not start any goroutine or panic
	store.Start()
	store.Stop()
}

func TestStartZeroIntervalNoPanic(t *testing.T) {
	store := NewFileMediaStoreWithCleanup(MediaCleanerConfig{
		Enabled:  true,
		MaxAge:   time.Minute,
		Interval: 0,
	})
	// Zero interval should not panic (time.NewTicker panics on <= 0)
	store.Start()
	store.Stop()
}

func TestStartZeroMaxAgeNoPanic(t *testing.T) {
	store := NewFileMediaStoreWithCleanup(MediaCleanerConfig{
		Enabled:  true,
		MaxAge:   0,
		Interval: time.Minute,
	})
	store.Start()
	store.Stop()
}

func TestConcurrentCleanupSafety(t *testing.T) {
	dir := t.TempDir()
	store := newTestStoreWithCleanup(50 * time.Millisecond)
	store.nowFunc = time.Now

	const workers = 10
	const ops = 20
	var wg sync.WaitGroup
	wg.Add(workers * 4)

	// Store workers
	for w := range workers {
		go func(wIdx int) {
			defer wg.Done()
			scope := fmt.Sprintf("scope-%d", wIdx)
			for i := range ops {
				p := createTempFile(t, dir, fmt.Sprintf("w%d-f%d.tmp", wIdx, i))
				store.Store(p, MediaMeta{Source: "test"}, scope)
			}
		}(w)
	}

	// Resolve workers
	for range workers {
		go func() {
			defer wg.Done()
			for range ops {
				store.Resolve("media://nonexistent")
			}
		}()
	}

	// ReleaseAll workers
	for w := range workers {
		go func(wIdx int) {
			defer wg.Done()
			for range ops {
				store.ReleaseAll(fmt.Sprintf("scope-%d", wIdx))
			}
		}(w)
	}

	// CleanExpired workers
	for range workers {
		go func() {
			defer wg.Done()
			for range ops {
				store.CleanExpired()
			}
		}()
	}

	wg.Wait()
}

func TestRefToScopeConsistency(t *testing.T) {
	dir := t.TempDir()
	store := NewFileMediaStore()

	// Store entries in two scopes
	ref1, _ := store.Store(createTempFile(t, dir, "a.jpg"), MediaMeta{Source: "test"}, "s1")
	ref2, _ := store.Store(createTempFile(t, dir, "b.jpg"), MediaMeta{Source: "test"}, "s1")
	ref3, _ := store.Store(createTempFile(t, dir, "c.jpg"), MediaMeta{Source: "test"}, "s2")

	store.mu.RLock()
	checkRef := func(ref, expectedScope string) {
		t.Helper()
		if scope, ok := store.refToScope[ref]; !ok || scope != expectedScope {
			t.Errorf("refToScope[%s] = %q, want %q", ref, scope, expectedScope)
		}
	}
	checkRef(ref1, "s1")
	checkRef(ref2, "s1")
	checkRef(ref3, "s2")
	store.mu.RUnlock()

	// Release s1 and verify refToScope is cleaned
	store.ReleaseAll("s1")

	store.mu.RLock()
	defer store.mu.RUnlock()
	if _, ok := store.refToScope[ref1]; ok {
		t.Error("refToScope should not contain ref1 after ReleaseAll")
	}
	if _, ok := store.refToScope[ref2]; ok {
		t.Error("refToScope should not contain ref2 after ReleaseAll")
	}
	if _, ok := store.refToScope[ref3]; !ok {
		t.Error("refToScope should still contain ref3")
	}
}

func TestPersistentIndexRecoversRefsAfterRestart(t *testing.T) {
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "workspace", "state", "media", "index.json")
	path := createTempFile(t, dir, "photo.jpg")
	meta := MediaMeta{Filename: "photo.jpg", ContentType: "image/jpeg", Source: "telegram"}

	store, err := NewFileMediaStoreWithPersistentIndex(indexPath, MediaCleanerConfig{})
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	ref, err := store.Store(path, meta, "chat:123")
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	meta.CleanupPolicy = CleanupPolicyDeleteOnCleanup

	restarted, err := NewFileMediaStoreWithPersistentIndex(indexPath, MediaCleanerConfig{})
	if err != nil {
		t.Fatalf("restart store: %v", err)
	}
	resolvedPath, resolvedMeta, err := restarted.ResolveWithMeta(ref)
	if err != nil {
		t.Fatalf("ResolveWithMeta after restart: %v", err)
	}
	if resolvedPath != path || resolvedMeta != meta {
		t.Fatalf("recovered entry = (%q, %#v), want (%q, %#v)", resolvedPath, resolvedMeta, path, meta)
	}
}

func TestPersistentIndexPromotesManagedTempFile(t *testing.T) {
	workspace := t.TempDir()
	indexPath := filepath.Join(workspace, "state", "media", "index.json")
	if err := os.MkdirAll(TempDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	sourceDir, err := os.MkdirTemp(TempDir(), "persistent-store-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sourceDir) })
	source := createTempFile(t, sourceDir, "photo.jpg")

	store, err := NewFileMediaStoreWithPersistentIndex(indexPath, MediaCleanerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if store.durableRoot != workspace {
		t.Fatalf("durable root = %q, want pre-existing workspace %q", store.durableRoot, workspace)
	}
	ref, err := store.Store(source, MediaMeta{Source: "telegram"}, "chat:123")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(source); !os.IsNotExist(err) {
		t.Fatalf("temporary source still exists: %v", err)
	}

	resolved, err := store.Resolve(ref)
	if err != nil {
		t.Fatal(err)
	}
	wantDir := filepath.Join(workspace, "state", "media", "files")
	if filepath.Dir(resolved) != wantDir {
		t.Fatalf("resolved path = %q, want directory %q", resolved, wantDir)
	}

	restarted, err := NewFileMediaStoreWithPersistentIndex(indexPath, MediaCleanerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := restarted.Resolve(ref); err != nil || got != resolved {
		t.Fatalf("Resolve after restart = (%q, %v), want (%q, nil)", got, err, resolved)
	}
}

func TestEnsureDurableDirectoryRetriesFromOriginalRoot(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "state", "media", "files")
	calls := 0
	mkdir := func(gotRoot, relative string, perm os.FileMode) error {
		calls++
		if gotRoot != root || relative != filepath.Join("state", "media", "files") || perm != 0o700 {
			t.Fatalf("durable mkdir args = (%q, %q, %v)", gotRoot, relative, perm)
		}
		if err := os.MkdirAll(filepath.Join(gotRoot, relative), perm); err != nil {
			return err
		}
		if calls == 1 {
			return &fileutil.CommittedWriteError{Err: errors.New("sync parent")}
		}
		return nil
	}

	if err := ensureDurableDirectory(root, target, mkdir); !fileutil.IsCommittedWriteError(err) {
		t.Fatalf("first ensure error = %v, want committed sync error", err)
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("target was not created before sync error: %v", err)
	}
	if err := ensureDurableDirectory(root, target, mkdir); err != nil {
		t.Fatalf("retry ensure: %v", err)
	}
	if calls != 2 {
		t.Fatalf("durable mkdir calls = %d, want retry despite existing target", calls)
	}
}

func TestPersistentIndexRejectsManagedTempSymlinkEscape(t *testing.T) {
	workspace := t.TempDir()
	indexPath := filepath.Join(workspace, "state", "media", "index.json")
	outside := t.TempDir()
	outsideFile := createTempFile(t, outside, "outside.jpg")

	if err := os.MkdirAll(TempDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(TempDir(), "persistent-store-link-"+uuid.NewString())
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(link) })

	store, err := NewFileMediaStoreWithPersistentIndex(indexPath, MediaCleanerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Store(
		filepath.Join(link, filepath.Base(outsideFile)),
		MediaMeta{Source: "telegram"},
		"chat:123",
	); err == nil || !strings.Contains(err.Error(), "contains symlink") {
		t.Fatalf("Store error = %v, want managed-temp symlink rejection", err)
	}
	if _, err := os.Stat(outsideFile); err != nil {
		t.Fatalf("outside file was changed: %v", err)
	}
}

func TestPersistentIndexCleanupReclaimsStalePromotionOrphan(t *testing.T) {
	workspace := t.TempDir()
	indexPath := filepath.Join(workspace, "state", "media", "index.json")
	managedDir := filepath.Join(filepath.Dir(indexPath), "files")
	if err := os.MkdirAll(managedDir, 0o700); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(managedDir, uuid.NewString()+".jpg")
	if err := os.WriteFile(orphan, []byte("orphan"), 0o600); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	old := now.Add(-2 * time.Hour)
	if err := os.Chtimes(orphan, old, old); err != nil {
		t.Fatal(err)
	}
	freshOrphan := filepath.Join(managedDir, uuid.NewString()+".jpg")
	if err := os.WriteFile(freshOrphan, []byte("in flight"), 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := NewFileMediaStoreWithPersistentIndex(
		indexPath,
		MediaCleanerConfig{MaxAge: time.Hour},
	)
	if err != nil {
		t.Fatal(err)
	}
	store.nowFunc = func() time.Time { return now }
	if removed := store.CleanExpired(); removed != 1 {
		t.Fatalf("CleanExpired removed = %d, want orphan", removed)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphan still exists: %v", err)
	}
	if _, err := os.Stat(freshOrphan); err != nil {
		t.Fatalf("fresh promotion candidate was removed: %v", err)
	}
}

func TestCleanExpiredSerializesSamePathRegistration(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	store := NewFileMediaStoreWithCleanup(MediaCleanerConfig{MaxAge: time.Minute})
	store.nowFunc = func() time.Time { return now.Add(-2 * time.Minute) }
	path := createTempFile(t, dir, "same-path.jpg")
	if _, err := store.Store(path, MediaMeta{Source: "telegram"}, "old"); err != nil {
		t.Fatal(err)
	}
	store.nowFunc = func() time.Time { return now }

	store.mu.Lock()
	cleanupDone := make(chan int, 1)
	go func() { cleanupDone <- store.CleanExpired() }()
	deadline := time.Now().Add(time.Second)
	for store.lifecycleMu.TryLock() {
		store.lifecycleMu.Unlock()
		if time.Now().After(deadline) {
			store.mu.Unlock()
			t.Fatal("cleanup did not acquire lifecycle lock")
		}
		runtime.Gosched()
	}

	registrationDone := make(chan error, 1)
	go func() {
		_, storeErr := store.Store(path, MediaMeta{
			Source:        "load_image",
			CleanupPolicy: CleanupPolicyForgetOnly,
		}, "new")
		registrationDone <- storeErr
	}()
	store.mu.Unlock()

	if removed := <-cleanupDone; removed != 1 {
		t.Fatalf("CleanExpired removed = %d, want 1", removed)
	}
	if err := <-registrationDone; err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("same-path Store error = %v, want missing source after serialized cleanup", err)
	}
}

func TestMediaPromotionDoesNotRemoveChangedSource(t *testing.T) {
	if err := os.MkdirAll(TempDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	sourceDir, err := os.MkdirTemp(TempDir(), "promotion-swap-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sourceDir) })
	source := createTempFile(t, sourceDir, "source.jpg")
	root, rel, file, info, err := openManagedTempFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	promotion := &mediaPromotion{sourceRoot: root, sourceRel: rel, sourceInfo: info}
	defer promotion.close()

	original := source + ".original"
	if err := os.Rename(source, original); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := promotion.removeSource(); err == nil || !strings.Contains(err.Error(), "changed") {
		t.Fatalf("removeSource error = %v, want changed-source rejection", err)
	}
	if got, err := os.ReadFile(source); err != nil || string(got) != "replacement" {
		t.Fatalf("replacement source = %q, %v; want preserved", got, err)
	}
}

func TestPersistentIndexRecoversIdempotentNodeTransferRef(t *testing.T) {
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "workspace", "state", "media", "index.json")
	path := createTempFile(t, dir, "download.bin")
	meta := MediaMeta{
		Filename:      "download.bin",
		ContentType:   "application/octet-stream",
		Source:        "tool:nodes_download",
		CleanupPolicy: CleanupPolicyDeleteOnCleanup,
	}
	store, err := NewFileMediaStoreWithPersistentIndex(indexPath, MediaCleanerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.StoreIdempotent(path, meta, "session-1", "delivery_1")
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewFileMediaStoreWithPersistentIndex(indexPath, MediaCleanerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := restarted.StoreIdempotent(path, meta, "session-1", "delivery_1")
	if err != nil || second != first {
		t.Fatalf("StoreIdempotent after restart = (%q, %v), want %q", second, err, first)
	}
	if _, err := restarted.StoreIdempotent(
		path,
		meta,
		"other-session",
		"delivery_1",
	); err == nil {
		t.Fatal("conflicting idempotent delivery scope was accepted")
	}
}

func TestPersistentMediaOwnerIsExactAndImmutable(t *testing.T) {
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "workspace", "state", "media", "index.json")
	path := createTempFile(t, dir, "owned.bin")
	store, err := NewFileMediaStoreWithPersistentIndex(indexPath, MediaCleanerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := store.Store(path, MediaMeta{Source: "telegram"}, "inbound")
	if err != nil {
		t.Fatal(err)
	}
	ownerA, err := NewMediaOwner(
		"/workspace/main", "main", "actor-a", "route-1", "telegram", "chat-1", "topic-1",
	)
	if err != nil {
		t.Fatal(err)
	}
	ownerB, err := NewMediaOwner(
		"/workspace/main", "main", "actor-b", "route-1", "telegram", "chat-1", "topic-1",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BindOwner(ref, ownerA); err != nil {
		t.Fatal(err)
	}
	if err := store.BindOwner(ref, ownerA); err != nil {
		t.Fatalf("idempotent owner bind: %v", err)
	}
	if err := store.BindOwner(ref, ownerB); err == nil {
		t.Fatal("owner rebinding was accepted")
	}
	if _, _, err := store.ResolveOwnedWithMeta(ref, ownerB); err == nil {
		t.Fatal("cross-actor owner resolved media")
	}

	restarted, err := NewFileMediaStoreWithPersistentIndex(indexPath, MediaCleanerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if resolved, _, err := restarted.ResolveOwnedWithMeta(ref, ownerA); err != nil || resolved != path {
		t.Fatalf("owned resolution after restart = (%q, %v), want %q", resolved, err, path)
	}
}

func TestPersistentIndexDropsMissingFilesDuringRecovery(t *testing.T) {
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "workspace", "state", "media", "index.json")
	path := createTempFile(t, dir, "expired.jpg")

	store, err := NewFileMediaStoreWithPersistentIndex(indexPath, MediaCleanerConfig{})
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	ref, err := store.Store(path, MediaMeta{Source: "telegram"}, "chat:123")
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if removeErr := os.Remove(path); removeErr != nil {
		t.Fatalf("remove media: %v", removeErr)
	}

	restarted, err := NewFileMediaStoreWithPersistentIndex(indexPath, MediaCleanerConfig{})
	if err != nil {
		t.Fatalf("restart store: %v", err)
	}
	if _, resolveErr := restarted.Resolve(ref); resolveErr == nil ||
		!strings.Contains(resolveErr.Error(), "unknown ref") {
		t.Fatalf("Resolve after missing-file recovery error = %v, want unknown ref", resolveErr)
	}
	entries, err := loadMediaIndex(indexPath)
	if err != nil {
		t.Fatalf("load index: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("index entries = %d, want 0", len(entries))
	}
}

func TestPersistentIndexResolvePrunesRemovedFile(t *testing.T) {
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "workspace", "state", "media", "index.json")
	path := createTempFile(t, dir, "removed.jpg")
	store, err := NewFileMediaStoreWithPersistentIndex(indexPath, MediaCleanerConfig{})
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	ref, err := store.Store(path, MediaMeta{Source: "telegram"}, "chat:123")
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if removeErr := os.Remove(path); removeErr != nil {
		t.Fatalf("remove media: %v", removeErr)
	}
	if _, resolveErr := store.Resolve(ref); resolveErr == nil ||
		!strings.Contains(resolveErr.Error(), "unavailable ref") {
		t.Fatalf("Resolve after removal error = %v, want unavailable ref", resolveErr)
	}

	restarted, err := NewFileMediaStoreWithPersistentIndex(indexPath, MediaCleanerConfig{})
	if err != nil {
		t.Fatalf("restart store: %v", err)
	}
	if _, err := restarted.Resolve(ref); err == nil || !strings.Contains(err.Error(), "unknown ref") {
		t.Fatalf("Resolve after restart error = %v, want unknown ref", err)
	}
}

func TestPersistentIndexCleanupSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "workspace", "state", "media", "index.json")
	now := time.Now()
	store, err := NewFileMediaStoreWithPersistentIndex(indexPath, MediaCleanerConfig{MaxAge: time.Minute})
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	store.nowFunc = func() time.Time { return now.Add(-2 * time.Minute) }
	path := createTempFile(t, dir, "old.jpg")
	ref, err := store.Store(path, MediaMeta{Source: "telegram"}, "chat:123")
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	store.nowFunc = func() time.Time { return now }
	if removed := store.CleanExpired(); removed != 1 {
		t.Fatalf("CleanExpired removed = %d, want 1", removed)
	}

	restarted, err := NewFileMediaStoreWithPersistentIndex(indexPath, MediaCleanerConfig{})
	if err != nil {
		t.Fatalf("restart store: %v", err)
	}
	if _, err := restarted.Resolve(ref); err == nil {
		t.Fatal("expired ref resolved after restart")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expired file still exists: %v", err)
	}
}

func TestPersistentIndexWorkspaceIsolation(t *testing.T) {
	dir := t.TempDir()
	pathA := createTempFile(t, dir, "a.jpg")
	pathB := createTempFile(t, dir, "b.jpg")
	workspaceAIndex := filepath.Join(dir, "workspace-a", "state", "media", "index.json")
	workspaceBIndex := filepath.Join(dir, "workspace-b", "state", "media", "index.json")
	storeA, err := NewFileMediaStoreWithPersistentIndex(workspaceAIndex, MediaCleanerConfig{})
	if err != nil {
		t.Fatalf("create store A: %v", err)
	}
	storeB, err := NewFileMediaStoreWithPersistentIndex(workspaceBIndex, MediaCleanerConfig{})
	if err != nil {
		t.Fatalf("create store B: %v", err)
	}
	refA, _ := storeA.Store(pathA, MediaMeta{Source: "a"}, "scope")
	refB, _ := storeB.Store(pathB, MediaMeta{Source: "b"}, "scope")

	restartedA, err := NewFileMediaStoreWithPersistentIndex(workspaceAIndex, MediaCleanerConfig{})
	if err != nil {
		t.Fatalf("restart store A: %v", err)
	}
	if _, err := restartedA.Resolve(refA); err != nil {
		t.Fatalf("workspace A did not recover its ref: %v", err)
	}
	if _, err := restartedA.Resolve(refB); err == nil {
		t.Fatal("workspace A recovered workspace B ref")
	}
}

func TestPersistentIndexPreservesSharedPathPolicyAfterRestart(t *testing.T) {
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "workspace", "state", "media", "index.json")
	path := createTempFile(t, dir, "shared.jpg")
	store, err := NewFileMediaStoreWithPersistentIndex(indexPath, MediaCleanerConfig{})
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	ownedRef, err := store.Store(path, MediaMeta{CleanupPolicy: CleanupPolicyDeleteOnCleanup}, "owned")
	if err != nil {
		t.Fatalf("Store owned: %v", err)
	}
	borrowedRef, err := store.Store(path, MediaMeta{CleanupPolicy: CleanupPolicyForgetOnly}, "borrowed")
	if err != nil {
		t.Fatalf("Store borrowed: %v", err)
	}

	restarted, err := NewFileMediaStoreWithPersistentIndex(indexPath, MediaCleanerConfig{})
	if err != nil {
		t.Fatalf("restart store: %v", err)
	}
	if releaseErr := restarted.ReleaseAll("owned"); releaseErr != nil {
		t.Fatalf("ReleaseAll owned: %v", releaseErr)
	}
	if releaseErr := restarted.ReleaseAll("borrowed"); releaseErr != nil {
		t.Fatalf("ReleaseAll borrowed: %v", releaseErr)
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("mixed-policy shared path was deleted after restart: %v", statErr)
	}
	finalStore, err := NewFileMediaStoreWithPersistentIndex(indexPath, MediaCleanerConfig{})
	if err != nil {
		t.Fatalf("restart after release: %v", err)
	}
	if _, err := finalStore.Resolve(ownedRef); err == nil {
		t.Fatal("released owned ref recovered after restart")
	}
	if _, err := finalStore.Resolve(borrowedRef); err == nil {
		t.Fatal("released borrowed ref recovered after restart")
	}
}
