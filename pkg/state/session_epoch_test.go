package state

import (
	"sync"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/session"
)

func TestResolveSessionEpochIdleRotatesAfterInactivity(t *testing.T) {
	manager := NewManager(t.TempDir())
	policy := session.LifecyclePolicy{
		Strategy:    session.LifecycleIdle,
		IdleTimeout: 30 * time.Minute,
	}
	start := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)

	first := mustResolveEpoch(t, manager, "route-1", policy, start)
	active := mustResolveEpoch(t, manager, "route-1", policy, start.Add(20*time.Minute))
	if active.ID != first.ID {
		t.Fatal("idle epoch rotated before the inactivity timeout")
	}
	rotated := mustResolveEpoch(t, manager, "route-1", policy, start.Add(51*time.Minute))
	if rotated.ID == first.ID {
		t.Fatal("idle epoch did not rotate after the inactivity timeout")
	}
}

func TestResolveSessionEpochConcurrentCallsShareCheckpoint(t *testing.T) {
	manager := NewManager(t.TempDir())
	policy := session.LifecyclePolicy{
		Strategy:    session.LifecycleIdle,
		IdleTimeout: time.Hour,
	}
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)

	const workers = 16
	ids := make(chan string, workers)
	errs := make(chan error, workers)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			epoch, err := manager.ResolveSessionEpoch("route-1", policy, now)
			if err != nil {
				errs <- err
				return
			}
			ids <- epoch.ID
		}()
	}
	group.Wait()
	close(ids)
	close(errs)

	for err := range errs {
		t.Fatalf("ResolveSessionEpoch() error = %v", err)
	}
	var first string
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatalf("concurrent epoch ID = %q, want %q", id, first)
		}
	}
}

func TestResolveSessionEpochMaxAgePersistsAcrossRestart(t *testing.T) {
	workspace := t.TempDir()
	policy := session.LifecyclePolicy{
		Strategy: session.LifecycleMaxAge,
		MaxAge:   time.Hour,
	}
	start := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	first := mustResolveEpoch(t, NewManager(workspace), "route-1", policy, start)

	reloaded := NewManager(workspace)
	beforeExpiry := mustResolveEpoch(t, reloaded, "route-1", policy, start.Add(59*time.Minute))
	if beforeExpiry.ID != first.ID {
		t.Fatal("persisted max-age epoch changed before expiry")
	}
	afterExpiry := mustResolveEpoch(t, reloaded, "route-1", policy, start.Add(time.Hour))
	if afterExpiry.ID == first.ID {
		t.Fatal("max-age epoch did not rotate at expiry")
	}
}

func TestResolveSessionEpochSeparatesRouteScopes(t *testing.T) {
	manager := NewManager(t.TempDir())
	policy := session.LifecyclePolicy{
		Strategy: session.LifecycleMaxAge,
		MaxAge:   time.Hour,
	}
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)

	first := mustResolveEpoch(t, manager, "route-1", policy, now)
	second := mustResolveEpoch(t, manager, "route-2", policy, now.Add(time.Second))
	if first.ID == second.ID {
		t.Fatal("different route scopes received the same stateful epoch ID")
	}
}

func TestTouchSessionEpochExtendsIdleCheckpointWithoutRotating(t *testing.T) {
	manager := NewManager(t.TempDir())
	policy := session.LifecyclePolicy{
		Strategy:    session.LifecycleIdle,
		IdleTimeout: 30 * time.Minute,
	}
	start := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	first := mustResolveEpoch(t, manager, "route-1", policy, start)
	if err := manager.TouchSessionEpoch("route-1", first.ID, start.Add(20*time.Minute)); err != nil {
		t.Fatalf("TouchSessionEpoch() error = %v", err)
	}

	active := mustResolveEpoch(t, manager, "route-1", policy, start.Add(40*time.Minute))
	if active.ID != first.ID {
		t.Fatal("idle epoch rotated relative to follow-up activity")
	}
	rotated := mustResolveEpoch(t, manager, "route-1", policy, start.Add(71*time.Minute))
	if rotated.ID == first.ID {
		t.Fatal("idle epoch did not rotate relative to follow-up activity")
	}
}

func mustResolveEpoch(
	t *testing.T,
	manager *Manager,
	routeScopeKey string,
	policy session.LifecyclePolicy,
	now time.Time,
) *session.SessionEpoch {
	t.Helper()
	epoch, err := manager.ResolveSessionEpoch(routeScopeKey, policy, now)
	if err != nil {
		t.Fatalf("ResolveSessionEpoch() error = %v", err)
	}
	if epoch == nil {
		t.Fatal("ResolveSessionEpoch() returned nil")
	}
	return epoch
}
