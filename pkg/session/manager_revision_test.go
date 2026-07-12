package session

import (
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/providers"
)

func TestSessionManagerHistoryRevisionIsMonotonic(t *testing.T) {
	manager := NewSessionManager("")
	const key = "legacy-revision"
	manager.GetOrCreate(key)

	manager.SetHistory(key, []providers.Message{{Role: "user", Content: "first"}})
	first, err := manager.GetHistoryRevision(key)
	if err != nil {
		t.Fatal(err)
	}

	// Hold Updated constant to prove the revision does not depend on wall time.
	fixedUpdated := time.Unix(123, 456)
	manager.mu.Lock()
	manager.sessions[key].Updated = fixedUpdated
	manager.mu.Unlock()
	manager.SetHistory(key, []providers.Message{{Role: "user", Content: "second"}})
	manager.mu.Lock()
	manager.sessions[key].Updated = fixedUpdated
	manager.mu.Unlock()

	second, err := manager.GetHistoryRevision(key)
	if err != nil {
		t.Fatal(err)
	}
	if second.Revision != first.Revision+1 {
		t.Fatalf("revision after same-tick rewrite = %d, want %d", second.Revision, first.Revision+1)
	}
	if second.Count != first.Count {
		t.Fatalf("test requires equal counts: first=%d second=%d", first.Count, second.Count)
	}
}

func TestSessionManagerHistoryRevisionPersists(t *testing.T) {
	dir := t.TempDir()
	const key = "persisted-revision"
	manager := NewSessionManager(dir)
	manager.GetOrCreate(key)
	manager.AddMessage(key, "user", "hello")
	want, err := manager.GetHistoryRevision(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Save(key); err != nil {
		t.Fatal(err)
	}

	reloaded := NewSessionManager(dir)
	got, err := reloaded.GetHistoryRevision(key)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("reloaded revision = %+v, want %+v", got, want)
	}
}
