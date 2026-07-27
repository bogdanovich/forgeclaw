package wecom

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/config"
)

func TestDefaultReqIDStorePathHonorsHomeOverride(t *testing.T) {
	firstHome := t.TempDir()
	t.Setenv(config.EnvHome, firstHome)
	firstPath := defaultReqIDStorePath()
	if want := filepath.Join(firstHome, "wecom", "reqid-store.json"); firstPath != want {
		t.Fatalf("defaultReqIDStorePath() = %q, want %q", firstPath, want)
	}

	secondHome := t.TempDir()
	t.Setenv(config.EnvHome, secondHome)
	secondPath := defaultReqIDStorePath()
	if want := filepath.Join(secondHome, "wecom", "reqid-store.json"); secondPath != want {
		t.Fatalf("defaultReqIDStorePath() after override change = %q, want %q", secondPath, want)
	}
	if secondPath == firstPath {
		t.Fatalf("defaultReqIDStorePath() did not change with %s", config.EnvHome)
	}
}

func TestReqIDStorePersistsRoutes(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "reqids.json")
	store := newReqIDStore(storePath)
	if err := store.Put("chat-1", "req-1", 2, time.Hour); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	reloaded := newReqIDStore(storePath)
	route, ok := reloaded.Get("chat-1")
	if !ok {
		t.Fatal("expected persisted route to be loaded")
	}
	if route.ChatID != "chat-1" || route.ReqID != "req-1" || route.ChatType != 2 {
		t.Fatalf("loaded route = %+v", route)
	}
}
