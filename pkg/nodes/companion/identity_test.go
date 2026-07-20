package companion

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIdentityPersistsWithPrivatePermissions(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "node-state")
	first, err := LoadOrCreateIdentity(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	second, err := LoadOrCreateIdentity(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID || !first.PrivateKey.Equal(second.PrivateKey) {
		t.Fatal("reloaded node identity changed")
	}
	info, err := os.Stat(filepath.Join(stateDir, "identity.json"))
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("identity mode = %04o", got)
	}
	dirInfo, err := os.Stat(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	if got := dirInfo.Mode().Perm(); got != 0o700 {
		t.Fatalf("state directory mode = %04o", got)
	}
}

func TestIdentityRejectsPublicPermissions(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "node-state")
	if _, err := LoadOrCreateIdentity(stateDir); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(stateDir, "identity.json")
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreateIdentity(stateDir); err == nil {
		t.Fatal("LoadOrCreateIdentity() accepted public key-file permissions")
	}
}
