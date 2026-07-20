package nodes

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFileRegistryPersistsPendingPairingSecurely(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "private", "registry.json")
	registry, err := NewFileRegistry(path, 4)
	if err != nil {
		t.Fatal(err)
	}
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id, err := DeriveID(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	pairing := PendingPairing{
		Node: Snapshot{
			ID:               id,
			State:            StatePendingPairing,
			ProtocolVersion:  ProtocolV1,
			Platform:         "linux",
			Architecture:     "amd64",
			SoftwareVersion:  "v0.1.0",
			CatalogHash:      emptyCatalogHash(t),
			Catalog:          CapabilityCatalog{},
			LastSeenAt:       1000,
			DisconnectReason: "",
		},
		PublicKey:     publicKey,
		RequestedRole: "companion",
		RequestedAt:   1000,
	}
	if upsertErr := registry.UpsertPending(pairing); upsertErr != nil {
		t.Fatal(upsertErr)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("registry mode = %04o", got)
	}
	dirInfo, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if got := dirInfo.Mode().Perm(); got != 0o700 {
		t.Fatalf("registry directory mode = %04o", got)
	}

	reloaded, err := NewFileRegistry(path, 4)
	if err != nil {
		t.Fatal(err)
	}
	got, exists, err := reloaded.Pending(id)
	if err != nil || !exists {
		t.Fatalf("Pending() = exists %v, error %v", exists, err)
	}
	if got.RequestedAt != pairing.RequestedAt || got.Node.CatalogHash != pairing.Node.CatalogHash {
		t.Fatalf("reloaded pairing = %#v", got)
	}
}

func TestFileRegistryBoundsPendingPairings(t *testing.T) {
	registry, err := NewFileRegistry(filepath.Join(t.TempDir(), "registry.json"), 1)
	if err != nil {
		t.Fatal(err)
	}
	first := testPendingPairing(t, 1)
	second := testPendingPairing(t, 2)
	if upsertErr := registry.UpsertPending(first); upsertErr != nil {
		t.Fatal(upsertErr)
	}
	if upsertErr := registry.UpsertPending(second); !errors.Is(upsertErr, ErrAdmissionBusy) {
		t.Fatalf("second UpsertPending() error = %v", upsertErr)
	}
	first.Node.SoftwareVersion = "v0.2.0"
	originalRequestedAt := first.RequestedAt
	first.RequestedAt++
	if upsertErr := registry.UpsertPending(first); upsertErr != nil {
		t.Fatalf("refresh existing pending pairing: %v", upsertErr)
	}
	refreshed, exists, err := registry.Pending(first.Node.ID)
	if err != nil || !exists || refreshed.RequestedAt != originalRequestedAt {
		t.Fatalf("refreshed pending pairing = %#v, exists %v, error %v", refreshed, exists, err)
	}
}

func TestFileRegistryRejectsPendingIdentityMismatch(t *testing.T) {
	registry, err := NewFileRegistry(filepath.Join(t.TempDir(), "registry.json"), 4)
	if err != nil {
		t.Fatal(err)
	}
	pairing := testPendingPairing(t, 1)
	other := testPendingPairing(t, 2)
	pairing.PublicKey = other.PublicKey
	if err := registry.UpsertPending(pairing); !errors.Is(err, ErrInvalidNode) {
		t.Fatalf("UpsertPending() error = %v", err)
	}
}

func TestFileRegistryRejectsAliasCollision(t *testing.T) {
	registry, err := NewFileRegistry(filepath.Join(t.TempDir(), "registry.json"), 4)
	if err != nil {
		t.Fatal(err)
	}
	first := Snapshot{ID: "node_first", State: StateDisconnected, Aliases: []Alias{"vpn-box"}}
	second := Snapshot{ID: "node_second", State: StateDisconnected, Aliases: []Alias{"vpn-box"}}
	if err := registry.Upsert(first); err != nil {
		t.Fatal(err)
	}
	if err := registry.Upsert(second); !errors.Is(err, ErrInvalidNode) {
		t.Fatalf("second Upsert() error = %v", err)
	}
}

func TestFileRegistryRejectsCorruptDocument(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	if err := os.WriteFile(path, []byte(`{"version":1,"records":{"wrong":{}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFileRegistry(path, 4); err == nil {
		t.Fatal("NewFileRegistry() accepted corrupt document")
	}
}

func testPendingPairing(t *testing.T, timestamp int64) PendingPairing {
	t.Helper()
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id, err := DeriveID(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	return PendingPairing{
		Node: Snapshot{
			ID:              id,
			State:           StatePendingPairing,
			ProtocolVersion: ProtocolV1,
			CatalogHash:     emptyCatalogHash(t),
			Catalog:         CapabilityCatalog{},
			LastSeenAt:      timestamp,
		},
		PublicKey:     publicKey,
		RequestedRole: "companion",
		RequestedAt:   timestamp,
	}
}

func emptyCatalogHash(t *testing.T) string {
	t.Helper()
	hash, err := (CapabilityCatalog{}).Hash()
	if err != nil {
		t.Fatal(err)
	}
	return hash
}
