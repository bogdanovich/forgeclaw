package nodes

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
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

func TestFileRegistryRejectsOperatorMetadataOnPendingAdmission(t *testing.T) {
	registry, err := NewFileRegistry(filepath.Join(t.TempDir(), "registry.json"), 4)
	if err != nil {
		t.Fatal(err)
	}
	pairing := testPendingPairing(t, 1)
	pairing.Node.Aliases = []Alias{"reserved"}
	pairing.Node.DisplayName = "Trusted-looking node"
	if upsertErr := registry.UpsertPending(pairing); !errors.Is(upsertErr, ErrInvalidNode) {
		t.Fatalf("UpsertPending(operator metadata) error = %v", upsertErr)
	}
	if _, exists, resolveErr := registry.Resolve("reserved"); resolveErr != nil || exists {
		t.Fatalf("pending alias Resolve() = exists %v, error %v", exists, resolveErr)
	}
	if nodes, listErr := registry.List(Filter{}); listErr != nil || len(nodes) != 0 {
		t.Fatalf("List() = %#v, error %v", nodes, listErr)
	}
}

func TestFileRegistryApprovesOnlyAdvertisedCommands(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	registry, err := NewFileRegistry(path, 4)
	if err != nil {
		t.Fatal(err)
	}
	pairing := testPendingPairing(t, 1)
	pairing.Node.Catalog = testCatalog(t)
	pairing.Node.CatalogHash, err = pairing.Node.Catalog.Hash()
	if err != nil {
		t.Fatal(err)
	}
	if upsertErr := registry.UpsertPending(pairing); upsertErr != nil {
		t.Fatal(upsertErr)
	}

	if _, approveErr := registry.Approve(pairing.Node.ID, PairingApproval{
		AllowedCommands: []string{"system.exec.v1"},
		At:              2,
	}); !errors.Is(approveErr, ErrInvalidCapability) {
		t.Fatalf("Approve(unadvertised) error = %v", approveErr)
	}
	approved, err := registry.Approve(pairing.Node.ID, PairingApproval{
		Aliases:         []Alias{"vpn-box", "primary"},
		DisplayName:     " VPN box ",
		AllowedCommands: []string{"service.status.v1", "node.info.v1"},
		At:              3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if approved.Snapshot.State != StateDisconnected || approved.Snapshot.DisplayName != "VPN box" {
		t.Fatalf("approved snapshot = %#v", approved.Snapshot)
	}
	if got := approved.Snapshot.Aliases; len(got) != 2 || got[0] != "primary" || got[1] != "vpn-box" {
		t.Fatalf("approved aliases = %#v", got)
	}
	if got := approved.AllowedCommands; len(got) != 2 || got[0] != "node.info.v1" || got[1] != "service.status.v1" {
		t.Fatalf("approved commands = %#v", got)
	}
	if approved.ApprovedAt != 3 || approved.RequestedAt != pairing.RequestedAt {
		t.Fatalf("approved registration = %#v", approved)
	}

	reloaded, err := NewFileRegistry(path, 4)
	if err != nil {
		t.Fatal(err)
	}
	persisted, exists, err := reloaded.Registration(pairing.Node.ID)
	if err != nil || !exists {
		t.Fatalf("Registration() = exists %v, error %v", exists, err)
	}
	if persisted.ApprovedAt != 3 || len(persisted.AllowedCommands) != 2 {
		t.Fatalf("persisted registration = %#v", persisted)
	}
}

func TestFileRegistryDenyAndRevokeFailClosed(t *testing.T) {
	registry, err := NewFileRegistry(filepath.Join(t.TempDir(), "registry.json"), 4)
	if err != nil {
		t.Fatal(err)
	}
	denied := testPendingPairing(t, 1)
	if upsertErr := registry.UpsertPending(denied); upsertErr != nil {
		t.Fatal(upsertErr)
	}
	deniedRecord, err := registry.Deny(denied.Node.ID, Revocation{Reason: "operator denied pairing", At: 2})
	if err != nil {
		t.Fatal(err)
	}
	if deniedRecord.Snapshot.State != StateRevoked || deniedRecord.RevokedAt != 2 {
		t.Fatalf("denied registration = %#v", deniedRecord)
	}
	if upsertErr := registry.UpsertPending(denied); !errors.Is(upsertErr, ErrInvalidNode) {
		t.Fatalf("denied identity recreated pending pairing: %v", upsertErr)
	}

	paired := testPendingPairing(t, 3)
	if upsertErr := registry.UpsertPending(paired); upsertErr != nil {
		t.Fatal(upsertErr)
	}
	if _, approveErr := registry.Approve(paired.Node.ID, PairingApproval{At: 4}); approveErr != nil {
		t.Fatal(approveErr)
	}
	revoked, err := registry.Revoke(paired.Node.ID, Revocation{Reason: "device retired", At: 5})
	if err != nil {
		t.Fatal(err)
	}
	if revoked.Snapshot.State != StateRevoked || revoked.Snapshot.DisconnectReason != "device retired" {
		t.Fatalf("revoked registration = %#v", revoked)
	}
	if len(revoked.AllowedCommands) != 0 || revoked.RevokedAt != 5 {
		t.Fatalf("revoked authority = %#v", revoked)
	}
	if _, err := registry.Revoke(paired.Node.ID, Revocation{Reason: "again", At: 6}); !errors.Is(err, ErrInvalidNode) {
		t.Fatalf("second Revoke() error = %v", err)
	}
}

func TestFileRegistryRuntimeUpsertCannotBypassPairingOrRevocation(t *testing.T) {
	registry, err := NewFileRegistry(filepath.Join(t.TempDir(), "registry.json"), 4)
	if err != nil {
		t.Fatal(err)
	}
	pairing := testPendingPairing(t, 1)
	if upsertErr := registry.UpsertPending(pairing); upsertErr != nil {
		t.Fatal(upsertErr)
	}
	runtimeSnapshot := pairing.Node
	runtimeSnapshot.State = StateConnected
	unknown := testPendingPairing(t, 2).Node
	unknown.State = StateConnected
	if upsertErr := registry.Upsert(unknown); !errors.Is(upsertErr, ErrInvalidNode) {
		t.Fatalf("Upsert() created unknown runtime identity: %v", upsertErr)
	}
	if upsertErr := registry.Upsert(runtimeSnapshot); !errors.Is(upsertErr, ErrInvalidNode) {
		t.Fatalf("Upsert() bypassed pending approval: %v", upsertErr)
	}
	if _, denyErr := registry.Deny(pairing.Node.ID, Revocation{Reason: "denied", At: 2}); denyErr != nil {
		t.Fatal(denyErr)
	}
	if upsertErr := registry.Upsert(runtimeSnapshot); !errors.Is(upsertErr, ErrInvalidNode) {
		t.Fatalf("Upsert() restored revoked identity: %v", upsertErr)
	}
}

func TestFileRegistryKeepsApprovalWhenAdvertisedCatalogNarrows(t *testing.T) {
	registry, err := NewFileRegistry(filepath.Join(t.TempDir(), "registry.json"), 4)
	if err != nil {
		t.Fatal(err)
	}
	pairing := testPendingPairing(t, 1)
	pairing.Node.Catalog = testCatalog(t)
	pairing.Node.CatalogHash, err = pairing.Node.Catalog.Hash()
	if err != nil {
		t.Fatal(err)
	}
	if upsertErr := registry.UpsertPending(pairing); upsertErr != nil {
		t.Fatal(upsertErr)
	}
	approved, err := registry.Approve(pairing.Node.ID, PairingApproval{
		AllowedCommands: []string{"node.info.v1", "service.status.v1"},
		At:              2,
	})
	if err != nil {
		t.Fatal(err)
	}
	narrowed := approved.Snapshot
	narrowed.State = StateConnected
	narrowed.Catalog.Commands = narrowed.Catalog.Commands[:1]
	narrowed.CatalogHash, err = narrowed.Catalog.Hash()
	if err != nil {
		t.Fatal(err)
	}
	if upsertErr := registry.Upsert(narrowed); upsertErr != nil {
		t.Fatalf("Upsert(narrowed catalog) error = %v", upsertErr)
	}
	registration, _, err := registry.Registration(pairing.Node.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(registration.AllowedCommands) != 2 || len(registration.Snapshot.Catalog.Commands) != 1 {
		t.Fatalf("narrowed registration = %#v", registration)
	}
}

func TestFileRegistryRuntimeUpsertPreservesOperatorMetadata(t *testing.T) {
	registry, err := NewFileRegistry(filepath.Join(t.TempDir(), "registry.json"), 4)
	if err != nil {
		t.Fatal(err)
	}
	pairing := testPendingPairing(t, 1)
	if upsertErr := registry.UpsertPending(pairing); upsertErr != nil {
		t.Fatal(upsertErr)
	}
	approved, err := registry.Approve(pairing.Node.ID, PairingApproval{
		Aliases:     []Alias{"vpn-box"},
		DisplayName: "VPN box",
		At:          2,
	})
	if err != nil {
		t.Fatal(err)
	}
	runtimeSnapshot := approved.Snapshot
	runtimeSnapshot.State = StateConnected
	runtimeSnapshot.Aliases = []Alias{"runtime-alias"}
	runtimeSnapshot.DisplayName = "runtime name"
	runtimeSnapshot.SoftwareVersion = "v0.2.0"
	if upsertErr := registry.Upsert(runtimeSnapshot); upsertErr != nil {
		t.Fatal(upsertErr)
	}
	registration, _, err := registry.Registration(pairing.Node.ID)
	if err != nil {
		t.Fatal(err)
	}
	if registration.Snapshot.DisplayName != "VPN box" ||
		len(registration.Snapshot.Aliases) != 1 || registration.Snapshot.Aliases[0] != "vpn-box" {
		t.Fatalf("operator metadata overwritten: %#v", registration.Snapshot)
	}
	if registration.Snapshot.SoftwareVersion != "v0.2.0" || registration.Snapshot.State != StateConnected {
		t.Fatalf("runtime metadata not updated: %#v", registration.Snapshot)
	}

	runtimeSnapshot.State = StateRevoked
	if upsertErr := registry.Upsert(runtimeSnapshot); !errors.Is(upsertErr, ErrInvalidNode) {
		t.Fatalf("runtime Upsert changed operator-owned lifecycle: %v", upsertErr)
	}
}

func TestFileRegistryRegistrationReturnsCopies(t *testing.T) {
	registry, err := NewFileRegistry(filepath.Join(t.TempDir(), "registry.json"), 4)
	if err != nil {
		t.Fatal(err)
	}
	pairing := testPendingPairing(t, 1)
	pairing.Node.Catalog = testCatalog(t)
	pairing.Node.CatalogHash, err = pairing.Node.Catalog.Hash()
	if err != nil {
		t.Fatal(err)
	}
	if upsertErr := registry.UpsertPending(pairing); upsertErr != nil {
		t.Fatal(upsertErr)
	}
	if _, approveErr := registry.Approve(pairing.Node.ID, PairingApproval{
		Aliases:         []Alias{"vpn-box"},
		AllowedCommands: []string{"node.info.v1"},
		At:              2,
	}); approveErr != nil {
		t.Fatal(approveErr)
	}
	first, _, err := registry.Registration(pairing.Node.ID)
	if err != nil {
		t.Fatal(err)
	}
	first.PublicKey[0] ^= 0xff
	first.Snapshot.Aliases[0] = "changed"
	first.AllowedCommands[0] = "changed"
	second, _, err := registry.Registration(pairing.Node.ID)
	if err != nil {
		t.Fatal(err)
	}
	if second.Snapshot.Aliases[0] != "vpn-box" || second.AllowedCommands[0] != "node.info.v1" ||
		second.PublicKey[0] != pairing.PublicKey[0] {
		t.Fatalf("registry slices were mutated: %#v", second)
	}
}

func TestFileRegistrySynchronizesIndependentInstances(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	first, err := NewFileRegistry(path, 4)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewFileRegistry(path, 4)
	if err != nil {
		t.Fatal(err)
	}
	pairing := testPendingPairing(t, 1)
	if upsertErr := first.UpsertPending(pairing); upsertErr != nil {
		t.Fatal(upsertErr)
	}
	pending, exists, pendingErr := second.Pending(pairing.Node.ID)
	if pendingErr != nil || !exists || pending.Node.ID != pairing.Node.ID {
		t.Fatalf("second Pending() = %#v, exists %v, error %v", pending, exists, pendingErr)
	}
	approved, err := second.Approve(pairing.Node.ID, PairingApproval{
		Aliases: []Alias{"vpn-box"},
		At:      2,
	})
	if err != nil {
		t.Fatal(err)
	}
	connected := approved.Snapshot
	connected.State = StateConnected
	connected.LastSeenAt = 3
	if upsertErr := first.Upsert(connected); upsertErr != nil {
		t.Fatal(upsertErr)
	}
	observed, exists, err := second.Registration(pairing.Node.ID)
	if err != nil || !exists {
		t.Fatalf("second Registration() = exists %v, error %v", exists, err)
	}
	if observed.Snapshot.State != StateConnected || observed.ApprovedAt != 2 ||
		len(observed.Snapshot.Aliases) != 1 || observed.Snapshot.Aliases[0] != "vpn-box" {
		t.Fatalf("observed registration = %#v", observed)
	}
	if _, revokeErr := first.Revoke(pairing.Node.ID, Revocation{Reason: "retired", At: 4}); revokeErr != nil {
		t.Fatal(revokeErr)
	}
	connected.LastSeenAt = 5
	if upsertErr := second.Upsert(connected); !errors.Is(upsertErr, ErrInvalidNode) {
		t.Fatalf("stale second instance restored revoked node: %v", upsertErr)
	}
	final, _, err := first.Registration(pairing.Node.ID)
	if err != nil || final.Snapshot.State != StateRevoked {
		t.Fatalf("final registration = %#v, error %v", final, err)
	}
}

func TestFileRegistryRejectsAliasCollision(t *testing.T) {
	registry, err := NewFileRegistry(filepath.Join(t.TempDir(), "registry.json"), 4)
	if err != nil {
		t.Fatal(err)
	}
	first := testPendingPairing(t, 1)
	second := testPendingPairing(t, 2)
	if upsertErr := registry.UpsertPending(first); upsertErr != nil {
		t.Fatal(upsertErr)
	}
	_, approveErr := registry.Approve(
		first.Node.ID,
		PairingApproval{Aliases: []Alias{"vpn-box"}, At: 1},
	)
	if approveErr != nil {
		t.Fatal(approveErr)
	}
	if upsertErr := registry.UpsertPending(second); upsertErr != nil {
		t.Fatal(upsertErr)
	}
	_, approveErr = registry.Approve(
		second.Node.ID,
		PairingApproval{Aliases: []Alias{"vpn-box"}, At: 2},
	)
	if !errors.Is(approveErr, ErrInvalidNode) {
		t.Fatalf("second Approve() error = %v", approveErr)
	}
}

func TestFileRegistryRejectsAliasAndNodeIDCollisions(t *testing.T) {
	tests := []struct {
		name              string
		aliasOnFirst      bool
		aliasOnSecond     bool
		collisionOnUpsert bool
	}{
		{
			name:          "alias added after node id",
			aliasOnSecond: true,
		},
		{
			name:              "node id added after alias",
			aliasOnFirst:      true,
			collisionOnUpsert: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry, err := NewFileRegistry(filepath.Join(t.TempDir(), "registry.json"), 4)
			if err != nil {
				t.Fatal(err)
			}
			first := testPendingPairing(t, 1)
			second := testPendingPairing(t, 2)
			firstAliases := []Alias(nil)
			secondAliases := []Alias(nil)
			if test.aliasOnFirst {
				firstAliases = []Alias{Alias(second.Node.ID)}
			}
			if test.aliasOnSecond {
				secondAliases = []Alias{Alias(first.Node.ID)}
			}
			if upsertErr := registry.UpsertPending(first); upsertErr != nil {
				t.Fatal(upsertErr)
			}
			_, approveErr := registry.Approve(
				first.Node.ID,
				PairingApproval{Aliases: firstAliases, At: 1},
			)
			if approveErr != nil {
				t.Fatal(approveErr)
			}
			upsertErr := registry.UpsertPending(second)
			if test.collisionOnUpsert {
				if !errors.Is(upsertErr, ErrInvalidNode) {
					t.Fatalf("second UpsertPending() error = %v", upsertErr)
				}
				return
			}
			if upsertErr != nil {
				t.Fatal(upsertErr)
			}
			_, approveErr = registry.Approve(
				second.Node.ID,
				PairingApproval{Aliases: secondAliases, At: 2},
			)
			if !errors.Is(approveErr, ErrInvalidNode) {
				t.Fatalf("second Approve() error = %v", approveErr)
			}
		})
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

func testCatalog(t *testing.T) CapabilityCatalog {
	t.Helper()
	objectSchema := json.RawMessage(`{"type":"object","additionalProperties":false}`)
	catalog := CapabilityCatalog{Commands: []CommandDescriptor{
		{
			Name:         "node.info.v1",
			InputSchema:  objectSchema,
			OutputSchema: objectSchema,
			Risk:         RiskRead,
		},
		{
			Name:         "service.status.v1",
			InputSchema:  objectSchema,
			OutputSchema: objectSchema,
			Risk:         RiskRead,
		},
	}}
	if err := catalog.Validate(); err != nil {
		t.Fatal(err)
	}
	return catalog
}
