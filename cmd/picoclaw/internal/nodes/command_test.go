package nodes

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/config"
	nodepkg "github.com/sipeed/picoclaw/pkg/nodes"
)

func TestNodesCommandApproveDescribeAndRevoke(t *testing.T) {
	configPath, workspace := writeTestConfig(t)
	pairing := writePendingPairing(t, workspace, 100)
	now := time.Unix(200, 0)

	approvedOutput := executeNodesCommand(t, configPath, now,
		"approve", string(pairing.Node.ID),
		"--alias", "vpn-box",
		"--display-name", "VPN box",
		"--allow-command", "node.info.v1",
		"--json",
	)
	var approved registrationView
	if err := json.Unmarshal(approvedOutput, &approved); err != nil {
		t.Fatal(err)
	}
	if approved.Node.State != nodepkg.StateDisconnected || approved.Node.DisplayName != "VPN box" {
		t.Fatalf("approved output = %#v", approved)
	}
	if len(approved.AllowedCommands) != 1 || approved.AllowedCommands[0] != "node.info.v1" {
		t.Fatalf("approved commands = %#v", approved.AllowedCommands)
	}
	if approved.PublicKeySHA256 == "" || approved.ApprovedAt != 200 {
		t.Fatalf("approved metadata = %#v", approved)
	}

	describedOutput := executeNodesCommand(t, configPath, now, "describe", "vpn-box", "--json")
	var described registrationView
	if err := json.Unmarshal(describedOutput, &described); err != nil {
		t.Fatal(err)
	}
	if described.Node.ID != pairing.Node.ID || described.PublicKeySHA256 != approved.PublicKeySHA256 {
		t.Fatalf("described output = %#v", described)
	}

	revokedOutput := executeNodesCommand(t, configPath, now,
		"revoke", "vpn-box", "--reason", "retired", "--json",
	)
	var revoked registrationView
	if err := json.Unmarshal(revokedOutput, &revoked); err != nil {
		t.Fatal(err)
	}
	if revoked.Node.State != nodepkg.StateRevoked || revoked.RevokedAt != 200 ||
		len(revoked.AllowedCommands) != 0 {
		t.Fatalf("revoked output = %#v", revoked)
	}

	registry, err := nodepkg.NewFileRegistry(nodepkg.RegistryPath(workspace), 4)
	if err != nil {
		t.Fatal(err)
	}
	persisted, exists, err := registry.Registration(pairing.Node.ID)
	if err != nil || !exists || persisted.Snapshot.DisconnectReason != "retired" {
		t.Fatalf("persisted registration = %#v, exists %v, error %v", persisted, exists, err)
	}
}

func TestNodesCommandDenyAndListByState(t *testing.T) {
	configPath, workspace := writeTestConfig(t)
	pairing := writePendingPairing(t, workspace, 100)
	now := time.Unix(200, 0)

	executeNodesCommand(t, configPath, now, "deny", string(pairing.Node.ID), "--json")
	output := executeNodesCommand(t, configPath, now, "list", "--state", "revoked", "--json")
	var snapshots []nodepkg.Snapshot
	if err := json.Unmarshal(output, &snapshots); err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 1 || snapshots[0].ID != pairing.Node.ID ||
		snapshots[0].State != nodepkg.StateRevoked {
		t.Fatalf("listed snapshots = %#v", snapshots)
	}
}

func TestNodesCommandRejectsInvalidStateFilter(t *testing.T) {
	configPath, _ := writeTestConfig(t)
	cmd := newNodesCommand(commandDeps{
		configPath: func() string { return configPath },
		now:        func() time.Time { return time.Unix(200, 0) },
	})
	cmd.SetArgs([]string{"list", "--state", "trusted"})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	if err := cmd.Execute(); err == nil {
		t.Fatal("list accepted invalid node state")
	}
}

func executeNodesCommand(
	t *testing.T,
	configPath string,
	now time.Time,
	args ...string,
) []byte {
	t.Helper()
	cmd := newNodesCommand(commandDeps{
		configPath: func() string { return configPath },
		now:        func() time.Time { return now },
	})
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs(append([]string{"--config", configPath}, args...))
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func writeTestConfig(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	workspace := filepath.Join(dir, "workspace")
	configPath := filepath.Join(dir, "config.json")
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = workspace
	cfg.Nodes.Enabled = true
	cfg.Nodes.MaxPendingPairings = 4
	if err := config.SaveConfig(configPath, cfg); err != nil {
		t.Fatal(err)
	}
	return configPath, workspace
}

func writePendingPairing(t *testing.T, workspace string, timestamp int64) nodepkg.PendingPairing {
	t.Helper()
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id, err := nodepkg.DeriveID(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	objectSchema := json.RawMessage(`{"type":"object","additionalProperties":false}`)
	catalog := nodepkg.CapabilityCatalog{Commands: []nodepkg.CommandDescriptor{
		{
			Name:         "node.info.v1",
			InputSchema:  objectSchema,
			OutputSchema: objectSchema,
			Risk:         nodepkg.RiskRead,
		},
	}}
	catalogHash, err := catalog.Hash()
	if err != nil {
		t.Fatal(err)
	}
	pairing := nodepkg.PendingPairing{
		Node: nodepkg.Snapshot{
			ID:              id,
			State:           nodepkg.StatePendingPairing,
			ProtocolVersion: nodepkg.ProtocolV1,
			CatalogHash:     catalogHash,
			Catalog:         catalog,
			LastSeenAt:      timestamp,
		},
		PublicKey:     publicKey,
		RequestedRole: "companion",
		RequestedAt:   timestamp,
	}
	registry, err := nodepkg.NewFileRegistry(nodepkg.RegistryPath(workspace), 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.UpsertPending(pairing); err != nil {
		t.Fatal(err)
	}
	return pairing
}
