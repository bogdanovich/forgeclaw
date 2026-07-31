//go:build linux

package companion

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFileHelperConfigRequiresRootOwnedProtectedFile(t *testing.T) {
	base := authorityBrokerServiceTestDir(t)
	root := base
	config := FileHelperServiceConfig{
		SocketPath: filepath.Join(base, "helper.sock"),
		StateDir:   filepath.Join(base, "state"),
		AllowedUID: 12345,
		AllowedGID: 12345,
		Profiles: FilePolicies{
			"server-admin": {
				Enabled:        true,
				Revision:       "server-admin-v1",
				ReadableRoots:  []string{root},
				WritableRoots:  []string{root},
				AllowCreate:    true,
				AllowOverwrite: true,
				Approval: FileApprovalPolicy{
					Read:  FileApprovalRequired,
					Write: FileApprovalRequired,
				},
			},
		},
	}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(base, "helper.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if os.Geteuid() != 0 {
		if _, err := LoadFileHelperServiceConfig(path); err == nil {
			t.Fatal("non-root-owned helper config was accepted")
		}
		return
	}
	loaded, err := LoadFileHelperServiceConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.normalized || len(loaded.Profiles) != 1 {
		t.Fatalf("loaded helper config = %#v", loaded)
	}
	if err := os.Chmod(path, 0o620); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFileHelperServiceConfig(path); err == nil {
		t.Fatal("group-writable helper config was accepted")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"allowed_uid":1,"allowed_uid":2}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFileHelperServiceConfig(path); err == nil {
		t.Fatal("helper config with duplicate fields was accepted")
	}
}

func TestRunFileHelperRequiresRoot(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("unprivileged rejection requires a non-root test process")
	}
	if err := RunFileHelper(context.Background(), FileHelperServiceConfig{}); err == nil {
		t.Fatal("unprivileged helper start was accepted")
	}
}
