//go:build linux

package companion

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAuthorityBrokerConfigRequiresRootOwnedProtectedFile(t *testing.T) {
	base := authorityBrokerServiceTestDir(t)
	path := filepath.Join(base, "broker.json")
	config := AuthorityBrokerConfig{
		SocketPath: filepath.Join(base, "broker.sock"),
		AllowedUID: 12345,
		AllowedGID: 12345,
		Revision:   "broker-v1",
		Profiles: map[string]AuthorityBrokerProfile{
			"owner-root": {
				Revision: "profile-v1", ShellPath: "/bin/sh",
				WorkingScopes:    map[string]string{"workspace": base},
				FixedEnvironment: map[string]string{"PATH": "/usr/bin:/bin"},
				Network:          "inherit", TimeoutSecondsMax: 30,
				OutputBytesMax: 8192, ConcurrentCommands: 1,
			},
		},
	}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if os.Geteuid() != 0 {
		if _, err := LoadAuthorityBrokerConfig(path); err == nil {
			t.Fatal("non-root-owned authority broker config was accepted")
		}
		return
	}
	loaded, err := LoadAuthorityBrokerConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Revision != config.Revision {
		t.Fatalf("loaded config = %#v", loaded)
	}
	if err := os.Chmod(path, 0o620); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadAuthorityBrokerConfig(path); err == nil {
		t.Fatal("group-writable authority broker config was accepted")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		path,
		[]byte(`{"revision":"broker-v1","revision":"broker-v2"}`),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadAuthorityBrokerConfig(path); err == nil {
		t.Fatal("authority broker config with duplicate fields was accepted")
	}
	unsafeBase := t.TempDir()
	unsafePath := filepath.Join(unsafeBase, "broker.json")
	if err := os.WriteFile(unsafePath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadAuthorityBrokerConfig(unsafePath); err == nil {
		t.Fatal("authority broker config below a writable ancestor was accepted")
	}
	realDirectory := filepath.Join(base, "real")
	if err := os.Mkdir(realDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	realPath := filepath.Join(realDirectory, "broker.json")
	if err := os.WriteFile(realPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(base, "linked")
	if err := os.Symlink(realDirectory, symlink); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadAuthorityBrokerConfig(
		filepath.Join(symlink, "broker.json"),
	); err == nil {
		t.Fatal("authority broker config below a symlinked ancestor was accepted")
	}
}

func TestPrepareAuthorityBrokerSocketFailsClosed(t *testing.T) {
	base := t.TempDir()
	path := filepath.Join(base, "broker.sock")
	if os.Geteuid() != 0 {
		if err := prepareAuthorityBrokerSocket(path); err == nil {
			t.Fatal("non-root-owned authority broker directory was accepted")
		}
		return
	}
	if err := prepareAuthorityBrokerSocket(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("not a socket"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := prepareAuthorityBrokerSocket(path); err == nil {
		t.Fatal("regular file at authority broker socket path was replaced")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(false)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if err := prepareAuthorityBrokerSocket(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("stale socket remains: %v", err)
	}
}

func TestRunAuthorityBrokerRequiresRoot(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("unprivileged rejection requires a non-root test process")
	}
	if err := RunAuthorityBroker(
		context.Background(),
		AuthorityBrokerConfig{},
		"/bin/false",
	); err == nil {
		t.Fatal("unprivileged authority broker start was accepted")
	}
}

func authorityBrokerServiceTestDir(t *testing.T) string {
	t.Helper()
	if os.Geteuid() != 0 {
		return t.TempDir()
	}
	path, err := os.MkdirTemp("/run", "mintclaw-authority-broker-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("remove authority broker test directory: %v", err)
		}
	})
	return path
}
