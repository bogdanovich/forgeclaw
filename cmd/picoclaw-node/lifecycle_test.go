package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os/user"
	"strings"
	"testing"
)

func TestNodeServiceInstanceValidation(t *testing.T) {
	for _, valid := range []string{"default", "main", "nutrition-2", "VPN_box"} {
		if !nodeInstancePattern.MatchString(valid) {
			t.Errorf("valid instance %q rejected", valid)
		}
	}
	for _, invalid := range []string{"", "-main", "../main", "main.service", strings.Repeat("x", 65)} {
		if nodeInstancePattern.MatchString(invalid) {
			t.Errorf("invalid instance %q accepted", invalid)
		}
	}
	if err := runServiceLifecycle("status", []string{"--instance", "../main"}); err == nil {
		t.Fatal("status accepted an unsafe instance")
	}
}

func TestSystemInstallRequiresExplicitConfigurationAndUser(t *testing.T) {
	if err := runServiceLifecycle("install", []string{"--system", "--config", "/missing.json"}); err == nil ||
		!strings.Contains(err.Error(), "--service-user") {
		t.Fatalf("system install error = %v", err)
	}
	if err := runServiceLifecycle("install", []string{
		"--service-user", "root", "--config", "missing.json",
	}); err == nil || !strings.Contains(err.Error(), "requires --system") {
		t.Fatalf("user install error = %v", err)
	}
	if _, err := resolveLifecycleConfigPath(defaultNodeConfigPath, true, false); err == nil ||
		!strings.Contains(err.Error(), "explicit --config") {
		t.Fatalf("implicit system config error = %v", err)
	}
	if _, err := resolveLifecycleConfigPath("config.json", true, true); err == nil ||
		!strings.Contains(err.Error(), "absolute --config") {
		t.Fatalf("relative system config error = %v", err)
	}
	got, err := resolveLifecycleConfigPath("/etc/forgeclaw/node.json", true, true)
	if err != nil || got != "/etc/forgeclaw/node.json" {
		t.Fatalf("resolveLifecycleConfigPath() = %q, %v", got, err)
	}
}

func TestResolveServiceAccountRequiresUnprivilegedUser(t *testing.T) {
	groups := func(*user.User) ([]string, error) { return []string{"1001"}, nil }
	for _, name := range []string{"root", "root-alias", "malformed"} {
		_, err := resolveServiceAccount(name, func(string) (*user.User, error) {
			uid := "0"
			if name == "malformed" {
				uid = "not-a-uid"
			}
			return &user.User{Uid: uid, Gid: "1001", Username: name}, nil
		}, groups)
		if err == nil || !strings.Contains(err.Error(), "unprivileged") {
			t.Fatalf("resolveServiceAccount(%q) error = %v", name, err)
		}
	}
	got, err := resolveServiceAccount("forgeclaw", func(string) (*user.User, error) {
		return &user.User{Uid: "1001", Gid: "1001", Username: "forgeclaw"}, nil
	}, groups)
	if err != nil || got != "forgeclaw" {
		t.Fatalf("resolveServiceAccount() = %q, %v", got, err)
	}
	lookupErr := errors.New("lookup failed")
	if _, err = resolveServiceAccount("missing", func(string) (*user.User, error) {
		return nil, lookupErr
	}, groups); !errors.Is(err, lookupErr) {
		t.Fatalf("resolveServiceAccount() error = %v", err)
	}
	for _, test := range []struct {
		name     string
		username string
		gid      string
		groups   []string
	}{
		{name: "numeric username", username: "1234", gid: "1001", groups: []string{"1001"}},
		{name: "root primary group", username: "service", gid: "0", groups: []string{"0"}},
		{name: "root supplementary group", username: "service", gid: "1001", groups: []string{"1001", "0"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, resolveErr := resolveServiceAccount("service", func(string) (*user.User, error) {
				return &user.User{Uid: "1001", Gid: test.gid, Username: test.username}, nil
			}, func(*user.User) ([]string, error) { return test.groups, nil })
			if resolveErr == nil {
				t.Fatal("resolveServiceAccount() accepted a privileged or ambiguous account")
			}
		})
	}
}

func TestWriteLifecycleStatusJSON(t *testing.T) {
	want := lifecycleStatus{
		Instance:  "main",
		Manager:   "systemd",
		Scope:     "user",
		Service:   "picoclaw-node-main.service",
		UnitPath:  "/tmp/picoclaw-node-main.service",
		Installed: true,
		Active:    true,
		State:     "active",
	}
	var output bytes.Buffer
	if err := writeLifecycleStatus(&output, want, true); err != nil {
		t.Fatal(err)
	}
	var got lifecycleStatus
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("status = %#v, want %#v", got, want)
	}
}
