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

func TestSystemInstallRequiresExplicitServiceUser(t *testing.T) {
	if err := runServiceLifecycle("install", []string{"--system", "--config", "/missing.json"}); err == nil ||
		!strings.Contains(err.Error(), "--service-user") {
		t.Fatalf("system install error = %v", err)
	}
	if err := runServiceLifecycle("install", []string{
		"--service-user", "root", "--config", "missing.json",
	}); err == nil || !strings.Contains(err.Error(), "requires --system") {
		t.Fatalf("user install error = %v", err)
	}
}

func TestSystemInstallRequiresExplicitAbsoluteConfig(t *testing.T) {
	if err := runServiceLifecycle("install", []string{"--system"}); err == nil ||
		!strings.Contains(err.Error(), "explicit --config") {
		t.Fatalf("implicit system config error = %v", err)
	}
	if err := runServiceLifecycle("install", []string{
		"--system", "--service-user", "not-looked-up", "--config", "config.json",
	}); err == nil || !strings.Contains(err.Error(), "absolute --config") {
		t.Fatalf("relative system config error = %v", err)
	}

	for _, test := range []struct {
		name     string
		value    string
		explicit bool
		want     string
	}{
		{name: "implicit default", value: defaultNodeConfigPath, want: "explicit --config"},
		{name: "relative", value: "config.json", explicit: true, want: "absolute --config"},
		{name: "home relative", value: "~/.picoclaw-node/config.json", explicit: true, want: "absolute --config"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := resolveLifecycleConfigPath(test.value, true, test.explicit)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("resolveLifecycleConfigPath() error = %v, want %q", err, test.want)
			}
		})
	}

	got, err := resolveLifecycleConfigPath("/etc/forgeclaw/node.json", true, true)
	if err != nil || got != "/etc/forgeclaw/node.json" {
		t.Fatalf("resolveLifecycleConfigPath() = %q, %v", got, err)
	}
}

func TestResolveServiceAccountRejectsUIDZero(t *testing.T) {
	for _, name := range []string{"root", "root-alias"} {
		_, err := resolveServiceAccount(name, func(string) (*user.User, error) {
			return &user.User{Uid: "0", Username: name}, nil
		})
		if err == nil || !strings.Contains(err.Error(), "unprivileged") {
			t.Fatalf("resolveServiceAccount(%q) error = %v", name, err)
		}
	}
	got, err := resolveServiceAccount("forgeclaw", func(string) (*user.User, error) {
		return &user.User{Uid: "1001", Username: "forgeclaw"}, nil
	})
	if err != nil || got != "forgeclaw" {
		t.Fatalf("resolveServiceAccount() = %q, %v", got, err)
	}
	lookupErr := errors.New("lookup failed")
	if _, err := resolveServiceAccount("missing", func(string) (*user.User, error) {
		return nil, lookupErr
	}); !errors.Is(err, lookupErr) {
		t.Fatalf("resolveServiceAccount() error = %v", err)
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
