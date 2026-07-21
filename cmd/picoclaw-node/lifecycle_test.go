package main

import (
	"bytes"
	"encoding/json"
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
	if err := runServiceLifecycle("install", []string{"--system", "--config", "missing.json"}); err == nil ||
		!strings.Contains(err.Error(), "--service-user") {
		t.Fatalf("system install error = %v", err)
	}
	if err := runServiceLifecycle("install", []string{
		"--service-user", "root", "--config", "missing.json",
	}); err == nil || !strings.Contains(err.Error(), "requires --system") {
		t.Fatalf("user install error = %v", err)
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
