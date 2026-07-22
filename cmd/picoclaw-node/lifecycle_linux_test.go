//go:build linux

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSystemdLifecycleStatus(t *testing.T) {
	unitDir := t.TempDir()
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	if err := os.WriteFile(unitPath, managedSystemdUnitData("test"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		state  string
		active bool
	}{
		{state: "active", active: true},
		{state: "activating"},
		{state: "reloading"},
		{state: "inactive"},
		{state: "deactivating"},
		{state: "failed"},
	} {
		t.Run(test.state, func(t *testing.T) {
			lifecycle := &systemdLifecycle{
				unitDir: unitDir,
				run: func(_ context.Context, system bool, args ...string) (systemdRunResult, error) {
					if system || !reflect.DeepEqual(args, systemdUnitShowArgs("picoclaw-node-main.service")) {
						t.Fatalf("status call = system:%t args:%v", system, args)
					}
					return loadedSystemdUnitResult(unitPath, test.state), nil
				},
			}
			status, err := lifecycle.Status(t.Context(), lifecycleRequest{Instance: "main"})
			if err != nil {
				t.Fatal(err)
			}
			if !status.Installed || status.Active != test.active || status.State != test.state {
				t.Fatalf("status = %#v", status)
			}
		})
	}
}

func TestSystemdLifecycleStatusRejectsResolvedServiceWithoutManagedUnit(t *testing.T) {
	for _, test := range []struct {
		name    string
		result  systemdRunResult
		wantErr bool
	}{
		{name: "missing", result: missingSystemdUnitResult()},
		{
			name: "resolved elsewhere",
			result: loadedSystemdUnitResult(
				"/usr/lib/systemd/user/picoclaw-node-main.service",
				"inactive",
			),
			wantErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			lifecycle := &systemdLifecycle{
				unitDir: t.TempDir(),
				run: func(_ context.Context, _ bool, args ...string) (systemdRunResult, error) {
					if !reflect.DeepEqual(args, systemdUnitShowArgs("picoclaw-node-main.service")) {
						t.Fatalf("status call = %v", args)
					}
					return test.result, nil
				},
			}
			status, err := lifecycle.Status(t.Context(), lifecycleRequest{Instance: "main"})
			if (err != nil) != test.wantErr {
				t.Fatalf("Status() error = %v", err)
			}
			if test.wantErr && !strings.Contains(err.Error(), "resolved outside") {
				t.Fatalf("Status() error = %v", err)
			}
			if !test.wantErr && (status.Installed || status.Active) {
				t.Fatalf("status = %#v", status)
			}
		})
	}
}

func TestSystemdLifecycleStatusRejectsManagedUnitWithDropIn(t *testing.T) {
	unitDir := t.TempDir()
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	if err := os.WriteFile(unitPath, managedSystemdUnitData("test"), 0o644); err != nil {
		t.Fatal(err)
	}
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(context.Context, bool, ...string) (systemdRunResult, error) {
			return systemdRunResult{
				Output: "LoadState=loaded\nActiveState=active\nFragmentPath=" + unitPath +
					"\nDropInPaths=/etc/systemd/user/picoclaw-node-main.service.d/override.conf" +
					"\nNeedDaemonReload=no\nUnitFileState=enabled",
			}, nil
		},
	}
	_, err := lifecycle.Status(t.Context(), lifecycleRequest{Instance: "main"})
	if err == nil || !strings.Contains(err.Error(), "drop-ins") {
		t.Fatalf("Status() error = %v", err)
	}
}

func TestSystemdLifecycleStatusRejectsPendingDaemonReload(t *testing.T) {
	unitDir := t.TempDir()
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	if err := os.WriteFile(unitPath, managedSystemdUnitData("test"), 0o644); err != nil {
		t.Fatal(err)
	}
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(context.Context, bool, ...string) (systemdRunResult, error) {
			return systemdRunResult{
				Output: "LoadState=loaded\nActiveState=active\nFragmentPath=" + unitPath +
					"\nDropInPaths=\nNeedDaemonReload=yes\nUnitFileState=enabled",
			}, nil
		},
	}
	_, err := lifecycle.Status(t.Context(), lifecycleRequest{Instance: "main"})
	if err == nil || !strings.Contains(err.Error(), "daemon reload is pending") {
		t.Fatalf("Status() error = %v", err)
	}
}

func TestSystemdLifecycleUnitPropertiesFailClosed(t *testing.T) {
	for _, test := range []struct {
		name   string
		result systemdRunResult
	}{
		{
			name:   "missing property",
			result: systemdRunResult{Output: "LoadState=not-found\nFragmentPath="},
		},
		{
			name: "duplicate property",
			result: systemdRunResult{
				Output: "LoadState=not-found\nLoadState=loaded\nActiveState=inactive\n" +
					"FragmentPath=\nDropInPaths=\nNeedDaemonReload=no\nUnitFileState=",
			},
		},
		{
			name: "invalid reload state",
			result: systemdRunResult{
				Output: "LoadState=not-found\nActiveState=inactive\nFragmentPath=\n" +
					"DropInPaths=\nNeedDaemonReload=unknown\nUnitFileState=",
			},
		},
		{
			name:   "command failure",
			result: systemdRunResult{Output: "manager unavailable", ExitCode: 1},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			lifecycle := &systemdLifecycle{
				run: func(context.Context, bool, ...string) (systemdRunResult, error) {
					return test.result, nil
				},
			}
			if _, err := lifecycle.queryUnitProperties(t.Context(), "test.service"); err == nil {
				t.Fatal("queryUnitProperties() accepted malformed systemd state")
			}
		})
	}
}

func TestSystemdLifecycleStatusRejectsUnitSymlink(t *testing.T) {
	unitDir := t.TempDir()
	target := filepath.Join(unitDir, "managed-elsewhere.service")
	if err := os.WriteFile(target, managedSystemdUnitData("test"), 0o644); err != nil {
		t.Fatal(err)
	}
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	if err := os.Symlink(target, unitPath); err != nil {
		t.Fatal(err)
	}
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(context.Context, bool, ...string) (systemdRunResult, error) {
			t.Fatal("systemctl should not run for a symlinked unit")
			return systemdRunResult{}, nil
		},
	}
	if _, err := lifecycle.Status(t.Context(), lifecycleRequest{Instance: "main"}); err == nil ||
		!strings.Contains(err.Error(), "not a bounded regular file") {
		t.Fatalf("Status() error = %v", err)
	}
}

func TestSystemdLifecycleStatusRefusesUnownedUnit(t *testing.T) {
	unitDir := t.TempDir()
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	unowned := []byte("[Unit]\nDescription=administrator managed\n")
	if err := os.WriteFile(unitPath, unowned, 0o640); err != nil {
		t.Fatal(err)
	}
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(context.Context, bool, ...string) (systemdRunResult, error) {
			t.Fatal("systemctl should not run for an unowned unit")
			return systemdRunResult{}, nil
		},
	}
	if _, err := lifecycle.Status(t.Context(), lifecycleRequest{Instance: "main"}); err == nil ||
		!strings.Contains(err.Error(), "unowned systemd unit") {
		t.Fatalf("Status() error = %v", err)
	}
	got, err := os.ReadFile(unitPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(unowned) {
		t.Fatalf("status changed unowned unit: %q", got)
	}
}

func TestRunSystemctlPreservesContextDeadline(t *testing.T) {
	binDir := t.TempDir()
	systemctlPath := filepath.Join(binDir, "systemctl")
	if err := os.WriteFile(systemctlPath, []byte("#!/bin/sh\nexec sleep 30\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err := runSystemctl(ctx, false, "show", "test.service")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runSystemctl() error = %v, want context deadline", err)
	}
}

func managedSystemdUnitData(body string) []byte {
	return []byte(managedSystemdUnitMarker + "\n" + body + "\n")
}

func systemdUnitShowArgs(service string) []string {
	return []string{
		"show", service,
		"--property=LoadState",
		"--property=ActiveState",
		"--property=FragmentPath",
		"--property=DropInPaths",
		"--property=NeedDaemonReload",
		"--property=UnitFileState",
	}
}

func missingSystemdUnitResult() systemdRunResult {
	return systemdRunResult{
		Output: "LoadState=not-found\nActiveState=inactive\nFragmentPath=\n" +
			"DropInPaths=\nNeedDaemonReload=no\nUnitFileState=",
	}
}

func loadedSystemdUnitResult(path, activeState string) systemdRunResult {
	return systemdRunResult{
		Output: "LoadState=loaded\nActiveState=" + activeState +
			"\nFragmentPath=" + path + "\nDropInPaths=\nNeedDaemonReload=no\nUnitFileState=disabled",
	}
}
