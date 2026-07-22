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

	"github.com/sipeed/picoclaw/pkg/fileutil"
)

type systemdCall struct {
	system bool
	args   []string
}

func TestSystemdLifecycleInstall(t *testing.T) {
	var calls []systemdCall
	activeChecks := 0
	lifecycle := &systemdLifecycle{
		unitDir: t.TempDir(),
		run: func(_ context.Context, system bool, args ...string) (systemdRunResult, error) {
			calls = append(calls, systemdCall{system: system, args: append([]string(nil), args...)})
			if reflect.DeepEqual(args, []string{"is-active", "picoclaw-node-main.service"}) {
				activeChecks++
				if activeChecks == 1 {
					return systemdRunResult{Output: "inactive", ExitCode: 4}, nil
				}
				return systemdRunResult{Output: "active"}, nil
			}
			return systemdRunResult{}, nil
		},
	}
	request := lifecycleRequest{
		Instance:       "main",
		ConfigPath:     "/home/test/node config/config%$MAIN.json",
		ExecutablePath: "/home/test/bin/picoclaw node",
	}
	status, err := lifecycle.Install(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Installed || !status.Active || status.State != "active" || status.Scope != "user" {
		t.Fatalf("install status = %#v", status)
	}
	data, err := os.ReadFile(status.UnitPath)
	if err != nil {
		t.Fatal(err)
	}
	unit := string(data)
	execStart := `ExecStart="/home/test/bin/picoclaw node" run --config ` +
		`"/home/test/node config/config%%$$MAIN.json"`
	if !strings.Contains(unit, execStart) || !strings.Contains(unit, "WantedBy=default.target") ||
		!strings.Contains(unit, "NoNewPrivileges=true") || strings.Contains(unit, "User=") {
		t.Fatalf("unexpected systemd unit:\n%s", unit)
	}
	wantInstallCalls := []systemdCall{
		{args: []string{"is-active", "picoclaw-node-main.service"}},
		{args: []string{"daemon-reload"}},
		{args: []string{"enable", "--now", "picoclaw-node-main.service"}},
		{args: []string{"is-active", "picoclaw-node-main.service"}},
		{args: []string{"is-active", "picoclaw-node-main.service"}},
		{args: []string{"is-active", "picoclaw-node-main.service"}},
	}
	if !reflect.DeepEqual(calls, wantInstallCalls) {
		t.Fatalf("install calls = %#v, want %#v", calls, wantInstallCalls)
	}
}

func TestSystemdLifecycleStatus(t *testing.T) {
	unitDir := t.TempDir()
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	if err := os.WriteFile(unitPath, managedSystemdUnitData("test"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		result  systemdRunResult
		active  bool
		wantErr bool
	}{
		{name: "active", result: systemdRunResult{Output: "active"}, active: true},
		{name: "activating", result: systemdRunResult{Output: "activating"}, active: true},
		{name: "inactive", result: systemdRunResult{Output: "inactive", ExitCode: 3}},
		{name: "missing", result: systemdRunResult{Output: "inactive", ExitCode: 4}},
		{name: "unexpected", result: systemdRunResult{Output: "unknown", ExitCode: 4}, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			lifecycle := &systemdLifecycle{
				unitDir: unitDir,
				run: func(_ context.Context, system bool, args ...string) (systemdRunResult, error) {
					if system || !reflect.DeepEqual(args, []string{"is-active", "picoclaw-node-main.service"}) {
						t.Fatalf("status call = system:%t args:%v", system, args)
					}
					return test.result, nil
				},
			}
			status, err := lifecycle.Status(t.Context(), lifecycleRequest{Instance: "main"})
			if (err != nil) != test.wantErr {
				t.Fatalf("Status() error = %v", err)
			}
			unexpectedStatus := !status.Installed || status.Active != test.active ||
				status.State != test.result.Output
			if !test.wantErr && unexpectedStatus {
				t.Fatalf("status = %#v", status)
			}
		})
	}
}

func TestSystemdLifecycleStatusDetectsOrphanedActiveService(t *testing.T) {
	for _, test := range []struct {
		name    string
		result  systemdRunResult
		wantErr bool
	}{
		{name: "inactive", result: systemdRunResult{Output: "inactive", ExitCode: 3}},
		{name: "missing", result: systemdRunResult{Output: "inactive", ExitCode: 4}},
		{name: "active", result: systemdRunResult{Output: "active"}, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			lifecycle := &systemdLifecycle{
				unitDir: t.TempDir(),
				run: func(_ context.Context, _ bool, args ...string) (systemdRunResult, error) {
					if !reflect.DeepEqual(args, []string{"is-active", "picoclaw-node-main.service"}) {
						t.Fatalf("status call = %v", args)
					}
					return test.result, nil
				},
			}
			status, err := lifecycle.Status(t.Context(), lifecycleRequest{Instance: "main"})
			if (err != nil) != test.wantErr {
				t.Fatalf("Status() error = %v", err)
			}
			if test.wantErr && !strings.Contains(err.Error(), "active without its managed unit") {
				t.Fatalf("Status() error = %v", err)
			}
			if !test.wantErr && (status.Installed || status.Active) {
				t.Fatalf("status = %#v", status)
			}
		})
	}
}

func TestSystemdLifecycleInstallRejectsExistingManagedUnit(t *testing.T) {
	unitDir := t.TempDir()
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	if err := os.WriteFile(unitPath, managedSystemdUnitData("previous unit"), 0o644); err != nil {
		t.Fatal(err)
	}
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(context.Context, bool, ...string) (systemdRunResult, error) {
			t.Fatal("systemctl should not run when the managed unit already exists")
			return systemdRunResult{}, nil
		},
	}
	_, err := lifecycle.Install(t.Context(), lifecycleRequest{
		Instance:       "main",
		ConfigPath:     "/home/test/config.json",
		ExecutablePath: "/home/test/picoclaw-node",
	})
	if err == nil || !strings.Contains(err.Error(), "install is create-only") {
		t.Fatalf("Install() error = %v", err)
	}
}

func TestSystemdLifecycleInstallRejectsOrphanedActiveService(t *testing.T) {
	unitDir := t.TempDir()
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		writeFile: func(string, []byte, os.FileMode) error {
			t.Fatal("unit should not be written for an active orphan")
			return nil
		},
		run: func(_ context.Context, _ bool, args ...string) (systemdRunResult, error) {
			if !reflect.DeepEqual(args, []string{"is-active", "picoclaw-node-main.service"}) {
				t.Fatalf("unexpected systemctl call: %v", args)
			}
			return systemdRunResult{Output: "active"}, nil
		},
	}
	_, err := lifecycle.Install(t.Context(), lifecycleRequest{
		Instance:       "main",
		ConfigPath:     "/home/test/config.json",
		ExecutablePath: "/home/test/picoclaw-node",
	})
	if err == nil || !strings.Contains(err.Error(), "active without its managed unit") {
		t.Fatalf("Install() error = %v", err)
	}
}

func TestSystemdLifecycleInstallRollbackUsesFreshContext(t *testing.T) {
	unitDir := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	daemonReloads := 0
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(callCtx context.Context, _ bool, args ...string) (systemdRunResult, error) {
			if reflect.DeepEqual(args, []string{"is-active", "picoclaw-node-main.service"}) {
				return systemdRunResult{Output: "inactive", ExitCode: 4}, nil
			}
			if !reflect.DeepEqual(args, []string{"daemon-reload"}) {
				t.Fatalf("unexpected systemctl call: %v", args)
			}
			daemonReloads++
			if daemonReloads == 1 {
				cancel()
				return systemdRunResult{}, callCtx.Err()
			}
			if callCtx.Err() != nil {
				t.Fatalf("rollback reused canceled context: %v", callCtx.Err())
			}
			return systemdRunResult{}, nil
		},
	}
	_, err := lifecycle.Install(ctx, lifecycleRequest{
		Instance:       "main",
		ConfigPath:     "/home/test/config.json",
		ExecutablePath: "/home/test/picoclaw-node",
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Install() error = %v, want context canceled", err)
	}
	if daemonReloads != 2 {
		t.Fatalf("daemon reload calls = %d, want 2", daemonReloads)
	}
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	if _, statErr := os.Stat(unitPath); !os.IsNotExist(statErr) {
		t.Fatalf("failed unit still exists: %v", statErr)
	}
}

func TestSystemdLifecycleInstallCleansUpCommittedWriteFailure(t *testing.T) {
	unitDir := t.TempDir()
	var calls []systemdCall
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		writeFile: func(path string, data []byte, mode os.FileMode) error {
			if err := os.WriteFile(path, data, mode); err != nil {
				return err
			}
			return &fileutil.CommittedWriteError{Err: errors.New("directory sync failed")}
		},
		run: func(_ context.Context, system bool, args ...string) (systemdRunResult, error) {
			calls = append(calls, systemdCall{system: system, args: append([]string(nil), args...)})
			if reflect.DeepEqual(args, []string{"is-active", "picoclaw-node-main.service"}) {
				return systemdRunResult{Output: "inactive", ExitCode: 4}, nil
			}
			return systemdRunResult{}, nil
		},
	}
	_, err := lifecycle.Install(t.Context(), lifecycleRequest{
		Instance:       "main",
		ConfigPath:     "/home/test/config.json",
		ExecutablePath: "/home/test/picoclaw-node",
	})
	if err == nil || !strings.Contains(err.Error(), "directory sync failed") {
		t.Fatalf("Install() error = %v", err)
	}
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	if _, statErr := os.Stat(unitPath); !os.IsNotExist(statErr) {
		t.Fatalf("committed unit still exists: %v", statErr)
	}
	wantCalls := []systemdCall{
		{args: []string{"is-active", "picoclaw-node-main.service"}},
		{args: []string{"daemon-reload"}},
	}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("install calls = %#v, want %#v", calls, wantCalls)
	}
}

func TestSystemdLifecycleInstallRejectsUnstableActivation(t *testing.T) {
	unitDir := t.TempDir()
	activeChecks := 0
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(_ context.Context, _ bool, args ...string) (systemdRunResult, error) {
			if reflect.DeepEqual(args, []string{"is-active", "picoclaw-node-main.service"}) {
				activeChecks++
				if activeChecks == 1 {
					return systemdRunResult{Output: "inactive", ExitCode: 4}, nil
				}
				return systemdRunResult{Output: "activating"}, nil
			}
			return systemdRunResult{}, nil
		},
	}
	_, err := lifecycle.Install(t.Context(), lifecycleRequest{
		Instance:       "main",
		ConfigPath:     "/home/test/config.json",
		ExecutablePath: "/home/test/picoclaw-node",
	})
	if err == nil || !strings.Contains(err.Error(), "did not stabilize") {
		t.Fatalf("Install() error = %v", err)
	}
	if activeChecks != systemdReadinessAttempts+1 {
		t.Fatalf("is-active calls = %d, want %d", activeChecks, systemdReadinessAttempts+1)
	}
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	if _, statErr := os.Stat(unitPath); !os.IsNotExist(statErr) {
		t.Fatalf("unstable unit still exists: %v", statErr)
	}
}

func TestSystemdLifecycleInstallRejectsUnitSymlink(t *testing.T) {
	unitDir := t.TempDir()
	target := filepath.Join(unitDir, "managed-elsewhere.service")
	if err := os.WriteFile(target, []byte("administrator unit\n"), 0o644); err != nil {
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
	_, err := lifecycle.Install(t.Context(), lifecycleRequest{
		Instance:       "main",
		ConfigPath:     "/home/test/config.json",
		ExecutablePath: "/home/test/picoclaw-node",
	})
	if err == nil || !strings.Contains(err.Error(), "not a bounded regular file") {
		t.Fatalf("Install() error = %v", err)
	}
	info, statErr := os.Lstat(unitPath)
	if statErr != nil {
		t.Fatal(statErr)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatal("unit symlink was replaced")
	}
}

func TestSystemdLifecycleRefusesUnownedUnit(t *testing.T) {
	for _, action := range []string{"install", "status"} {
		t.Run(action, func(t *testing.T) {
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
			request := lifecycleRequest{
				Instance:       "main",
				ConfigPath:     "/home/test/config.json",
				ExecutablePath: "/home/test/picoclaw-node",
			}
			var err error
			switch action {
			case "install":
				_, err = lifecycle.Install(t.Context(), request)
			case "status":
				_, err = lifecycle.Status(t.Context(), request)
			}
			if err == nil || !strings.Contains(err.Error(), "unowned systemd unit") {
				t.Fatalf("%s error = %v", action, err)
			}
			got, readErr := os.ReadFile(unitPath)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if string(got) != string(unowned) {
				t.Fatalf("%s changed unowned unit: %q", action, got)
			}
		})
	}
}

func TestSystemdLifecycleInstallRemovesNewUnitWhenServiceIsInactive(t *testing.T) {
	unitDir := t.TempDir()
	var calls []systemdCall
	activeChecks := 0
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(_ context.Context, system bool, args ...string) (systemdRunResult, error) {
			calls = append(calls, systemdCall{system: system, args: append([]string(nil), args...)})
			if reflect.DeepEqual(args, []string{"is-active", "picoclaw-node-main.service"}) {
				activeChecks++
				if activeChecks == 1 {
					return systemdRunResult{Output: "inactive", ExitCode: 4}, nil
				}
				return systemdRunResult{Output: "inactive", ExitCode: 3}, nil
			}
			return systemdRunResult{}, nil
		},
	}
	_, err := lifecycle.Install(t.Context(), lifecycleRequest{
		Instance:       "main",
		ConfigPath:     "/home/test/config.json",
		ExecutablePath: "/home/test/picoclaw-node",
	})
	if err == nil || !strings.Contains(err.Error(), `entered state "inactive"`) {
		t.Fatalf("Install() error = %v", err)
	}
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	if _, statErr := os.Stat(unitPath); !os.IsNotExist(statErr) {
		t.Fatalf("failed unit still exists: %v", statErr)
	}
	wantCalls := []systemdCall{
		{args: []string{"is-active", "picoclaw-node-main.service"}},
		{args: []string{"daemon-reload"}},
		{args: []string{"enable", "--now", "picoclaw-node-main.service"}},
		{args: []string{"is-active", "picoclaw-node-main.service"}},
		{args: []string{"disable", "--now", "picoclaw-node-main.service"}},
		{args: []string{"daemon-reload"}},
	}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("install calls = %#v, want %#v", calls, wantCalls)
	}
}

func TestRenderSystemdSystemUnit(t *testing.T) {
	unit, err := renderSystemdUnit(lifecycleRequest{
		Instance:       "vpn",
		ConfigPath:     "/etc/forgeclaw/node.json",
		ExecutablePath: "/usr/local/bin/picoclaw-node",
		ServiceUser:    "forgeclaw-node",
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(unit, "WantedBy=multi-user.target") ||
		!strings.Contains(unit, "User=forgeclaw-node") {
		t.Fatalf("system unit has wrong target:\n%s", unit)
	}
	if _, err := quoteSystemdArgument("bad\npath"); err == nil {
		t.Fatal("systemd argument accepted a newline")
	}
	if _, err := renderSystemdUnit(lifecycleRequest{
		Instance: "vpn", ConfigPath: "relative.json", ExecutablePath: "/bin/picoclaw-node",
	}, false); err == nil {
		t.Fatal("systemd unit accepted a relative config path")
	}
	if _, err := renderSystemdUnit(lifecycleRequest{
		Instance: "vpn", ConfigPath: "/etc/node.json", ExecutablePath: "/bin/picoclaw-node",
	}, true); err == nil {
		t.Fatal("systemd system unit accepted an empty service user")
	}
}

func managedSystemdUnitData(body string) []byte {
	return []byte(managedSystemdUnitMarker + "\n" + body + "\n")
}
