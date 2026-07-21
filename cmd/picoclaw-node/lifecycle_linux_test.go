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
)

type systemdCall struct {
	system bool
	args   []string
}

func TestSystemdLifecycleInstallAndUninstall(t *testing.T) {
	var calls []systemdCall
	lifecycle := &systemdLifecycle{
		unitDir: t.TempDir(),
		run: func(_ context.Context, system bool, args ...string) (systemdRunResult, error) {
			calls = append(calls, systemdCall{system: system, args: append([]string(nil), args...)})
			if reflect.DeepEqual(args, []string{"is-active", "picoclaw-node-main.service"}) {
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
		{args: []string{"daemon-reload"}},
		{args: []string{"enable", "--now", "picoclaw-node-main.service"}},
		{args: []string{"is-active", "picoclaw-node-main.service"}},
	}
	if !reflect.DeepEqual(calls, wantInstallCalls) {
		t.Fatalf("install calls = %#v, want %#v", calls, wantInstallCalls)
	}

	calls = nil
	status, err = lifecycle.Uninstall(t.Context(), lifecycleRequest{Instance: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if status.Installed || status.Active || status.State != "removed" {
		t.Fatalf("uninstall status = %#v", status)
	}
	if _, err := os.Stat(status.UnitPath); !os.IsNotExist(err) {
		t.Fatalf("unit still exists: %v", err)
	}
	wantUninstallCalls := []systemdCall{
		{args: []string{"disable", "--now", "picoclaw-node-main.service"}},
		{args: []string{"daemon-reload"}},
	}
	if !reflect.DeepEqual(calls, wantUninstallCalls) {
		t.Fatalf("uninstall calls = %#v, want %#v", calls, wantUninstallCalls)
	}
}

func TestSystemdLifecycleStatus(t *testing.T) {
	unitDir := t.TempDir()
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	if err := os.WriteFile(unitPath, []byte("test"), 0o644); err != nil {
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

func TestSystemdLifecycleInstallRestoresPreviousUnitOnRestartFailure(t *testing.T) {
	unitDir := t.TempDir()
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	previous := []byte("previous unit\n")
	if err := os.WriteFile(unitPath, previous, 0o600); err != nil {
		t.Fatal(err)
	}
	var calls []systemdCall
	restartAttempts := 0
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(_ context.Context, system bool, args ...string) (systemdRunResult, error) {
			calls = append(calls, systemdCall{system: system, args: append([]string(nil), args...)})
			if reflect.DeepEqual(args, []string{"is-active", "picoclaw-node-main.service"}) {
				return systemdRunResult{Output: "active"}, nil
			}
			if reflect.DeepEqual(args, []string{"is-enabled", "picoclaw-node-main.service"}) {
				return systemdRunResult{Output: "enabled"}, nil
			}
			if reflect.DeepEqual(args, []string{"restart", "picoclaw-node-main.service"}) {
				restartAttempts++
				if restartAttempts == 1 {
					return systemdRunResult{Output: "restart failed", ExitCode: 1}, nil
				}
			}
			return systemdRunResult{}, nil
		},
	}
	_, err := lifecycle.Install(t.Context(), lifecycleRequest{
		Instance:       "main",
		ConfigPath:     "/home/test/config.json",
		ExecutablePath: "/home/test/picoclaw-node",
	})
	if err == nil || !strings.Contains(err.Error(), "restart failed") {
		t.Fatalf("Install() error = %v", err)
	}
	got, readErr := os.ReadFile(unitPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(got) != string(previous) {
		t.Fatalf("restored unit = %q, want %q", got, previous)
	}
	info, statErr := os.Stat(unitPath)
	if statErr != nil {
		t.Fatal(statErr)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("restored unit mode = %o, want 600", info.Mode().Perm())
	}
	wantCalls := []systemdCall{
		{args: []string{"is-active", "picoclaw-node-main.service"}},
		{args: []string{"is-enabled", "picoclaw-node-main.service"}},
		{args: []string{"daemon-reload"}},
		{args: []string{"enable", "picoclaw-node-main.service"}},
		{args: []string{"restart", "picoclaw-node-main.service"}},
		{args: []string{"disable", "--now", "picoclaw-node-main.service"}},
		{args: []string{"daemon-reload"}},
		{args: []string{"enable", "--now", "picoclaw-node-main.service"}},
	}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("install calls = %#v, want %#v", calls, wantCalls)
	}
}

func TestSystemdLifecycleReinstallRestartsActiveService(t *testing.T) {
	unitDir := t.TempDir()
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	if err := os.WriteFile(unitPath, []byte("previous unit\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var calls []systemdCall
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(_ context.Context, system bool, args ...string) (systemdRunResult, error) {
			calls = append(calls, systemdCall{system: system, args: append([]string(nil), args...)})
			if reflect.DeepEqual(args, []string{"is-active", "picoclaw-node-main.service"}) {
				return systemdRunResult{Output: "active"}, nil
			}
			if reflect.DeepEqual(args, []string{"is-enabled", "picoclaw-node-main.service"}) {
				return systemdRunResult{Output: "enabled"}, nil
			}
			return systemdRunResult{}, nil
		},
	}
	status, err := lifecycle.Install(t.Context(), lifecycleRequest{
		Instance:       "main",
		ConfigPath:     "/home/test/config.json",
		ExecutablePath: "/home/test/picoclaw-node",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !status.Active {
		t.Fatalf("install status = %#v", status)
	}
	wantCalls := []systemdCall{
		{args: []string{"is-active", "picoclaw-node-main.service"}},
		{args: []string{"is-enabled", "picoclaw-node-main.service"}},
		{args: []string{"daemon-reload"}},
		{args: []string{"enable", "picoclaw-node-main.service"}},
		{args: []string{"restart", "picoclaw-node-main.service"}},
		{args: []string{"is-active", "picoclaw-node-main.service"}},
	}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("install calls = %#v, want %#v", calls, wantCalls)
	}
}

func TestSystemdLifecycleInstallRollbackUsesFreshContext(t *testing.T) {
	unitDir := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	daemonReloads := 0
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(callCtx context.Context, _ bool, args ...string) (systemdRunResult, error) {
			if !reflect.DeepEqual(args, []string{"daemon-reload"}) {
				t.Fatalf("unexpected systemctl call: %v", args)
			}
			daemonReloads++
			if daemonReloads == 1 {
				if callCtx.Err() == nil {
					t.Fatal("install context was not canceled")
				}
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

func TestSystemdLifecycleInstallRemovesNewUnitWhenServiceIsInactive(t *testing.T) {
	unitDir := t.TempDir()
	var calls []systemdCall
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(_ context.Context, system bool, args ...string) (systemdRunResult, error) {
			calls = append(calls, systemdCall{system: system, args: append([]string(nil), args...)})
			if reflect.DeepEqual(args, []string{"is-active", "picoclaw-node-main.service"}) {
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

func TestSystemdLifecycleUninstallMissingUnitReloadsManager(t *testing.T) {
	var calls []systemdCall
	lifecycle := &systemdLifecycle{
		unitDir: t.TempDir(),
		run: func(_ context.Context, system bool, args ...string) (systemdRunResult, error) {
			calls = append(calls, systemdCall{system: system, args: append([]string(nil), args...)})
			return systemdRunResult{}, nil
		},
	}
	status, err := lifecycle.Uninstall(t.Context(), lifecycleRequest{Instance: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if status.Installed || status.State != "not-installed" {
		t.Fatalf("uninstall status = %#v", status)
	}
	want := []systemdCall{{args: []string{"daemon-reload"}}}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("uninstall calls = %#v, want %#v", calls, want)
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
