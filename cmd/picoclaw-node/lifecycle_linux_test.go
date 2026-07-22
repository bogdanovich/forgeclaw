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
	showChecks := 0
	unitDir := t.TempDir()
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(_ context.Context, system bool, args ...string) (systemdRunResult, error) {
			calls = append(calls, systemdCall{system: system, args: append([]string(nil), args...)})
			if reflect.DeepEqual(args, missingSystemdUnitShowArgs("picoclaw-node-main.service")) {
				showChecks++
				if showChecks == 1 {
					return missingSystemdUnitResult(), nil
				}
				return loadedSystemdUnitResult(unitPath, "active"), nil
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
		{args: missingSystemdUnitShowArgs("picoclaw-node-main.service")},
		{args: []string{"daemon-reload"}},
		{args: []string{"enable", "--now", "picoclaw-node-main.service"}},
		{args: missingSystemdUnitShowArgs("picoclaw-node-main.service")},
		{args: missingSystemdUnitShowArgs("picoclaw-node-main.service")},
		{args: missingSystemdUnitShowArgs("picoclaw-node-main.service")},
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
		state  string
		active bool
	}{
		{state: "active", active: true},
		{state: "activating", active: true},
		{state: "reloading", active: true},
		{state: "inactive"},
		{state: "deactivating"},
		{state: "failed"},
	} {
		t.Run(test.state, func(t *testing.T) {
			lifecycle := &systemdLifecycle{
				unitDir: unitDir,
				run: func(_ context.Context, system bool, args ...string) (systemdRunResult, error) {
					if system || !reflect.DeepEqual(
						args,
						missingSystemdUnitShowArgs("picoclaw-node-main.service"),
					) {
						t.Fatalf("status call = system:%t args:%v", system, args)
					}
					return loadedSystemdUnitResult(unitPath, test.state), nil
				},
			}
			status, err := lifecycle.Status(t.Context(), lifecycleRequest{Instance: "main"})
			if err != nil {
				t.Fatal(err)
			}
			unexpectedStatus := !status.Installed || status.Active != test.active ||
				status.State != test.state
			if unexpectedStatus {
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
					if !reflect.DeepEqual(args, missingSystemdUnitShowArgs("picoclaw-node-main.service")) {
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
				Output: "LoadState=not-found\nLoadState=loaded\nActiveState=inactive\nFragmentPath=",
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

func TestSystemdLifecycleInstallRejectsResolvedUnitFromAnotherPath(t *testing.T) {
	for _, result := range []systemdRunResult{
		loadedSystemdUnitResult("/usr/lib/systemd/user/picoclaw-node-main.service", "inactive"),
		loadedSystemdUnitResult("", "active"),
	} {
		lifecycle := &systemdLifecycle{
			unitDir: t.TempDir(),
			writeFile: func(string, []byte, os.FileMode) error {
				t.Fatal("unit should not be written when the service name is already resolved")
				return nil
			},
			run: func(_ context.Context, _ bool, args ...string) (systemdRunResult, error) {
				if !reflect.DeepEqual(args, missingSystemdUnitShowArgs("picoclaw-node-main.service")) {
					t.Fatalf("unexpected systemctl call: %v", args)
				}
				return result, nil
			},
		}
		_, err := lifecycle.Install(t.Context(), lifecycleRequest{
			Instance:       "main",
			ConfigPath:     "/home/test/config.json",
			ExecutablePath: "/home/test/picoclaw-node",
		})
		if err == nil || !strings.Contains(err.Error(), "resolved outside") {
			t.Fatalf("Install() error = %v", err)
		}
	}
}

func TestSystemdLifecycleInstallRollbackUsesFreshContext(t *testing.T) {
	unitDir := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	daemonReloads := 0
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(callCtx context.Context, _ bool, args ...string) (systemdRunResult, error) {
			if reflect.DeepEqual(args, missingSystemdUnitShowArgs("picoclaw-node-main.service")) {
				return missingSystemdUnitResult(), nil
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
			if reflect.DeepEqual(args, missingSystemdUnitShowArgs("picoclaw-node-main.service")) {
				return missingSystemdUnitResult(), nil
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
		{args: missingSystemdUnitShowArgs("picoclaw-node-main.service")},
		{args: []string{"daemon-reload"}},
	}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("install calls = %#v, want %#v", calls, wantCalls)
	}
}

func TestSystemdLifecycleInstallRejectsUnstableActivation(t *testing.T) {
	unitDir := t.TempDir()
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	showChecks := 0
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(_ context.Context, _ bool, args ...string) (systemdRunResult, error) {
			if reflect.DeepEqual(args, missingSystemdUnitShowArgs("picoclaw-node-main.service")) {
				showChecks++
				if showChecks == 1 {
					return missingSystemdUnitResult(), nil
				}
				return loadedSystemdUnitResult(unitPath, "activating"), nil
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
	if showChecks != systemdReadinessAttempts+1 {
		t.Fatalf("systemctl show calls = %d, want %d", showChecks, systemdReadinessAttempts+1)
	}
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
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	var calls []systemdCall
	showChecks := 0
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(_ context.Context, system bool, args ...string) (systemdRunResult, error) {
			calls = append(calls, systemdCall{system: system, args: append([]string(nil), args...)})
			if reflect.DeepEqual(args, missingSystemdUnitShowArgs("picoclaw-node-main.service")) {
				showChecks++
				if showChecks == 1 {
					return missingSystemdUnitResult(), nil
				}
				return loadedSystemdUnitResult(unitPath, "inactive"), nil
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
	if _, statErr := os.Stat(unitPath); !os.IsNotExist(statErr) {
		t.Fatalf("failed unit still exists: %v", statErr)
	}
	wantCalls := []systemdCall{
		{args: missingSystemdUnitShowArgs("picoclaw-node-main.service")},
		{args: []string{"daemon-reload"}},
		{args: []string{"enable", "--now", "picoclaw-node-main.service"}},
		{args: missingSystemdUnitShowArgs("picoclaw-node-main.service")},
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

func missingSystemdUnitShowArgs(service string) []string {
	return []string{
		"show", service,
		"--property=LoadState",
		"--property=ActiveState",
		"--property=FragmentPath",
	}
}

func missingSystemdUnitResult() systemdRunResult {
	return systemdRunResult{Output: "LoadState=not-found\nActiveState=inactive\nFragmentPath="}
}

func loadedSystemdUnitResult(path, activeState string) systemdRunResult {
	return systemdRunResult{Output: "LoadState=loaded\nActiveState=" + activeState + "\nFragmentPath=" + path}
}
