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

type systemdCall struct {
	system bool
	args   []string
}

func TestSystemdReadinessWindowExceedsRestartDelay(t *testing.T) {
	stableWindow := time.Duration(systemdReadinessStable-1) * defaultReadinessInterval
	if stableWindow <= systemdRestartDelay {
		t.Fatalf("stable readiness window %s must exceed restart delay %s", stableWindow, systemdRestartDelay)
	}
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
			if reflect.DeepEqual(args, systemdUnitShowArgs("picoclaw-node-main.service")) {
				showChecks++
				if showChecks == 1 {
					return missingSystemdUnitResult(), nil
				}
				return loadedSystemdUnitResult(unitPath, "active"), nil
			}
			return systemdRunResult{}, nil
		},
		readinessInterval: 0,
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
		!strings.Contains(unit, "NoNewPrivileges=true") || strings.Contains(unit, "User=") ||
		!strings.Contains(unit, installTransactionMarker) {
		t.Fatalf("unexpected systemd unit:\n%s", unit)
	}
	wantCalls := []systemdCall{
		{args: systemdUnitShowArgs("picoclaw-node-main.service")},
		{args: []string{"daemon-reload"}},
		{args: systemdUnitShowArgs("picoclaw-node-main.service")},
		{args: []string{"start", "picoclaw-node-main.service"}},
		{args: systemdUnitShowArgs("picoclaw-node-main.service")},
		{args: systemdUnitShowArgs("picoclaw-node-main.service")},
		{args: systemdUnitShowArgs("picoclaw-node-main.service")},
		{args: []string{"enable", "picoclaw-node-main.service"}},
	}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("install calls = %#v, want %#v", calls, wantCalls)
	}
}

func TestSystemdLifecycleInstallRejectsShadowedPublishedUnit(t *testing.T) {
	unitDir := t.TempDir()
	showChecks := 0
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(_ context.Context, _ bool, args ...string) (systemdRunResult, error) {
			if reflect.DeepEqual(args, systemdUnitShowArgs("picoclaw-node-main.service")) {
				showChecks++
				if showChecks == 1 {
					return missingSystemdUnitResult(), nil
				}
				return loadedSystemdUnitResult("/usr/lib/systemd/user/picoclaw-node-main.service", "inactive"), nil
			}
			return systemdRunResult{}, nil
		},
	}
	_, err := lifecycle.Install(t.Context(), installTestRequest())
	if err == nil || !strings.Contains(err.Error(), "resolved outside") {
		t.Fatalf("Install() error = %v", err)
	}
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	if _, statErr := os.Stat(unitPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("shadowed unit still exists: %v", statErr)
	}
}

func TestSystemdLifecycleInstallIsCreateOnly(t *testing.T) {
	unitDir := t.TempDir()
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	if err := os.WriteFile(unitPath, managedSystemdUnitData("previous unit"), 0o644); err != nil {
		t.Fatal(err)
	}
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(context.Context, bool, ...string) (systemdRunResult, error) {
			t.Fatal("systemctl should not run for an existing unit")
			return systemdRunResult{}, nil
		},
	}
	_, err := lifecycle.Install(t.Context(), installTestRequest())
	if err == nil || !strings.Contains(err.Error(), "install is create-only") {
		t.Fatalf("Install() error = %v", err)
	}
}

func TestSystemdLifecycleInstallRejectsPreexistingEnablement(t *testing.T) {
	unitDir := t.TempDir()
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(_ context.Context, _ bool, args ...string) (systemdRunResult, error) {
			if !reflect.DeepEqual(args, systemdUnitShowArgs("picoclaw-node-main.service")) {
				t.Fatalf("unexpected systemctl call: %v", args)
			}
			return systemdRunResult{
				Output: "LoadState=not-found\nActiveState=inactive\nFragmentPath=\n" +
					"DropInPaths=\nNeedDaemonReload=no\nUnitFileState=enabled",
			}, nil
		},
	}
	_, err := lifecycle.Install(t.Context(), installTestRequest())
	if err == nil || !strings.Contains(err.Error(), "unit file state") {
		t.Fatalf("Install() error = %v", err)
	}
}

func TestSystemdLifecycleInstallPublicationDoesNotReplaceRacer(t *testing.T) {
	unitDir := t.TempDir()
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	foreign := []byte("administrator unit\n")
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(_ context.Context, _ bool, args ...string) (systemdRunResult, error) {
			if !reflect.DeepEqual(args, systemdUnitShowArgs("picoclaw-node-main.service")) {
				t.Fatalf("unexpected systemctl call: %v", args)
			}
			return missingSystemdUnitResult(), nil
		},
		publish: func(path string, data []byte, mode os.FileMode) (publishedSystemdUnit, error) {
			if err := os.WriteFile(path, foreign, 0o640); err != nil {
				t.Fatal(err)
			}
			return publishSystemdUnit(path, data, mode)
		},
	}
	_, err := lifecycle.Install(t.Context(), installTestRequest())
	if err == nil || !strings.Contains(err.Error(), "appeared during install") {
		t.Fatalf("Install() error = %v", err)
	}
	got, readErr := os.ReadFile(unitPath)
	if readErr != nil || !reflect.DeepEqual(got, foreign) {
		t.Fatalf("racing unit = %q, %v", got, readErr)
	}
}

func TestSystemdLifecycleInstallRejectsUnitModifiedDuringReload(t *testing.T) {
	unitDir := t.TempDir()
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	modified := []byte("modified during daemon reload\n")
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(_ context.Context, _ bool, args ...string) (systemdRunResult, error) {
			if reflect.DeepEqual(args, systemdUnitShowArgs("picoclaw-node-main.service")) {
				return missingSystemdUnitResult(), nil
			}
			if reflect.DeepEqual(args, []string{"daemon-reload"}) {
				if err := os.WriteFile(unitPath, modified, 0o644); err != nil {
					t.Fatal(err)
				}
				return systemdRunResult{}, nil
			}
			t.Fatalf("unexpected systemctl call: %v", args)
			return systemdRunResult{}, nil
		},
	}
	_, err := lifecycle.Install(t.Context(), installTestRequest())
	if err == nil || !strings.Contains(err.Error(), "no longer matches") {
		t.Fatalf("Install() error = %v", err)
	}
	got, readErr := os.ReadFile(unitPath)
	if readErr != nil || !reflect.DeepEqual(got, modified) {
		t.Fatalf("modified unit = %q, %v", got, readErr)
	}
}

func TestSystemdLifecycleInstallRollbackUsesFreshContext(t *testing.T) {
	unitDir := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	daemonReloads := 0
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(callCtx context.Context, _ bool, args ...string) (systemdRunResult, error) {
			if reflect.DeepEqual(args, systemdUnitShowArgs("picoclaw-node-main.service")) {
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
	_, err := lifecycle.Install(ctx, installTestRequest())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Install() error = %v, want context canceled", err)
	}
	if daemonReloads != 2 {
		t.Fatalf("daemon reload calls = %d, want 2", daemonReloads)
	}
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	if _, statErr := os.Stat(unitPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("failed unit still exists: %v", statErr)
	}
}

func TestSystemdLifecycleInstallRollbackRefusesReplacement(t *testing.T) {
	unitDir := t.TempDir()
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	replacement := []byte("administrator replacement\n")
	daemonReloads := 0
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(_ context.Context, _ bool, args ...string) (systemdRunResult, error) {
			if reflect.DeepEqual(args, systemdUnitShowArgs("picoclaw-node-main.service")) {
				return missingSystemdUnitResult(), nil
			}
			daemonReloads++
			return systemdRunResult{Output: "reload failed", ExitCode: 1}, nil
		},
		publish: func(path string, data []byte, mode os.FileMode) (publishedSystemdUnit, error) {
			publication, err := publishSystemdUnit(path, data, mode)
			if err != nil {
				return publication, err
			}
			if err = os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(path, replacement, 0o640); err != nil {
				t.Fatal(err)
			}
			return publication, nil
		},
	}
	_, err := lifecycle.Install(t.Context(), installTestRequest())
	if err == nil || !strings.Contains(err.Error(), "rollback refused") {
		t.Fatalf("Install() error = %v", err)
	}
	if daemonReloads != 1 {
		t.Fatalf("daemon reload calls = %d, want 1", daemonReloads)
	}
	got, readErr := os.ReadFile(unitPath)
	if readErr != nil || !reflect.DeepEqual(got, replacement) {
		t.Fatalf("replacement unit = %q, %v", got, readErr)
	}
}

func TestSystemdLifecycleInstallRollbackRefusesModifiedUnit(t *testing.T) {
	unitDir := t.TempDir()
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	modified := []byte("modified in place\n")
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(_ context.Context, _ bool, args ...string) (systemdRunResult, error) {
			if reflect.DeepEqual(args, systemdUnitShowArgs("picoclaw-node-main.service")) {
				return missingSystemdUnitResult(), nil
			}
			return systemdRunResult{Output: "reload failed", ExitCode: 1}, nil
		},
		publish: func(path string, data []byte, mode os.FileMode) (publishedSystemdUnit, error) {
			publication, err := publishSystemdUnit(path, data, mode)
			if err != nil {
				return publication, err
			}
			if err = os.WriteFile(path, modified, mode); err != nil {
				t.Fatal(err)
			}
			return publication, nil
		},
	}
	_, err := lifecycle.Install(t.Context(), installTestRequest())
	if err == nil || !strings.Contains(err.Error(), "rollback refused") {
		t.Fatalf("Install() error = %v", err)
	}
	got, readErr := os.ReadFile(unitPath)
	if readErr != nil || !reflect.DeepEqual(got, modified) {
		t.Fatalf("modified unit = %q, %v", got, readErr)
	}
}

func TestSystemdLifecycleInstallCleansCommittedPublicationFailure(t *testing.T) {
	unitDir := t.TempDir()
	daemonReloads := 0
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(_ context.Context, _ bool, args ...string) (systemdRunResult, error) {
			if reflect.DeepEqual(args, systemdUnitShowArgs("picoclaw-node-main.service")) {
				return missingSystemdUnitResult(), nil
			}
			daemonReloads++
			return systemdRunResult{}, nil
		},
		publish: func(path string, data []byte, mode os.FileMode) (publishedSystemdUnit, error) {
			publication, err := publishSystemdUnit(path, data, mode)
			if err != nil {
				return publication, err
			}
			return publication, errors.New("post-publication sync failed")
		},
	}
	_, err := lifecycle.Install(t.Context(), installTestRequest())
	if err == nil || !strings.Contains(err.Error(), "post-publication sync failed") {
		t.Fatalf("Install() error = %v", err)
	}
	if daemonReloads != 1 {
		t.Fatalf("rollback daemon reloads = %d, want 1", daemonReloads)
	}
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	if _, statErr := os.Stat(unitPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("committed unit still exists: %v", statErr)
	}
}

func TestSystemdLifecycleInstallRollsBackInactiveService(t *testing.T) {
	unitDir := t.TempDir()
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	var calls []systemdCall
	showChecks := 0
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(_ context.Context, system bool, args ...string) (systemdRunResult, error) {
			calls = append(calls, systemdCall{system: system, args: append([]string(nil), args...)})
			if reflect.DeepEqual(args, systemdUnitShowArgs("picoclaw-node-main.service")) {
				showChecks++
				if showChecks == 1 {
					return missingSystemdUnitResult(), nil
				}
				return loadedSystemdUnitResult(unitPath, "inactive"), nil
			}
			return systemdRunResult{}, nil
		},
		readinessInterval: 0,
	}
	_, err := lifecycle.Install(t.Context(), installTestRequest())
	if err == nil || !strings.Contains(err.Error(), `entered state "inactive"`) {
		t.Fatalf("Install() error = %v", err)
	}
	if _, statErr := os.Stat(unitPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("inactive unit still exists: %v", statErr)
	}
	wantTail := []systemdCall{
		{args: systemdUnitShowArgs("picoclaw-node-main.service")},
		{args: []string{"stop", "picoclaw-node-main.service"}},
		{args: []string{"daemon-reload"}},
	}
	if !reflect.DeepEqual(calls[len(calls)-3:], wantTail) {
		t.Fatalf("rollback calls = %#v", calls)
	}
}

func TestSystemdLifecycleInstallRollsBackFailedEnable(t *testing.T) {
	unitDir := t.TempDir()
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	var calls []systemdCall
	enabled := false
	failedOnce := false
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(_ context.Context, system bool, args ...string) (systemdRunResult, error) {
			calls = append(calls, systemdCall{system: system, args: append([]string(nil), args...)})
			if reflect.DeepEqual(args, systemdUnitShowArgs("picoclaw-node-main.service")) {
				if _, statErr := os.Stat(unitPath); errors.Is(statErr, os.ErrNotExist) {
					result := missingSystemdUnitResult()
					if enabled {
						result.Output = strings.Replace(result.Output, "UnitFileState=", "UnitFileState=enabled", 1)
					}
					return result, nil
				}
				return loadedSystemdUnitResult(unitPath, "active"), nil
			}
			if reflect.DeepEqual(args, []string{"enable", "picoclaw-node-main.service"}) {
				enabled = true
				if !failedOnce {
					failedOnce = true
					return systemdRunResult{Output: "enable failed", ExitCode: 1}, nil
				}
			}
			if reflect.DeepEqual(args, []string{"disable", "--now", "picoclaw-node-main.service"}) {
				enabled = false
			}
			return systemdRunResult{}, nil
		},
		readinessInterval: 0,
	}
	_, err := lifecycle.Install(t.Context(), installTestRequest())
	if err == nil || !strings.Contains(err.Error(), "enable failed") {
		t.Fatalf("Install() error = %v", err)
	}
	if _, statErr := os.Stat(unitPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("failed unit still exists: %v", statErr)
	}
	if enabled {
		t.Fatal("failed install left stale enablement")
	}
	wantTail := []systemdCall{
		{args: systemdUnitShowArgs("picoclaw-node-main.service")},
		{args: []string{"disable", "--now", "picoclaw-node-main.service"}},
		{args: []string{"daemon-reload"}},
	}
	if !reflect.DeepEqual(calls[len(calls)-3:], wantTail) {
		t.Fatalf("rollback calls = %#v", calls)
	}
	status, err := lifecycle.Install(t.Context(), installTestRequest())
	if err != nil || !status.Active || !enabled {
		t.Fatalf("retry install status = %#v, enabled = %t, error = %v", status, enabled, err)
	}
}

func TestSystemdInstallLockHonorsContext(t *testing.T) {
	unitDir := t.TempDir()
	lock, err := acquireSystemdUnitLock(t.Context(), unitDir, "picoclaw-node-main.service")
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 75*time.Millisecond)
	defer cancel()
	_, err = acquireSystemdUnitLock(ctx, unitDir, "picoclaw-node-main.service")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second lock error = %v", err)
	}
}

func TestSystemdInstallLockRejectsSymlink(t *testing.T) {
	unitDir := t.TempDir()
	target := filepath.Join(unitDir, "target")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(unitDir, ".picoclaw-node-main.service.install.lock")
	if err := os.Symlink(target, lockPath); err != nil {
		t.Fatal(err)
	}
	if _, err := acquireSystemdUnitLock(t.Context(), unitDir, "picoclaw-node-main.service"); err == nil {
		t.Fatal("acquireSystemdUnitLock() accepted a symlink")
	}
}

func TestRenderSystemdSystemUnit(t *testing.T) {
	unit, err := renderSystemdUnit(lifecycleRequest{
		Instance:       "vpn",
		ConfigPath:     "/etc/forgeclaw/node.json",
		ExecutablePath: "/usr/local/bin/picoclaw-node",
		ServiceUser:    "forgeclaw-node",
	}, true, strings.Repeat("a", 32))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(unit, "WantedBy=multi-user.target") ||
		!strings.Contains(unit, "User=forgeclaw-node") {
		t.Fatalf("system unit has wrong target:\n%s", unit)
	}
	if _, err = quoteSystemdArgument("bad\npath"); err == nil {
		t.Fatal("systemd argument accepted a newline")
	}
	if _, err = renderSystemdUnit(lifecycleRequest{
		Instance: "vpn", ConfigPath: "relative.json", ExecutablePath: "/bin/picoclaw-node",
	}, false, strings.Repeat("a", 32)); err == nil {
		t.Fatal("systemd unit accepted a relative config path")
	}
	if _, err = renderSystemdUnit(lifecycleRequest{
		Instance: "../vpn", ConfigPath: "/etc/node.json", ExecutablePath: "/bin/picoclaw-node",
	}, false, strings.Repeat("a", 32)); err == nil {
		t.Fatal("systemd unit accepted an invalid instance")
	}
}

func installTestRequest() lifecycleRequest {
	return lifecycleRequest{
		Instance:       "main",
		ConfigPath:     "/home/test/config.json",
		ExecutablePath: "/home/test/picoclaw-node",
	}
}
