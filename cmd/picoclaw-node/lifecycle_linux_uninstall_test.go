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

func TestSystemdLifecycleUninstall(t *testing.T) {
	unitDir := trustedSystemdTempDir(t)
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	if err := os.WriteFile(unitPath, managedUninstallUnitData("installed unit"), 0o644); err != nil {
		t.Fatal(err)
	}
	var calls []systemdCall
	showChecks := 0
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(_ context.Context, system bool, args ...string) (systemdRunResult, error) {
			calls = append(calls, systemdCall{system: system, args: append([]string(nil), args...)})
			if reflect.DeepEqual(args, systemdUnitShowArgs("picoclaw-node-main.service")) {
				showChecks++
				if showChecks == 1 {
					return enabledSystemdUnitResult(unitPath, "active"), nil
				}
				return missingSystemdUnitResult(), nil
			}
			return systemdRunResult{}, nil
		},
	}
	status, err := lifecycle.Uninstall(t.Context(), lifecycleRequest{Instance: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if status.Installed || status.Active || status.State != "not-installed" {
		t.Fatalf("uninstall status = %#v", status)
	}
	if _, statErr := os.Stat(unitPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("uninstalled unit still exists: %v", statErr)
	}
	wantCalls := []systemdCall{
		{args: systemdUnitShowArgs("picoclaw-node-main.service")},
		{args: []string{"disable", "--now", "picoclaw-node-main.service"}},
		{args: []string{"daemon-reload"}},
		{args: systemdUnitShowArgs("picoclaw-node-main.service")},
	}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("uninstall calls = %#v, want %#v", calls, wantCalls)
	}
}

func TestSystemdLifecycleUninstallIsIdempotentWhenMissing(t *testing.T) {
	unitDir := filepath.Join(trustedSystemdTempDir(t), "missing", "systemd-user")
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(_ context.Context, _ bool, args ...string) (systemdRunResult, error) {
			if !reflect.DeepEqual(args, systemdUnitShowArgs("picoclaw-node-main.service")) {
				t.Fatalf("unexpected systemctl call: %v", args)
			}
			return missingSystemdUnitResult(), nil
		},
	}
	status, err := lifecycle.Uninstall(t.Context(), lifecycleRequest{Instance: "main"})
	if err != nil || status.Installed || status.State != "not-installed" {
		t.Fatalf("Uninstall() = %#v, %v", status, err)
	}
	if _, statErr := os.Stat(unitDir); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("uninstall created missing unit directory: %v", statErr)
	}
}

func TestSystemdLifecycleUninstallRejectsUnmanagedUnit(t *testing.T) {
	unitDir := trustedSystemdTempDir(t)
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	foreign := []byte("administrator unit\n")
	if err := os.WriteFile(unitPath, foreign, 0o644); err != nil {
		t.Fatal(err)
	}
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(context.Context, bool, ...string) (systemdRunResult, error) {
			t.Fatal("unmanaged unit reached systemctl")
			return systemdRunResult{}, nil
		},
	}
	_, err := lifecycle.Uninstall(t.Context(), lifecycleRequest{Instance: "main"})
	if err == nil || !strings.Contains(err.Error(), "unowned") {
		t.Fatalf("Uninstall() error = %v", err)
	}
	got, readErr := os.ReadFile(unitPath)
	if readErr != nil || !reflect.DeepEqual(got, foreign) {
		t.Fatalf("unmanaged unit = %q, %v", got, readErr)
	}
}

func TestSystemdLifecycleUninstallRefusesReplacementDuringDisable(t *testing.T) {
	unitDir := trustedSystemdTempDir(t)
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	if err := os.WriteFile(unitPath, managedUninstallUnitData("installed unit"), 0o644); err != nil {
		t.Fatal(err)
	}
	foreign := []byte("administrator replacement\n")
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(_ context.Context, _ bool, args ...string) (systemdRunResult, error) {
			if reflect.DeepEqual(args, systemdUnitShowArgs("picoclaw-node-main.service")) {
				return enabledSystemdUnitResult(unitPath, "active"), nil
			}
			if reflect.DeepEqual(args, []string{"disable", "--now", "picoclaw-node-main.service"}) {
				if err := os.Remove(unitPath); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(unitPath, foreign, 0o644); err != nil {
					t.Fatal(err)
				}
				return systemdRunResult{}, nil
			}
			t.Fatalf("unexpected systemctl call: %v", args)
			return systemdRunResult{}, nil
		},
	}
	_, err := lifecycle.Uninstall(t.Context(), lifecycleRequest{Instance: "main"})
	if err == nil || !strings.Contains(err.Error(), "no longer matches") {
		t.Fatalf("Uninstall() error = %v", err)
	}
	got, readErr := os.ReadFile(unitPath)
	if readErr != nil || !reflect.DeepEqual(got, foreign) {
		t.Fatalf("replacement unit = %q, %v", got, readErr)
	}
}

func TestSystemdLifecycleUninstallRollsBackReloadFailure(t *testing.T) {
	unitDir := trustedSystemdTempDir(t)
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	original := managedUninstallUnitData("installed unit")
	if err := os.WriteFile(unitPath, original, 0o644); err != nil {
		t.Fatal(err)
	}
	var calls []systemdCall
	daemonReloads := 0
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(_ context.Context, system bool, args ...string) (systemdRunResult, error) {
			calls = append(calls, systemdCall{system: system, args: append([]string(nil), args...)})
			if reflect.DeepEqual(args, systemdUnitShowArgs("picoclaw-node-main.service")) {
				return enabledSystemdUnitResult(unitPath, "active"), nil
			}
			if reflect.DeepEqual(args, []string{"daemon-reload"}) {
				daemonReloads++
				if daemonReloads == 1 {
					return systemdRunResult{Output: "reload failed", ExitCode: 1}, nil
				}
			}
			return systemdRunResult{}, nil
		},
	}
	_, err := lifecycle.Uninstall(t.Context(), lifecycleRequest{Instance: "main"})
	if err == nil || !strings.Contains(err.Error(), "reload failed") {
		t.Fatalf("Uninstall() error = %v", err)
	}
	got, readErr := os.ReadFile(unitPath)
	if readErr != nil || !reflect.DeepEqual(got, original) {
		t.Fatalf("restored unit = %q, %v", got, readErr)
	}
	wantTail := []systemdCall{
		{args: []string{"daemon-reload"}},
		{args: systemdUnitShowArgs("picoclaw-node-main.service")},
		{args: []string{"enable", "picoclaw-node-main.service"}},
		{args: []string{"start", "picoclaw-node-main.service"}},
	}
	if !reflect.DeepEqual(calls[len(calls)-4:], wantTail) {
		t.Fatalf("rollback calls = %#v", calls)
	}
}

func TestSystemdLifecycleUninstallCompensatesFailedDisable(t *testing.T) {
	unitDir := trustedSystemdTempDir(t)
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	if err := os.WriteFile(unitPath, managedUninstallUnitData("installed unit"), 0o644); err != nil {
		t.Fatal(err)
	}
	enabled := true
	active := true
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(_ context.Context, _ bool, args ...string) (systemdRunResult, error) {
			switch {
			case reflect.DeepEqual(args, systemdUnitShowArgs("picoclaw-node-main.service")):
				return enabledSystemdUnitResult(unitPath, "active"), nil
			case reflect.DeepEqual(args, []string{"disable", "--now", "picoclaw-node-main.service"}):
				enabled = false
				active = false
				return systemdRunResult{Output: "partial disable failed", ExitCode: 1}, nil
			case reflect.DeepEqual(args, []string{"enable", "picoclaw-node-main.service"}):
				enabled = true
			case reflect.DeepEqual(args, []string{"start", "picoclaw-node-main.service"}):
				active = true
			default:
				t.Fatalf("unexpected systemctl call: %v", args)
			}
			return systemdRunResult{}, nil
		},
	}
	_, err := lifecycle.Uninstall(t.Context(), lifecycleRequest{Instance: "main"})
	if err == nil || !strings.Contains(err.Error(), "partial disable failed") {
		t.Fatalf("Uninstall() error = %v", err)
	}
	if !enabled || !active {
		t.Fatalf("failed disable compensation: enabled=%t active=%t", enabled, active)
	}
	if _, statErr := os.Stat(unitPath); statErr != nil {
		t.Fatalf("managed unit missing after failed disable: %v", statErr)
	}
}

func TestSystemdLifecycleUninstallRollsBackPreRemovalFailure(t *testing.T) {
	unitDir := trustedSystemdTempDir(t)
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	original := managedUninstallUnitData("installed unit")
	if err := os.WriteFile(unitPath, original, 0o644); err != nil {
		t.Fatal(err)
	}
	showChecks := 0
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(_ context.Context, _ bool, args ...string) (systemdRunResult, error) {
			if reflect.DeepEqual(args, systemdUnitShowArgs("picoclaw-node-main.service")) {
				showChecks++
				if showChecks == 2 {
					return missingSystemdUnitResult(), nil
				}
				return enabledSystemdUnitResult(unitPath, "active"), nil
			}
			return systemdRunResult{}, nil
		},
		remove: func(publishedSystemdUnit) (bool, error) {
			return false, errors.New("pre-removal failure")
		},
	}
	_, err := lifecycle.Uninstall(t.Context(), lifecycleRequest{Instance: "main"})
	if err == nil || !strings.Contains(err.Error(), "pre-removal failure") {
		t.Fatalf("Uninstall() error = %v", err)
	}
	got, readErr := os.ReadFile(unitPath)
	if readErr != nil || !reflect.DeepEqual(got, original) {
		t.Fatalf("restored unit = %q, %v", got, readErr)
	}
}

func TestSystemdLifecycleUninstallDoesNotRollbackCommittedRemoval(t *testing.T) {
	unitDir := trustedSystemdTempDir(t)
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	if err := os.WriteFile(unitPath, managedUninstallUnitData("installed unit"), 0o644); err != nil {
		t.Fatal(err)
	}
	showChecks := 0
	lifecycle := &systemdLifecycle{
		unitDir: unitDir,
		run: func(_ context.Context, _ bool, args ...string) (systemdRunResult, error) {
			if reflect.DeepEqual(args, systemdUnitShowArgs("picoclaw-node-main.service")) {
				showChecks++
				if showChecks == 1 {
					return enabledSystemdUnitResult(unitPath, "active"), nil
				}
				return missingSystemdUnitResult(), nil
			}
			return systemdRunResult{}, nil
		},
		remove: func(publication publishedSystemdUnit) (bool, error) {
			removed, err := removeQuarantinedSystemdUnit(publication)
			if err != nil {
				return removed, err
			}
			return true, errors.New("post-removal sync failure")
		},
	}
	_, err := lifecycle.Uninstall(t.Context(), lifecycleRequest{Instance: "main"})
	if err == nil || !strings.Contains(err.Error(), "post-removal sync failure") {
		t.Fatalf("Uninstall() error = %v", err)
	}
	if _, statErr := os.Stat(unitPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("committed uninstall restored unit: %v", statErr)
	}
	if showChecks != 2 {
		t.Fatalf("systemd show checks = %d, want no rollback check", showChecks)
	}
}

func TestCaptureSystemdUninstallStateRejectsNonRestorableStates(t *testing.T) {
	for _, activeState := range []string{"activating", "reloading", "deactivating", "failed"} {
		t.Run(activeState, func(t *testing.T) {
			_, err := captureSystemdUninstallState(systemdUnitProperties{
				activeState: activeState, unitFileState: "enabled",
			})
			if err == nil || !strings.Contains(err.Error(), "ActiveState") {
				t.Fatalf("captureSystemdUninstallState() error = %v", err)
			}
		})
	}
	runtimeEnabled, err := captureSystemdUninstallState(systemdUnitProperties{
		activeState: "inactive", unitFileState: "enabled-runtime",
	})
	if err != nil || !reflect.DeepEqual(runtimeEnabled.enableArguments, []string{"enable", "--runtime"}) {
		t.Fatalf("runtime enablement state = %#v, %v", runtimeEnabled, err)
	}
}

func TestValidateManagedSystemdInstallSectionRejectsSideEffects(t *testing.T) {
	for _, directive := range []string{
		"Also=unrelated.service",
		"Alias=trusted-looking.service",
		"RequiredBy=multi-user.target",
		"WantedBy=graphical.target",
	} {
		t.Run(directive, func(t *testing.T) {
			data := []byte(managedSystemdUnitMarker + "\n[Install]\nWantedBy=default.target\n" + directive + "\n")
			if err := validateManagedSystemdInstallSection(data, false); err == nil {
				t.Fatal("unsafe install directive accepted")
			}
		})
	}
	if err := validateManagedSystemdInstallSection(managedUninstallUnitData("installed unit"), false); err != nil {
		t.Fatalf("generated user install section rejected: %v", err)
	}
	systemData := []byte(managedSystemdUnitMarker + "\n[Install]\nWantedBy=multi-user.target\n")
	if err := validateManagedSystemdInstallSection(systemData, true); err != nil {
		t.Fatalf("generated system install section rejected: %v", err)
	}
}

func enabledSystemdUnitResult(path, activeState string) systemdRunResult {
	result := loadedSystemdUnitResult(path, activeState)
	result.Output = strings.Replace(result.Output, "UnitFileState=disabled", "UnitFileState=enabled", 1)
	return result
}

func managedUninstallUnitData(body string) []byte {
	return []byte(managedSystemdUnitMarker + "\n" + body + "\n[Install]\nWantedBy=default.target\n")
}
