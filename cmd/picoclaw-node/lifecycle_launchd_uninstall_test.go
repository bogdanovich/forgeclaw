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

func TestLaunchdUninstallBootsOutAndRemovesManagedService(t *testing.T) {
	t.Parallel()
	dir := trustedLaunchdTempDir(t)
	path := filepath.Join(dir, defaultLaunchdLabel+".plist")
	writeManagedLaunchdPlist(t, dir, "default")
	loaded := true
	var calls [][]string
	lifecycle := &launchdLifecycle{
		plistDir: dir,
		domains:  []string{"user/501"},
		run: func(_ context.Context, args ...string) (launchdRunResult, error) {
			calls = append(calls, append([]string(nil), args...))
			switch args[0] {
			case "print":
				if loaded {
					return launchdRunResult{
						Output: launchdPrintOutput(args[1], path, "running"),
					}, nil
				}
				return launchdRunResult{Output: launchdMissingOutput, ExitCode: 113}, nil
			case "bootout":
				loaded = false
				return launchdRunResult{}, nil
			default:
				t.Fatalf("unexpected launchctl call: %v", args)
				return launchdRunResult{}, nil
			}
		},
	}

	status, err := lifecycle.Uninstall(t.Context(), lifecycleRequest{Instance: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if status.Installed || status.Active || status.State != "not-installed" {
		t.Fatalf("unexpected status: %+v", status)
	}
	if !containsLaunchdCall(calls, []string{"bootout", "user/501/" + defaultLaunchdLabel}) {
		t.Fatalf("launchctl calls omitted bootout: %v", calls)
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("uninstalled plist remains: %v", statErr)
	}
}

func TestLaunchdUninstallRemovesUnloadedManagedPlist(t *testing.T) {
	t.Parallel()
	dir := trustedLaunchdTempDir(t)
	path := filepath.Join(dir, defaultLaunchdLabel+".plist")
	writeManagedLaunchdPlist(t, dir, "default")
	lifecycle := missingLaunchdLifecycle(dir)

	if _, err := lifecycle.Uninstall(t.Context(), lifecycleRequest{Instance: "default"}); err != nil {
		t.Fatal(err)
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("uninstalled plist remains: %v", statErr)
	}
}

func TestLaunchdUninstallDoesNotCreateMissingUserDirectory(t *testing.T) {
	t.Parallel()
	parent := trustedLaunchdTempDir(t)
	dir := filepath.Join(parent, "LaunchAgents")
	lifecycle := &launchdLifecycle{
		plistDir: dir,
		domains:  []string{"user/501"},
		run: func(context.Context, ...string) (launchdRunResult, error) {
			return launchdRunResult{Output: launchdMissingOutput, ExitCode: 113}, nil
		},
	}

	status, err := lifecycle.Uninstall(t.Context(), lifecycleRequest{Instance: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if status.Installed || status.State != "not-installed" {
		t.Fatalf("unexpected status: %+v", status)
	}
	if _, statErr := os.Stat(dir); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("uninstall created user directory: %v", statErr)
	}
}

func TestLaunchdUninstallRejectsForeignLoadedPath(t *testing.T) {
	t.Parallel()
	dir := trustedLaunchdTempDir(t)
	path := filepath.Join(dir, defaultLaunchdLabel+".plist")
	writeManagedLaunchdPlist(t, dir, "default")
	lifecycle := &launchdLifecycle{
		plistDir: dir,
		domains:  []string{"user/501"},
		run: func(_ context.Context, args ...string) (launchdRunResult, error) {
			return launchdRunResult{
				Output: launchdPrintOutput(args[1], "/tmp/foreign.plist", "running"),
			}, nil
		},
	}

	_, err := lifecycle.Uninstall(t.Context(), lifecycleRequest{Instance: "default"})
	if err == nil || !strings.Contains(err.Error(), "outside its managed plist") {
		t.Fatalf("Uninstall() error = %v", err)
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("managed plist was removed: %v", statErr)
	}
}

func TestLaunchdUninstallPreservesStateWhenBootoutFails(t *testing.T) {
	t.Parallel()
	dir := trustedLaunchdTempDir(t)
	path := filepath.Join(dir, defaultLaunchdLabel+".plist")
	writeManagedLaunchdPlist(t, dir, "default")
	loaded := true
	lifecycle := &launchdLifecycle{
		plistDir: dir,
		domains:  []string{"user/501"},
		run: func(_ context.Context, args ...string) (launchdRunResult, error) {
			if args[0] == "bootout" {
				return launchdRunResult{Output: "bootout denied", ExitCode: 5}, nil
			}
			if loaded {
				return launchdRunResult{
					Output: launchdPrintOutput(args[1], path, "running"),
				}, nil
			}
			return launchdRunResult{Output: launchdMissingOutput, ExitCode: 113}, nil
		},
	}

	_, err := lifecycle.Uninstall(t.Context(), lifecycleRequest{Instance: "default"})
	if err == nil || !strings.Contains(err.Error(), "bootout denied") {
		t.Fatalf("Uninstall() error = %v", err)
	}
	if !loaded {
		t.Fatal("failed bootout changed loaded state")
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("managed plist was removed: %v", statErr)
	}
}

func TestLaunchdUninstallRestoresPlistAndServiceAfterRemovalFailure(t *testing.T) {
	t.Parallel()
	dir := trustedLaunchdTempDir(t)
	path := filepath.Join(dir, defaultLaunchdLabel+".plist")
	writeManagedLaunchdPlist(t, dir, "default")
	loaded := true
	bootstrapCalls := 0
	lifecycle := &launchdLifecycle{
		plistDir: dir,
		domains:  []string{"user/501"},
		run: func(_ context.Context, args ...string) (launchdRunResult, error) {
			switch args[0] {
			case "print":
				if loaded {
					return launchdRunResult{
						Output: launchdPrintOutput(args[1], path, "running"),
					}, nil
				}
				return launchdRunResult{Output: launchdMissingOutput, ExitCode: 113}, nil
			case "bootout":
				loaded = false
				return launchdRunResult{}, nil
			case "bootstrap":
				bootstrapCalls++
				loaded = true
				return launchdRunResult{}, nil
			default:
				t.Fatalf("unexpected launchctl call: %v", args)
				return launchdRunResult{}, nil
			}
		},
		remove: func(publishedLaunchdPlist) (bool, error) {
			return false, errors.New("remove failed")
		},
	}

	_, err := lifecycle.Uninstall(t.Context(), lifecycleRequest{Instance: "default"})
	if err == nil || !strings.Contains(err.Error(), "remove failed") {
		t.Fatalf("Uninstall() error = %v", err)
	}
	if !loaded || bootstrapCalls != 1 {
		t.Fatalf("restored loaded=%t bootstrap calls=%d", loaded, bootstrapCalls)
	}
	data, readErr := os.ReadFile(path)
	if readErr != nil || !hasLaunchdPlistMarker(data) {
		t.Fatalf("restored plist = %q, %v", data, readErr)
	}
}

func TestLaunchdUninstallReportsCommittedRemovalFailureWithoutRollback(t *testing.T) {
	t.Parallel()
	dir := trustedLaunchdTempDir(t)
	path := filepath.Join(dir, defaultLaunchdLabel+".plist")
	writeManagedLaunchdPlist(t, dir, "default")
	lifecycle := missingLaunchdLifecycle(dir)
	lifecycle.remove = func(publication publishedLaunchdPlist) (bool, error) {
		if err := os.Remove(filepath.Join(publication.directory.path, publication.name)); err != nil {
			t.Fatal(err)
		}
		return true, errors.New("directory sync failed")
	}

	_, err := lifecycle.Uninstall(t.Context(), lifecycleRequest{Instance: "default"})
	if err == nil || !strings.Contains(err.Error(), "directory sync failed") {
		t.Fatalf("Uninstall() error = %v", err)
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("committed removal was rolled back: %v", statErr)
	}
}

func TestLaunchdUninstallRejectsUnownedPlist(t *testing.T) {
	t.Parallel()
	dir := trustedLaunchdTempDir(t)
	path := filepath.Join(dir, defaultLaunchdLabel+".plist")
	foreign := []byte("<plist><dict/></plist>\n")
	if err := os.WriteFile(path, foreign, 0o600); err != nil {
		t.Fatal(err)
	}
	lifecycle := missingLaunchdLifecycle(dir)

	_, err := lifecycle.Uninstall(t.Context(), lifecycleRequest{Instance: "default"})
	if err == nil || !strings.Contains(err.Error(), "unowned launchd plist") {
		t.Fatalf("Uninstall() error = %v", err)
	}
	got, readErr := os.ReadFile(path)
	if readErr != nil || !reflect.DeepEqual(got, foreign) {
		t.Fatalf("foreign plist = %q, %v", got, readErr)
	}
}
