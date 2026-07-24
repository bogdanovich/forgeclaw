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

func TestLaunchdInstallBootstrapsGUIAndWaitsForStableRunning(t *testing.T) {
	t.Parallel()
	dir := trustedLaunchdTempDir(t)
	path := filepath.Join(dir, defaultLaunchdLabel+".plist")
	bootstrapped := false
	runningObservations := 0
	var calls [][]string
	lifecycle := &launchdLifecycle{
		plistDir:          dir,
		domains:           []string{"gui/501", "user/501"},
		readinessInterval: time.Nanosecond,
		readinessAttempts: 4,
		readinessStable:   3,
		run: func(_ context.Context, args ...string) (launchdRunResult, error) {
			calls = append(calls, append([]string(nil), args...))
			if reflect.DeepEqual(args, []string{"bootstrap", "gui/501", path}) {
				bootstrapped = true
				return launchdRunResult{}, nil
			}
			if len(args) != 2 || args[0] != "print" {
				t.Fatalf("unexpected launchctl call: %v", args)
			}
			if bootstrapped && strings.HasPrefix(args[1], "gui/") {
				runningObservations++
				return launchdRunResult{
					Output: launchdPrintOutput(defaultLaunchdLabel, path, "running"),
				}, nil
			}
			return launchdRunResult{Output: launchdMissingOutput, ExitCode: 113}, nil
		},
	}

	status, err := lifecycle.Install(t.Context(), launchdInstallRequest())
	if err != nil {
		t.Fatal(err)
	}
	if !status.Installed || !status.Active || status.State != "running" {
		t.Fatalf("unexpected status: %+v", status)
	}
	if runningObservations != 3 {
		t.Fatalf("running observations = %d, want 3", runningObservations)
	}
	if !containsLaunchdCall(calls, []string{"bootstrap", "gui/501", path}) {
		t.Fatalf("launchctl calls omitted GUI bootstrap: %v", calls)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !hasLaunchdPlistMarker(data) {
		t.Fatal("installed plist omitted management marker")
	}
	label, err := parseLaunchdPlistLabel(data)
	if err != nil || label != defaultLaunchdLabel {
		t.Fatalf("installed plist label = %q, %v", label, err)
	}
}

func TestLaunchdInstallFallsBackToHeadlessUserDomain(t *testing.T) {
	t.Parallel()
	dir := trustedLaunchdTempDir(t)
	path := filepath.Join(dir, defaultLaunchdLabel+".plist")
	bootstrapped := false
	var bootstrap []string
	lifecycle := &launchdLifecycle{
		plistDir:          dir,
		domains:           []string{"gui/501", "user/501"},
		readinessInterval: time.Nanosecond,
		readinessAttempts: 1,
		readinessStable:   1,
		run: func(_ context.Context, args ...string) (launchdRunResult, error) {
			if args[0] == "bootstrap" {
				bootstrap = append([]string(nil), args...)
				bootstrapped = true
				return launchdRunResult{}, nil
			}
			if strings.HasPrefix(args[1], "gui/") {
				return launchdRunResult{
					Output:   "Could not find domain for user gui: 501",
					ExitCode: 113,
				}, nil
			}
			if bootstrapped {
				return launchdRunResult{
					Output: launchdPrintOutput(defaultLaunchdLabel, path, "running"),
				}, nil
			}
			return launchdRunResult{Output: launchdMissingOutput, ExitCode: 113}, nil
		},
	}

	if _, err := lifecycle.Install(t.Context(), launchdInstallRequest()); err != nil {
		t.Fatal(err)
	}
	want := []string{"bootstrap", "user/501", path}
	if !reflect.DeepEqual(bootstrap, want) {
		t.Fatalf("bootstrap = %v, want %v", bootstrap, want)
	}
}

func TestLaunchdInstallCreatesMissingUserLaunchAgentsDirectory(t *testing.T) {
	t.Parallel()
	parent := trustedLaunchdTempDir(t)
	dir := filepath.Join(parent, "LaunchAgents")
	path := filepath.Join(dir, defaultLaunchdLabel+".plist")
	loaded := false
	lifecycle := &launchdLifecycle{
		plistDir:          dir,
		domains:           []string{"user/501"},
		readinessInterval: time.Nanosecond,
		readinessAttempts: 1,
		readinessStable:   1,
		run: func(_ context.Context, args ...string) (launchdRunResult, error) {
			if args[0] == "bootstrap" {
				loaded = true
				return launchdRunResult{}, nil
			}
			if loaded {
				return launchdRunResult{
					Output: launchdPrintOutput(defaultLaunchdLabel, path, "running"),
				}, nil
			}
			return launchdRunResult{Output: launchdMissingOutput, ExitCode: 113}, nil
		},
	}

	if _, err := lifecycle.Install(t.Context(), launchdInstallRequest()); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("created LaunchAgents mode = %v", info.Mode())
	}
}

func TestOpenLaunchdPlistDirectoryDoesNotCreateSystemDirectory(t *testing.T) {
	t.Parallel()
	parent := trustedLaunchdTempDir(t)
	path := filepath.Join(parent, "LaunchDaemons")
	_, err := openLaunchdPlistDirectory(path, true)
	if err == nil || !strings.Contains(err.Error(), "open launchd plist directory") {
		t.Fatalf("open system directory error = %v", err)
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("system directory was created: %v", statErr)
	}
}

func TestLaunchdInstallIsCreateOnly(t *testing.T) {
	t.Parallel()
	dir := trustedLaunchdTempDir(t)
	writeManagedLaunchdPlist(t, dir, "default")
	called := false
	lifecycle := &launchdLifecycle{
		plistDir: dir,
		domains:  []string{"user/501"},
		run: func(context.Context, ...string) (launchdRunResult, error) {
			called = true
			return launchdRunResult{}, nil
		},
	}

	_, err := lifecycle.Install(t.Context(), launchdInstallRequest())
	if err == nil || !strings.Contains(err.Error(), "create-only") {
		t.Fatalf("Install() error = %v", err)
	}
	if called {
		t.Fatal("queried launchd after finding an existing managed plist")
	}
}

func TestLaunchdInstallRejectsLoadedServiceBeforePublishing(t *testing.T) {
	t.Parallel()
	dir := trustedLaunchdTempDir(t)
	lifecycle := &launchdLifecycle{
		plistDir: dir,
		domains:  []string{"user/501"},
		run: func(_ context.Context, args ...string) (launchdRunResult, error) {
			return launchdRunResult{
				Output: launchdPrintOutput(defaultLaunchdLabel, "/tmp/foreign.plist", "running"),
			}, nil
		},
	}

	_, err := lifecycle.Install(t.Context(), launchdInstallRequest())
	if err == nil || !strings.Contains(err.Error(), "already loaded") {
		t.Fatalf("Install() error = %v", err)
	}
	path := filepath.Join(dir, defaultLaunchdLabel+".plist")
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("plist was published before rejecting loaded service: %v", statErr)
	}
}

func TestLaunchdInstallRollsBackFailedBootstrap(t *testing.T) {
	t.Parallel()
	dir := trustedLaunchdTempDir(t)
	path := filepath.Join(dir, defaultLaunchdLabel+".plist")
	bootstrapAttempted := false
	lifecycle := &launchdLifecycle{
		plistDir: dir,
		domains:  []string{"user/501"},
		run: func(_ context.Context, args ...string) (launchdRunResult, error) {
			if args[0] == "bootstrap" {
				bootstrapAttempted = true
				return launchdRunResult{Output: "bootstrap failed", ExitCode: 5}, nil
			}
			return launchdRunResult{Output: launchdMissingOutput, ExitCode: 113}, nil
		},
	}

	_, err := lifecycle.Install(t.Context(), launchdInstallRequest())
	if err == nil || !strings.Contains(err.Error(), "bootstrap failed") {
		t.Fatalf("Install() error = %v", err)
	}
	if !bootstrapAttempted {
		t.Fatal("bootstrap was not attempted")
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("failed install left plist behind: %v", statErr)
	}
}

func TestLaunchdInstallBootsOutAndRemovesUnreadyService(t *testing.T) {
	t.Parallel()
	dir := trustedLaunchdTempDir(t)
	path := filepath.Join(dir, defaultLaunchdLabel+".plist")
	loaded := false
	bootout := false
	lifecycle := &launchdLifecycle{
		plistDir:          dir,
		domains:           []string{"user/501"},
		readinessInterval: time.Nanosecond,
		readinessAttempts: 2,
		readinessStable:   2,
		run: func(_ context.Context, args ...string) (launchdRunResult, error) {
			switch args[0] {
			case "bootstrap":
				loaded = true
				return launchdRunResult{}, nil
			case "bootout":
				bootout = true
				loaded = false
				return launchdRunResult{}, nil
			case "print":
				if loaded {
					return launchdRunResult{
						Output: launchdPrintOutput(defaultLaunchdLabel, path, "waiting"),
					}, nil
				}
				return launchdRunResult{Output: launchdMissingOutput, ExitCode: 113}, nil
			default:
				t.Fatalf("unexpected launchctl call: %v", args)
				return launchdRunResult{}, nil
			}
		},
	}

	_, err := lifecycle.Install(t.Context(), launchdInstallRequest())
	if err == nil || !strings.Contains(err.Error(), "did not remain running") {
		t.Fatalf("Install() error = %v", err)
	}
	if !bootout {
		t.Fatal("unready service was not booted out")
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("failed install left plist behind: %v", statErr)
	}
}

func TestLaunchdInstallPreservesPlistWhenRollbackCannotInspectJob(t *testing.T) {
	t.Parallel()
	dir := trustedLaunchdTempDir(t)
	path := filepath.Join(dir, defaultLaunchdLabel+".plist")
	bootstrapAttempted := false
	lifecycle := &launchdLifecycle{
		plistDir: dir,
		domains:  []string{"user/501"},
		run: func(_ context.Context, args ...string) (launchdRunResult, error) {
			if args[0] == "bootstrap" {
				bootstrapAttempted = true
				return launchdRunResult{Output: "bootstrap failed", ExitCode: 5}, nil
			}
			if bootstrapAttempted {
				return launchdRunResult{Output: "Operation not permitted", ExitCode: 1}, nil
			}
			return launchdRunResult{Output: launchdMissingOutput, ExitCode: 113}, nil
		},
	}

	_, err := lifecycle.Install(t.Context(), launchdInstallRequest())
	if err == nil || !strings.Contains(err.Error(), "inspect launchd service during rollback") {
		t.Fatalf("Install() error = %v", err)
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("uncertain rollback removed managed plist: %v", statErr)
	}
}

func TestLaunchdInstallRollbackRefusesModifiedPlist(t *testing.T) {
	t.Parallel()
	dir := trustedLaunchdTempDir(t)
	path := filepath.Join(dir, defaultLaunchdLabel+".plist")
	replacement := []byte("administrator replacement\n")
	bootstrapAttempted := false
	lifecycle := &launchdLifecycle{
		plistDir: dir,
		domains:  []string{"user/501"},
		run: func(_ context.Context, args ...string) (launchdRunResult, error) {
			if args[0] == "bootstrap" {
				bootstrapAttempted = true
				if err := os.WriteFile(path, replacement, 0o600); err != nil {
					t.Fatal(err)
				}
				return launchdRunResult{Output: "bootstrap failed", ExitCode: 5}, nil
			}
			return launchdRunResult{Output: launchdMissingOutput, ExitCode: 113}, nil
		},
	}

	_, err := lifecycle.Install(t.Context(), launchdInstallRequest())
	if err == nil || !strings.Contains(err.Error(), "rollback refused") {
		t.Fatalf("Install() error = %v", err)
	}
	if !bootstrapAttempted {
		t.Fatal("bootstrap was not attempted")
	}
	got, readErr := os.ReadFile(path)
	if readErr != nil || !reflect.DeepEqual(got, replacement) {
		t.Fatalf("replacement plist = %q, %v", got, readErr)
	}
}

func TestRenderLaunchdPlistIncludesBoundedServiceDefinition(t *testing.T) {
	t.Parallel()
	request := launchdInstallRequest()
	request.ConfigPath = "/tmp/config & policy.json"
	_, err := renderLaunchdPlist(
		request,
		true,
		defaultLaunchdLabel,
		"00112233445566778899aabbccddeeff",
	)
	if err == nil {
		t.Fatal("system plist accepted missing service user")
	}
	request.ServiceUser = "node_runner"
	plist, err := renderLaunchdPlist(
		request,
		true,
		defaultLaunchdLabel,
		"00112233445566778899aabbccddeeff",
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		managedLaunchdPlistMarker,
		launchdInstallTransactionMarker + "00112233445566778899aabbccddeeff",
		"<string>/opt/forgeclaw/picoclaw-node</string>",
		"<string>/tmp/config &amp; policy.json</string>",
		"<key>UserName</key>",
		"<string>node_runner</string>",
		"<key>RunAtLoad</key>",
		"<key>KeepAlive</key>",
	} {
		if !strings.Contains(plist, want) {
			t.Fatalf("rendered plist omitted %q:\n%s", want, plist)
		}
	}
	label, err := parseLaunchdPlistLabel([]byte(plist))
	if err != nil || label != defaultLaunchdLabel {
		t.Fatalf("rendered plist label = %q, %v", label, err)
	}
}

func launchdInstallRequest() lifecycleRequest {
	return lifecycleRequest{
		Instance:       "default",
		ConfigPath:     "/tmp/picoclaw-node-config.json",
		ExecutablePath: "/opt/forgeclaw/picoclaw-node",
	}
}

func trustedLaunchdTempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func containsLaunchdCall(calls [][]string, want []string) bool {
	for _, call := range calls {
		if reflect.DeepEqual(call, want) {
			return true
		}
	}
	return false
}
