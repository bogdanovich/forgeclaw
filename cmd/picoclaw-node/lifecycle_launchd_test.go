package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLaunchdStatusReportsManagedJob(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	lifecycle := &launchdLifecycle{
		plistDir: dir,
		domains:  []string{"gui/501", "user/501"},
		run: func(_ context.Context, args ...string) (launchdRunResult, error) {
			target := args[1]
			if strings.HasPrefix(target, "gui/") {
				return launchdRunResult{Output: launchdMissingOutput, ExitCode: 113}, nil
			}
			return launchdRunResult{Output: launchdPrintOutput(target, filepath.Join(
				dir,
				"com.forgeclaw.picoclaw-node.default.plist",
			), "running")}, nil
		},
	}
	writeManagedLaunchdPlist(t, dir, "default")

	status, err := lifecycle.Status(context.Background(), lifecycleRequest{Instance: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if !status.Installed || !status.Active || status.State != "running" {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestLaunchdStatusReportsManagedPlistNotLoaded(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	lifecycle := missingLaunchdLifecycle(dir)
	writeManagedLaunchdPlist(t, dir, "default")

	status, err := lifecycle.Status(context.Background(), lifecycleRequest{Instance: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if !status.Installed || status.Active || status.State != "not-loaded" {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestLaunchdStatusReportsNotInstalled(t *testing.T) {
	t.Parallel()
	lifecycle := missingLaunchdLifecycle(t.TempDir())

	status, err := lifecycle.Status(context.Background(), lifecycleRequest{Instance: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if status.Installed || status.Active || status.State != "not-installed" {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestLaunchdStatusRejectsUnownedPlist(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "com.forgeclaw.picoclaw-node.default.plist")
	if err := os.WriteFile(path, []byte("<plist/>"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := missingLaunchdLifecycle(dir).Status(
		context.Background(),
		lifecycleRequest{Instance: "default"},
	)
	if err == nil || !strings.Contains(err.Error(), "unowned launchd plist") {
		t.Fatalf("expected unowned plist error, got %v", err)
	}
}

func TestLaunchdStatusRejectsLoadedJobWithoutManagedPlist(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	lifecycle := &launchdLifecycle{
		plistDir: dir,
		domains:  []string{"gui/501"},
		run: func(_ context.Context, args ...string) (launchdRunResult, error) {
			target := args[1]
			return launchdRunResult{
				Output: launchdPrintOutput(target, filepath.Join(
					dir,
					"com.forgeclaw.picoclaw-node.default.plist",
				), "running"),
			}, nil
		},
	}

	_, err := lifecycle.Status(context.Background(), lifecycleRequest{Instance: "default"})
	if err == nil || !strings.Contains(err.Error(), "without its managed plist") {
		t.Fatalf("expected orphaned service error, got %v", err)
	}
}

func TestLaunchdStatusRejectsDifferentLoadedPath(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	lifecycle := &launchdLifecycle{
		plistDir: dir,
		domains:  []string{"gui/501"},
		run: func(_ context.Context, args ...string) (launchdRunResult, error) {
			return launchdRunResult{
				Output: launchdPrintOutput(args[1], "/tmp/other.plist", "running"),
			}, nil
		},
	}
	writeManagedLaunchdPlist(t, dir, "default")

	_, err := lifecycle.Status(context.Background(), lifecycleRequest{Instance: "default"})
	if err == nil || !strings.Contains(err.Error(), "outside its managed plist") {
		t.Fatalf("expected path mismatch error, got %v", err)
	}
}

func TestLaunchdStatusRejectsMultipleLoadedDomains(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	lifecycle := &launchdLifecycle{
		plistDir: dir,
		domains:  []string{"gui/501", "user/501"},
		run: func(_ context.Context, args ...string) (launchdRunResult, error) {
			return launchdRunResult{
				Output: launchdPrintOutput(args[1], filepath.Join(
					dir,
					"com.forgeclaw.picoclaw-node.default.plist",
				), "running"),
			}, nil
		},
	}
	writeManagedLaunchdPlist(t, dir, "default")

	_, err := lifecycle.Status(context.Background(), lifecycleRequest{Instance: "default"})
	if err == nil || !strings.Contains(err.Error(), "multiple domains") {
		t.Fatalf("expected ambiguous domain error, got %v", err)
	}
}

func TestLaunchdStatusFailsClosedOnUnexpectedLaunchctlError(t *testing.T) {
	t.Parallel()
	lifecycle := &launchdLifecycle{
		plistDir: t.TempDir(),
		domains:  []string{"gui/501"},
		run: func(context.Context, ...string) (launchdRunResult, error) {
			return launchdRunResult{Output: "Operation not permitted", ExitCode: 1}, nil
		},
	}

	_, err := lifecycle.Status(context.Background(), lifecycleRequest{Instance: "default"})
	if err == nil || !strings.Contains(err.Error(), "Operation not permitted") {
		t.Fatalf("expected launchctl error, got %v", err)
	}
}

func TestLaunchdStatusFailsWhenEveryDomainIsUnavailable(t *testing.T) {
	t.Parallel()
	lifecycle := &launchdLifecycle{
		plistDir: t.TempDir(),
		domains:  []string{"gui/501", "user/501"},
		run: func(context.Context, ...string) (launchdRunResult, error) {
			return launchdRunResult{
				Output:   "Domain does not support specified action",
				ExitCode: 125,
			}, nil
		},
	}

	_, err := lifecycle.Status(context.Background(), lifecycleRequest{Instance: "default"})
	if err == nil || !strings.Contains(err.Error(), "could not be inspected") {
		t.Fatalf("expected unavailable-domain error, got %v", err)
	}
}

func TestLaunchdStatusAllowsUnavailableGUIWhenUserDomainIsInspected(t *testing.T) {
	t.Parallel()
	lifecycle := &launchdLifecycle{
		plistDir: t.TempDir(),
		domains:  []string{"gui/501", "user/501"},
		run: func(_ context.Context, args ...string) (launchdRunResult, error) {
			if strings.HasPrefix(args[1], "gui/") {
				return launchdRunResult{
					Output:   "Domain does not support specified action",
					ExitCode: 125,
				}, nil
			}
			return launchdRunResult{Output: launchdMissingOutput, ExitCode: 113}, nil
		},
	}

	status, err := lifecycle.Status(context.Background(), lifecycleRequest{Instance: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if status.Installed || status.State != "not-installed" {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestLaunchdStatusAllowsMissingGUIWhenUserDomainIsLoaded(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	lifecycle := &launchdLifecycle{
		plistDir: dir,
		domains:  []string{"gui/501", "user/501"},
		run: func(_ context.Context, args ...string) (launchdRunResult, error) {
			if strings.HasPrefix(args[1], "gui/") {
				return launchdRunResult{
					Output:   "Could not find domain for user gui: 501",
					ExitCode: 113,
				}, nil
			}
			return launchdRunResult{
				Output: launchdPrintOutput(args[1], filepath.Join(
					dir,
					"com.forgeclaw.picoclaw-node.default.plist",
				), "running"),
			}, nil
		},
	}
	writeManagedLaunchdPlist(t, dir, "default")

	status, err := lifecycle.Status(context.Background(), lifecycleRequest{Instance: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if !status.Installed || !status.Active || status.State != "running" {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestLaunchdStatusRejectsMissingRequiredDomain(t *testing.T) {
	t.Parallel()
	lifecycle := &launchdLifecycle{
		plistDir: t.TempDir(),
		domains:  []string{"user/501"},
		run: func(context.Context, ...string) (launchdRunResult, error) {
			return launchdRunResult{
				Output:   "Could not find domain for user user: 501",
				ExitCode: 113,
			}, nil
		},
	}

	_, err := lifecycle.Status(context.Background(), lifecycleRequest{Instance: "default"})
	if err == nil || !strings.Contains(err.Error(), "launchctl print") {
		t.Fatalf("expected required-domain error, got %v", err)
	}
}

func TestLaunchdStatusPropagatesRunnerError(t *testing.T) {
	t.Parallel()
	want := errors.New("runner failed")
	lifecycle := &launchdLifecycle{
		plistDir: t.TempDir(),
		domains:  []string{"gui/501"},
		run: func(context.Context, ...string) (launchdRunResult, error) {
			return launchdRunResult{}, want
		},
	}

	_, err := lifecycle.Status(context.Background(), lifecycleRequest{Instance: "default"})
	if !errors.Is(err, want) {
		t.Fatalf("expected runner error, got %v", err)
	}
}

func TestParseLaunchdJobRejectsMalformedOutput(t *testing.T) {
	t.Parallel()
	target := "gui/501/com.forgeclaw.picoclaw-node.default"
	tests := []string{
		"wrong = {\n\tpath = /tmp/test.plist\n\tstate = running\n}",
		target + " = {\n\tstate = running\n}",
		target + " = {\n\tpath = /tmp/test.plist\n}",
		target + " = {\n\tpath = /tmp/test.plist\n\tpath = /tmp/other.plist\n\tstate = running\n}",
	}
	for _, output := range tests {
		if _, err := parseLaunchdJob(target, output); err == nil {
			t.Fatalf("expected malformed output to fail:\n%s", output)
		}
	}
}

func TestParseLaunchdJobIgnoresNestedState(t *testing.T) {
	t.Parallel()
	target := "gui/501/com.forgeclaw.picoclaw-node.default"
	output := target + ` = {
	path = /tmp/test.plist
	state = running
	properties = {
		state = inactive
	}
}`

	job, err := parseLaunchdJob(target, output)
	if err != nil {
		t.Fatal(err)
	}
	if job.path != "/tmp/test.plist" || job.state != "running" {
		t.Fatalf("unexpected parsed job: %+v", job)
	}
}

func missingLaunchdLifecycle(dir string) *launchdLifecycle {
	return &launchdLifecycle{
		plistDir: dir,
		domains:  []string{"gui/501", "user/501"},
		run: func(context.Context, ...string) (launchdRunResult, error) {
			return launchdRunResult{Output: launchdMissingOutput, ExitCode: 113}, nil
		},
	}
}

func writeManagedLaunchdPlist(t *testing.T, dir, instance string) {
	t.Helper()
	path := filepath.Join(dir, "com.forgeclaw.picoclaw-node."+instance+".plist")
	data := managedLaunchdPlistMarker + "\n<plist version=\"1.0\"></plist>\n"
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
}

func launchdPrintOutput(target, path, state string) string {
	return target + " = {\n\tpath = " + path + "\n\tstate = " + state + "\n}"
}

const launchdMissingOutput = "Could not find service"
