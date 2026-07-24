package main

import (
	"context"
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveLaunchdUserHomeUsesUIDAccountInsteadOfEnvironment(t *testing.T) {
	t.Setenv("HOME", "/tmp/wrong-home")
	calledWith := ""

	home, err := resolveLaunchdUserHome(501, func(uid string) (*user.User, error) {
		calledWith = uid
		return &user.User{Uid: "501", HomeDir: "/Users/forgeclaw"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calledWith != "501" || home != "/Users/forgeclaw" {
		t.Fatalf("resolved uid %q home %q", calledWith, home)
	}
}

func TestResolveLaunchdUserHomeRejectsMismatchedIdentity(t *testing.T) {
	t.Parallel()
	_, err := resolveLaunchdUserHome(501, func(string) (*user.User, error) {
		return &user.User{Uid: "502", HomeDir: "/Users/other"}, nil
	})
	if err == nil || !strings.Contains(err.Error(), "invalid identity") {
		t.Fatalf("expected identity error, got %v", err)
	}
}

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
			return launchdRunResult{Output: launchdPrintOutput(defaultLaunchdLabel, filepath.Join(
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

func TestLaunchdStatusRejectsInvalidManagedPlistIdentity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "malformed XML",
			body: "<plist><dict>",
			want: "parse managed launchd plist",
		},
		{
			name: "missing Label",
			body: "<plist><dict><key>Program</key><string>/bin/true</string></dict></plist>",
			want: "omits Label",
		},
		{
			name: "mismatched Label",
			body: "<plist><dict><key>Label</key><string>com.example.other</string></dict></plist>",
			want: "with label",
		},
		{
			name: "duplicate Label",
			body: "<plist><dict><key>Label</key><string>com.forgeclaw.picoclaw-node.default</string>" +
				"<key>Label</key><string>com.forgeclaw.picoclaw-node.default</string></dict></plist>",
			want: "duplicate Label",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			path := filepath.Join(dir, "com.forgeclaw.picoclaw-node.default.plist")
			data := managedLaunchdPlistMarker + "\n" + test.body + "\n"
			if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			called := false
			lifecycle := &launchdLifecycle{
				plistDir: dir,
				domains:  []string{"user/501"},
				run: func(context.Context, ...string) (launchdRunResult, error) {
					called = true
					return launchdRunResult{}, nil
				},
			}

			_, err := lifecycle.Status(
				context.Background(),
				lifecycleRequest{Instance: "default"},
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected %q error, got %v", test.want, err)
			}
			if called {
				t.Fatal("queried launchd before validating plist identity")
			}
		})
	}
}

func TestLaunchdStatusRejectsLoadedJobWithoutManagedPlist(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	lifecycle := &launchdLifecycle{
		plistDir: dir,
		domains:  []string{"gui/501"},
		run: func(context.Context, ...string) (launchdRunResult, error) {
			return launchdRunResult{
				Output: launchdPrintOutput(defaultLaunchdLabel, filepath.Join(
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
				Output: launchdPrintOutput(defaultLaunchdLabel, "/tmp/other.plist", "running"),
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
				Output: launchdPrintOutput(defaultLaunchdLabel, filepath.Join(
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

func TestLaunchdStatusRejectsUnavailableGUIWithoutTrustingUserDomain(t *testing.T) {
	t.Parallel()
	for _, userLoaded := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing user job", true: "loaded user job"}[userLoaded], func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			calls := 0
			lifecycle := &launchdLifecycle{
				plistDir: dir,
				domains:  []string{"gui/501", "user/501"},
				run: func(_ context.Context, args ...string) (launchdRunResult, error) {
					calls++
					if strings.HasPrefix(args[1], "gui/") {
						return launchdRunResult{
							Output:   "Domain does not support specified action",
							ExitCode: 125,
						}, nil
					}
					if userLoaded {
						return launchdRunResult{
							Output: launchdPrintOutput(defaultLaunchdLabel, filepath.Join(
								dir,
								"com.forgeclaw.picoclaw-node.default.plist",
							), "running"),
						}, nil
					}
					return launchdRunResult{Output: launchdMissingOutput, ExitCode: 113}, nil
				},
			}
			if userLoaded {
				writeManagedLaunchdPlist(t, dir, "default")
			}

			_, err := lifecycle.Status(
				context.Background(),
				lifecycleRequest{Instance: "default"},
			)
			if err == nil || !strings.Contains(err.Error(), "Domain does not support") {
				t.Fatalf("expected unavailable-domain error, got %v", err)
			}
			if calls != 1 {
				t.Fatalf("queried %d domains after inconclusive GUI result, want 1", calls)
			}
		})
	}
}

func TestLaunchdStatusFailsWhenSystemDomainIsUnavailable(t *testing.T) {
	t.Parallel()
	lifecycle := &launchdLifecycle{
		system:   true,
		plistDir: t.TempDir(),
		domains:  []string{"system"},
		run: func(context.Context, ...string) (launchdRunResult, error) {
			return launchdRunResult{
				Output:   "Domain does not support specified action",
				ExitCode: 125,
			}, nil
		},
	}

	_, err := lifecycle.Status(context.Background(), lifecycleRequest{Instance: "default"})
	if err == nil || !strings.Contains(err.Error(), "Domain does not support") {
		t.Fatalf("expected unavailable-domain error, got %v", err)
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
				Output: launchdPrintOutput(defaultLaunchdLabel, filepath.Join(
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
		target + " = {\n\tpath = /tmp/test.plist\n\tstate = running\n}",
		defaultLaunchdLabel + " = {\n\tstate = running\n}",
		defaultLaunchdLabel + " = {\n\tpath = /tmp/test.plist\n}",
		defaultLaunchdLabel + " = {\n\tpath = /tmp/test.plist\n\tpath = /tmp/other.plist\n\tstate = running\n}",
	}
	for _, output := range tests {
		if _, err := parseLaunchdJob(target, defaultLaunchdLabel, output); err == nil {
			t.Fatalf("expected malformed output to fail:\n%s", output)
		}
	}
}

func TestParseLaunchdJobIgnoresNestedState(t *testing.T) {
	t.Parallel()
	target := "gui/501/com.forgeclaw.picoclaw-node.default"
	output := defaultLaunchdLabel + ` = {
	path = /tmp/test.plist
	state = running
	properties = {
		state = inactive
	}
}`

	job, err := parseLaunchdJob(target, defaultLaunchdLabel, output)
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
	label := "com.forgeclaw.picoclaw-node." + instance
	path := filepath.Join(dir, label+".plist")
	data := managedLaunchdPlistMarker + "\n" +
		"<plist version=\"1.0\"><dict><key>Label</key><string>" + label +
		"</string></dict></plist>\n"
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
}

func launchdPrintOutput(label, path, state string) string {
	return label + " = {\n\tpath = " + path + "\n\tstate = " + state + "\n}"
}

const (
	defaultLaunchdLabel  = "com.forgeclaw.picoclaw-node.default"
	launchdMissingOutput = "Could not find service"
)
