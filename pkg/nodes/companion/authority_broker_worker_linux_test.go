//go:build linux

package companion

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const authorityBrokerWorkerHelperEnvironment = "MINTCLAW_AUTHORITY_BROKER_WORKER_TEST"

func TestAuthorityBrokerWorkerHelper(t *testing.T) {
	if os.Getenv(authorityBrokerWorkerHelperEnvironment) != "1" {
		return
	}
	if err := RunAuthorityBrokerWorker(context.Background(), false); err != nil {
		os.Exit(2)
	}
}

func TestAuthorityBrokerWorkerExecutesShellSemantics(t *testing.T) {
	runner := testAuthorityBrokerProcessRunner(t)
	request := testAuthorityBrokerRequest()
	prepared := testPreparedAuthorityBrokerExecution(t)
	result, err := runner.Execute(t.Context(), prepared, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.ExitCode != 7 ||
		result.Stdout != "beta\nalpha\n" ||
		result.Stderr != "failure\n" ||
		result.Signal != "" ||
		result.Truncated {
		t.Fatalf("worker result = %#v", result)
	}
}

func TestAuthorityBrokerWorkerExecutesConfiguredUIDZero(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("UID 0 proof requires root test process")
	}
	runner := testAuthorityBrokerProcessRunner(t)
	request := testAuthorityBrokerRequest()
	request.Script = "id -u"
	prepared := testPreparedAuthorityBrokerExecution(t)
	prepared.profile.UID = 0
	prepared.profile.GID = 0
	prepared.shellArguments = []string{"-c", request.Script}
	result, err := runner.Execute(t.Context(), prepared, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.ExitCode != 0 || result.Stdout != "0\n" || result.Stderr != "" {
		t.Fatalf("UID 0 worker result = %#v", result)
	}
}

func TestAuthorityBrokerWorkerReapsSetsidDescendant(t *testing.T) {
	if _, err := os.Stat("/usr/bin/setsid"); err != nil {
		t.Skip("setsid unavailable")
	}
	pidFile := t.TempDir() + "/descendant.pid"
	runner := testAuthorityBrokerProcessRunner(t)
	request := testAuthorityBrokerRequest()
	request.Script = `/usr/bin/setsid /bin/sh -c 'echo $$ > "$1"; trap "" TERM; while :; do sleep 1; done' worker ` +
		pidFile + ` & printf done`
	prepared := testPreparedAuthorityBrokerExecution(t)
	prepared.shellArguments = []string{"-c", request.Script}
	result, err := runner.Execute(t.Context(), prepared, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Stdout != "done" {
		t.Fatalf("worker output = %#v", result)
	}
	rawPID, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(rawPID)))
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Kill(pid, 0); !errors.Is(err, unix.ESRCH) {
		t.Fatalf("setsid descendant %d remains alive: %v", pid, err)
	}
}

func TestAuthorityBrokerWorkerConfirmsCancellationAfterCleanup(t *testing.T) {
	runner := testAuthorityBrokerProcessRunner(t)
	request := testAuthorityBrokerRequest()
	readyFile := t.TempDir() + "/ready"
	request.Script = `printf ready > "$1"; trap "" TERM; while :; do sleep 1; done`
	prepared := testPreparedAuthorityBrokerExecution(t)
	prepared.shellArguments = []string{"-c", request.Script, "worker", readyFile}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := runner.Execute(ctx, prepared, request)
		done <- err
	}()
	waitForAuthorityBrokerFile(t, readyFile)
	cancel()
	if err := <-done; !errors.Is(err, ErrShellBrokerCancellationConfirmed) {
		t.Fatalf("worker cancellation = %v", err)
	}
}

func testAuthorityBrokerProcessRunner(t *testing.T) *authorityBrokerProcessRunner {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return &authorityBrokerProcessRunner{
		executable: executable,
		arguments:  []string{"-test.run=TestAuthorityBrokerWorkerHelper"},
		environment: append(
			os.Environ(),
			authorityBrokerWorkerHelperEnvironment+"=1",
		),
	}
}

func testAuthorityBrokerRequest() ShellBrokerRequest {
	return ShellBrokerRequest{
		InvocationID:    "inv_test",
		PlanHash:        strings.Repeat("a", 64),
		Profile:         "owner",
		ProfileRevision: "profile-v1",
		Script:          `printf 'alpha\n' | sed 's/alpha/beta/'; value=alpha; if test "$value" = alpha; then printf 'alpha\n'; fi; printf 'failure\n' >&2; exit 7`,
		WorkingScope:    "workspace",
		Environment:     map[string]string{},
		TimeoutSeconds:  10,
		OutputBytesMax:  4096,
	}
}

func testPreparedAuthorityBrokerExecution(t *testing.T) preparedAuthorityBrokerExecution {
	t.Helper()
	return preparedAuthorityBrokerExecution{
		profile: normalizedAuthorityBrokerProfile{
			AuthorityBrokerProfile: AuthorityBrokerProfile{
				UID: uint32(os.Getuid()), GID: uint32(os.Getgid()),
			},
		},
		shellPath: "/bin/sh",
		shellArguments: []string{
			"-c",
			testAuthorityBrokerRequest().Script,
		},
		workingDirectory: t.TempDir(),
		environment:      []string{"PATH=/usr/bin:/bin"},
	}
}

func waitForAuthorityBrokerFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("authority broker process barrier was not reached")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
