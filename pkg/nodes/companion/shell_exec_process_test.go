//go:build !windows

package companion

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

func TestShellExecCancellationProvesDescendantTermination(t *testing.T) {
	runtime, ledger, root := newShellExecRuntime(t)
	pidPath := root + "/child.pid"
	plan := prepareShellExecPlan(t, runtime, shellExecInput{
		Profile:        "owner",
		Script:         "sleep 30 & child=$!; printf '%s' \"$child\" > child.pid; wait \"$child\"",
		CWD:            "workspace",
		Env:            map[string]string{},
		TimeoutSeconds: 20,
	}, 25, 4096)
	result := make(chan error, 1)
	go func() {
		_, err := runtime.Invoke(t.Context(), plan)
		result <- err
	}()
	childPID := waitForShellPID(t, pidPath)
	record, err := runtime.Cancel(nodes.InvocationCancelRequest{
		InvocationID: plan.InvocationID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if record.State != nodes.InvocationRunning || record.Cancellation == nil {
		t.Fatalf("cancel request record = %+v", record)
	}
	if err := <-result; !errors.Is(err, ErrInvocationCanceled) {
		t.Fatalf("Invoke() cancellation error = %v", err)
	}
	record, found := ledger.Get(plan.InvocationID)
	if !found || record.State != nodes.InvocationCanceled {
		t.Fatalf("canceled record = %+v, found = %v", record, found)
	}
	if err := syscall.Kill(childPID, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("descendant pid %d still exists after confirmed cancellation: %v", childPID, err)
	}
}

func TestShellExecTimeoutTerminatesDescendantAndRecordsFailure(t *testing.T) {
	runtime, ledger, root := newShellExecRuntime(t)
	pidPath := root + "/timeout-child.pid"
	plan := prepareShellExecPlan(t, runtime, shellExecInput{
		Profile:        "owner",
		Script:         "sleep 30 & child=$!; printf '%s' \"$child\" > timeout-child.pid; wait \"$child\"",
		CWD:            "workspace",
		Env:            map[string]string{},
		TimeoutSeconds: 1,
	}, 3, 4096)
	result := make(chan error, 1)
	go func() {
		_, err := runtime.Invoke(t.Context(), plan)
		result <- err
	}()
	childPID := waitForShellPID(t, pidPath)
	if err := <-result; err == nil {
		t.Fatal("timed-out shell invocation succeeded")
	}
	record, found := ledger.Get(plan.InvocationID)
	if !found ||
		record.State != nodes.InvocationFailed ||
		record.Failure == nil ||
		record.Failure.Code != "TIMEOUT" {
		t.Fatalf("timeout record = %+v, found = %v", record, found)
	}
	if err := syscall.Kill(childPID, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("timeout descendant pid %d still exists: %v", childPID, err)
	}
}

func waitForShellPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		data, err := os.ReadFile(path)
		if err == nil && strings.TrimSpace(string(data)) != "" {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(data)))
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			return pid
		}
		if time.Now().After(deadline) {
			t.Fatalf("shell child pid handshake did not appear: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
