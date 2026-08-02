//go:build !windows

package mcp

import (
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestStopIsolatedCommandProcessTreeTerminatesDescendants(t *testing.T) {
	childPIDPath := t.TempDir() + "/child.pid"
	command := exec.Command(
		"sh",
		"-c",
		`sleep 60 & child=$!; printf '%s' "$child" > "$1"; wait`,
		"sh",
		childPIDPath,
	)
	prepareIsolatedCommandProcessTree(command)
	if err := command.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitCh := make(chan error, 1)
	go func() { waitCh <- command.Wait() }()
	t.Cleanup(func() {
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		select {
		case <-waitCh:
		default:
		}
	})

	childPID := waitForChildPID(t, childPIDPath)
	processGroup, err := syscall.Getpgid(command.Process.Pid)
	if err != nil || processGroup != command.Process.Pid {
		t.Fatalf("process group = %d, %v; want %d", processGroup, err, command.Process.Pid)
	}
	if err = stopIsolatedCommandProcessTree(command, 2*time.Second); err != nil {
		t.Fatalf("stopIsolatedCommandProcessTree() error = %v", err)
	}
	select {
	case <-waitCh:
	case <-time.After(time.Second):
		t.Fatal("direct process was not reaped")
	}
	if err = syscall.Kill(childPID, 0); err == nil || !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("descendant process %d remains alive: %v", childPID, err)
	}
}

func waitForChildPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		encoded, err := os.ReadFile(path)
		if err == nil && strings.TrimSpace(string(encoded)) != "" {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(encoded)))
			if parseErr != nil {
				t.Fatalf("parse child PID: %v", parseErr)
			}
			return pid
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("child PID was not written")
	return 0
}
