//go:build windows

package mcp

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

func TestWindowsProcessTreeJobTerminatesDescendants(t *testing.T) {
	childPIDPath := t.TempDir() + `\child.pid`
	command := exec.Command(
		"powershell.exe",
		"-NoProfile",
		"-NonInteractive",
		"-Command",
		`$child = Start-Process -FilePath powershell.exe -ArgumentList '-NoProfile','-NonInteractive','-Command','Start-Sleep -Seconds 60' -PassThru; Set-Content -NoNewline -Path $env:MINTCLAW_CHILD_PID_PATH -Value $child.Id; Wait-Process -Id $child.Id`,
	)
	command.Env = append(command.Environ(), "MINTCLAW_CHILD_PID_PATH="+childPIDPath)
	processTree, err := prepareIsolatedCommandProcessTree(command)
	if err != nil {
		t.Fatalf("prepareIsolatedCommandProcessTree() error = %v", err)
	}
	if err = command.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err = processTree.started(); err != nil {
		_ = command.Process.Kill()
		t.Fatalf("processTree.started() error = %v", err)
	}
	waitCh := make(chan error, 1)
	go func() { waitCh <- command.Wait() }()
	t.Cleanup(func() {
		_ = processTree.stop(time.Second)
		select {
		case <-waitCh:
		default:
		}
	})

	childPID := waitForWindowsChildPID(t, childPIDPath)
	deadline := time.Now().Add(2 * time.Second)
	for {
		active, queryErr := processTree.activeProcesses()
		if queryErr != nil {
			t.Fatalf("activeProcesses() error = %v", queryErr)
		}
		if active >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("job active processes = %d, want at least wrapper and descendant", active)
		}
		time.Sleep(10 * time.Millisecond)
	}
	child, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(childPID))
	if err != nil {
		t.Fatalf("OpenProcess(descendant) error = %v", err)
	}
	defer windows.CloseHandle(child)

	if err = processTree.stop(2 * time.Second); err != nil {
		t.Fatalf("processTree.stop() error = %v", err)
	}
	select {
	case <-waitCh:
	case <-time.After(time.Second):
		t.Fatal("direct process was not reaped")
	}
	result, err := windows.WaitForSingleObject(child, 2_000)
	if err != nil {
		t.Fatalf("WaitForSingleObject(descendant) error = %v", err)
	}
	if result != windows.WAIT_OBJECT_0 {
		t.Fatalf("descendant process %d remains alive", childPID)
	}
}

func waitForWindowsChildPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
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
