//go:build windows

package mcp

import (
	"fmt"
	"os/exec"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/sys/windows"
)

func prepareIsolatedCommandProcessTree(command *exec.Cmd) {
	if command == nil {
		return
	}
	if command.SysProcAttr == nil {
		command.SysProcAttr = &syscall.SysProcAttr{}
	}
	command.SysProcAttr.CreationFlags |= windows.CREATE_NEW_PROCESS_GROUP
}

func stopIsolatedCommandProcessTree(command *exec.Cmd, timeout time.Duration) error {
	if command == nil || command.Process == nil || command.Process.Pid <= 0 {
		return nil
	}
	if timeout <= 0 {
		timeout = isolatedCommandTerminateDuration
	}
	pid := command.Process.Pid
	if err := exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(pid)).Run(); err != nil {
		_ = command.Process.Kill()
	}
	process, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(pid))
	if err != nil {
		if err == windows.ERROR_INVALID_PARAMETER {
			return nil
		}
		return fmt.Errorf("open terminated MCP process: %w", err)
	}
	defer windows.CloseHandle(process)
	waitMillis := uint32(timeout / time.Millisecond)
	result, err := windows.WaitForSingleObject(process, waitMillis)
	if err != nil {
		return fmt.Errorf("wait for MCP process tree: %w", err)
	}
	if result != windows.WAIT_OBJECT_0 {
		return fmt.Errorf("MCP process tree remained alive after termination")
	}
	return nil
}
