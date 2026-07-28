//go:build windows

package companion

import (
	"errors"
	"os/exec"
	"strconv"
)

func prepareOwnedShellProcess(*exec.Cmd) {}

func terminateOwnedShellProcess(command *exec.Cmd) error {
	if command == nil || command.Process == nil || command.Process.Pid <= 0 {
		return nil
	}
	_ = exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(command.Process.Pid)).Run()
	return command.Process.Kill()
}

func ownedShellProcessTreeGone(*exec.Cmd) bool {
	return false
}

func ownedShellExit(waitErr error) (int, string, error) {
	if waitErr == nil {
		return 0, "", nil
	}
	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) {
		return 0, "", waitErr
	}
	return exitErr.ExitCode(), "", nil
}
