//go:build !windows

package companion

import (
	"errors"
	"os/exec"
	"syscall"
)

func prepareOwnedShellProcess(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func terminateOwnedShellProcess(command *exec.Cmd) error {
	if command == nil || command.Process == nil {
		return nil
	}
	pid := command.Process.Pid
	if pid <= 0 {
		return nil
	}
	groupErr := syscall.Kill(-pid, syscall.SIGKILL)
	processErr := command.Process.Kill()
	if groupErr != nil && !errors.Is(groupErr, syscall.ESRCH) {
		return groupErr
	}
	if processErr != nil && !errors.Is(processErr, syscall.ESRCH) {
		return processErr
	}
	return nil
}

func ownedShellProcessTreeGone(command *exec.Cmd) bool {
	if command == nil || command.Process == nil || command.Process.Pid <= 0 {
		return false
	}
	err := syscall.Kill(-command.Process.Pid, 0)
	return errors.Is(err, syscall.ESRCH)
}

func ownedShellExit(waitErr error) (int, string, error) {
	if waitErr == nil {
		return 0, "", nil
	}
	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) {
		return 0, "", waitErr
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok {
		return exitErr.ExitCode(), "", nil
	}
	if status.Signaled() {
		return exitErr.ExitCode(), status.Signal().String(), nil
	}
	return exitErr.ExitCode(), "", nil
}
