//go:build !windows

package companion

import (
	"os/exec"
	"syscall"
)

func prepareSystemExecProcess(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func terminateSystemExecProcess(command *exec.Cmd) bool {
	if command == nil || command.Process == nil || command.Process.Pid <= 0 {
		return false
	}
	groupErr := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	processErr := command.Process.Kill()
	return groupErr == nil || processErr == nil
}
