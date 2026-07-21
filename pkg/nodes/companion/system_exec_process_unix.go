//go:build !windows

package companion

import (
	"errors"
	"os/exec"
	"syscall"
)

type unixSystemExecProcess struct {
	command               *exec.Cmd
	terminationSuccessful bool
}

func startSystemExecProcess(command *exec.Cmd) (systemExecProcess, error) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := command.Start(); err != nil {
		return nil, err
	}
	return &unixSystemExecProcess{command: command}, nil
}

func (process *unixSystemExecProcess) wait() error {
	return process.command.Wait()
}

func (process *unixSystemExecProcess) terminate() error {
	if process == nil || process.command == nil || process.command.Process == nil ||
		process.command.Process.Pid <= 0 {
		return errors.New("system.exec process is unavailable")
	}
	groupErr := syscall.Kill(-process.command.Process.Pid, syscall.SIGKILL)
	processErr := process.command.Process.Kill()
	process.terminationSuccessful = groupErr == nil || processErr == nil
	if process.terminationSuccessful {
		return nil
	}
	return errors.Join(groupErr, processErr)
}

func (process *unixSystemExecProcess) terminationConfirmed() bool {
	return process != nil && process.terminationSuccessful
}

func (*unixSystemExecProcess) close() {}
