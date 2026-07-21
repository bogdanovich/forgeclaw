//go:build !windows

package companion

import (
	"errors"
	"fmt"
	"os/exec"
	"syscall"
	"time"
)

type unixSystemExecProcess struct {
	command *exec.Cmd
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
	if groupErr == nil || processErr == nil {
		return nil
	}
	return errors.Join(groupErr, processErr)
}

func (process *unixSystemExecProcess) finish() error {
	if process == nil || process.command == nil || process.command.Process == nil ||
		process.command.Process.Pid <= 0 {
		return errors.New("system.exec process is unavailable")
	}
	processGroup := -process.command.Process.Pid
	if err := syscall.Kill(processGroup, syscall.SIGKILL); err != nil &&
		!errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("terminate system.exec descendants: %w", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		err := syscall.Kill(processGroup, 0)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("confirm system.exec descendants: %w", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return errors.New("system.exec descendants did not terminate")
}

func (*unixSystemExecProcess) close() {}
