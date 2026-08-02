//go:build !windows

package mcp

import (
	"errors"
	"fmt"
	"os/exec"
	"syscall"
	"time"
)

type isolatedCommandProcessTree struct {
	command *exec.Cmd
}

func prepareIsolatedCommandProcessTree(command *exec.Cmd) (*isolatedCommandProcessTree, error) {
	if command == nil {
		return &isolatedCommandProcessTree{}, nil
	}
	if command.SysProcAttr == nil {
		command.SysProcAttr = &syscall.SysProcAttr{}
	}
	if !command.SysProcAttr.Setsid {
		command.SysProcAttr.Setpgid = true
	}
	return &isolatedCommandProcessTree{command: command}, nil
}

func (t *isolatedCommandProcessTree) started() error { return nil }

func (t *isolatedCommandProcessTree) stop(timeout time.Duration) error {
	command := t.command
	if command == nil || command.Process == nil || command.Process.Pid <= 0 {
		return nil
	}
	if timeout <= 0 {
		timeout = isolatedCommandTerminateDuration
	}
	processGroup := command.Process.Pid
	deadline := time.Now().Add(timeout)
	graceDeadline := time.Now().Add(timeout / 3)
	if waitForProcessGroupExit(processGroup, graceDeadline) {
		return nil
	}
	if err := syscall.Kill(-processGroup, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("terminate MCP process group: %w", err)
	}
	termDeadline := time.Now().Add(timeout / 3)
	if termDeadline.After(deadline) {
		termDeadline = deadline
	}
	if waitForProcessGroupExit(processGroup, termDeadline) {
		return nil
	}
	if err := syscall.Kill(-processGroup, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("kill MCP process group: %w", err)
	}
	_ = command.Process.Kill()
	if waitForProcessGroupExit(processGroup, deadline) {
		return nil
	}
	return errors.New("MCP process tree remained alive after termination")
}

func waitForProcessGroupExit(processGroup int, deadline time.Time) bool {
	for {
		err := syscall.Kill(-processGroup, 0)
		if errors.Is(err, syscall.ESRCH) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
}
