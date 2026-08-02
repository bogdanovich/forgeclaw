//go:build windows

package mcp

import (
	"fmt"
	"os/exec"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

var ntResumeProcess = windows.NewLazySystemDLL("ntdll.dll").NewProc("NtResumeProcess")

type isolatedCommandProcessTree struct {
	command *exec.Cmd
	job     windows.Handle
}

type jobObjectBasicAccountingInformation struct {
	TotalUserTime             int64
	TotalKernelTime           int64
	ThisPeriodTotalUserTime   int64
	ThisPeriodTotalKernelTime int64
	TotalPageFaultCount       uint32
	TotalProcesses            uint32
	ActiveProcesses           uint32
	TotalTerminatedProcesses  uint32
}

func prepareIsolatedCommandProcessTree(command *exec.Cmd) (*isolatedCommandProcessTree, error) {
	if command == nil {
		return &isolatedCommandProcessTree{}, nil
	}
	if command.SysProcAttr == nil {
		command.SysProcAttr = &syscall.SysProcAttr{}
	}
	// Starting suspended closes the race in which the wrapper could spawn a
	// browser before it is assigned to the Job Object.
	command.SysProcAttr.CreationFlags |= windows.CREATE_NEW_PROCESS_GROUP | windows.CREATE_SUSPENDED
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return nil, fmt.Errorf("create MCP process-tree job: %w", err)
	}
	limits := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err = windows.SetInformationJobObject(
		job,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&limits)),
		uint32(unsafe.Sizeof(limits)),
	); err != nil {
		_ = windows.CloseHandle(job)
		return nil, fmt.Errorf("configure MCP process-tree job: %w", err)
	}
	return &isolatedCommandProcessTree{command: command, job: job}, nil
}

func (t *isolatedCommandProcessTree) started() error {
	if t == nil || t.command == nil || t.command.Process == nil {
		return fmt.Errorf("MCP process tree started without a process")
	}
	process, err := windows.OpenProcess(
		windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE|windows.PROCESS_SUSPEND_RESUME,
		false,
		uint32(t.command.Process.Pid),
	)
	if err != nil {
		return fmt.Errorf("open suspended MCP process: %w", err)
	}
	defer windows.CloseHandle(process)
	if err = windows.AssignProcessToJobObject(t.job, process); err != nil {
		return fmt.Errorf("assign MCP process to job: %w", err)
	}
	status, _, callErr := ntResumeProcess.Call(uintptr(process))
	if status != 0 {
		return fmt.Errorf("resume MCP process: NTSTATUS %#x: %v", status, callErr)
	}
	return nil
}

func (t *isolatedCommandProcessTree) stop(timeout time.Duration) error {
	if t == nil {
		return nil
	}
	if t.job == 0 {
		return nil
	}
	command := t.command
	if command == nil || command.Process == nil || command.Process.Pid <= 0 {
		return t.closeJobIfEmpty()
	}
	if timeout <= 0 {
		timeout = isolatedCommandTerminateDuration
	}
	if err := windows.TerminateJobObject(t.job, 1); err != nil {
		return fmt.Errorf("terminate MCP process-tree job: %w", err)
	}
	deadline := time.Now().Add(timeout)
	for {
		active, err := t.activeProcesses()
		if err != nil {
			return err
		}
		if active == 0 {
			return t.closeJobIfEmpty()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("MCP process tree remained alive after termination")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (t *isolatedCommandProcessTree) activeProcesses() (uint32, error) {
	var info jobObjectBasicAccountingInformation
	err := windows.QueryInformationJobObject(
		t.job,
		windows.JobObjectBasicAccountingInformation,
		uintptr(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info)),
		nil,
	)
	if err != nil {
		return 0, fmt.Errorf("query MCP process-tree job: %w", err)
	}
	return info.ActiveProcesses, nil
}

func (t *isolatedCommandProcessTree) closeJobIfEmpty() error {
	if t.job == 0 {
		return nil
	}
	active, err := t.activeProcesses()
	if err != nil {
		return err
	}
	if active != 0 {
		return fmt.Errorf("MCP process tree still owns %d process(es)", active)
	}
	if err = windows.CloseHandle(t.job); err != nil {
		return fmt.Errorf("close MCP process-tree job: %w", err)
	}
	t.job = 0
	return nil
}
