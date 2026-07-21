//go:build windows

package companion

import (
	"errors"
	"os/exec"
	"unsafe"

	"golang.org/x/sys/windows"
)

type windowsSystemExecProcess struct {
	command *exec.Cmd
	job     windows.Handle
	handle  windows.Handle
}

type windowsJobAccounting struct {
	TotalUserTime             int64
	TotalKernelTime           int64
	ThisPeriodTotalUserTime   int64
	ThisPeriodTotalKernelTime int64
	TotalPageFaultCount       uint32
	TotalProcesses            uint32
	ActiveProcesses           uint32
	TotalTerminatedProcesses  uint32
}

func startSystemExecProcess(command *exec.Cmd) (systemExecProcess, error) {
	if command == nil {
		return nil, errors.New("system.exec command is unavailable")
	}
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return nil, err
	}
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err = windows.SetInformationJobObject(
		job,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info)),
	); err != nil {
		_ = windows.CloseHandle(job)
		return nil, err
	}
	if err = command.Start(); err != nil {
		_ = windows.CloseHandle(job)
		return nil, err
	}
	handle, err := windows.OpenProcess(
		windows.PROCESS_SET_QUOTA|
			windows.PROCESS_TERMINATE|
			windows.PROCESS_QUERY_LIMITED_INFORMATION|
			windows.SYNCHRONIZE,
		false,
		uint32(command.Process.Pid),
	)
	if err != nil {
		return nil, cleanupUncontainedWindowsSystemExec(command, job, 0, err)
	}
	if err = windows.AssignProcessToJobObject(job, handle); err != nil {
		return nil, cleanupUncontainedWindowsSystemExec(command, job, handle, err)
	}
	return &windowsSystemExecProcess{command: command, job: job, handle: handle}, nil
}

func cleanupUncontainedWindowsSystemExec(
	command *exec.Cmd,
	job windows.Handle,
	handle windows.Handle,
	cause error,
) error {
	if command != nil && command.Process != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
	}
	if handle != 0 {
		_ = windows.CloseHandle(handle)
	}
	_ = windows.CloseHandle(job)
	return cause
}

func (process *windowsSystemExecProcess) wait() error {
	return process.command.Wait()
}

func (process *windowsSystemExecProcess) terminate() error {
	if process == nil || process.job == 0 {
		return errors.New("system.exec job is unavailable")
	}
	return windows.TerminateJobObject(process.job, 1)
}

func (process *windowsSystemExecProcess) terminationConfirmed() bool {
	if process == nil || process.job == 0 {
		return false
	}
	accounting := windowsJobAccounting{}
	var returned uint32
	if err := windows.QueryInformationJobObject(
		process.job,
		windows.JobObjectBasicAccountingInformation,
		uintptr(unsafe.Pointer(&accounting)),
		uint32(unsafe.Sizeof(accounting)),
		&returned,
	); err != nil {
		return false
	}
	return accounting.ActiveProcesses == 0
}

func (process *windowsSystemExecProcess) close() {
	if process == nil {
		return
	}
	if process.handle != 0 {
		_ = windows.CloseHandle(process.handle)
		process.handle = 0
	}
	if process.job != 0 {
		_ = windows.CloseHandle(process.job)
		process.job = 0
	}
}
