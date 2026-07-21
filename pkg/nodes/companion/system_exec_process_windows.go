//go:build windows

package companion

import (
	"errors"
	"fmt"
	"os/exec"
	"syscall"
	"time"
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
	if command.SysProcAttr == nil {
		command.SysProcAttr = &syscall.SysProcAttr{}
	}
	// User code cannot spawn before the process belongs to the job.
	command.SysProcAttr.CreationFlags |= windows.CREATE_SUSPENDED
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
		return nil, cleanupWindowsSystemExecStart(command, job, 0, false, err)
	}
	if err = windows.AssignProcessToJobObject(job, handle); err != nil {
		return nil, cleanupWindowsSystemExecStart(command, job, handle, false, err)
	}
	if err = resumeWindowsSystemExecProcess(uint32(command.Process.Pid)); err != nil {
		return nil, cleanupWindowsSystemExecStart(command, job, handle, true, err)
	}
	return &windowsSystemExecProcess{command: command, job: job, handle: handle}, nil
}

func cleanupWindowsSystemExecStart(
	command *exec.Cmd,
	job windows.Handle,
	handle windows.Handle,
	contained bool,
	cause error,
) error {
	if contained {
		_ = windows.TerminateJobObject(job, 1)
	} else if command != nil && command.Process != nil {
		_ = command.Process.Kill()
	}
	if command != nil && command.Process != nil {
		_ = command.Wait()
	}
	if handle != 0 {
		_ = windows.CloseHandle(handle)
	}
	_ = windows.CloseHandle(job)
	return cause
}

func resumeWindowsSystemExecProcess(pid uint32) error {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPTHREAD, 0)
	if err != nil {
		return fmt.Errorf("snapshot system.exec process threads: %w", err)
	}
	defer windows.CloseHandle(snapshot)
	entry := windows.ThreadEntry32{Size: uint32(unsafe.Sizeof(windows.ThreadEntry32{}))}
	if err = windows.Thread32First(snapshot, &entry); err != nil {
		return fmt.Errorf("enumerate system.exec process threads: %w", err)
	}
	for {
		if entry.OwnerProcessID == pid {
			thread, openErr := windows.OpenThread(
				windows.THREAD_SUSPEND_RESUME,
				false,
				entry.ThreadID,
			)
			if openErr != nil {
				return fmt.Errorf("open system.exec initial thread: %w", openErr)
			}
			previousCount, resumeErr := windows.ResumeThread(thread)
			_ = windows.CloseHandle(thread)
			if resumeErr != nil {
				return fmt.Errorf("resume system.exec initial thread: %w", resumeErr)
			}
			if previousCount != 1 {
				return fmt.Errorf(
					"system.exec initial thread had unexpected suspend count %d",
					previousCount,
				)
			}
			return nil
		}
		if err = windows.Thread32Next(snapshot, &entry); err != nil {
			break
		}
	}
	return errors.New("system.exec initial thread was not found")
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

func (process *windowsSystemExecProcess) finish() error {
	if process == nil || process.job == 0 {
		return errors.New("system.exec job is unavailable")
	}
	active, err := activeWindowsSystemExecProcesses(process.job)
	if err != nil {
		return err
	}
	if active == 0 {
		return nil
	}
	if err = windows.TerminateJobObject(process.job, 1); err != nil {
		return fmt.Errorf("terminate system.exec descendants: %w", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		active, err = activeWindowsSystemExecProcesses(process.job)
		if err != nil {
			return err
		}
		if active == 0 {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return errors.New("system.exec descendants did not terminate")
}

func activeWindowsSystemExecProcesses(job windows.Handle) (uint32, error) {
	accounting := windowsJobAccounting{}
	var returned uint32
	if err := windows.QueryInformationJobObject(
		job,
		windows.JobObjectBasicAccountingInformation,
		uintptr(unsafe.Pointer(&accounting)),
		uint32(unsafe.Sizeof(accounting)),
		&returned,
	); err != nil {
		return 0, fmt.Errorf("confirm system.exec descendants: %w", err)
	}
	return accounting.ActiveProcesses, nil
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
