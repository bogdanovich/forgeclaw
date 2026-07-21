//go:build windows

package companion

import "golang.org/x/sys/windows"

const systemExecWindowsStillActive = 259

func systemExecTestProcessAlive(pid int) bool {
	handle, err := windows.OpenProcess(
		windows.PROCESS_QUERY_LIMITED_INFORMATION,
		false,
		uint32(pid),
	)
	if err != nil {
		return false
	}
	defer windows.CloseHandle(handle)
	var exitCode uint32
	return windows.GetExitCodeProcess(handle, &exitCode) == nil &&
		exitCode == systemExecWindowsStillActive
}
