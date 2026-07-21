//go:build windows

package companion

import "os/exec"

func prepareSystemExecProcess(*exec.Cmd) {}

func terminateSystemExecProcess(command *exec.Cmd) bool {
	return command != nil && command.Process != nil && command.Process.Kill() == nil
}
