//go:build !windows

package companion

import (
	"errors"
	"syscall"
)

func systemExecTestProcessAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
