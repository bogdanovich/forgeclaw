//go:build !windows

package mcp

import (
	"errors"
	"os"
	"syscall"
)

func tryAcquireExclusiveFileLock(file *os.File) error {
	err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
		return errExclusiveLeaseBusy
	}
	return err
}

func releaseExclusiveFileLock(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}
