//go:build !windows

package gateway

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

func acquireDeployLock(path string) (*os.File, error) {
	lock, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open deploy lock: %w", err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrDeployAlreadyRunning
		}
		return nil, fmt.Errorf("lock deploy: %w", err)
	}
	return lock, nil
}
