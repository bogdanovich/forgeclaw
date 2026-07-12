//go:build windows

package gateway

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func acquireDeployLock(path string) (*os.File, error) {
	lock, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open deploy lock: %w", err)
	}
	overlapped := windows.Overlapped{}
	err = windows.LockFileEx(
		windows.Handle(lock.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0,
		1,
		0,
		&overlapped,
	)
	if err == nil {
		return lock, nil
	}
	_ = lock.Close()
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		return nil, ErrDeployAlreadyRunning
	}
	return nil, fmt.Errorf("lock deploy: %w", err)
}
