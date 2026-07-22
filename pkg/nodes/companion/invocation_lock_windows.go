//go:build windows

package companion

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func acquireInvocationLedgerLock(path string) (func(), error) {
	lock, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open node invocation ledger lock: %w", err)
	}
	overlapped := &windows.Overlapped{}
	err = windows.LockFileEx(
		windows.Handle(lock.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0,
		1,
		0,
		overlapped,
	)
	if err != nil {
		_ = lock.Close()
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return nil, ErrInvocationLedgerOwned
		}
		return nil, fmt.Errorf("lock node invocation ledger: %w", err)
	}
	return func() {
		_ = windows.UnlockFileEx(windows.Handle(lock.Fd()), 0, 1, 0, overlapped)
		_ = lock.Close()
	}, nil
}
