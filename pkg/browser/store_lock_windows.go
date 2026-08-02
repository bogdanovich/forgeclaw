//go:build windows

package browser

import (
	"errors"
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

func acquireStoreLock(path string) (func(), error) {
	owner, err := currentWindowsStoreOwner()
	if err != nil {
		return nil, err
	}
	descriptor, _, err := ownerOnlyWindowsStoreDescriptor(owner)
	if err != nil {
		return nil, err
	}
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	attributes := &windows.SecurityAttributes{
		Length:             uint32(unsafe.Sizeof(windows.SecurityAttributes{})),
		SecurityDescriptor: descriptor,
	}
	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_READ|windows.GENERIC_WRITE|windows.READ_CONTROL|windows.WRITE_DAC|windows.WRITE_OWNER,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		attributes,
		windows.OPEN_ALWAYS,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("open browser state lock: %w", err)
	}
	lock := os.NewFile(uintptr(handle), path)
	if err = secureWindowsStoreHandle(handle, owner); err != nil {
		_ = lock.Close()
		return nil, err
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
			return nil, ErrStoreOwned
		}
		return nil, fmt.Errorf("lock browser state store: %w", err)
	}
	return func() {
		_ = windows.UnlockFileEx(windows.Handle(lock.Fd()), 0, 1, 0, overlapped)
		_ = lock.Close()
	}, nil
}
