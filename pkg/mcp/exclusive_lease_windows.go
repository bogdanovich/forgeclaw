//go:build windows

package mcp

import (
	"errors"
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

func openExclusiveLeaseFile(path string) (*os.File, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return nil, fmt.Errorf("get current Windows user: %w", err)
	}
	owner := user.User.Sid
	descriptor, err := windows.SecurityDescriptorFromString(
		"O:" + owner.String() + "D:P(A;;GA;;;" + owner.String() + ")",
	)
	if err != nil {
		return nil, fmt.Errorf("build owner-only Windows security descriptor: %w", err)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		return nil, fmt.Errorf("read owner-only Windows DACL: %w", err)
	}

	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	securityAttributes := &windows.SecurityAttributes{
		Length:             uint32(unsafe.Sizeof(windows.SecurityAttributes{})),
		SecurityDescriptor: descriptor,
	}
	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_READ|windows.GENERIC_WRITE|windows.READ_CONTROL|windows.WRITE_DAC,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		securityAttributes,
		windows.OPEN_ALWAYS,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		return nil, err
	}
	closeOnError := func(err error) (*os.File, error) {
		_ = windows.CloseHandle(handle)
		return nil, err
	}

	if err := validateWindowsLeaseOwner(handle, owner); err != nil {
		return closeOnError(err)
	}
	securityInformation := windows.SECURITY_INFORMATION(
		windows.DACL_SECURITY_INFORMATION | windows.PROTECTED_DACL_SECURITY_INFORMATION,
	)
	if err := windows.SetSecurityInfo(
		handle,
		windows.SE_FILE_OBJECT,
		securityInformation,
		nil,
		nil,
		dacl,
		nil,
	); err != nil {
		return closeOnError(fmt.Errorf("apply owner-only Windows DACL: %w", err))
	}
	if err := validateOwnerOnlyWindowsDACL(handle, owner); err != nil {
		return closeOnError(err)
	}

	return os.NewFile(uintptr(handle), path), nil
}

func validateWindowsLeaseOwner(handle windows.Handle, owner *windows.SID) error {
	descriptor, err := windows.GetSecurityInfo(
		handle,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION,
	)
	if err != nil {
		return fmt.Errorf("read Windows lease owner: %w", err)
	}
	actualOwner, _, err := descriptor.Owner()
	if err != nil || !actualOwner.Equals(owner) {
		return fmt.Errorf("Windows lease owner validation failed")
	}
	return nil
}

func validateOwnerOnlyWindowsDACL(handle windows.Handle, owner *windows.SID) error {
	descriptor, err := windows.GetSecurityInfo(
		handle,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION,
	)
	if err != nil {
		return fmt.Errorf("read Windows lease security descriptor: %w", err)
	}
	actualOwner, _, err := descriptor.Owner()
	if err != nil || !actualOwner.Equals(owner) {
		return fmt.Errorf("Windows lease owner validation failed")
	}
	control, _, err := descriptor.Control()
	if err != nil || control&windows.SE_DACL_PROTECTED == 0 {
		return fmt.Errorf("Windows lease DACL is not protected")
	}
	dacl, _, err := descriptor.DACL()
	if err != nil || dacl == nil || dacl.AceCount != 1 {
		return fmt.Errorf("Windows lease DACL is not owner-only")
	}
	var ace *windows.ACCESS_ALLOWED_ACE
	if err := windows.GetAce(dacl, 0, &ace); err != nil {
		return fmt.Errorf("read Windows lease DACL entry: %w", err)
	}
	aceOwner := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
	const fileAllAccess windows.ACCESS_MASK = 0x1F01FF
	grantsFullControl := ace.Mask&windows.GENERIC_ALL != 0 || ace.Mask&fileAllAccess == fileAllAccess
	if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE ||
		!grantsFullControl ||
		!aceOwner.Equals(owner) {
		return fmt.Errorf("Windows lease DACL is not owner-only")
	}
	return nil
}

func tryAcquireExclusiveFileLock(file *os.File) error {
	err := windows.LockFileEx(
		windows.Handle(file.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0,
		1,
		0,
		&windows.Overlapped{},
	)
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		return errExclusiveLeaseBusy
	}
	return err
}

func releaseExclusiveFileLock(file *os.File) error {
	return windows.UnlockFileEx(
		windows.Handle(file.Fd()),
		0,
		1,
		0,
		&windows.Overlapped{},
	)
}
