//go:build windows

package browser

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"unsafe"

	"golang.org/x/sys/windows"

	"github.com/bogdanovich/mintclaw/pkg/fileutil"
)

func prepareStorePath(path string) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create browser state directory: %w", err)
	}
	if err := secureWindowsStorePath(directory, true); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect browser state store: %w", err)
	}
	if !info.Mode().IsRegular() {
		return errors.New("browser state store must be a regular file")
	}
	return secureWindowsStorePath(path, false)
}

func writeSecureStoreFile(path string, data []byte, mode os.FileMode) error {
	if err := fileutil.WriteFileAtomic(path, data, mode); err != nil {
		return err
	}
	if err := secureWindowsStorePath(path, false); err != nil {
		return &fileutil.CommittedWriteError{Err: err}
	}
	return nil
}

func currentWindowsStoreOwner() (*windows.SID, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return nil, fmt.Errorf("get current Windows browser-store user: %w", err)
	}
	return user.User.Sid, nil
}

func ownerOnlyWindowsStoreDescriptor(owner *windows.SID) (*windows.SECURITY_DESCRIPTOR, *windows.ACL, error) {
	descriptor, err := windows.SecurityDescriptorFromString(
		"O:" + owner.String() + "D:P(A;;GA;;;" + owner.String() + ")",
	)
	if err != nil {
		return nil, nil, fmt.Errorf("build owner-only Windows browser-store descriptor: %w", err)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		return nil, nil, fmt.Errorf("read owner-only Windows browser-store DACL: %w", err)
	}
	return descriptor, dacl, nil
}

func secureWindowsStorePath(path string, directory bool) error {
	owner, err := currentWindowsStoreOwner()
	if err != nil {
		return err
	}
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	flags := uint32(windows.FILE_ATTRIBUTE_NORMAL)
	if directory {
		flags = windows.FILE_FLAG_BACKUP_SEMANTICS
	}
	handle, err := windows.CreateFile(
		pathPtr,
		windows.READ_CONTROL|windows.WRITE_DAC,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		flags,
		0,
	)
	if err != nil {
		return fmt.Errorf("open Windows browser-store path: %w", err)
	}
	defer windows.CloseHandle(handle)
	return secureWindowsStoreHandle(handle, owner)
}

func secureWindowsStoreHandle(handle windows.Handle, owner *windows.SID) error {
	descriptor, err := windows.GetSecurityInfo(
		handle,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION,
	)
	if err != nil {
		return fmt.Errorf("read Windows browser-store owner: %w", err)
	}
	actualOwner, _, err := descriptor.Owner()
	if err != nil || !actualOwner.Equals(owner) {
		return errors.New("Windows browser-store owner validation failed")
	}
	_, dacl, err := ownerOnlyWindowsStoreDescriptor(owner)
	if err != nil {
		return err
	}
	information := windows.SECURITY_INFORMATION(
		windows.DACL_SECURITY_INFORMATION | windows.PROTECTED_DACL_SECURITY_INFORMATION,
	)
	if err = windows.SetSecurityInfo(handle, windows.SE_FILE_OBJECT, information, nil, nil, dacl, nil); err != nil {
		return fmt.Errorf("apply owner-only Windows browser-store DACL: %w", err)
	}
	return validateOwnerOnlyWindowsStoreDACL(handle, owner)
}

func validateOwnerOnlyWindowsStoreDACL(handle windows.Handle, owner *windows.SID) error {
	descriptor, err := windows.GetSecurityInfo(
		handle,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION,
	)
	if err != nil {
		return fmt.Errorf("read Windows browser-store security descriptor: %w", err)
	}
	actualOwner, _, err := descriptor.Owner()
	if err != nil || !actualOwner.Equals(owner) {
		return errors.New("Windows browser-store owner validation failed")
	}
	control, _, err := descriptor.Control()
	if err != nil || control&windows.SE_DACL_PROTECTED == 0 {
		return errors.New("Windows browser-store DACL is not protected")
	}
	dacl, _, err := descriptor.DACL()
	if err != nil || dacl == nil || dacl.AceCount != 1 {
		return errors.New("Windows browser-store DACL is not owner-only")
	}
	var ace *windows.ACCESS_ALLOWED_ACE
	if err = windows.GetAce(dacl, 0, &ace); err != nil {
		return fmt.Errorf("read Windows browser-store DACL entry: %w", err)
	}
	aceOwner := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
	const fileAllAccess windows.ACCESS_MASK = 0x1F01FF
	fullControl := ace.Mask&windows.GENERIC_ALL != 0 || ace.Mask&fileAllAccess == fileAllAccess
	if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE || !fullControl || !aceOwner.Equals(owner) {
		return errors.New("Windows browser-store DACL is not owner-only")
	}
	return nil
}
