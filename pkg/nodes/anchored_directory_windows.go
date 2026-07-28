//go:build windows

package nodes

import (
	"errors"
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"

	"github.com/bogdanovich/mintclaw/pkg/fileutil"
)

type anchoredDirectory struct {
	handle windows.Handle
}

type anchoredFileRenameInformation struct {
	ReplaceIfExists uint32
	RootDirectory   windows.Handle
	FileNameLength  uint32
	FileName        [1]uint16
}

func openAnchoredDirectory(path string) (*anchoredDirectory, error) {
	pathPointer, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	handle, err := windows.CreateFile(
		pathPointer,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return nil, err
	}
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		_ = windows.CloseHandle(handle)
		return nil, err
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0 ||
		info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		_ = windows.CloseHandle(handle)
		return nil, fmt.Errorf("open anchored directory: linked or non-directory %q", path)
	}
	return &anchoredDirectory{handle: handle}, nil
}

func (directory *anchoredDirectory) openRegular(name string) (*os.File, os.FileInfo, error) {
	handle, err := directory.openRelative(
		name,
		windows.FILE_GENERIC_READ,
		windows.FILE_OPEN,
		windows.FILE_NON_DIRECTORY_FILE,
	)
	if err != nil {
		return nil, nil, err
	}
	file := os.NewFile(uintptr(handle), name)
	if file == nil {
		_ = windows.CloseHandle(handle)
		return nil, nil, errors.New("open anchored regular file: invalid handle")
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, nil, fmt.Errorf("open anchored regular file: non-regular file %q", name)
	}
	return file, info, nil
}

func (directory *anchoredDirectory) acquireLock(name string) (func(), error) {
	handle, err := directory.openRelative(
		name,
		windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE,
		windows.FILE_OPEN_IF,
		windows.FILE_NON_DIRECTORY_FILE,
	)
	if err != nil {
		return nil, fmt.Errorf("open gateway terminal store lock: %w", err)
	}
	lock := os.NewFile(uintptr(handle), name)
	if lock == nil {
		_ = windows.CloseHandle(handle)
		return nil, errors.New("open gateway terminal store lock: invalid handle")
	}
	info, err := lock.Stat()
	if err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("inspect gateway terminal store lock: %w", err)
	}
	if !info.Mode().IsRegular() {
		_ = lock.Close()
		return nil, fmt.Errorf("gateway terminal store lock is non-regular: %q", name)
	}
	overlapped := &windows.Overlapped{}
	if err := windows.LockFileEx(
		windows.Handle(lock.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK,
		0,
		1,
		0,
		overlapped,
	); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("lock gateway terminal store: %w", err)
	}
	return func() {
		_ = windows.UnlockFileEx(windows.Handle(lock.Fd()), 0, 1, 0, overlapped)
		_ = lock.Close()
	}, nil
}

func (directory *anchoredDirectory) writeFileAtomic(
	name string,
	data []byte,
	mode os.FileMode,
) error {
	if err := validateAnchoredName(name); err != nil {
		return err
	}
	tempName, err := randomAnchoredTempName()
	if err != nil {
		return fmt.Errorf("generate gateway terminal store temp name: %w", err)
	}
	handle, err := directory.openRelative(
		tempName,
		windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE|windows.DELETE,
		windows.FILE_CREATE,
		windows.FILE_NON_DIRECTORY_FILE,
	)
	if err != nil {
		return fmt.Errorf("create gateway terminal store temp file: %w", err)
	}
	temp := os.NewFile(uintptr(handle), tempName)
	if temp == nil {
		_ = windows.CloseHandle(handle)
		return errors.New("create gateway terminal store temp file: invalid handle")
	}
	renamed := false
	defer func() {
		if !renamed {
			directory.deleteOnClose(handle)
		}
		_ = temp.Close()
	}()
	if _, err := temp.Write(data); err != nil {
		return fmt.Errorf("write gateway terminal store temp file: %w", err)
	}
	if err := temp.Chmod(mode); err != nil {
		return fmt.Errorf("set gateway terminal store permissions: %w", err)
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("sync gateway terminal store temp file: %w", err)
	}
	if err := directory.rename(handle, name); err != nil {
		return fmt.Errorf("replace gateway terminal store: %w", err)
	}
	renamed = true
	if err := temp.Close(); err != nil {
		return &fileutil.CommittedWriteError{
			Err: fmt.Errorf("close committed gateway terminal store: %w", err),
		}
	}
	return nil
}

func (directory *anchoredDirectory) openRelative(
	name string,
	access uint32,
	disposition uint32,
	options uint32,
) (windows.Handle, error) {
	if directory == nil || directory.handle == 0 {
		return 0, errors.New("anchored directory is closed")
	}
	if err := validateAnchoredName(name); err != nil {
		return 0, err
	}
	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return 0, err
	}
	attributes := &windows.OBJECT_ATTRIBUTES{
		RootDirectory: directory.handle,
		ObjectName:    objectName,
		Attributes:    windows.OBJ_CASE_INSENSITIVE | windows.OBJ_DONT_REPARSE,
	}
	attributes.Length = uint32(unsafe.Sizeof(*attributes))
	var (
		handle         windows.Handle
		status         windows.IO_STATUS_BLOCK
		allocationSize int64
	)
	err = windows.NtCreateFile(
		&handle,
		access,
		attributes,
		&status,
		&allocationSize,
		windows.FILE_ATTRIBUTE_NORMAL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		disposition,
		options|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		0,
		0,
	)
	if errors.Is(err, windows.STATUS_OBJECT_NAME_NOT_FOUND) ||
		errors.Is(err, windows.STATUS_OBJECT_PATH_NOT_FOUND) {
		return 0, os.ErrNotExist
	}
	if err != nil {
		return 0, err
	}
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		_ = windows.CloseHandle(handle)
		return 0, err
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		_ = windows.CloseHandle(handle)
		return 0, fmt.Errorf("open anchored file: reparse point %q", name)
	}
	if info.NumberOfLinks != 1 {
		_ = windows.CloseHandle(handle)
		return 0, fmt.Errorf("open anchored file: multiply linked file %q", name)
	}
	return handle, nil
}

func (directory *anchoredDirectory) rename(handle windows.Handle, name string) error {
	nameUTF16, err := windows.UTF16FromString(name)
	if err != nil {
		return err
	}
	nameBytes := (len(nameUTF16) - 1) * 2
	var template anchoredFileRenameInformation
	bufferSize := int(unsafe.Offsetof(template.FileName)) + nameBytes
	buffer := make([]byte, bufferSize)
	information := (*anchoredFileRenameInformation)(unsafe.Pointer(&buffer[0]))
	information.ReplaceIfExists = windows.FILE_RENAME_REPLACE_IF_EXISTS | windows.FILE_RENAME_POSIX_SEMANTICS
	information.RootDirectory = directory.handle
	information.FileNameLength = uint32(nameBytes)
	target := unsafe.Slice(&information.FileName[0], nameBytes/2)
	copy(target, nameUTF16[:len(nameUTF16)-1])
	var status windows.IO_STATUS_BLOCK
	return windows.NtSetInformationFile(
		handle,
		&status,
		&buffer[0],
		uint32(bufferSize),
		windows.FileRenameInformation,
	)
}

func (directory *anchoredDirectory) deleteOnClose(handle windows.Handle) {
	var (
		status windows.IO_STATUS_BLOCK
		remove = byte(1)
	)
	_ = windows.NtSetInformationFile(
		handle,
		&status,
		&remove,
		1,
		windows.FileDispositionInformation,
	)
}

func (directory *anchoredDirectory) close() error {
	if directory == nil || directory.handle == 0 {
		return nil
	}
	err := windows.CloseHandle(directory.handle)
	directory.handle = 0
	return err
}
