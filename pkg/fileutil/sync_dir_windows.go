//go:build windows

package fileutil

import (
	"fmt"
	"path/filepath"

	"golang.org/x/sys/windows"
)

func syncDirectory(path string) error {
	pathPtr, err := windows.UTF16PtrFromString(filepath.Clean(path))
	if err != nil {
		return fmt.Errorf("encode directory path: %w", err)
	}
	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return fmt.Errorf("open directory for flush: %w", err)
	}
	if err := windows.FlushFileBuffers(handle); err != nil {
		_ = windows.CloseHandle(handle)
		return fmt.Errorf("flush directory buffers: %w", err)
	}
	if err := windows.CloseHandle(handle); err != nil {
		return fmt.Errorf("close flushed directory: %w", err)
	}
	return nil
}
