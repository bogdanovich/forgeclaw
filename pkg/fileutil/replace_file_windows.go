//go:build windows

package fileutil

import (
	"fmt"

	"golang.org/x/sys/windows"
)

func replaceFileAtomic(from, to string) error {
	fromPtr, err := windows.UTF16PtrFromString(from)
	if err != nil {
		return fmt.Errorf("encode source path: %w", err)
	}
	toPtr, err := windows.UTF16PtrFromString(to)
	if err != nil {
		return fmt.Errorf("encode destination path: %w", err)
	}
	if err := windows.MoveFileEx(
		fromPtr,
		toPtr,
		windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH,
	); err != nil {
		return fmt.Errorf("replace file with write-through: %w", err)
	}
	return nil
}
