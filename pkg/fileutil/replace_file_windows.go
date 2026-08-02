//go:build windows

package fileutil

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sys/windows"
)

const (
	replaceFileMaxAttempts = 10
	replaceFileRetryDelay  = 10 * time.Millisecond
)

var replaceFileMu sync.Mutex

func replaceFileAtomic(from, to string) error {
	replaceFileMu.Lock()
	defer replaceFileMu.Unlock()

	return replaceFileAtomicWith(from, to, windows.MoveFileEx, time.Sleep)
}

func replaceFileAtomicWith(
	from, to string,
	move func(*uint16, *uint16, uint32) error,
	sleep func(time.Duration),
) error {
	fromPtr, err := windows.UTF16PtrFromString(from)
	if err != nil {
		return fmt.Errorf("encode source path: %w", err)
	}
	toPtr, err := windows.UTF16PtrFromString(to)
	if err != nil {
		return fmt.Errorf("encode destination path: %w", err)
	}
	for attempt := 1; attempt <= replaceFileMaxAttempts; attempt++ {
		err = move(
			fromPtr,
			toPtr,
			windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH,
		)
		if err == nil {
			return nil
		}
		if !isRetryableReplaceError(err) || attempt == replaceFileMaxAttempts {
			return fmt.Errorf("replace file with write-through: %w", err)
		}
		sleep(replaceFileRetryDelay)
	}

	return nil // unreachable
}

func isRetryableReplaceError(err error) bool {
	return errors.Is(err, windows.ERROR_ACCESS_DENIED) ||
		errors.Is(err, windows.ERROR_SHARING_VIOLATION)
}
