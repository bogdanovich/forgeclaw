// PicoClaw - Ultra-lightweight personal AI agent
// Inspired by and based on nanobot: https://github.com/HKUDS/nanobot
// License: MIT
//
// Copyright (c) 2026 PicoClaw contributors

// Package fileutil provides file manipulation utilities.
package fileutil

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// CommittedWriteError reports that rename committed the new file, but the
// parent-directory sync failed, so crash durability could not be confirmed.
type CommittedWriteError struct {
	Err error
}

func (e *CommittedWriteError) Error() string {
	return fmt.Sprintf("write committed but durability was not confirmed: %v", e.Err)
}

func (e *CommittedWriteError) Unwrap() error {
	return e.Err
}

// IsCommittedWriteError distinguishes post-rename failures from failures that
// leave the original target unchanged.
func IsCommittedWriteError(err error) bool {
	var committedErr *CommittedWriteError
	return errors.As(err, &committedErr)
}

// WriteFileAtomic atomically writes data to a file using a temp file + rename pattern.
//
// This guarantees that the target file is either:
// - Completely written with the new data
// - Unchanged (if any step fails before rename)
//
// The function:
// 1. Creates a temp file in the same directory (original untouched)
// 2. Writes data to temp file
// 3. Syncs data to disk (critical for SD cards/flash storage)
// 4. Sets file permissions
// 5. Atomically renames temp file to target path
// 6. Syncs directory metadata where supported (ensures rename is durable)
//
// Safety guarantees:
// - Original file is NEVER modified until successful rename
// - Temp file is always cleaned up on error
// - Data is flushed to physical storage before rename
// - Directory entry is synced to prevent orphaned inodes
//
// Parameters:
//   - path: Target file path
//   - data: Data to write
//   - perm: File permission mode (e.g., 0o600 for secure, 0o644 for readable)
//
// Returns:
//   - Error if any step fails, nil on success
//
// Example:
//
//	// Secure config file (owner read/write only)
//	err := utils.WriteFileAtomic("config.json", data, 0o600)
//
//	// Public readable file
//	err := utils.WriteFileAtomic("public.txt", data, 0o644)
func WriteFileAtomic(path string, data []byte, perm os.FileMode) error {
	return writeFileAtomicPrepared(path, data, perm, syncDirectory, nil)
}

// WriteFileAtomicWithModTime applies modTime to the temporary file before the
// atomic rename, so timestamp failure leaves the original target unchanged.
func WriteFileAtomicWithModTime(
	path string,
	data []byte,
	perm os.FileMode,
	modTime time.Time,
) error {
	return writeFileAtomicPrepared(path, data, perm, syncDirectory, func(tmpPath string) error {
		return os.Chtimes(tmpPath, modTime, modTime)
	})
}

func writeFileAtomic(
	path string,
	data []byte,
	perm os.FileMode,
	syncDir func(string) error,
) error {
	return writeFileAtomicPrepared(path, data, perm, syncDir, nil)
}

func writeFileAtomicPrepared(
	path string,
	data []byte,
	perm os.FileMode,
	syncDir func(string) error,
	prepare func(string) error,
) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Create temp file in the same directory (ensures atomic rename works)
	// Using a hidden prefix (.tmp-) to avoid issues with some tools
	tmpFile, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf(".tmp-%d-%d", os.Getpid(), time.Now().UnixNano())),
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		perm,
	)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	tmpPath := tmpFile.Name()
	cleanup := true

	defer func() {
		if cleanup {
			tmpFile.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	// Write data to temp file
	// Note: Original file is untouched at this point
	if _, err := tmpFile.Write(data); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	// CRITICAL: Force sync to storage medium before any other operations.
	// This ensures data is physically written to disk, not just cached.
	// Essential for SD cards, eMMC, and other flash storage on edge devices.
	if err := tmpFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync temp file: %w", err)
	}

	// Set file permissions before closing
	if err := tmpFile.Chmod(perm); err != nil {
		return fmt.Errorf("failed to set permissions: %w", err)
	}
	if prepare != nil {
		if err := prepare(tmpPath); err != nil {
			return fmt.Errorf("failed to prepare temp file: %w", err)
		}
	}

	// Close file before rename (required on Windows)
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// Atomic rename: temp file becomes the target
	// On POSIX: rename() is atomic
	// On Windows: Rename() is atomic for files
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}
	// The temp path no longer exists after rename, including when directory
	// durability cannot be confirmed below.
	cleanup = false

	// Sync directory to ensure rename is durable
	// This prevents the renamed file from disappearing after a crash
	if err := syncDir(dir); err != nil {
		return &CommittedWriteError{Err: fmt.Errorf("failed to sync parent directory: %w", err)}
	}

	return nil
}

func CopyFile(src, dst string, perm os.FileMode) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return WriteFileAtomic(dst, data, perm)
}
