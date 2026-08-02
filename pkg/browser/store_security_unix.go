//go:build !windows

package browser

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bogdanovich/mintclaw/pkg/fileutil"
)

func prepareStorePath(path string) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create browser state directory: %w", err)
	}
	directoryInfo, err := os.Lstat(directory)
	if err != nil || !directoryInfo.IsDir() || directoryInfo.Mode().Perm()&0o077 != 0 {
		return errors.New("browser state directory must be an owner-only directory")
	}
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect browser state store: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return errors.New("browser state store must be an owner-only regular file")
	}
	return nil
}

func writeSecureStoreFile(path string, data []byte, mode os.FileMode) error {
	return fileutil.WriteFileAtomic(path, data, mode)
}
