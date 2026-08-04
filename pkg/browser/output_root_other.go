//go:build !linux && !darwin

package browser

import (
	"errors"
	"os"
)

func validatePrivateBrowserOutputRoot(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
		return errors.New("browser output root is not a private directory")
	}
	return nil
}
