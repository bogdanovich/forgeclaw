//go:build linux || darwin

package browser

import (
	"errors"
	"os"
	"syscall"
)

func validatePrivateBrowserOutputRoot(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
		return errors.New("browser output root is not a private directory")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return errors.New("browser output root has an unexpected owner")
	}
	return nil
}
