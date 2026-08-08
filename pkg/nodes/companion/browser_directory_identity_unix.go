//go:build darwin || linux

package companion

import (
	"errors"
	"os"
	"syscall"
)

func validateBrowserProfileDirectory(info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return errors.New("profile_directory must be owned by the companion account")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return errors.New("profile_directory must not grant group or world access")
	}
	return nil
}
