//go:build linux

package companion

import (
	"syscall"

	"golang.org/x/sys/unix"
)

func fileStatDevice(stat *unix.Stat_t) uint64 {
	return uint64(stat.Dev)
}

func fileStatLinks(stat *unix.Stat_t) uint64 {
	return uint64(stat.Nlink)
}

func syscallStatDevice(stat *syscall.Stat_t) uint64 {
	return uint64(stat.Dev)
}

func syscallStatLinks(stat *syscall.Stat_t) uint64 {
	return uint64(stat.Nlink)
}

func platformDescriptorMountIdentity(
	descriptor int,
) (fileMountIdentity, error) {
	var stat unix.Statx_t
	if err := unix.Statx(
		descriptor,
		"",
		unix.AT_EMPTY_PATH|unix.AT_STATX_SYNC_AS_STAT,
		unix.STATX_MNT_ID,
		&stat,
	); err != nil {
		return fileMountIdentity{}, err
	}
	if stat.Mask&unix.STATX_MNT_ID == 0 {
		return fileMountIdentity{}, ErrFileAccessDenied
	}
	return fileMountIdentity{primary: stat.Mnt_id}, nil
}
