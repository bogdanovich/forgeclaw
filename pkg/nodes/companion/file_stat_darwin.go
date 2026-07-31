//go:build darwin

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
	var stat unix.Statfs_t
	if err := unix.Fstatfs(descriptor, &stat); err != nil {
		return fileMountIdentity{}, err
	}
	return fileMountIdentity{
		primary:   uint64(uint32(stat.Fsid.Val[0])),
		secondary: uint64(uint32(stat.Fsid.Val[1])),
	}, nil
}
