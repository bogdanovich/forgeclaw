//go:build linux

package companion

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

const (
	configFSSuperMagic = 0x62656570
	fuseCtlSuperMagic  = 0x65735543
	mqueueSuperMagic   = 0x19800202
	rpcPipeSuperMagic  = 0x67596969
	maxMountInfoBytes  = 4 << 20
)

func deniedFileSystem(descriptor int) (bool, error) {
	var stat unix.Statfs_t
	if err := unix.Fstatfs(descriptor, &stat); err != nil {
		return false, err
	}
	switch stat.Type {
	case unix.AUTOFS_SUPER_MAGIC,
		unix.BDEVFS_MAGIC,
		unix.PROC_SUPER_MAGIC,
		unix.SYSFS_MAGIC,
		unix.DEVPTS_SUPER_MAGIC,
		unix.DEBUGFS_MAGIC,
		unix.SECURITYFS_MAGIC,
		unix.TRACEFS_MAGIC,
		unix.BPF_FS_MAGIC,
		unix.CGROUP_SUPER_MAGIC,
		unix.CGROUP2_SUPER_MAGIC,
		unix.EFIVARFS_MAGIC,
		unix.HUGETLBFS_MAGIC,
		unix.NSFS_MAGIC,
		unix.PSTOREFS_MAGIC,
		unix.SELINUX_MAGIC,
		unix.SMACK_MAGIC,
		configFSSuperMagic,
		fuseCtlSuperMagic,
		mqueueSuperMagic,
		rpcPipeSuperMagic:
		return true, nil
	case unix.TMPFS_MAGIC:
		device, err := descriptorDeviceIdentity(descriptor)
		if err != nil {
			return false, err
		}
		mountInfo, err := os.ReadFile("/proc/self/mountinfo")
		if err != nil {
			return false, err
		}
		if len(mountInfo) > maxMountInfoBytes {
			return false, errors.New("linux mount metadata exceeds safety bound")
		}
		return deviceMountedBelowDev(device, string(mountInfo))
	default:
		return false, nil
	}
}

func descriptorDeviceIdentity(descriptor int) (string, error) {
	var stat unix.Statx_t
	if err := unix.Statx(
		descriptor,
		"",
		unix.AT_EMPTY_PATH|unix.AT_STATX_SYNC_AS_STAT,
		unix.STATX_BASIC_STATS,
		&stat,
	); err != nil {
		return "", err
	}
	return fmt.Sprintf("%d:%d", stat.Dev_major, stat.Dev_minor), nil
}

func deviceMountedBelowDev(device, mountInfo string) (bool, error) {
	if device == "" {
		return false, ErrFileAccessDenied
	}
	for _, line := range strings.Split(mountInfo, "\n") {
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 10 {
			return false, errors.New("malformed linux mount metadata")
		}
		mountPoint := fields[4]
		if fields[2] == device &&
			(mountPoint == "/dev" || strings.HasPrefix(mountPoint, "/dev/")) {
			return true, nil
		}
	}
	return false, nil
}
