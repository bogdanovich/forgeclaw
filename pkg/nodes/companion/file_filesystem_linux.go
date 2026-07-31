//go:build linux

package companion

import (
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
		path, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", descriptor))
		if err != nil {
			return false, err
		}
		return path == "/dev" || strings.HasPrefix(path, "/dev/"), nil
	default:
		return false, nil
	}
}
