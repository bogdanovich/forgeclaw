//go:build linux

package companion

import "golang.org/x/sys/unix"

func deniedFileSystem(descriptor int) (bool, error) {
	var stat unix.Statfs_t
	if err := unix.Fstatfs(descriptor, &stat); err != nil {
		return false, err
	}
	switch stat.Type {
	case unix.PROC_SUPER_MAGIC,
		unix.SYSFS_MAGIC,
		unix.DEVPTS_SUPER_MAGIC,
		unix.DEBUGFS_MAGIC,
		unix.SECURITYFS_MAGIC,
		unix.TRACEFS_MAGIC,
		unix.BPF_FS_MAGIC,
		unix.CGROUP_SUPER_MAGIC,
		unix.CGROUP2_SUPER_MAGIC:
		return true, nil
	default:
		return false, nil
	}
}
