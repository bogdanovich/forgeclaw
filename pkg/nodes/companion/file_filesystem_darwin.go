//go:build darwin

package companion

import (
	"bytes"

	"golang.org/x/sys/unix"
)

func deniedFileSystem(descriptor int) (bool, error) {
	var stat unix.Statfs_t
	if err := unix.Fstatfs(descriptor, &stat); err != nil {
		return false, err
	}
	name := make([]byte, 0, len(stat.Fstypename))
	for _, character := range stat.Fstypename {
		if character == 0 {
			break
		}
		name = append(name, character)
	}
	return bytes.Equal(name, []byte("devfs")) ||
		bytes.Equal(name, []byte("procfs")), nil
}
