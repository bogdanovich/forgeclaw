//go:build !windows

package nodes

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func openRegularFileNoFollow(path string) (*os.File, os.FileInfo, error) {
	descriptor, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, nil, err
	}
	file := os.NewFile(uintptr(descriptor), path)
	if file == nil {
		_ = unix.Close(descriptor)
		return nil, nil, errors.New("open regular file: invalid descriptor")
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, nil, fmt.Errorf("open regular file: non-regular file %q", path)
	}
	return file, info, nil
}
