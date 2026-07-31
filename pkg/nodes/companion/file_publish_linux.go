//go:build linux

package companion

import "golang.org/x/sys/unix"

func platformRenameFileStage(directoryFD int, stagingName, finalName string) error {
	return unix.Renameat2(directoryFD, stagingName, directoryFD, finalName, 0)
}
