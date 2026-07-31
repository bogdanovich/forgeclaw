//go:build darwin

package companion

import "golang.org/x/sys/unix"

func platformRenameFileStage(directoryFD int, stagingName, finalName string) error {
	return unix.RenameatxNp(directoryFD, stagingName, directoryFD, finalName, 0)
}
