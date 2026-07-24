//go:build darwin

package main

import "golang.org/x/sys/unix"

func renameLaunchdNoReplace(directoryFD int, oldName, newName string) error {
	return unix.RenameatxNp(
		directoryFD,
		oldName,
		directoryFD,
		newName,
		unix.RENAME_EXCL,
	)
}
