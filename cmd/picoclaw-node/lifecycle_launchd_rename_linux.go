//go:build linux

package main

import "golang.org/x/sys/unix"

func renameLaunchdNoReplace(directoryFD int, oldName, newName string) error {
	return unix.Renameat2(
		directoryFD,
		oldName,
		directoryFD,
		newName,
		unix.RENAME_NOREPLACE,
	)
}
