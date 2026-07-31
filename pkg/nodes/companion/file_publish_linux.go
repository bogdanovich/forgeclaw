//go:build linux

package companion

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func platformPublishFileStage(
	stageFD int,
	stagingDirectoryFD int,
	_ string,
	destinationDirectoryFD int,
	finalName string,
	publication string,
) error {
	source := fmt.Sprintf("/proc/self/fd/%d", stageFD)
	switch publication {
	case filePublicationCreate:
		return unix.Linkat(
			unix.AT_FDCWD,
			source,
			destinationDirectoryFD,
			finalName,
			unix.AT_SYMLINK_FOLLOW,
		)
	case filePublicationReplace:
		publicationName, err := randomFileStageName()
		if err != nil {
			return err
		}
		if err := unix.Linkat(
			unix.AT_FDCWD,
			source,
			stagingDirectoryFD,
			publicationName,
			unix.AT_SYMLINK_FOLLOW,
		); err != nil {
			return err
		}
		if err := unix.Renameat2(
			stagingDirectoryFD,
			publicationName,
			destinationDirectoryFD,
			finalName,
			0,
		); err != nil {
			_ = unix.Unlinkat(stagingDirectoryFD, publicationName, 0)
			return err
		}
		return nil
	default:
		return ErrFileAccessDenied
	}
}
