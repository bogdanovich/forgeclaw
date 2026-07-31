//go:build darwin

package companion

import "golang.org/x/sys/unix"

func platformPublishFileStage(
	_ int,
	stagingDirectoryFD int,
	stageName string,
	destinationDirectoryFD int,
	finalName string,
	publication string,
) error {
	switch publication {
	case filePublicationCreate:
		return unix.RenameatxNp(
			stagingDirectoryFD,
			stageName,
			destinationDirectoryFD,
			finalName,
			unix.RENAME_EXCL,
		)
	case filePublicationReplace:
		return unix.RenameatxNp(
			stagingDirectoryFD,
			stageName,
			destinationDirectoryFD,
			finalName,
			0,
		)
	default:
		return ErrFileAccessDenied
	}
}
