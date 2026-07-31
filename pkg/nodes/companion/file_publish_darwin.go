//go:build darwin

package companion

import "golang.org/x/sys/unix"

func platformPublishFileStage(
	stageFD int,
	stagingDirectoryFD int,
	destinationDirectoryFD int,
	finalName string,
	publication string,
) error {
	switch publication {
	case filePublicationCreate:
		return unix.Fclonefileat(
			stageFD,
			destinationDirectoryFD,
			finalName,
			0,
		)
	case filePublicationReplace:
		publicationName, err := randomFileStageName()
		if err != nil {
			return err
		}
		if err := unix.Fclonefileat(
			stageFD,
			stagingDirectoryFD,
			publicationName,
			0,
		); err != nil {
			return err
		}
		if err := unix.RenameatxNp(
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
