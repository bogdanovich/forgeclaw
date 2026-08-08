//go:build !darwin && !linux

package companion

import (
	"errors"
	"os"
)

func validateBrowserProfileDirectory(os.FileInfo) error {
	return errors.New("profile_directory ownership validation is unsupported on this platform")
}
