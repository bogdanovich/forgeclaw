//go:build !linux && !darwin

package gateway

import (
	"errors"
	"os"
)

func openNodeTransferMedia(string) (*os.File, os.FileInfo, error) {
	return nil, nil, errors.New("node file transfer is unsupported on this gateway platform")
}

func syncNodeTransferDirectory(string) error {
	return errors.New("node file transfer is unsupported on this gateway platform")
}
