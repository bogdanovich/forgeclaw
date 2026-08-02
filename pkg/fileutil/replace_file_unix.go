//go:build !windows

package fileutil

import "os"

func replaceFileAtomic(from, to string) error {
	return os.Rename(from, to)
}
