//go:build windows

package fileutil

// Windows does not expose POSIX-style directory fsync through os.File.Sync.
// The file itself is flushed and closed before the atomic rename.
func syncDirectory(string) error {
	return nil
}
