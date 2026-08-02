//go:build windows

package fileutil

import "testing"

func TestSyncDirectoryFlushesDirectoryHandle(t *testing.T) {
	if err := syncDirectory(t.TempDir()); err != nil {
		t.Fatalf("syncDirectory() error = %v", err)
	}
}
