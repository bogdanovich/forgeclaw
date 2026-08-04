//go:build linux || darwin

package browser

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidatePrivateBrowserOutputRootRejectsWritableAndSymlinkRoots(t *testing.T) {
	private := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(private, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := validatePrivateBrowserOutputRoot(private); err != nil {
		t.Fatalf("private root error = %v", err)
	}
	writable := filepath.Join(t.TempDir(), "writable")
	if err := os.Mkdir(writable, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(writable, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := validatePrivateBrowserOutputRoot(writable); err == nil {
		t.Fatal("writable root unexpectedly accepted")
	}
	symlink := filepath.Join(t.TempDir(), "root-link")
	if err := os.Symlink(private, symlink); err != nil {
		t.Fatal(err)
	}
	if err := validatePrivateBrowserOutputRoot(symlink); err == nil {
		t.Fatal("symlink root unexpectedly accepted")
	}
}
