//go:build linux

package companion

import (
	"errors"
	"testing"
)

func TestFileResolverRejectsPseudoFilesystemsAndMountCrossing(t *testing.T) {
	if root, err := openFileRoot("/proc"); !errors.Is(err, ErrFileAccessDenied) {
		if root != nil {
			_ = root.close()
		}
		t.Fatalf("openFileRoot(/proc) error = %v", err)
	}
	root, err := openFileRoot("/")
	if err != nil {
		t.Fatal(err)
	}
	defer root.close()
	if file, err := root.openRegular("/proc/version", 1024*1024, false); !errors.Is(
		err,
		ErrFileAccessDenied,
	) {
		if file != nil {
			_ = file.file.Close()
		}
		t.Fatalf("cross-mount /proc open error = %v", err)
	}
	if file, err := root.openRegular("/proc/version", 1024*1024, true); !errors.Is(
		err,
		ErrFileAccessDenied,
	) {
		if file != nil {
			_ = file.file.Close()
		}
		t.Fatalf("pseudo-filesystem /proc open error = %v", err)
	}
}
