//go:build linux

package companion

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileResolverRejectsPseudoFilesystemsAndMountCrossing(t *testing.T) {
	if root, err := openFileRoot("/proc"); !errors.Is(err, ErrFileAccessDenied) {
		if root != nil {
			_ = root.close()
		}
		t.Fatalf("openFileRoot(/proc) error = %v", err)
	}
	for _, path := range []string{"/dev", "/dev/mqueue"} {
		if _, statErr := os.Stat(path); statErr != nil {
			continue
		}
		if root, openErr := openFileRoot(path); !errors.Is(
			openErr,
			ErrFileAccessDenied,
		) {
			if root != nil {
				_ = root.close()
			}
			t.Fatalf("openFileRoot(%s) error = %v", path, openErr)
		}
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

func TestFileResolverRejectsSameDeviceMountIdentityChange(t *testing.T) {
	rootPath := canonicalTempDir(t)
	nested := filepath.Join(rootPath, "nested")
	if err := os.Mkdir(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(nested, "file.txt")
	if err := os.WriteFile(path, []byte("same device"), 0o600); err != nil {
		t.Fatal(err)
	}
	root, err := openFileRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	defer root.close()

	original := descriptorMountIdentity
	descriptorMountIdentity = func(
		descriptor int,
	) (fileMountIdentity, error) {
		resolved, readErr := os.Readlink(
			filepath.Join("/proc/self/fd", fmt.Sprint(descriptor)),
		)
		if readErr != nil {
			return fileMountIdentity{}, readErr
		}
		if strings.Contains(resolved, string(filepath.Separator)+"nested") {
			return fileMountIdentity{primary: root.mount.primary + 1}, nil
		}
		return original(descriptor)
	}
	t.Cleanup(func() { descriptorMountIdentity = original })

	if file, openErr := root.openRegular(path, 1024, false); !errors.Is(
		openErr,
		ErrFileAccessDenied,
	) {
		if file != nil {
			_ = file.file.Close()
		}
		t.Fatalf("same-device mount identity change error = %v", openErr)
	}
}
