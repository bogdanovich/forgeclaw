//go:build linux

package companion

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
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

func TestFileResolverPublishesPinnedStageAfterNameSubstitution(t *testing.T) {
	rootPath := canonicalTempDir(t)
	destination := filepath.Join(rootPath, "published.txt")
	root, err := openFileRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.close() })
	parent, err := root.resolveParent(destination, false)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.close()
	stage, err := parent.createStage("transfer_stage_substitution")
	if err != nil {
		t.Fatal(err)
	}
	defer stage.file.Close()
	if _, err := stage.file.Write([]byte("trusted")); err != nil {
		t.Fatal(err)
	}
	stolen := stage.name + ".stolen"
	if err := unix.Renameat(
		int(parent.staging.Fd()),
		stage.name,
		int(parent.staging.Fd()),
		stolen,
	); err != nil {
		t.Fatal(err)
	}
	attackerDescriptor, err := unix.Openat(
		int(parent.staging.Fd()),
		stage.name,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC,
		0o600,
	)
	if err != nil {
		t.Fatal(err)
	}
	attacker := os.NewFile(uintptr(attackerDescriptor), stage.name)
	if _, err := attacker.Write([]byte("attacker")); err != nil {
		_ = attacker.Close()
		t.Fatal(err)
	}
	if err := attacker.Close(); err != nil {
		t.Fatal(err)
	}

	var committed *committedFileMutationError
	if err := stage.publish(filePublicationCreate); !errors.As(err, &committed) {
		t.Fatalf("substituted stage publication error = %v", err)
	}
	data, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "trusted" {
		t.Fatalf("published substituted stage = %q", data)
	}
}

func TestDevicePseudoFilesystemClassificationSurvivesBindAlias(t *testing.T) {
	mountInfo := strings.Join([]string{
		"21 1 0:42 / /dev rw,nosuid - tmpfs tmpfs rw",
		"22 1 0:43 / /ordinary rw - tmpfs tmpfs rw",
		"23 1 0:42 / /admitted/alias rw - tmpfs tmpfs rw",
	}, "\n")
	denied, err := deviceMountedBelowDev("0:42", mountInfo)
	if err != nil {
		t.Fatal(err)
	}
	if !denied {
		t.Fatal("bind alias of /dev tmpfs was not denied by stable device metadata")
	}
	denied, err = deviceMountedBelowDev("0:43", mountInfo)
	if err != nil {
		t.Fatal(err)
	}
	if denied {
		t.Fatal("ordinary tmpfs device was blanket denied")
	}
}
