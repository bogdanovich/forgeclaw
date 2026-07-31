//go:build linux || darwin

package companion

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestFileResolverPinsSourceDescriptorAcrossRename(t *testing.T) {
	rootPath := canonicalTempDir(t)
	original := filepath.Join(rootPath, "source.txt")
	if err := os.WriteFile(original, []byte("trusted"), 0o600); err != nil {
		t.Fatal(err)
	}
	root, err := openFileRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.close() })
	source, err := root.openRegular(original, 1024, false)
	if err != nil {
		t.Fatal(err)
	}
	defer source.file.Close()
	moved := filepath.Join(rootPath, "moved.txt")
	if err := os.Rename(original, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(original, []byte("attacker"), 0o600); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(source.file.Name())
	if err == nil {
		t.Fatalf("descriptor name unexpectedly resolved after rename: %q", data)
	}
	if _, err := source.file.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 7)
	if _, err := source.file.Read(buffer); err != nil {
		t.Fatal(err)
	}
	if string(buffer) != "trusted" {
		t.Fatalf("pinned descriptor read = %q", buffer)
	}
}

func TestFileResolverRejectsTraversalSymlinksAndSpecialFiles(t *testing.T) {
	rootPath := canonicalTempDir(t)
	regular := filepath.Join(rootPath, "regular.txt")
	if err := os.WriteFile(regular, []byte("safe"), 0o600); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(canonicalTempDir(t), "outside.txt")
	if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(rootPath, "linked.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(rootPath, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(
		filepath.Dir(outside),
		filepath.Join(rootPath, "nested", "escape"),
	); err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(rootPath, "pipe")
	if err := unix.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	root, err := openFileRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.close() })
	tests := []string{
		"",
		rootPath,
		rootPath + string(filepath.Separator),
		filepath.Join(rootPath, "line\nbreak"),
		filepath.Join(rootPath, "..", filepath.Base(outside)),
		filepath.Join(rootPath, "linked.txt"),
		filepath.Join(rootPath, "nested", "escape", filepath.Base(outside)),
		fifo,
	}
	for _, path := range tests {
		if opened, openErr := root.openRegular(path, 1024, false); openErr == nil {
			_ = opened.file.Close()
			t.Errorf("openRegular(%q) unexpectedly succeeded", path)
		}
	}
}

func TestFileResolverBindsDestinationParentAcrossRename(t *testing.T) {
	rootPath := canonicalTempDir(t)
	originalParent := filepath.Join(rootPath, "project")
	if err := os.Mkdir(originalParent, 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := openFileRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.close() })
	destination := filepath.Join(originalParent, "config.txt")
	parent, err := root.resolveParent(destination, false)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.close()
	stage, err := parent.createStage("transfer_parent_race")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stage.file.Write([]byte("trusted")); err != nil {
		t.Fatal(err)
	}
	movedParent := filepath.Join(rootPath, "moved")
	if err := os.Rename(originalParent, movedParent); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(originalParent, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := stage.publish(filePublicationCreate); err != nil {
		t.Fatal(err)
	}
	_ = stage.file.Close()
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement path was mutated: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(movedParent, "config.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "trusted" {
		t.Fatalf("descriptor-bound publication = %q", data)
	}
}

func TestFileResolverCreateAndReplaceAreExplicit(t *testing.T) {
	rootPath := canonicalTempDir(t)
	path := filepath.Join(rootPath, "config.txt")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	root, err := openFileRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.close() })

	createParent, err := root.resolveParent(path, false)
	if err != nil {
		t.Fatal(err)
	}
	createStage, err := createParent.createStage("transfer_create")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := createStage.file.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := createStage.publish(filePublicationCreate); !errors.Is(err, ErrFileConflict) {
		t.Fatalf("create publication error = %v", err)
	}
	_ = createStage.file.Close()
	if err := createParent.removeStage(createStage.identity, createStage.name); err != nil {
		t.Fatal(err)
	}
	_ = createParent.close()

	replaceParent, err := root.resolveParent(path, false)
	if err != nil {
		t.Fatal(err)
	}
	replaceStage, err := replaceParent.createStage("transfer_replace")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := replaceStage.file.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := replaceStage.publish(filePublicationReplace); err != nil {
		t.Fatal(err)
	}
	_ = replaceStage.file.Close()
	_ = replaceParent.close()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "new" {
		t.Fatalf("replace publication = %q", data)
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

func canonicalTempDir(t *testing.T) string {
	t.Helper()
	temporary := t.TempDir()
	if err := os.Chmod(temporary, 0o700); err != nil {
		t.Fatal(err)
	}
	path, err := filepath.EvalSymlinks(temporary)
	if err != nil {
		t.Fatal(err)
	}
	return path
}
