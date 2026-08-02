//go:build windows

package browser

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

func TestWindowsStoreReplacementIsSecuredBeforeRename(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "browser.json")
	store, err := NewFileStore(path, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	wantErr := errors.New("injected pre-commit stop")
	originalRename := renameWindowsStoreFile
	renameWindowsStoreFile = func(temporaryPath, destination string) error {
		if destination != path {
			t.Fatalf("rename destination = %q, want %q", destination, path)
		}
		assertOwnerOnlyWindowsStorePath(t, temporaryPath, false)
		return wantErr
	}
	defer func() { renameWindowsStoreFile = originalRename }()
	if err = store.CreateSession(context.Background(), testOpeningSession(testOwner())); !errors.Is(err, wantErr) {
		t.Fatalf("CreateSession() error = %v, want injected pre-commit stop", err)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("canonical store exists after pre-commit stop: %v", err)
	}
}

func TestWindowsStoreRejectsReparsePointDirectory(t *testing.T) {
	root := t.TempDir()
	target := t.TempDir()
	directory := filepath.Join(root, "state")
	if err := os.Symlink(target, directory); err != nil {
		t.Fatalf("create Windows directory symlink: %v", err)
	}
	store, err := NewFileStore(filepath.Join(directory, "browser.json"), 0, 0)
	if store != nil {
		store.Close()
	}
	if err == nil {
		t.Fatal("NewFileStore() through reparse point error = nil")
	}
	if _, statErr := os.Stat(filepath.Join(target, "browser.json")); !os.IsNotExist(statErr) {
		t.Fatalf("reparse target was modified: %v", statErr)
	}
}

func TestFileStoreCreatesAndReopensWithOwnerOnlyWindowsDACL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "browser.json")
	store, err := NewFileStore(path, 0, 0)
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}
	broker := lifecycleTestBroker(t, admittedBrowserConfig(), store, &fakeWorkerFactory{})
	if _, err = broker.Open(context.Background(), OpenRequest{
		Owner: testOwner(), Target: "gateway", Profile: "managed",
	}); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	store.Close()

	assertOwnerOnlyWindowsStorePath(t, filepath.Dir(path), true)
	assertOwnerOnlyWindowsStorePath(t, path, false)
	assertOwnerOnlyWindowsStorePath(t, path+".lock", false)

	reopened, err := NewFileStore(path, 0, 0)
	if err != nil {
		t.Fatalf("reopen FileStore error = %v", err)
	}
	reopened.Close()
}

func assertOwnerOnlyWindowsStorePath(t *testing.T, path string, directory bool) {
	t.Helper()
	owner, err := currentWindowsStoreOwner()
	if err != nil {
		t.Fatal(err)
	}
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	flags := uint32(windows.FILE_ATTRIBUTE_NORMAL)
	if directory {
		flags = windows.FILE_FLAG_BACKUP_SEMANTICS
	}
	handle, err := windows.CreateFile(
		pathPtr,
		windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		flags,
		0,
	)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	file := os.NewFile(uintptr(handle), path)
	defer file.Close()
	if err = validateOwnerOnlyWindowsStoreDACL(handle, owner); err != nil {
		t.Fatalf("validate owner-only DACL for %s: %v", path, err)
	}
}
