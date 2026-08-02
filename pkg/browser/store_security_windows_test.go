//go:build windows

package browser

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

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
