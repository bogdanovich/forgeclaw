//go:build windows

package mcp

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

func assertExclusiveLeaseFileSecurity(t *testing.T, path string) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	defer func() { _ = file.Close() }()
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatalf("GetTokenUser() error = %v", err)
	}
	if err := validateOwnerOnlyWindowsDACL(windows.Handle(file.Fd()), user.User.Sid); err != nil {
		t.Fatalf("validateOwnerOnlyWindowsDACL() error = %v", err)
	}
}

func TestOpenExclusiveLeaseFileReplacesInheritedWindowsDACL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "playwright.lock")
	if err := os.WriteFile(path, []byte("existing"), 0o666); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	file, err := openExclusiveLeaseFile(path)
	if err != nil {
		t.Fatalf("openExclusiveLeaseFile() error = %v", err)
	}
	defer func() { _ = file.Close() }()

	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatalf("GetTokenUser() error = %v", err)
	}
	if err := validateOwnerOnlyWindowsDACL(windows.Handle(file.Fd()), user.User.Sid); err != nil {
		t.Fatalf("validateOwnerOnlyWindowsDACL() error = %v", err)
	}
}
