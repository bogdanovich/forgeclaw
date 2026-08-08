//go:build darwin || linux

package companion

import (
	"os"
	"strings"
	"syscall"
	"testing"
)

func TestValidateBrowserProfileDirectoryRejectsForeignOwner(t *testing.T) {
	info, err := os.Stat(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	stat := *info.Sys().(*syscall.Stat_t)
	stat.Uid = uint32(os.Geteuid()) + 1
	err = validateBrowserProfileDirectory(fileInfoWithSystem{FileInfo: info, system: &stat})
	if err == nil || !strings.Contains(err.Error(), "must be owned by the companion account") {
		t.Fatalf("validateBrowserProfileDirectory() foreign-owner error = %v", err)
	}
}

type fileInfoWithSystem struct {
	os.FileInfo
	system any
}

func (info fileInfoWithSystem) Sys() any {
	return info.system
}
