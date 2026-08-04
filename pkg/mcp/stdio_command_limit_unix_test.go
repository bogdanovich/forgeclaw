//go:build linux || darwin

package mcp

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestStdioCommandEnforcesFileSizeLimitAtWriteBoundary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bounded-output")
	cmd, err := stdioCommand(
		context.Background(),
		"/bin/sh",
		[]string{"-c", `dd if=/dev/zero of="$1" bs=4096 count=4 2>/dev/null`, "sh", path},
		1024,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Run(); err == nil {
		t.Fatal("oversize writer unexpectedly succeeded")
	}
	info, statErr := os.Stat(path)
	if statErr != nil {
		t.Fatal(statErr)
	}
	if info.Size() > 1024 {
		t.Fatalf("bounded output size = %d, want <= 1024", info.Size())
	}
}
