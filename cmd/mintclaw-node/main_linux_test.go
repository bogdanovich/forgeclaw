//go:build linux

package main

import (
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/nodes/companion"
)

func TestFileHelperAuthorityRejectsRootCompanion(t *testing.T) {
	config := companion.Config{
		FileHelper: &companion.FileHelperClientConfig{Enabled: true, SocketPath: "/run/helper.sock"},
	}
	if err := validateFileHelperProcessIdentity(config, 0); err == nil {
		t.Fatal("root-run full companion was accepted with helper authority")
	}
	if err := validateFileHelperProcessIdentity(config, 1000); err != nil {
		t.Fatalf("unprivileged companion rejected: %v", err)
	}
	if err := validateFileHelperProcessIdentity(companion.Config{}, 0); err != nil {
		t.Fatalf("unrelated root-run companion behavior changed: %v", err)
	}
}
