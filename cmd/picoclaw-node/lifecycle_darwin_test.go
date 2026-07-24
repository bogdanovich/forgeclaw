//go:build darwin

package main

import (
	"strings"
	"testing"
)

func TestDarwinLifecycleRejectsUninstallBeforeConfigValidation(t *testing.T) {
	t.Parallel()
	err := runServiceLifecycle("uninstall", nil)
	if err == nil || !strings.Contains(err.Error(), "launchd uninstall is not implemented") {
		t.Fatalf("uninstall returned unexpected error: %v", err)
	}
}

func TestDarwinLifecycleAllowsInstall(t *testing.T) {
	t.Parallel()
	if err := validatePlatformServiceAction("install"); err != nil {
		t.Fatalf("install validation failed: %v", err)
	}
}
