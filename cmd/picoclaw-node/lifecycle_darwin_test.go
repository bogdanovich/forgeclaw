//go:build darwin

package main

import "testing"

func TestDarwinLifecycleAllowsInstall(t *testing.T) {
	t.Parallel()
	if err := validatePlatformServiceAction("install"); err != nil {
		t.Fatalf("install validation failed: %v", err)
	}
}

func TestDarwinLifecycleAllowsUninstall(t *testing.T) {
	t.Parallel()
	if err := validatePlatformServiceAction("uninstall"); err != nil {
		t.Fatalf("uninstall validation failed: %v", err)
	}
}
