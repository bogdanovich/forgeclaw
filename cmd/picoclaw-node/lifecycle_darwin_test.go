//go:build darwin

package main

import (
	"strings"
	"testing"
)

func TestDarwinLifecycleRejectsMutationBeforeConfigValidation(t *testing.T) {
	t.Parallel()
	for _, action := range []string{"install", "uninstall"} {
		err := runServiceLifecycle(action, nil)
		if err == nil || !strings.Contains(err.Error(), "launchd "+action+" is not implemented") {
			t.Fatalf("%s returned unexpected error: %v", action, err)
		}
	}
}
