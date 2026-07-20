package main

import (
	"os/exec"
	"strings"
	"testing"
)

func TestSlimNodeDependencyBoundary(t *testing.T) {
	output, err := exec.Command("go", "list", "-deps", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go list node dependencies: %v\n%s", err, output)
	}
	forbidden := []string{
		"github.com/sipeed/picoclaw/pkg/agent",
		"github.com/sipeed/picoclaw/pkg/channels",
		"github.com/sipeed/picoclaw/pkg/mcp",
		"github.com/sipeed/picoclaw/pkg/memory",
		"github.com/sipeed/picoclaw/pkg/providers",
		"github.com/sipeed/picoclaw/pkg/session",
	}
	for _, dependency := range strings.Fields(string(output)) {
		for _, forbiddenPrefix := range forbidden {
			if dependency == forbiddenPrefix || strings.HasPrefix(dependency, forbiddenPrefix+"/") {
				t.Errorf("picoclaw-node imports forbidden runtime dependency %s", dependency)
			}
		}
	}
}
