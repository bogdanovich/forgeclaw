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
		"github.com/bogdanovich/mintclaw/pkg/agent",
		"github.com/bogdanovich/mintclaw/pkg/channels",
		"github.com/bogdanovich/mintclaw/pkg/mcp",
		"github.com/bogdanovich/mintclaw/pkg/memory",
		"github.com/bogdanovich/mintclaw/pkg/providers",
		"github.com/bogdanovich/mintclaw/pkg/session",
	}
	for _, dependency := range strings.Fields(string(output)) {
		for _, forbiddenPrefix := range forbidden {
			if dependency == forbiddenPrefix || strings.HasPrefix(dependency, forbiddenPrefix+"/") {
				t.Errorf("mintclaw-node imports forbidden runtime dependency %s", dependency)
			}
		}
	}
}
