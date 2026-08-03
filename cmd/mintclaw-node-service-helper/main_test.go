//go:build linux

package main

import (
	"os/exec"
	"strings"
	"testing"
)

func TestExecuteRequiresBoundedRunCommand(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"status"},
		{"run", "extra"},
	} {
		err := execute(args)
		if err == nil {
			t.Fatalf("execute(%q) unexpectedly succeeded", args)
		}
	}
	err := execute([]string{"run", "--config", "/does/not/exist"})
	if err == nil || !strings.Contains(err.Error(), "service helper config") {
		t.Fatalf("missing root-owned service helper config error = %v", err)
	}
}

func TestServiceHelperDependencyBoundary(t *testing.T) {
	output, err := exec.Command("go", "list", "-deps", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go list service helper dependencies: %v\n%s", err, output)
	}
	forbidden := []string{
		"github.com/bogdanovich/mintclaw/pkg/agent",
		"github.com/bogdanovich/mintclaw/pkg/channels",
		"github.com/bogdanovich/mintclaw/pkg/gateway",
		"github.com/bogdanovich/mintclaw/pkg/mcp",
		"github.com/bogdanovich/mintclaw/pkg/providers",
		"github.com/bogdanovich/mintclaw/pkg/tools",
	}
	for _, dependency := range strings.Fields(string(output)) {
		for _, forbiddenPrefix := range forbidden {
			if dependency == forbiddenPrefix || strings.HasPrefix(dependency, forbiddenPrefix+"/") {
				t.Errorf("service helper imports forbidden runtime dependency %s", dependency)
			}
		}
	}
}
