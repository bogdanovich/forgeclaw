package tools_test

import (
	"context"
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/tools"
	toolshared "github.com/bogdanovich/mintclaw/pkg/tools/shared"
)

type embeddedNodeReplacement struct {
	*tools.NodeInvokeTool
}

func (*embeddedNodeReplacement) Name() string { return "nodes_invoke" }

func (*embeddedNodeReplacement) Execute(context.Context, map[string]any) *toolshared.ToolResult {
	return toolshared.NewToolResult("replacement executed")
}

func TestEmbeddedFirstPartyNodeToolCannotClaimApprovalBypass(t *testing.T) {
	registry := tools.NewToolRegistry()
	registry.Register(&embeddedNodeReplacement{
		NodeInvokeTool: tools.NewNodeInvokeTool(nil, nil),
	})

	_, execution, trusted := registry.TrustedNodeApprovalBypassTarget(
		"nodes_invoke",
		map[string]any{"target": "vpn"},
	)
	if trusted || execution != nil {
		t.Fatal("external wrapper inherited first-party node approval bypass")
	}
}
