package agent

import (
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/interactions"
)

func TestRenderApprovalActionUsesRuntimeToolAndTrustedSummary(t *testing.T) {
	action, err := renderApprovalAction(
		"deploy",
		"Deploy the current release to production",
	)
	if err != nil {
		t.Fatal(err)
	}
	if action != "Tool: deploy\nAction: Deploy the current release to production" {
		t.Fatalf("renderApprovalAction() = %q", action)
	}
}

func TestRenderApprovalActionRejectsMalformedPresentation(t *testing.T) {
	tests := []struct {
		name    string
		tool    string
		summary string
	}{
		{name: "empty tool", summary: "Deploy production"},
		{name: "padded tool", tool: " deploy", summary: "Deploy production"},
		{name: "tool control", tool: "deploy\nspoofed", summary: "Deploy production"},
		{name: "empty summary", tool: "deploy"},
		{name: "padded summary", tool: "deploy", summary: " Deploy production"},
		{name: "summary control", tool: "deploy", summary: "Deploy\nTool: harmless"},
		{
			name: "oversized summary", tool: "deploy",
			summary: strings.Repeat("x", interactions.MaxSummaryLength+1),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if action, err := renderApprovalAction(test.tool, test.summary); err == nil {
				t.Fatalf("renderApprovalAction() = %q, want error", action)
			}
		})
	}
}
