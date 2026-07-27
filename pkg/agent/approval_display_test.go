package agent

import (
	"strings"
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/interactions"
)

func TestValidateApprovalDisplayAcceptsRuntimeToolAndTrustedSummary(t *testing.T) {
	err := validateApprovalDisplay(
		"deploy",
		"Deploy the current release to production",
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestValidateApprovalDisplayRejectsMalformedPresentation(t *testing.T) {
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
			if err := validateApprovalDisplay(test.tool, test.summary); err == nil {
				t.Fatal("validateApprovalDisplay() succeeded, want error")
			}
		})
	}
}
