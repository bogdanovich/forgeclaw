package agent

import (
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/sipeed/picoclaw/pkg/interactions"
)

func TestRenderApprovalActionIdentifiesToolAndRedactsSecrets(t *testing.T) {
	action, err := renderApprovalAction("deploy", map[string]any{
		"path":  "/srv/app",
		"token": "plain-secret",
		"nested": map[string]any{
			"password": "nested-secret",
			"url":      "https://user:pass@example.test/run?api_key=url-secret&mode=fast",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, expected := range []string{
		"Tool: deploy",
		`"path":"/srv/app"`,
		`"token":"[REDACTED]"`,
		`"password":"[REDACTED]"`,
		"api_key=%5BREDACTED%5D",
	} {
		if !strings.Contains(action, expected) {
			t.Fatalf("approval action omitted %q: %s", expected, action)
		}
	}
	for _, secret := range []string{
		"plain-secret", "nested-secret", "url-secret", "user:pass",
	} {
		if strings.Contains(action, secret) {
			t.Fatalf("approval action leaked %q: %s", secret, action)
		}
	}
	if utf8.RuneCountInString(action) > interactions.MaxApprovalAction {
		t.Fatalf("approval action exceeds bound: %d", utf8.RuneCountInString(action))
	}
}

func TestRenderApprovalActionFailsClosedWhenDisplayWouldHideSemantics(t *testing.T) {
	tests := []struct {
		name string
		tool string
		args map[string]any
	}{
		{name: "opaque command", tool: "exec", args: map[string]any{"command": "rm -rf /srv/app"}},
		{name: "opaque body", tool: "http", args: map[string]any{"body": "password=hunter2"}},
		{name: "unknown string", tool: "custom", args: map[string]any{"selector": "production"}},
		{name: "oversized string", tool: "read", args: map[string]any{"path": strings.Repeat("x", 501)}},
		{name: "invalid tool", tool: "exec\nspoofed", args: map[string]any{"target": "production"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if action, err := renderApprovalAction(test.tool, test.args); err == nil {
				t.Fatalf("renderApprovalAction() = %q, want fail-closed error", action)
			}
		})
	}

	tooMany := make(map[string]any, approvalDisplayMaxItems+1)
	for index := 0; index < approvalDisplayMaxItems+1; index++ {
		tooMany[fmt.Sprintf("flag_%d", index)] = index
	}
	if action, err := renderApprovalAction("custom", tooMany); err == nil {
		t.Fatalf("oversized argument map rendered with omissions: %q", action)
	}

	tooLarge := map[string]any{
		"path":   strings.Repeat("p", approvalDisplayMaxStringRune),
		"source": strings.Repeat("s", approvalDisplayMaxStringRune),
		"target": strings.Repeat("t", approvalDisplayMaxStringRune),
		"name":   strings.Repeat("n", approvalDisplayMaxStringRune),
	}
	if action, err := renderApprovalAction("custom", tooLarge); err == nil {
		t.Fatalf("oversized complete rendering was truncated: %q", action)
	}
}
