package agent

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/sipeed/picoclaw/pkg/interactions"
)

func TestRenderApprovalActionIdentifiesToolAndRedactsSecrets(t *testing.T) {
	action := renderApprovalAction("deploy\nignored", map[string]any{
		"path":  "/srv/app",
		"token": "plain-secret",
		"nested": map[string]any{
			"password": "nested-secret",
			"url":      "https://user:pass@example.test/run?api_key=url-secret&mode=fast",
		},
		"command": "API_TOKEN=env-secret curl -H 'Authorization: Bearer bearer-secret'",
		"data":    "data:text/plain;base64,hidden",
		"key":     "sk-1234567890abcdefghijkl",
	})

	for _, expected := range []string{
		"Tool: deployignored",
		`"path":"/srv/app"`,
		`"token":"[REDACTED]"`,
		`"password":"[REDACTED]"`,
		"API_TOKEN=[REDACTED]",
		"Bearer [REDACTED]",
		"api_key=%5BREDACTED%5D",
		"[DATA URL REDACTED]",
	} {
		if !strings.Contains(action, expected) {
			t.Fatalf("approval action omitted %q: %s", expected, action)
		}
	}
	for _, secret := range []string{
		"plain-secret", "nested-secret", "url-secret", "env-secret",
		"bearer-secret", "1234567890abcdefghijkl", "user:pass", "hidden",
	} {
		if strings.Contains(action, secret) {
			t.Fatalf("approval action leaked %q: %s", secret, action)
		}
	}
	if utf8.RuneCountInString(action) > interactions.MaxApprovalAction {
		t.Fatalf("approval action exceeds bound: %d", utf8.RuneCountInString(action))
	}
}
