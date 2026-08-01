package mcp

import (
	"fmt"
	"strings"
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/config"
)

func TestRenderServerFailureTargetIsBoundedAndOmitsSensitiveDetails(t *testing.T) {
	tests := []struct {
		name   string
		server config.MCPServerConfig
		want   string
	}{
		{
			name: "stdio executable basename only",
			server: config.MCPServerConfig{
				Type:    "stdio",
				Command: "/private/operator/bin/playwright-mcp",
				Args:    []string{"--token", "secret-value"},
			},
			want: "playwright-mcp",
		},
		{
			name: "remote scheme and host only",
			server: config.MCPServerConfig{
				Type: "http",
				URL:  "https://user:secret@example.com/private/path?token=secret#fragment",
			},
			want: "https://example.com",
		},
		{
			name: "invalid remote target",
			server: config.MCPServerConfig{
				Type: "sse",
				URL:  "not a valid URL with secret-value",
			},
			want: "<remote target configured>",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := renderServerFailureTarget(test.server); got != test.want {
				t.Fatalf("renderServerFailureTarget() = %q, want %q", got, test.want)
			}
		})
	}

	longCommand := strings.Repeat("a", maxMCPFailureTargetRunes+1)
	got := renderServerFailureTarget(config.MCPServerConfig{Type: "stdio", Command: longCommand})
	if len([]rune(got)) != maxMCPFailureTargetRunes || !strings.HasSuffix(got, "...") {
		t.Fatalf("bounded target length/suffix = %d/%q", len([]rune(got)), got)
	}
}

func TestBuildServerInfoProjectsEffectiveSessionLossReplay(t *testing.T) {
	defaultInfo := buildServerInfo("default", config.MCPServerConfig{}, false)
	if defaultInfo.SessionLossReplay != string(config.MCPSessionLossReplayOnce) {
		t.Fatalf(
			"default SessionLossReplay = %q, want %q",
			defaultInfo.SessionLossReplay,
			config.MCPSessionLossReplayOnce,
		)
	}

	neverInfo := buildServerInfo("never", config.MCPServerConfig{
		SessionLossReplay: config.MCPSessionLossReplayNever,
	}, false)
	if neverInfo.SessionLossReplay != string(config.MCPSessionLossReplayNever) {
		t.Fatalf(
			"never SessionLossReplay = %q, want %q",
			neverInfo.SessionLossReplay,
			config.MCPSessionLossReplayNever,
		)
	}
}

func TestBuildServerInfoReportsExclusiveLockWithoutPath(t *testing.T) {
	lockPath := "/private/operator/playwright.lock"
	info := buildServerInfo("playwright", config.MCPServerConfig{
		Command:           "npx",
		ExclusiveLockFile: lockPath,
	}, false)
	if !info.ExclusiveLock {
		t.Fatal("ExclusiveLock = false, want true")
	}
	if strings.Contains(fmt.Sprintf("%+v", info), lockPath) {
		t.Fatalf("server info leaked exclusive lock path: %+v", info)
	}
}

func TestMCPConfigSchemaValidatesSessionLossReplay(t *testing.T) {
	valid := []byte(`{
		"tools": {
			"mcp": {
				"enabled": true,
				"servers": {
					"playwright": {
						"enabled": true,
						"command": "npx",
						"session_loss_replay": "never",
						"exclusive_lock_file": "/tmp/playwright.lock"
					}
				}
			}
		}
	}`)
	if err := validateConfigDocument(valid); err != nil {
		t.Fatalf("validateConfigDocument(valid) error = %v", err)
	}

	invalid := []byte(`{
		"tools": {
			"mcp": {
				"enabled": true,
				"servers": {
					"playwright": {
						"enabled": true,
						"command": "npx",
						"session_loss_replay": "automatic"
					}
				}
			}
		}
	}`)
	if err := validateConfigDocument(invalid); err == nil {
		t.Fatal("validateConfigDocument(invalid) error = nil, want enum failure")
	}
}
