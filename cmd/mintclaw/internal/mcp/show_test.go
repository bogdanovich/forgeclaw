package mcp

import (
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/config"
)

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

func TestMCPConfigSchemaValidatesSessionLossReplay(t *testing.T) {
	valid := []byte(`{
		"tools": {
			"mcp": {
				"enabled": true,
				"servers": {
					"playwright": {
						"enabled": true,
						"command": "npx",
						"session_loss_replay": "never"
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
