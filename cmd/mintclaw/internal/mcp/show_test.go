package mcp

import (
	"fmt"
	"strings"
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
