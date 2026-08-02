package config

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestBrowserConfigDisabledByDefault(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Tools.Browser.Enabled {
		t.Fatal("browser tools must be disabled by default")
	}
	if err := cfg.ValidateBrowserConfig(); err != nil {
		t.Fatalf("ValidateBrowserConfig() default error = %v", err)
	}
}

func TestBrowserConfigAcceptsAdmittedB1Shape(t *testing.T) {
	cfg := browserConfigFixture(t)
	if err := cfg.ValidateBrowserConfig(); err != nil {
		t.Fatalf("ValidateBrowserConfig() error = %v", err)
	}

	limits := cfg.Tools.Browser.Limits.Effective()
	if limits.Sessions != BrowserMaxSessions || limits.Tabs != BrowserMaxTabs ||
		limits.SnapshotBytes != BrowserMaxSnapshotBytes {
		t.Fatalf("effective browser limits = %+v", limits)
	}
	revision, err := cfg.Tools.Browser.PolicyRevision()
	if err != nil || len(revision) != 64 {
		t.Fatalf("PolicyRevision() = %q, %v", revision, err)
	}
}

func TestBrowserConfigRequiresSessionScopedNoReplayDriver(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{
			name: "generic MCP server enabled",
			mutate: func(cfg *Config) {
				server := cfg.Tools.MCP.Servers["playwright"]
				server.Enabled = true
				cfg.Tools.MCP.Servers["playwright"] = server
			},
			wantErr: "must not be enabled in the generic MCP manager",
		},
		{
			name: "replay once",
			mutate: func(cfg *Config) {
				server := cfg.Tools.MCP.Servers["playwright"]
				server.SessionLossReplay = MCPSessionLossReplayOnce
				cfg.Tools.MCP.Servers["playwright"] = server
			},
			wantErr: "session_loss_replay=never",
		},
		{
			name: "missing lease",
			mutate: func(cfg *Config) {
				server := cfg.Tools.MCP.Servers["playwright"]
				server.ExclusiveLockFile = ""
				cfg.Tools.MCP.Servers["playwright"] = server
			},
			wantErr: "requires exclusive_lock_file",
		},
		{
			name: "remote transport",
			mutate: func(cfg *Config) {
				server := cfg.Tools.MCP.Servers["playwright"]
				server.Type = "http"
				server.URL = "https://browser.invalid/mcp"
				cfg.Tools.MCP.Servers["playwright"] = server
			},
			wantErr: "must use stdio",
		},
		{
			name: "missing command",
			mutate: func(cfg *Config) {
				server := cfg.Tools.MCP.Servers["playwright"]
				server.Command = ""
				cfg.Tools.MCP.Servers["playwright"] = server
			},
			wantErr: "requires a command",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := browserConfigFixture(t)
			test.mutate(cfg)
			err := cfg.ValidateBrowserConfig()
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("ValidateBrowserConfig() error = %v, want %q", err, test.wantErr)
			}
		})
	}
}

func TestBrowserConfigRejectsAuthorityExpansion(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{
			name: "companion target",
			mutate: func(cfg *Config) {
				cfg.Tools.Browser.Targets["companion"] = cfg.Tools.Browser.Targets["gateway"]
				delete(cfg.Tools.Browser.Targets, "gateway")
			},
			wantErr: "supports only the \"gateway\" browser target",
		},
		{
			name: "attached profile",
			mutate: func(cfg *Config) {
				target := cfg.Tools.Browser.Targets["gateway"]
				profile := target.Profiles["managed"]
				profile.Mode = "attached_user"
				target.Profiles["managed"] = profile
				cfg.Tools.Browser.Targets["gateway"] = target
			},
			wantErr: "supports only mode \"managed\"",
		},
		{
			name: "non-dry-run profile",
			mutate: func(cfg *Config) {
				target := cfg.Tools.Browser.Targets["gateway"]
				profile := target.Profiles["managed"]
				profile.DryRun = false
				target.Profiles["managed"] = profile
				cfg.Tools.Browser.Targets["gateway"] = target
			},
			wantErr: "requires dry_run=true in B1",
		},
		{
			name: "second profile",
			mutate: func(cfg *Config) {
				target := cfg.Tools.Browser.Targets["gateway"]
				target.Profiles["other"] = BrowserProfileConfig{
					Enabled: true, Mode: BrowserProfileManaged, AllowedOrigins: []string{"https://example.com"},
				}
				cfg.Tools.Browser.Targets["gateway"] = target
			},
			wantErr: "supports only the \"managed\" browser profile",
		},
		{
			name: "private origin",
			mutate: func(cfg *Config) {
				target := cfg.Tools.Browser.Targets["gateway"]
				profile := target.Profiles["managed"]
				profile.AllowedOrigins = []string{"http://127.0.0.1:8080"}
				target.Profiles["managed"] = profile
				cfg.Tools.Browser.Targets["gateway"] = target
			},
			wantErr: "outside the public network policy",
		},
		{
			name: "localhost origin",
			mutate: func(cfg *Config) {
				target := cfg.Tools.Browser.Targets["gateway"]
				profile := target.Profiles["managed"]
				profile.AllowedOrigins = []string{"http://localhost:8080"}
				target.Profiles["managed"] = profile
				cfg.Tools.Browser.Targets["gateway"] = target
			},
			wantErr: "outside the public network policy",
		},
		{
			name: "origin path",
			mutate: func(cfg *Config) {
				target := cfg.Tools.Browser.Targets["gateway"]
				profile := target.Profiles["managed"]
				profile.AllowedOrigins = []string{"https://example.com/path"}
				target.Profiles["managed"] = profile
				cfg.Tools.Browser.Targets["gateway"] = target
			},
			wantErr: "must not contain user information, path, query, or fragment",
		},
		{
			name: "single-label origin",
			mutate: func(cfg *Config) {
				target := cfg.Tools.Browser.Targets["gateway"]
				profile := target.Profiles["managed"]
				profile.AllowedOrigins = []string{"https://intranet"}
				target.Profiles["managed"] = profile
				cfg.Tools.Browser.Targets["gateway"] = target
			},
			wantErr: "exact public DNS name",
		},
		{
			name: "expanded session limit",
			mutate: func(cfg *Config) {
				cfg.Tools.Browser.Limits.Sessions = 2
			},
			wantErr: "sessions must be between 0 and 1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := browserConfigFixture(t)
			test.mutate(cfg)
			err := cfg.ValidateBrowserConfig()
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("ValidateBrowserConfig() error = %v, want %q", err, test.wantErr)
			}
		})
	}
}

func TestNormalizeBrowserOriginCanonicalizesDefaultPortsAndDNSCase(t *testing.T) {
	origin, err := NormalizeBrowserOrigin("HTTPS://Example.COM.:443/")
	if err != nil {
		t.Fatalf("NormalizeBrowserOrigin() error = %v", err)
	}
	if origin != "https://example.com" {
		t.Fatalf("NormalizeBrowserOrigin() = %q", origin)
	}
}

func browserConfigFixture(t *testing.T) *Config {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Tools.MCP.Servers["playwright"] = MCPServerConfig{
		Enabled:           false,
		Command:           "npx",
		Args:              []string{"-y", "@playwright/mcp@0.0.78"},
		Type:              "stdio",
		SessionLossReplay: MCPSessionLossReplayNever,
		ExclusiveLockFile: filepath.Join(t.TempDir(), "playwright.lock"),
	}
	cfg.Tools.Browser = BrowserToolsConfig{
		Enabled: true,
		Agents:  []string{"browser"},
		Targets: map[string]BrowserTargetConfig{
			"gateway": {
				Enabled:      true,
				Driver:       BrowserDriverPlaywrightMCP,
				DriverServer: "playwright",
				Profiles: map[string]BrowserProfileConfig{
					"managed": {
						Enabled:        true,
						Mode:           BrowserProfileManaged,
						DryRun:         true,
						AllowedOrigins: []string{"https://example.com"},
					},
				},
			},
		},
	}
	return cfg
}
