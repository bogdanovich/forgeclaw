package config

import (
	"net"
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

func TestBrowserConfigAcceptsPublicWebWithoutExactOrigins(t *testing.T) {
	cfg := browserConfigFixture(t)
	target := cfg.Tools.Browser.Targets[BrowserDefaultTarget]
	profile := target.Profiles[BrowserDefaultProfile]
	profile.NetworkMode = BrowserNetworkPublicWeb
	profile.AllowedOrigins = nil
	target.Profiles[BrowserDefaultProfile] = profile
	cfg.Tools.Browser.Targets[BrowserDefaultTarget] = target
	if err := cfg.ValidateBrowserConfig(); err != nil {
		t.Fatalf("ValidateBrowserConfig() public_web error = %v", err)
	}
}

func TestBrowserConfigAcceptsExplicitAnyHTTPWithoutExactOrigins(t *testing.T) {
	cfg := browserConfigFixture(t)
	target := cfg.Tools.Browser.Targets[BrowserDefaultTarget]
	profile := target.Profiles[BrowserDefaultProfile]
	profile.NetworkMode = BrowserNetworkAnyHTTP
	profile.AllowedOrigins = nil
	target.Profiles[BrowserDefaultProfile] = profile
	cfg.Tools.Browser.Targets[BrowserDefaultTarget] = target
	if err := cfg.ValidateBrowserConfig(); err != nil {
		t.Fatalf("ValidateBrowserConfig() any_http error = %v", err)
	}
}

func TestBrowserPolicyRevisionCanonicalizesDefaultNetworkMode(t *testing.T) {
	cfg := browserConfigFixture(t)
	omitted, err := cfg.Tools.Browser.PolicyRevision()
	if err != nil {
		t.Fatalf("PolicyRevision() omitted error = %v", err)
	}
	target := cfg.Tools.Browser.Targets[BrowserDefaultTarget]
	profile := target.Profiles[BrowserDefaultProfile]
	profile.NetworkMode = BrowserNetworkExactOrigins
	target.Profiles[BrowserDefaultProfile] = profile
	cfg.Tools.Browser.Targets[BrowserDefaultTarget] = target
	explicit, err := cfg.Tools.Browser.PolicyRevision()
	if err != nil || omitted != explicit {
		t.Fatalf("PolicyRevision() omitted = %q, explicit = %q, error = %v", omitted, explicit, err)
	}
	original := browserConfigFixture(t).Tools.Browser
	if _, err = original.PolicyRevision(); err != nil {
		t.Fatal(err)
	}
	if got := original.Targets[BrowserDefaultTarget].Profiles[BrowserDefaultProfile].NetworkMode; got != "" {
		t.Fatalf("PolicyRevision() mutated network mode to %q", got)
	}
}

func TestBrowserConfigRequiresToolResultEnvelopeHeadroom(t *testing.T) {
	cfg := browserConfigFixture(t)
	cfg.Tools.Browser.Limits.ToolResultBytes = BrowserToolResultEnvelopeBytes - 1
	if err := cfg.ValidateBrowserConfig(); err == nil ||
		!strings.Contains(err.Error(), "tool_result_bytes must be 0 or at least") {
		t.Fatalf("ValidateBrowserConfig() error = %v", err)
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
			name: "unsupported network mode",
			mutate: func(cfg *Config) {
				target := cfg.Tools.Browser.Targets["gateway"]
				profile := target.Profiles["managed"]
				profile.NetworkMode = "unbounded_network"
				target.Profiles["managed"] = profile
				cfg.Tools.Browser.Targets["gateway"] = target
			},
			wantErr: "unsupported network_mode",
		},
		{
			name: "public web with exact origins",
			mutate: func(cfg *Config) {
				target := cfg.Tools.Browser.Targets["gateway"]
				profile := target.Profiles["managed"]
				profile.NetworkMode = BrowserNetworkPublicWeb
				target.Profiles["managed"] = profile
				cfg.Tools.Browser.Targets["gateway"] = target
			},
			wantErr: "must not set allowed_origins",
		},
		{
			name: "any HTTP with exact origins",
			mutate: func(cfg *Config) {
				target := cfg.Tools.Browser.Targets["gateway"]
				profile := target.Profiles["managed"]
				profile.NetworkMode = BrowserNetworkAnyHTTP
				target.Profiles["managed"] = profile
				cfg.Tools.Browser.Targets["gateway"] = target
			},
			wantErr: "must not set allowed_origins",
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
			name: "short numeric loopback",
			mutate: func(cfg *Config) {
				target := cfg.Tools.Browser.Targets["gateway"]
				profile := target.Profiles["managed"]
				profile.AllowedOrigins = []string{"http://127.1"}
				target.Profiles["managed"] = profile
				cfg.Tools.Browser.Targets["gateway"] = target
			},
			wantErr: "outside the public network policy",
		},
		{
			name: "octal numeric loopback",
			mutate: func(cfg *Config) {
				target := cfg.Tools.Browser.Targets["gateway"]
				profile := target.Profiles["managed"]
				profile.AllowedOrigins = []string{"http://0177.0.0.1"}
				target.Profiles["managed"] = profile
				cfg.Tools.Browser.Targets["gateway"] = target
			},
			wantErr: "outside the public network policy",
		},
		{
			name: "hex numeric loopback",
			mutate: func(cfg *Config) {
				target := cfg.Tools.Browser.Targets["gateway"]
				profile := target.Profiles["managed"]
				profile.AllowedOrigins = []string{"http://0x7f.0.0.1"}
				target.Profiles["managed"] = profile
				cfg.Tools.Browser.Targets["gateway"] = target
			},
			wantErr: "outside the public network policy",
		},
		{
			name: "single-integer numeric loopback",
			mutate: func(cfg *Config) {
				target := cfg.Tools.Browser.Targets["gateway"]
				profile := target.Profiles["managed"]
				profile.AllowedOrigins = []string{"http://2130706433"}
				target.Profiles["managed"] = profile
				cfg.Tools.Browser.Targets["gateway"] = target
			},
			wantErr: "outside the public network policy",
		},
		{
			name: "overflowing numeric IPv4",
			mutate: func(cfg *Config) {
				target := cfg.Tools.Browser.Targets["gateway"]
				profile := target.Profiles["managed"]
				profile.AllowedOrigins = []string{"http://256.256"}
				target.Profiles["managed"] = profile
				cfg.Tools.Browser.Targets["gateway"] = target
			},
			wantErr: "invalid numeric IPv4 address",
		},
		{
			name: "invalid octal numeric IPv4",
			mutate: func(cfg *Config) {
				target := cfg.Tools.Browser.Targets["gateway"]
				profile := target.Profiles["managed"]
				profile.AllowedOrigins = []string{"http://09.0.0.1"}
				target.Profiles["managed"] = profile
				cfg.Tools.Browser.Targets["gateway"] = target
			},
			wantErr: "invalid numeric IPv4 address",
		},
		{
			name: "too many numeric IPv4 parts",
			mutate: func(cfg *Config) {
				target := cfg.Tools.Browser.Targets["gateway"]
				profile := target.Profiles["managed"]
				profile.AllowedOrigins = []string{"http://1.2.3.4.5"}
				target.Profiles["managed"] = profile
				cfg.Tools.Browser.Targets["gateway"] = target
			},
			wantErr: "invalid numeric IPv4 address",
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
			name: "empty origin port",
			mutate: func(cfg *Config) {
				target := cfg.Tools.Browser.Targets["gateway"]
				profile := target.Profiles["managed"]
				profile.AllowedOrigins = []string{"https://example.com:"}
				target.Profiles["managed"] = profile
				cfg.Tools.Browser.Targets["gateway"] = target
			},
			wantErr: "port must be between 1 and 65535",
		},
		{
			name: "out-of-range origin port",
			mutate: func(cfg *Config) {
				target := cfg.Tools.Browser.Targets["gateway"]
				profile := target.Profiles["managed"]
				profile.AllowedOrigins = []string{"https://example.com:65536"}
				target.Profiles["managed"] = profile
				cfg.Tools.Browser.Targets["gateway"] = target
			},
			wantErr: "port must be between 1 and 65535",
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

func TestNormalizeBrowserOriginCanonicalizesPublicNumericIPv4(t *testing.T) {
	origin, err := NormalizeBrowserOrigin("http://0x8.0x8.0x8.0x8")
	if err != nil {
		t.Fatalf("NormalizeBrowserOrigin() error = %v", err)
	}
	if origin != "http://8.8.8.8" {
		t.Fatalf("NormalizeBrowserOrigin() = %q", origin)
	}
}

func TestNormalizeBrowserHTTPOriginAdmitsPrivateScopesButRejectsAmbiguousNumericHosts(t *testing.T) {
	tests := map[string]string{
		"http://127.0.0.1:8080":           "http://127.0.0.1:8080",
		"HTTP://LOCALHOST:80/":            "http://localhost",
		"http://service.internal/":        "http://service.internal",
		"http://metadata.google.internal": "http://metadata.google.internal",
		"http://169.254.169.254/":         "http://169.254.169.254",
		"http://[fe80::1]:8080/":          "http://[fe80::1]:8080",
		"http://[fe80::1%25en0]:8080/":    "http://[fe80::1%25en0]:8080",
		"http://[FE80::1%25EtherNet]/":    "http://[fe80::1%25EtherNet]",
		"http://[::ffff:7f00:1]/":         "http://[::ffff:127.0.0.1]",
	}
	for raw, want := range tests {
		got, err := NormalizeBrowserHTTPOrigin(raw)
		if err != nil || got != want {
			t.Errorf("NormalizeBrowserHTTPOrigin(%q) = %q, %v, want %q", raw, got, err, want)
		}
	}
	for _, raw := range []string{"http://127.1", "http://0x7f000001", "file:///tmp/test", "http://user@localhost"} {
		if got, err := NormalizeBrowserHTTPOrigin(raw); err == nil {
			t.Errorf("NormalizeBrowserHTTPOrigin(%q) = %q, want error", raw, got)
		}
	}
}

func TestIsPublicBrowserIPRejectsIANASpecialPurposeRanges(t *testing.T) {
	denied := []string{
		"0.0.0.1", "10.0.0.1", "100.64.0.1", "127.0.0.1", "168.63.129.16", "169.254.1.1",
		"172.16.0.1", "192.0.0.1", "192.0.2.1", "192.31.196.1", "192.52.193.1",
		"192.88.99.1", "192.168.0.1", "192.175.48.1", "198.18.0.1", "198.51.100.1",
		"203.0.113.1", "240.0.0.1", "255.255.255.255", "::", "::1", "::ffff:100.64.0.1",
		"64:ff9b::1", "64:ff9b:1::1", "100::1", "100:0:0:1::1", "2001::1",
		"2001:db8::1", "2002::1", "2620:4f:8000::1", "3fff::1", "5f00::1",
		"fc00::1", "fe80::1", "ff00::1",
	}
	for _, raw := range denied {
		if IsPublicBrowserIP(net.ParseIP(raw)) {
			t.Errorf("IsPublicBrowserIP(%q) = true, want false", raw)
		}
	}
	for _, raw := range []string{"8.8.8.8", "1.1.1.1", "2001:4860:4860::8888", "2606:4700:4700::1111"} {
		if !IsPublicBrowserIP(net.ParseIP(raw)) {
			t.Errorf("IsPublicBrowserIP(%q) = false, want true", raw)
		}
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
