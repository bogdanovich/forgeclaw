package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strings"
)

const (
	BrowserDriverPlaywrightMCP = "playwright_mcp"
	BrowserProfileManaged      = "managed"

	BrowserMaxSessions          = 1
	BrowserMaxTabs              = 4
	BrowserMaxSessionSeconds    = 60 * 60
	BrowserMaxIdleSeconds       = 10 * 60
	BrowserMaxActionSeconds     = 60
	BrowserMaxSnapshotBytes     = 256 * 1024
	BrowserMaxSnapshotRefs      = 500
	BrowserMaxTextInputBytes    = 16 * 1024
	BrowserMaxToolResultBytes   = 320 * 1024
	BrowserMaxRetentionSeconds  = 7 * 24 * 60 * 60
	BrowserMaxPreparedSeconds   = 5 * 60
	BrowserDefaultTarget        = "gateway"
	BrowserDefaultProfile       = "managed"
	BrowserMaxConfiguredOrigins = 64
)

var (
	browserAliasPattern    = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)
	browserHostnamePattern = regexp.MustCompile(`^[A-Za-z0-9.-]+$`)
)

type BrowserToolsConfig struct {
	Enabled bool                           `json:"enabled"           yaml:"-"`
	Agents  []string                       `json:"agents,omitempty"  yaml:"-"`
	Targets map[string]BrowserTargetConfig `json:"targets,omitempty" yaml:"-"`
	Limits  BrowserLimitsConfig            `json:"limits,omitempty"  yaml:"-"`
}

type BrowserTargetConfig struct {
	Enabled      bool                            `json:"enabled"                 yaml:"-"`
	Driver       string                          `json:"driver,omitempty"        yaml:"-"`
	DriverServer string                          `json:"driver_server,omitempty" yaml:"-"`
	Profiles     map[string]BrowserProfileConfig `json:"profiles,omitempty"      yaml:"-"`
}

type BrowserProfileConfig struct {
	Enabled        bool     `json:"enabled"                   yaml:"-"`
	Mode           string   `json:"mode,omitempty"            yaml:"-"`
	DryRun         bool     `json:"dry_run"                   yaml:"-"`
	AllowedOrigins []string `json:"allowed_origins,omitempty" yaml:"-"`
}

type BrowserLimitsConfig struct {
	Sessions        int `json:"sessions,omitempty"          yaml:"-"`
	Tabs            int `json:"tabs,omitempty"              yaml:"-"`
	SessionSeconds  int `json:"session_seconds,omitempty"   yaml:"-"`
	IdleSeconds     int `json:"idle_seconds,omitempty"      yaml:"-"`
	PreparedSeconds int `json:"prepared_seconds,omitempty"  yaml:"-"`
	ActionSeconds   int `json:"action_seconds,omitempty"    yaml:"-"`
	SnapshotBytes   int `json:"snapshot_bytes,omitempty"    yaml:"-"`
	SnapshotRefs    int `json:"snapshot_refs,omitempty"     yaml:"-"`
	TextInputBytes  int `json:"text_input_bytes,omitempty"  yaml:"-"`
	ToolResultBytes int `json:"tool_result_bytes,omitempty" yaml:"-"`
	RetentionSecs   int `json:"retention_seconds,omitempty" yaml:"-"`
}

func (limits BrowserLimitsConfig) Effective() BrowserLimitsConfig {
	return BrowserLimitsConfig{
		Sessions:        effectiveBrowserLimit(limits.Sessions, BrowserMaxSessions),
		Tabs:            effectiveBrowserLimit(limits.Tabs, BrowserMaxTabs),
		SessionSeconds:  effectiveBrowserLimit(limits.SessionSeconds, BrowserMaxSessionSeconds),
		IdleSeconds:     effectiveBrowserLimit(limits.IdleSeconds, BrowserMaxIdleSeconds),
		PreparedSeconds: effectiveBrowserLimit(limits.PreparedSeconds, BrowserMaxPreparedSeconds),
		ActionSeconds:   effectiveBrowserLimit(limits.ActionSeconds, BrowserMaxActionSeconds),
		SnapshotBytes:   effectiveBrowserLimit(limits.SnapshotBytes, BrowserMaxSnapshotBytes),
		SnapshotRefs:    effectiveBrowserLimit(limits.SnapshotRefs, BrowserMaxSnapshotRefs),
		TextInputBytes:  effectiveBrowserLimit(limits.TextInputBytes, BrowserMaxTextInputBytes),
		ToolResultBytes: effectiveBrowserLimit(limits.ToolResultBytes, BrowserMaxToolResultBytes),
		RetentionSecs:   effectiveBrowserLimit(limits.RetentionSecs, BrowserMaxRetentionSeconds),
	}
}

func (cfg BrowserToolsConfig) PolicyRevision() (string, error) {
	encoded, err := json.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("encode browser policy revision: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func effectiveBrowserLimit(value, fallback int) int {
	if value <= 0 {
		return fallback
	}
	return value
}

func (cfg *Config) ValidateBrowserConfig() error {
	if cfg == nil {
		return errors.New("browser config requires a root config")
	}
	browser := cfg.Tools.Browser
	if err := validateBrowserLimits(browser.Limits); err != nil {
		return fmt.Errorf("invalid tools.browser.limits: %w", err)
	}
	if len(browser.Agents) > 64 {
		return errors.New("invalid tools.browser.agents: exceeds 64 entries")
	}
	seenAgents := make(map[string]struct{}, len(browser.Agents))
	for _, agent := range browser.Agents {
		if !browserAliasPattern.MatchString(agent) {
			return fmt.Errorf("invalid tools.browser agent alias %q", agent)
		}
		if _, exists := seenAgents[agent]; exists {
			return fmt.Errorf("duplicate tools.browser agent alias %q", agent)
		}
		seenAgents[agent] = struct{}{}
	}
	if len(browser.Targets) > 8 {
		return errors.New("invalid tools.browser.targets: exceeds 8 entries")
	}
	for targetName, target := range browser.Targets {
		if err := cfg.validateBrowserTarget(targetName, target); err != nil {
			return err
		}
	}
	if browser.Enabled && len(browser.Agents) == 0 {
		return errors.New("tools.browser.enabled requires at least one agent")
	}
	if browser.Enabled && !hasEnabledBrowserProfile(browser.Targets) {
		return errors.New("tools.browser.enabled requires an enabled target and profile")
	}
	return nil
}

func (cfg *Config) validateBrowserTarget(name string, target BrowserTargetConfig) error {
	if !browserAliasPattern.MatchString(name) {
		return fmt.Errorf("invalid tools.browser target alias %q", name)
	}
	if target.Driver != "" && target.Driver != BrowserDriverPlaywrightMCP {
		return fmt.Errorf("invalid tools.browser.targets.%s.driver %q", name, target.Driver)
	}
	if len(target.Profiles) > 8 {
		return fmt.Errorf("invalid tools.browser.targets.%s.profiles: exceeds 8 entries", name)
	}
	for profileName, profile := range target.Profiles {
		if err := validateBrowserProfile(name, profileName, profile); err != nil {
			return err
		}
	}
	if !target.Enabled {
		return nil
	}
	if name != BrowserDefaultTarget {
		return fmt.Errorf("B1 supports only the %q browser target", BrowserDefaultTarget)
	}
	if target.Driver != BrowserDriverPlaywrightMCP {
		return fmt.Errorf("enabled browser target %q requires driver %q", name, BrowserDriverPlaywrightMCP)
	}
	if !browserAliasPattern.MatchString(target.DriverServer) {
		return fmt.Errorf("enabled browser target %q requires a valid driver_server", name)
	}
	server, ok := cfg.Tools.MCP.Servers[target.DriverServer]
	if !ok {
		return fmt.Errorf("browser target %q references unknown MCP server template", name)
	}
	if server.Enabled {
		return fmt.Errorf(
			"browser driver server %q must not be enabled in the generic MCP manager",
			target.DriverServer,
		)
	}
	if EffectiveMCPTransportType(server) != "stdio" {
		return fmt.Errorf("browser driver server %q must use stdio", target.DriverServer)
	}
	if strings.TrimSpace(server.Command) == "" {
		return fmt.Errorf("browser driver server %q requires a command", target.DriverServer)
	}
	if EffectiveMCPSessionLossReplay(server) != MCPSessionLossReplayNever {
		return fmt.Errorf("browser driver server %q must use session_loss_replay=never", target.DriverServer)
	}
	if strings.TrimSpace(server.ExclusiveLockFile) == "" {
		return fmt.Errorf("browser driver server %q requires exclusive_lock_file", target.DriverServer)
	}
	if !hasEnabledBrowserProfile(map[string]BrowserTargetConfig{name: target}) {
		return fmt.Errorf("enabled browser target %q requires an enabled profile", name)
	}
	return nil
}

func validateBrowserProfile(targetName, name string, profile BrowserProfileConfig) error {
	if !browserAliasPattern.MatchString(name) {
		return fmt.Errorf("invalid tools.browser.targets.%s profile alias %q", targetName, name)
	}
	if profile.Mode != "" && profile.Mode != BrowserProfileManaged {
		return fmt.Errorf("browser profile %q supports only mode %q", name, BrowserProfileManaged)
	}
	if len(profile.AllowedOrigins) > BrowserMaxConfiguredOrigins {
		return fmt.Errorf("browser profile %q exceeds %d allowed origins", name, BrowserMaxConfiguredOrigins)
	}
	seen := make(map[string]struct{}, len(profile.AllowedOrigins))
	for _, rawOrigin := range profile.AllowedOrigins {
		origin, err := NormalizeBrowserOrigin(rawOrigin)
		if err != nil {
			return fmt.Errorf("invalid browser profile %q origin: %w", name, err)
		}
		if _, exists := seen[origin]; exists {
			return fmt.Errorf("browser profile %q contains duplicate origin %q", name, origin)
		}
		seen[origin] = struct{}{}
	}
	if profile.Enabled {
		if name != BrowserDefaultProfile {
			return fmt.Errorf("B1 supports only the %q browser profile", BrowserDefaultProfile)
		}
		if profile.Mode != BrowserProfileManaged {
			return fmt.Errorf("enabled browser profile %q requires mode %q", name, BrowserProfileManaged)
		}
		if len(profile.AllowedOrigins) == 0 {
			return fmt.Errorf("enabled browser profile %q requires allowed_origins", name)
		}
	}
	return nil
}

func NormalizeBrowserOrigin(raw string) (string, error) {
	if strings.TrimSpace(raw) != raw || raw == "" || len(raw) > 2048 {
		return "", errors.New("origin must be non-empty, trimmed, and at most 2048 bytes")
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("origin must be an absolute URL origin")
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", errors.New("origin scheme must be http or https")
	}
	if parsed.User != nil || (parsed.Path != "" && parsed.Path != "/") ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("origin must not contain user information, path, query, or fragment")
	}
	host := parsed.Hostname()
	if host == "" || strings.Contains(host, "*") {
		return "", errors.New("origin host must be exact")
	}
	lowerHost := strings.ToLower(strings.TrimSuffix(host, "."))
	if lowerHost == "localhost" || strings.HasSuffix(lowerHost, ".localhost") ||
		lowerHost == "metadata.google.internal" {
		return "", errors.New("origin host is outside the public network policy")
	}
	if ip := net.ParseIP(host); ip != nil && !browserPublicIP(ip) {
		return "", errors.New("origin IP is outside the public network policy")
	} else if ip == nil {
		if !browserHostnamePattern.MatchString(host) || !strings.Contains(lowerHost, ".") ||
			strings.HasPrefix(lowerHost, ".") || strings.HasSuffix(lowerHost, ".") ||
			strings.Contains(lowerHost, "..") {
			return "", errors.New("origin host must be an exact public DNS name")
		}
		for _, label := range strings.Split(lowerHost, ".") {
			if strings.HasPrefix(label, "-") || strings.HasSuffix(label, "-") {
				return "", errors.New("origin host must be an exact public DNS name")
			}
		}
	}
	port := parsed.Port()
	if (scheme == "http" && port == "80") || (scheme == "https" && port == "443") {
		port = ""
	}
	normalizedHost := lowerHost
	if port != "" {
		normalizedHost = net.JoinHostPort(lowerHost, port)
	} else if strings.Contains(lowerHost, ":") {
		normalizedHost = "[" + lowerHost + "]"
	}
	return scheme + "://" + normalizedHost, nil
}

func browserPublicIP(ip net.IP) bool {
	return !ip.IsLoopback() && !ip.IsPrivate() && !ip.IsLinkLocalUnicast() &&
		!ip.IsLinkLocalMulticast() && !ip.IsMulticast() && !ip.IsUnspecified()
}

func validateBrowserLimits(limits BrowserLimitsConfig) error {
	checks := []struct {
		name  string
		value int
		max   int
	}{
		{"sessions", limits.Sessions, BrowserMaxSessions},
		{"tabs", limits.Tabs, BrowserMaxTabs},
		{"session_seconds", limits.SessionSeconds, BrowserMaxSessionSeconds},
		{"idle_seconds", limits.IdleSeconds, BrowserMaxIdleSeconds},
		{"prepared_seconds", limits.PreparedSeconds, BrowserMaxPreparedSeconds},
		{"action_seconds", limits.ActionSeconds, BrowserMaxActionSeconds},
		{"snapshot_bytes", limits.SnapshotBytes, BrowserMaxSnapshotBytes},
		{"snapshot_refs", limits.SnapshotRefs, BrowserMaxSnapshotRefs},
		{"text_input_bytes", limits.TextInputBytes, BrowserMaxTextInputBytes},
		{"tool_result_bytes", limits.ToolResultBytes, BrowserMaxToolResultBytes},
		{"retention_seconds", limits.RetentionSecs, BrowserMaxRetentionSeconds},
	}
	for _, check := range checks {
		if check.value < 0 || check.value > check.max {
			return fmt.Errorf("%s must be between 0 and %d", check.name, check.max)
		}
	}
	effective := limits.Effective()
	if effective.IdleSeconds > effective.SessionSeconds {
		return errors.New("idle_seconds must not exceed session_seconds")
	}
	if effective.PreparedSeconds > effective.SessionSeconds {
		return errors.New("prepared_seconds must not exceed session_seconds")
	}
	return nil
}

func hasEnabledBrowserProfile(targets map[string]BrowserTargetConfig) bool {
	for _, target := range targets {
		if !target.Enabled {
			continue
		}
		for _, profile := range target.Profiles {
			if profile.Enabled {
				return true
			}
		}
	}
	return false
}
