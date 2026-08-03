package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

const (
	BrowserDriverPlaywrightMCP = "playwright_mcp"
	BrowserProfileManaged      = "managed"
	BrowserNetworkExactOrigins = "exact_origins"
	BrowserNetworkPublicWeb    = "public_web"
	BrowserNetworkAnyHTTP      = "any_http"

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

// BrowserToolResultEnvelopeBytes reserves encoded space for bounded page and
// dialog metadata, opaque authority IDs, tab metadata, limits, and the
// browser_act wrapper. Snapshot content is budgeted separately.
const BrowserToolResultEnvelopeBytes = 64 * 1024

var (
	browserAliasPattern    = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)
	browserHostnamePattern = regexp.MustCompile(`^[A-Za-z0-9.-]+$`)
)

var browserSpecialPurposePrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("168.63.129.16/32"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.31.196.0/24"),
	netip.MustParsePrefix("192.52.193.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("192.175.48.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("100:0:0:1::/64"),
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("2620:4f:8000::/48"),
	netip.MustParsePrefix("3fff::/20"),
	netip.MustParsePrefix("5f00::/16"),
	netip.MustParsePrefix("fc00::/7"),
	netip.MustParsePrefix("fe80::/10"),
}

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
	NetworkMode    string   `json:"network_mode,omitempty"    yaml:"-"`
	DryRun         bool     `json:"dry_run"                   yaml:"-"`
	AllowedOrigins []string `json:"allowed_origins,omitempty" yaml:"-"`
}

func (profile BrowserProfileConfig) EffectiveNetworkMode() string {
	if profile.NetworkMode == "" {
		return BrowserNetworkExactOrigins
	}
	return profile.NetworkMode
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
	canonical := cfg
	canonical.Targets = make(map[string]BrowserTargetConfig, len(cfg.Targets))
	for targetName, target := range cfg.Targets {
		profiles := make(map[string]BrowserProfileConfig, len(target.Profiles))
		for profileName, profile := range target.Profiles {
			profile.NetworkMode = profile.EffectiveNetworkMode()
			profiles[profileName] = profile
		}
		target.Profiles = profiles
		canonical.Targets[targetName] = target
	}
	encoded, err := json.Marshal(canonical)
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
	networkMode := profile.EffectiveNetworkMode()
	if networkMode != BrowserNetworkExactOrigins && networkMode != BrowserNetworkPublicWeb &&
		networkMode != BrowserNetworkAnyHTTP {
		return fmt.Errorf("browser profile %q has unsupported network_mode %q", name, profile.NetworkMode)
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
		if !profile.DryRun {
			return fmt.Errorf("enabled browser profile %q requires dry_run=true in B1", name)
		}
		if networkMode == BrowserNetworkExactOrigins && len(profile.AllowedOrigins) == 0 {
			return fmt.Errorf("enabled browser profile %q requires allowed_origins", name)
		}
		if (networkMode == BrowserNetworkPublicWeb || networkMode == BrowserNetworkAnyHTTP) &&
			len(profile.AllowedOrigins) != 0 {
			return fmt.Errorf("enabled %s browser profile %q must not set allowed_origins", networkMode, name)
		}
	}
	return nil
}

func NormalizeBrowserOrigin(raw string) (string, error) {
	return normalizeBrowserOrigin(raw, true)
}

// NormalizeBrowserHTTPOrigin canonicalizes an HTTP or HTTPS origin without
// applying an address-scope policy. It is used only after an operator has
// explicitly selected the high-risk any_http mode, and for validating durable
// browser state whose network authority is checked separately.
func NormalizeBrowserHTTPOrigin(raw string) (string, error) {
	return normalizeBrowserOrigin(raw, false)
}

func normalizeBrowserOrigin(raw string, publicOnly bool) (string, error) {
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
	if publicOnly && (lowerHost == "localhost" || strings.HasSuffix(lowerHost, ".localhost") ||
		lowerHost == "metadata.google.internal") {
		return "", errors.New("origin host is outside the public network policy")
	}
	if address, addressErr := netip.ParseAddr(host); addressErr == nil {
		if publicOnly && !IsPublicBrowserIP(net.IP(address.AsSlice())) {
			return "", errors.New("origin IP is outside the public network policy")
		}
		lowerHost = address.String()
	} else if legacyIP, recognized := parseBrowserIPv4(lowerHost); recognized {
		if !publicOnly {
			return "", errors.New("origin host is an ambiguous numeric IPv4 address")
		}
		if !IsPublicBrowserIP(legacyIP) {
			return "", errors.New("origin IP is outside the public network policy")
		}
		lowerHost = legacyIP.String()
	} else {
		if browserIPv4Candidate(lowerHost) {
			return "", errors.New("origin host is an invalid numeric IPv4 address")
		}
		dnsError := "origin host must be an exact DNS name"
		if publicOnly {
			dnsError = "origin host must be an exact public DNS name"
		}
		if !browserHostnamePattern.MatchString(host) || (publicOnly && !strings.Contains(lowerHost, ".")) ||
			strings.HasPrefix(lowerHost, ".") || strings.HasSuffix(lowerHost, ".") ||
			strings.Contains(lowerHost, "..") {
			return "", errors.New(dnsError)
		}
		for _, label := range strings.Split(lowerHost, ".") {
			if strings.HasPrefix(label, "-") || strings.HasSuffix(label, "-") {
				return "", errors.New(dnsError)
			}
		}
	}
	port := parsed.Port()
	if strings.HasSuffix(parsed.Host, ":") && port == "" {
		return "", errors.New("origin port must be between 1 and 65535")
	}
	if port != "" {
		portNumber, portErr := strconv.Atoi(port)
		if portErr != nil || portNumber < 1 || portNumber > 65535 {
			return "", errors.New("origin port must be between 1 and 65535")
		}
	}
	if (scheme == "http" && port == "80") || (scheme == "https" && port == "443") {
		port = ""
	}
	normalizedHost := lowerHost
	if strings.Contains(lowerHost, ":") {
		normalizedHost = strings.ReplaceAll(normalizedHost, "%", "%25")
	}
	if port != "" {
		normalizedHost = net.JoinHostPort(normalizedHost, port)
	} else if strings.Contains(lowerHost, ":") {
		normalizedHost = "[" + normalizedHost + "]"
	}
	return scheme + "://" + normalizedHost, nil
}

// browserIPv4Candidate mirrors the WHATWG "ends in a number" discriminator.
// Browsers route such hosts through IPv4 parsing rather than DNS, so a failed
// parse must be rejected instead of falling through to the DNS policy.
func browserIPv4Candidate(host string) bool {
	parts := strings.Split(host, ".")
	if len(parts) == 0 {
		return false
	}
	last := parts[len(parts)-1]
	if last == "" {
		return false
	}
	allDecimalDigits := true
	for _, char := range last {
		if char < '0' || char > '9' {
			allDecimalDigits = false
			break
		}
	}
	if allDecimalDigits {
		return true
	}
	_, recognized := parseBrowserIPv4Number(last)
	return recognized
}

func parseBrowserIPv4(host string) (net.IP, bool) {
	parts := strings.Split(host, ".")
	if len(parts) == 0 || len(parts) > 4 {
		return nil, false
	}
	numbers := make([]uint64, len(parts))
	for index, part := range parts {
		value, ok := parseBrowserIPv4Number(part)
		if !ok {
			return nil, false
		}
		numbers[index] = value
	}
	for _, value := range numbers[:len(numbers)-1] {
		if value > 255 {
			return nil, false
		}
	}
	lastLimit := uint64(1) << (8 * (5 - len(numbers)))
	if numbers[len(numbers)-1] >= lastLimit {
		return nil, false
	}
	value := numbers[len(numbers)-1]
	for index, part := range numbers[:len(numbers)-1] {
		value += part << (8 * (3 - index))
	}
	return net.IPv4(byte(value>>24), byte(value>>16), byte(value>>8), byte(value)), true
}

func parseBrowserIPv4Number(part string) (uint64, bool) {
	if part == "" {
		return 0, false
	}
	base := 10
	digits := part
	if strings.HasPrefix(digits, "0x") {
		base = 16
		digits = digits[2:]
	} else if len(digits) > 1 && digits[0] == '0' {
		base = 8
		digits = digits[1:]
	}
	if digits == "" {
		return 0, true
	}
	value, err := strconv.ParseUint(digits, base, 32)
	return value, err == nil
}

// IsPublicBrowserIP applies the browser network boundary to a resolved
// address. It denies every block in IANA's IPv4 and IPv6 special-purpose
// registries, cloud-provider metadata endpoints, and non-unicast addresses.
func IsPublicBrowserIP(ip net.IP) bool {
	address, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}
	address = address.Unmap()
	if !address.IsGlobalUnicast() {
		return false
	}
	for _, prefix := range browserSpecialPurposePrefixes {
		if prefix.Contains(address) {
			return false
		}
	}
	return true
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
	if limits.ToolResultBytes != 0 && limits.ToolResultBytes < BrowserToolResultEnvelopeBytes {
		return fmt.Errorf(
			"tool_result_bytes must be 0 or at least %d",
			BrowserToolResultEnvelopeBytes,
		)
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
