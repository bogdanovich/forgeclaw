package browserpolicy

import (
	"errors"
	"net"
	"net/netip"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

var hostnamePattern = regexp.MustCompile(`^[A-Za-z0-9.-]+$`)

var specialPurposePrefixes = []netip.Prefix{
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

// NormalizePublicOrigin canonicalizes an exact public HTTP or HTTPS origin.
func NormalizePublicOrigin(raw string) (string, error) {
	return normalizeOrigin(raw, true)
}

// NormalizeHTTPOrigin canonicalizes an HTTP or HTTPS origin without applying
// address-scope policy. Callers must separately enforce an explicit any-HTTP
// grant before using the result.
func NormalizeHTTPOrigin(raw string) (string, error) {
	return normalizeOrigin(raw, false)
}

func normalizeOrigin(raw string, publicOnly bool) (string, error) {
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
	trimmedHost := strings.TrimSuffix(host, ".")
	lowerHost := strings.ToLower(trimmedHost)
	if publicOnly && (lowerHost == "localhost" || strings.HasSuffix(lowerHost, ".localhost") ||
		lowerHost == "metadata.google.internal") {
		return "", errors.New("origin host is outside the public network policy")
	}
	address, addressErr := netip.ParseAddr(host)
	if addressErr != nil && trimmedHost != host {
		if trimmedAddress, trimmedErr := netip.ParseAddr(trimmedHost); trimmedErr == nil && trimmedAddress.Is4() {
			address, addressErr = trimmedAddress, nil
		}
	}
	if addressErr == nil {
		if publicOnly && !IsPublicIP(net.IP(address.AsSlice())) {
			return "", errors.New("origin IP is outside the public network policy")
		}
		lowerHost = address.String()
	} else if legacyIP, recognized := parseIPv4(lowerHost); recognized {
		if !publicOnly {
			return "", errors.New("origin host is an ambiguous numeric IPv4 address")
		}
		if !IsPublicIP(legacyIP) {
			return "", errors.New("origin IP is outside the public network policy")
		}
		lowerHost = legacyIP.String()
	} else {
		if ipv4Candidate(lowerHost) {
			return "", errors.New("origin host is an invalid numeric IPv4 address")
		}
		dnsError := "origin host must be an exact DNS name"
		if publicOnly {
			dnsError = "origin host must be an exact public DNS name"
		}
		if !hostnamePattern.MatchString(host) || (publicOnly && !strings.Contains(lowerHost, ".")) ||
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
	if port != "" {
		normalizedHost = net.JoinHostPort(normalizedHost, port)
	} else if strings.Contains(lowerHost, ":") {
		normalizedHost = "[" + normalizedHost + "]"
	}
	return (&url.URL{Scheme: scheme, Host: normalizedHost}).String(), nil
}

func ipv4Candidate(host string) bool {
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
	_, recognized := parseIPv4Number(last)
	return recognized
}

func parseIPv4(host string) (net.IP, bool) {
	parts := strings.Split(host, ".")
	if len(parts) == 0 || len(parts) > 4 {
		return nil, false
	}
	numbers := make([]uint64, len(parts))
	for index, part := range parts {
		value, ok := parseIPv4Number(part)
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

func parseIPv4Number(part string) (uint64, bool) {
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

// IsPublicIP rejects special-purpose and non-unicast IPv4 and IPv6 addresses.
func IsPublicIP(ip net.IP) bool {
	address, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}
	address = address.Unmap()
	if !address.IsGlobalUnicast() {
		return false
	}
	for _, prefix := range specialPurposePrefixes {
		if prefix.Contains(address) {
			return false
		}
	}
	return true
}
