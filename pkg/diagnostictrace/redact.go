package diagnostictrace

import (
	"encoding/json"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

type Redactor struct {
	Filter func(string) string
}

var (
	sensitiveKeyPattern = regexp.MustCompile(`(?i)(authorization|cookie|secret|token|api[_-]?key|password|credential)`)
	bearerPattern       = regexp.MustCompile(`(?i)bearer\s+[A-Za-z0-9._~+/=-]+`)
	basicAuthPattern    = regexp.MustCompile(`(?i)basic\s+[A-Za-z0-9+/=]+`)
	authHeaderPattern   = regexp.MustCompile(
		`(?i)(authorization|proxy-authorization|cookie|set-cookie)\s*:\s*[^\r\n]+`,
	)
	envSecretPattern   = regexp.MustCompile(`(?i)([A-Z0-9_]*(?:TOKEN|SECRET|PASSWORD|API_KEY)[A-Z0-9_]*)=([^\s]+)`)
	embeddedURLPattern = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.-]*://[^\s"'<>]+`)
	dataURLPattern     = regexp.MustCompile(`(?i)\bdata:[^\s"'<>]+`)
	privateKeyPattern  = regexp.MustCompile(
		`(?is)-----BEGIN [^-]*(?:PRIVATE KEY)-----.*?-----END [^-]*(?:PRIVATE KEY)-----`,
	)
	privateKeyStartPattern = regexp.MustCompile(`(?is)-----BEGIN [^-]*(?:PRIVATE KEY)-----`)
	commonTokenPatterns    = []*regexp.Regexp{
		regexp.MustCompile(`\b(?:sk|rk)-[A-Za-z0-9][A-Za-z0-9._-]{7,}\b`),
		regexp.MustCompile(`\bgsk_[A-Za-z0-9_-]{8,}\b`),
		regexp.MustCompile(`\bAIza[0-9A-Za-z_-]{16,}\b`),
		regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9_]{16,}\b`),
		regexp.MustCompile(`\bgithub_pat_[A-Za-z0-9_]{16,}\b`),
		regexp.MustCompile(`\bxox[baprs]-[A-Za-z0-9-]{8,}\b`),
		regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`),
		regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b`),
	}
)

const maxRedactionNodes = 2048

const redactionLookaheadBytes = 1024

// RedactText removes credential-shaped values and applies the configured
// secret-value filter before returning a bounded UTF-8 preview.
func (r Redactor) RedactText(value string, maxBytes int) (preview string) {
	defer func() {
		if recover() != nil {
			preview = ""
		}
	}()
	if maxBytes <= 0 {
		return ""
	}
	value = boundRedactionInput(value, maxBytes)
	value = scrubString(value)
	if r.Filter != nil {
		value = r.Filter(value)
	}
	return truncateUTF8(value, maxBytes)
}

// RedactJSON recursively removes values under sensitive keys and returns a
// bounded JSON preview. Unsupported values are represented without formatting
// their contents.
func (r Redactor) RedactJSON(value any, maxBytes int) (preview string) {
	defer func() {
		if recover() != nil {
			preview = ""
		}
	}()
	if maxBytes <= 0 {
		return ""
	}
	nodes := 0
	sanitized := r.sanitizeJSON(value, maxBytes, &nodes)
	data, err := json.Marshal(sanitized)
	if err != nil {
		return `"[UNSUPPORTED]"`
	}
	return truncateUTF8(string(data), maxBytes)
}

func (r Redactor) sanitizeJSON(value any, maxBytes int, nodes *int) any {
	*nodes++
	if *nodes > maxRedactionNodes {
		return "[TRUNCATED: node limit]"
	}
	switch value := value.(type) {
	case nil, bool, float32, float64, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, json.Number:
		return value
	case string:
		return r.RedactText(value, maxBytes)
	case map[string]any:
		remaining := max(0, maxRedactionNodes-*nodes)
		out := make(map[string]any, min(len(value), remaining))
		for key, item := range value {
			if *nodes >= maxRedactionNodes {
				out["[TRUNCATED]"] = "node limit"
				break
			}
			*nodes++
			if sensitiveKeyPattern.MatchString(key) {
				safeKey := r.RedactText(key, 256)
				if safeKey == "" {
					safeKey = "[SENSITIVE_KEY]"
				}
				out[uniqueMapKey(out, safeKey)] = "[REDACTED]"
				continue
			}
			safeKey := r.RedactText(key, 256)
			if safeKey != key {
				safeKey = uniqueMapKey(out, "[REDACTED_KEY]")
				out[safeKey] = "[REDACTED]"
				continue
			}
			out[safeKey] = r.sanitizeJSON(item, maxBytes, nodes)
		}
		return out
	case []any:
		remaining := max(0, maxRedactionNodes-*nodes)
		out := make([]any, 0, min(len(value), remaining))
		for _, item := range value {
			if *nodes >= maxRedactionNodes {
				out = append(out, "[TRUNCATED: node limit]")
				break
			}
			out = append(out, r.sanitizeJSON(item, maxBytes, nodes))
		}
		return out
	default:
		return "[UNSUPPORTED]"
	}
}

func uniqueMapKey(values map[string]any, key string) string {
	if _, exists := values[key]; !exists {
		return key
	}
	for suffix := 2; ; suffix++ {
		candidate := key + "_" + strconv.Itoa(suffix)
		if _, exists := values[candidate]; !exists {
			return candidate
		}
	}
}

func boundRedactionInput(value string, maxBytes int) string {
	limit := maxBytes + redactionLookaheadBytes
	if len(value) <= limit {
		return value
	}
	value = truncateValidUTF8(value, limit)
	cut := strings.LastIndexAny(value, " \t\r\n,;\"'<>[]{}()")
	if cut < 0 {
		return "[TRUNCATED: oversized field]"
	}
	return value[:cut+1] + "...[TRUNCATED]"
}

func scrubString(value string) string {
	value = privateKeyPattern.ReplaceAllString(value, "[PRIVATE KEY REDACTED]")
	if start := privateKeyStartPattern.FindStringIndex(value); start != nil {
		value = value[:start[0]] + "[PRIVATE KEY REDACTED]"
	}
	value = bearerPattern.ReplaceAllString(value, "Bearer [REDACTED]")
	value = basicAuthPattern.ReplaceAllString(value, "Basic [REDACTED]")
	value = authHeaderPattern.ReplaceAllString(value, "$1: [REDACTED]")
	value = envSecretPattern.ReplaceAllString(value, "$1=[REDACTED]")
	for _, pattern := range commonTokenPatterns {
		value = pattern.ReplaceAllString(value, "[REDACTED]")
	}
	value = dataURLPattern.ReplaceAllString(value, "[DATA_URL REDACTED]")
	value = embeddedURLPattern.ReplaceAllStringFunc(value, redactURLCredentials)
	return value
}

func redactURLCredentials(value string) string {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return value
	}
	if parsed.User != nil {
		parsed.User = url.User("[REDACTED]")
	}
	query := parsed.Query()
	for key := range query {
		if sensitiveKeyPattern.MatchString(key) {
			query.Set(key, "[REDACTED]")
		}
	}
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func truncateUTF8(value string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(value) <= maxBytes {
		return value
	}
	const marker = "...[TRUNCATED]"
	if maxBytes <= len(marker) {
		return truncateValidUTF8(value, maxBytes)
	}
	value = truncateValidUTF8(value, maxBytes-len(marker))
	return value + marker
}

func truncateValidUTF8(value string, maxBytes int) string {
	value = value[:min(len(value), maxBytes)]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}
