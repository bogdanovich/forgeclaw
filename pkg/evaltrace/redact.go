package evaltrace

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

type FieldClass string

const (
	FieldMetadata FieldClass = "metadata"
	FieldContent  FieldClass = "content"
)

type FieldPolicy struct {
	Class    FieldClass
	MaxBytes int
}

type Redactor struct {
	Mode   ContentMode
	Filter func(string) string
}

var (
	sensitiveKeyPattern = regexp.MustCompile(`(?i)(authorization|cookie|secret|token|api[_-]?key|password|credential)`)
	bearerPattern       = regexp.MustCompile(`(?i)bearer\s+[A-Za-z0-9._~+/=-]+`)
	envSecretPattern    = regexp.MustCompile(`(?i)([A-Z0-9_]*(?:TOKEN|SECRET|PASSWORD|API_KEY)[A-Z0-9_]*)=([^\s]+)`)
)

func (r Redactor) Project(input map[string]any, allow map[string]FieldPolicy) map[string]any {
	out := make(map[string]any)
	for key, policy := range allow {
		value, ok := input[key]
		if !ok || value == nil || sensitiveKeyPattern.MatchString(key) {
			continue
		}
		if policy.Class == FieldContent && r.Mode == ContentMetadataOnly {
			text := fmt.Sprint(value)
			out[key+"_len"] = len(text)
			continue
		}
		out[key] = r.sanitize(value, policy.MaxBytes)
	}
	return out
}

func (r Redactor) sanitize(value any, maxBytes int) any {
	switch value := value.(type) {
	case string:
		value = scrubString(value)
		if r.Filter != nil {
			value = r.Filter(value)
		}
		if maxBytes > 0 && len(value) > maxBytes {
			value = value[:maxBytes]
		}
		return value
	case []string:
		out := make([]string, 0, len(value))
		for _, item := range value {
			out = append(out, r.sanitize(item, maxBytes).(string))
		}
		return out
	case bool, float64, float32, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return value
	default:
		return "[UNSUPPORTED]"
	}
}

func scrubString(value string) string {
	value = bearerPattern.ReplaceAllString(value, "Bearer [REDACTED]")
	value = envSecretPattern.ReplaceAllString(value, "$1=[REDACTED]")
	if parsed, err := url.Parse(value); err == nil && parsed.Scheme != "" && parsed.Host != "" {
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
		value = parsed.String()
	}
	if strings.HasPrefix(value, "data:") {
		return "[DATA_URL REDACTED]"
	}
	return value
}
