package agent

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/sipeed/picoclaw/pkg/interactions"
)

const (
	approvalDisplayMaxDepth      = 8
	approvalDisplayMaxItems      = 32
	approvalDisplayMaxStringRune = 500
)

var (
	approvalSensitiveKey = regexp.MustCompile(
		`(?i)(authorization|cookie|credential|password|passwd|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|client[_-]?secret)`,
	)
	approvalBearerValue = regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]+`)
	approvalEnvSecret   = regexp.MustCompile(
		`(?i)([A-Z0-9_]*(?:TOKEN|SECRET|PASSWORD|PASSWD|API_KEY|PRIVATE_KEY)[A-Z0-9_]*)=([^\s]+)`,
	)
	approvalKnownCredential = regexp.MustCompile(
		`\b(?:sk-[A-Za-z0-9_-]{12,}|ghp_[A-Za-z0-9]{12,}|github_pat_[A-Za-z0-9_]{12,}|xox[baprs]-[A-Za-z0-9-]{12,})\b`,
	)
)

func renderApprovalAction(toolName string, arguments map[string]any) string {
	toolName = sanitizeApprovalToolName(toolName)
	redacted := redactApprovalValue("", arguments, 0)
	encoded, err := json.Marshal(redacted)
	if err != nil {
		encoded = []byte(`"[UNAVAILABLE]"`)
	}
	description := fmt.Sprintf(
		"Tool: %s\nArguments (redacted JSON): %s",
		toolName,
		encoded,
	)
	return truncateRunes(description, interactions.MaxApprovalAction)
}

func redactApprovalValue(key string, value any, depth int) any {
	if approvalSensitiveKey.MatchString(key) {
		return "[REDACTED]"
	}
	if depth >= approvalDisplayMaxDepth {
		return "[DEPTH LIMIT]"
	}
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, min(len(typed), approvalDisplayMaxItems))
		keys := make([]string, 0, len(typed))
		for childKey := range typed {
			keys = append(keys, childKey)
		}
		sort.Strings(keys)
		limit := min(len(keys), approvalDisplayMaxItems)
		for _, childKey := range keys[:limit] {
			out[childKey] = redactApprovalValue(childKey, typed[childKey], depth+1)
		}
		if len(keys) > limit {
			out["[TRUNCATED]"] = len(keys) - limit
		}
		return out
	case []any:
		limit := min(len(typed), approvalDisplayMaxItems)
		out := make([]any, 0, limit+1)
		for _, item := range typed[:limit] {
			out = append(out, redactApprovalValue(key, item, depth+1))
		}
		if len(typed) > limit {
			out = append(out, fmt.Sprintf("[%d MORE ITEMS]", len(typed)-limit))
		}
		return out
	case string:
		return scrubApprovalString(typed)
	case nil, bool, float64, float32, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, json.Number:
		return typed
	default:
		return "[UNSUPPORTED]"
	}
}

func scrubApprovalString(value string) string {
	value = strings.ToValidUTF8(value, "[INVALID UTF-8]")
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(value)), "data:") {
		return "[DATA URL REDACTED]"
	}
	value = approvalBearerValue.ReplaceAllString(value, "$1 [REDACTED]")
	value = approvalEnvSecret.ReplaceAllString(value, "$1=[REDACTED]")
	value = approvalKnownCredential.ReplaceAllString(value, "[REDACTED]")
	if parsed, err := url.Parse(value); err == nil && parsed.Scheme != "" && parsed.Host != "" {
		if parsed.User != nil {
			parsed.User = url.User("[REDACTED]")
		}
		query := parsed.Query()
		for queryKey := range query {
			if approvalSensitiveKey.MatchString(queryKey) {
				query.Set(queryKey, "[REDACTED]")
			}
		}
		parsed.RawQuery = query.Encode()
		value = parsed.String()
	}
	return truncateRunes(value, approvalDisplayMaxStringRune)
}

func sanitizeApprovalToolName(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, value)
	if value == "" {
		return "[unknown tool]"
	}
	return truncateRunes(value, 256)
}

func truncateRunes(value string, maxRunes int) string {
	if maxRunes <= 0 || utf8.RuneCountInString(value) <= maxRunes {
		return value
	}
	runes := []rune(value)
	if maxRunes <= 3 {
		return string(runes[:maxRunes])
	}
	return string(runes[:maxRunes-3]) + "..."
}
