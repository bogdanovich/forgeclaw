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
	approvalOpaqueKey = regexp.MustCompile(
		`(?i)^(args?|body|command|commands|content|data|env|environment|headers?|input|payload|script|stdin|text)$`,
	)
	approvalBearerValue = regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]+`)
	approvalEnvSecret   = regexp.MustCompile(
		`(?i)([A-Z0-9_]*(?:TOKEN|SECRET|PASSWORD|PASSWD|API_KEY|PRIVATE_KEY)[A-Z0-9_]*)=([^\s]+)`,
	)
	approvalKnownCredential = regexp.MustCompile(
		`\b(?:sk-[A-Za-z0-9_-]{12,}|ghp_[A-Za-z0-9]{12,}|github_pat_[A-Za-z0-9_]{12,}|xox[baprs]-[A-Za-z0-9-]{12,})\b`,
	)
	approvalSafeStringKeys = map[string]struct{}{
		"action": {}, "agent_id": {}, "branch": {}, "channel": {}, "chat_id": {},
		"commit": {}, "cwd": {}, "dest": {}, "destination": {}, "dir": {},
		"directory": {}, "file": {}, "file_path": {}, "host": {}, "hostname": {},
		"id": {}, "ids": {}, "message_id": {}, "method": {}, "mode": {}, "name": {},
		"operation": {}, "path": {}, "paths": {}, "port": {}, "ref": {}, "remote": {},
		"repo": {}, "repository": {}, "reply_to_message_id": {}, "session_key": {},
		"sha": {}, "source": {}, "src": {}, "target": {}, "targets": {}, "task_id": {},
		"topic_id": {}, "type": {}, "uri": {}, "url": {}, "urls": {},
	}
	approvalURLKeys = map[string]struct{}{
		"uri": {}, "url": {}, "urls": {},
	}
)

func renderApprovalAction(toolName string, arguments map[string]any) (string, error) {
	toolName, err := sanitizeApprovalToolName(toolName)
	if err != nil {
		return "", err
	}
	redacted, err := redactApprovalValue("", arguments, 0)
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal(redacted)
	if err != nil {
		return "", fmt.Errorf("encode approval arguments: %w", err)
	}
	description := fmt.Sprintf(
		"Tool: %s\nArguments (redacted JSON): %s",
		toolName,
		encoded,
	)
	if utf8.RuneCountInString(description) > interactions.MaxApprovalAction {
		return "", fmt.Errorf("approval action exceeds the complete display limit")
	}
	return description, nil
}

func redactApprovalValue(key string, value any, depth int) (any, error) {
	if approvalSensitiveKey.MatchString(key) {
		return "[REDACTED]", nil
	}
	if approvalOpaqueKey.MatchString(strings.TrimSpace(key)) {
		return nil, fmt.Errorf("approval field %q is opaque and cannot be displayed safely", key)
	}
	if depth >= approvalDisplayMaxDepth {
		return nil, fmt.Errorf("approval arguments exceed the display depth limit")
	}
	switch typed := value.(type) {
	case map[string]any:
		if len(typed) > approvalDisplayMaxItems {
			return nil, fmt.Errorf("approval object exceeds the complete display item limit")
		}
		out := make(map[string]any, len(typed))
		keys := make([]string, 0, len(typed))
		for childKey := range typed {
			keys = append(keys, childKey)
		}
		sort.Strings(keys)
		for _, childKey := range keys {
			childValue, err := redactApprovalValue(childKey, typed[childKey], depth+1)
			if err != nil {
				return nil, err
			}
			out[childKey] = childValue
		}
		return out, nil
	case []any:
		if len(typed) > approvalDisplayMaxItems {
			return nil, fmt.Errorf("approval array exceeds the complete display item limit")
		}
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			childValue, err := redactApprovalValue(key, item, depth+1)
			if err != nil {
				return nil, err
			}
			out = append(out, childValue)
		}
		return out, nil
	case string:
		if _, allowed := approvalSafeStringKeys[strings.ToLower(strings.TrimSpace(key))]; !allowed {
			return nil, fmt.Errorf("approval string field %q has no safe display policy", key)
		}
		if !utf8.ValidString(typed) || utf8.RuneCountInString(typed) > approvalDisplayMaxStringRune {
			return nil, fmt.Errorf("approval string field %q exceeds display bounds", key)
		}
		return scrubApprovalString(key, typed)
	case nil, bool, float64, float32, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, json.Number:
		return typed, nil
	default:
		return nil, fmt.Errorf("approval field %q has unsupported type %T", key, value)
	}
}

func scrubApprovalString(key, value string) (string, error) {
	if _, isURL := approvalURLKeys[strings.ToLower(strings.TrimSpace(key))]; isURL {
		return renderCredentialFreeApprovalOrigin(value)
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(value)), "data:") {
		return "[DATA URL REDACTED]", nil
	}
	value = approvalBearerValue.ReplaceAllString(value, "$1 [REDACTED]")
	value = approvalEnvSecret.ReplaceAllString(value, "$1=[REDACTED]")
	value = approvalKnownCredential.ReplaceAllString(value, "[REDACTED]")
	return value, nil
}

func renderCredentialFreeApprovalOrigin(value string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return "", fmt.Errorf("approval URL must be an absolute HTTP(S) origin")
	}
	if parsed.User != nil || (parsed.Path != "" && parsed.Path != "/") ||
		parsed.RawPath != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" {
		return "", fmt.Errorf("approval URL contains opaque or credential-bearing components")
	}
	parsed.Path = ""
	return parsed.String(), nil
}

func sanitizeApprovalToolName(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || utf8.RuneCountInString(value) > 256 {
		return "", fmt.Errorf("approval tool name exceeds display bounds")
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return "", fmt.Errorf("approval tool name contains control characters")
		}
	}
	return value, nil
}
