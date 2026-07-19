package agent

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/sipeed/picoclaw/pkg/interactions"
)

// renderApprovalAction combines runtime-owned identity with trusted policy
// presentation. Tool arguments are deliberately never serialized here.
func renderApprovalAction(toolName, actionSummary string) (string, error) {
	if err := validateApprovalDisplayText("tool name", toolName, 256); err != nil {
		return "", err
	}
	if err := validateApprovalDisplayText(
		"action summary",
		actionSummary,
		interactions.MaxSummaryLength,
	); err != nil {
		return "", err
	}
	action := fmt.Sprintf("Tool: %s\nAction: %s", toolName, actionSummary)
	if utf8.RuneCountInString(action) > interactions.MaxApprovalAction {
		return "", fmt.Errorf("approval action exceeds the display limit")
	}
	return action, nil
}

func validateApprovalDisplayText(field, value string, maxRunes int) error {
	if value == "" || strings.TrimSpace(value) != value || !utf8.ValidString(value) ||
		utf8.RuneCountInString(value) > maxRunes {
		return fmt.Errorf("approval %s exceeds display bounds", field)
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return fmt.Errorf("approval %s contains control characters", field)
		}
	}
	return nil
}
