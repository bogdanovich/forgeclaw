package agent

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/bogdanovich/mintclaw/pkg/interactions"
)

// validateApprovalDisplay validates runtime-owned identity and trusted policy
// presentation before the interaction persists them separately. Tool arguments
// are deliberately never serialized here.
func validateApprovalDisplay(toolName, actionSummary string) error {
	if err := validateApprovalDisplayText("tool name", toolName, 256); err != nil {
		return err
	}
	if err := validateApprovalDisplayText(
		"action summary",
		actionSummary,
		interactions.MaxSummaryLength,
	); err != nil {
		return err
	}
	if utf8.RuneCountInString(actionSummary) > interactions.MaxApprovalAction {
		return fmt.Errorf("approval action exceeds the display limit")
	}
	return nil
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
