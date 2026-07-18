package toolpolicy

import (
	"fmt"
	"strings"
)

// ResultRetentionMode controls how successful tool output appears in future prompts.
type ResultRetentionMode string

const (
	ResultRetentionPreserve       ResultRetentionMode = "preserve"
	ResultRetentionCompactReceipt ResultRetentionMode = "compact_receipt"
	ResultRetentionTransient      ResultRetentionMode = "transient"
	ResultRetentionDurable        ResultRetentionMode = "durable"

	maxResultReceiptBytes = 1024
)

// ResultRetentionRule applies future-context behavior to an exact tool name.
// Runtime safety rules may preserve more data than the configured mode requests.
type ResultRetentionRule struct {
	Mode    ResultRetentionMode `json:"mode"`
	Receipt string              `json:"receipt,omitempty"`
}

// ResultRetentionPolicy maps canonical tool names to future-context behavior.
type ResultRetentionPolicy map[string]ResultRetentionRule

// Validate checks the user-configurable retention contract.
func (p ResultRetentionPolicy) Validate() error {
	for toolName, rule := range p {
		if strings.TrimSpace(toolName) == "" || toolName != strings.TrimSpace(toolName) {
			return fmt.Errorf("tool names must be non-empty and trimmed")
		}
		switch rule.Mode {
		case ResultRetentionPreserve, ResultRetentionTransient:
			if rule.Receipt != "" {
				return fmt.Errorf("tool %q mode %q does not accept a receipt", toolName, rule.Mode)
			}
		case ResultRetentionCompactReceipt, ResultRetentionDurable:
			if strings.TrimSpace(rule.Receipt) == "" {
				return fmt.Errorf("tool %q mode %q requires a receipt", toolName, rule.Mode)
			}
			if rule.Receipt != strings.TrimSpace(rule.Receipt) {
				return fmt.Errorf("tool %q receipt must be trimmed", toolName)
			}
		default:
			return fmt.Errorf("tool %q has unsupported mode %q", toolName, rule.Mode)
		}
		if len(rule.Receipt) > maxResultReceiptBytes {
			return fmt.Errorf(
				"tool %q receipt exceeds %d bytes",
				toolName,
				maxResultReceiptBytes,
			)
		}
	}
	return nil
}
