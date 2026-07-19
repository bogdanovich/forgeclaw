package config

import (
	"testing"
	"time"
)

func TestDefaultRequestUserInputConfigIsEnabledAndBounded(t *testing.T) {
	cfg := DefaultConfig()
	tool := cfg.Tools.RequestUserInput
	if !tool.Enabled || tool.DefaultTimeout() != time.Hour ||
		tool.MaxTimeout() != 24*time.Hour || tool.Retention() != 7*24*time.Hour {
		t.Fatalf("request_user_input defaults = %#v", tool)
	}
	if err := cfg.ValidateRequestUserInputConfig(); err != nil {
		t.Fatalf("ValidateRequestUserInputConfig() error = %v", err)
	}
}

func TestRequestUserInputConfigRejectsInvalidBounds(t *testing.T) {
	tests := []RequestUserInputToolsConfig{
		{DefaultTimeoutSeconds: 59, MaxTimeoutSeconds: 3600, RetentionHours: 168},
		{DefaultTimeoutSeconds: 3600, MaxTimeoutSeconds: 3599, RetentionHours: 168},
		{DefaultTimeoutSeconds: 3600, MaxTimeoutSeconds: 86401, RetentionHours: 168},
		{DefaultTimeoutSeconds: 3600, MaxTimeoutSeconds: 86400, RetentionHours: -1},
	}
	for _, tool := range tests {
		cfg := DefaultConfig()
		cfg.Tools.RequestUserInput = tool
		if err := cfg.ValidateRequestUserInputConfig(); err == nil {
			t.Fatalf("ValidateRequestUserInputConfig(%#v) succeeded", tool)
		}
	}
}

func TestRequestUserInputConfigUsesDefaultsForZeroValues(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Tools.RequestUserInput = RequestUserInputToolsConfig{Enabled: true}
	if err := cfg.ValidateRequestUserInputConfig(); err != nil {
		t.Fatalf("ValidateRequestUserInputConfig() error = %v", err)
	}
	tool := cfg.Tools.RequestUserInput
	if tool.DefaultTimeout() != time.Hour || tool.MaxTimeout() != 24*time.Hour ||
		tool.Retention() != 7*24*time.Hour {
		t.Fatalf("zero-value request_user_input defaults = %#v", tool)
	}
}
