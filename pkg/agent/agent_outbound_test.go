package agent

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/providers"
)

func TestFormatUserFacingAgentError_FailoverError(t *testing.T) {
	t.Parallel()

	rawErr := errors.New(
		"API request failed:\n  Status: 401\n  Body: " +
			"{\"error\":{\"message\":\"No cookie auth credentials found\",\"code\":401}}",
	)
	failErr := &providers.FailoverError{
		Reason:   providers.FailoverAuth,
		Provider: "openrouter",
		Model:    "deepseek/deepseek-v4-flash",
		Status:   401,
		Wrapped:  rawErr,
	}

	msg := formatUserFacingAgentError(fmt.Errorf("LLM call failed after retries: %w", failErr))

	for _, want := range []string{
		"Error processing message:",
		"Failover classification: auth",
		"Failover target: openrouter/deepseek/deepseek-v4-flash",
		"Provider error: API request failed:",
		"No cookie auth credentials found",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("expected message to contain %q, got:\n%s", want, msg)
		}
	}
}

func TestFormatUserFacingAgentError_FallbackExhausted(t *testing.T) {
	t.Parallel()

	exhausted := &providers.FallbackExhaustedError{
		Attempts: []providers.FallbackAttempt{
			{
				Provider: "openai",
				Model:    "gpt-5.4",
				Reason:   providers.FailoverRateLimit,
				Error: &providers.FailoverError{
					Reason:   providers.FailoverRateLimit,
					Provider: "openai",
					Model:    "gpt-5.4",
					Status:   429,
					Wrapped:  errors.New("quota exceeded"),
				},
			},
			{
				Provider: "openrouter",
				Model:    "deepseek/deepseek-v4-flash",
				Reason:   providers.FailoverAuth,
				Error: &providers.FailoverError{
					Reason:   providers.FailoverAuth,
					Provider: "openrouter",
					Model:    "deepseek/deepseek-v4-flash",
					Status:   401,
					Wrapped:  errors.New("No cookie auth credentials found"),
				},
			},
		},
	}

	msg := formatUserFacingAgentError(fmt.Errorf("LLM call failed after retries: %w", exhausted))

	for _, want := range []string{
		"Error processing message:",
		"Failover details:",
		"1. openai/gpt-5.4 — classification: rate_limit",
		"provider error: quota exceeded",
		"2. openrouter/deepseek/deepseek-v4-flash — classification: auth",
		"provider error: No cookie auth credentials found",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("expected message to contain %q, got:\n%s", want, msg)
		}
	}
}
