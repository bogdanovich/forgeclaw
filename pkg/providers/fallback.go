package providers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// FallbackChain orchestrates model fallback across multiple candidates.
type FallbackChain struct {
	cooldown *CooldownTracker
	rl       *RateLimiterRegistry
}

// FallbackCandidate represents one model/provider to try.
type FallbackCandidate struct {
	Provider    string
	Model       string
	DisplayName string // optional configured alias/raw model label for persistence/UI
	RPM         int    // requests per minute; 0 means unrestricted
	IdentityKey string // optional stable config identity for cooldown/rate limiting
}

// StableKey returns the candidate's config-level identity when available,
// otherwise it falls back to the runtime provider/model key.
func (c FallbackCandidate) StableKey() string {
	if key := strings.TrimSpace(c.IdentityKey); key != "" {
		return key
	}
	return ModelKey(c.Provider, c.Model)
}

// FallbackResult contains the successful response and metadata about all attempts.
type FallbackResult struct {
	Response    *LLMResponse
	Provider    string
	Model       string
	IdentityKey string
	Attempts    []FallbackAttempt
}

// FallbackAttempt records one attempt in the fallback chain.
type FallbackAttempt struct {
	Provider    string
	Model       string
	IdentityKey string
	Error       error
	Reason      FailoverReason
	Duration    time.Duration
	Skipped     bool // true if skipped due to cooldown
	Succeeded   bool
}

type FallbackAttemptObserver func(FallbackAttempt)

// NewFallbackChain creates a new fallback chain with the given cooldown tracker
// and rate limiter registry.
func NewFallbackChain(cooldown *CooldownTracker, rl *RateLimiterRegistry) *FallbackChain {
	return &FallbackChain{cooldown: cooldown, rl: rl}
}

// ResolveCandidates parses model config into a deduplicated candidate list.
func ResolveCandidates(cfg ModelConfig, defaultProvider string) []FallbackCandidate {
	return ResolveCandidatesWithLookup(cfg, defaultProvider, nil)
}

func ResolveCandidatesWithLookup(
	cfg ModelConfig,
	defaultProvider string,
	lookup func(raw string) (resolved string, ok bool),
) []FallbackCandidate {
	seen := make(map[string]bool)
	var candidates []FallbackCandidate

	addCandidate := func(raw string) {
		candidateRaw := strings.TrimSpace(raw)
		if lookup != nil {
			if resolved, ok := lookup(candidateRaw); ok {
				candidateRaw = resolved
			}
		}

		ref := ParseModelRef(candidateRaw, defaultProvider)
		if ref == nil {
			return
		}
		key := ModelKey(ref.Provider, ref.Model)
		if seen[key] {
			return
		}
		seen[key] = true
		candidates = append(candidates, FallbackCandidate{
			Provider:    ref.Provider,
			Model:       ref.Model,
			DisplayName: candidateRaw,
		})
	}

	// Primary first.
	addCandidate(cfg.Primary)

	// Then fallbacks.
	for _, fb := range cfg.Fallbacks {
		addCandidate(fb)
	}

	return candidates
}

// Execute runs the fallback chain for text/chat requests.
// It tries each candidate in order, respecting cooldowns and error classification.
//
// Behavior:
//   - Candidates in cooldown are skipped (logged as skipped attempt).
//   - context.Canceled aborts immediately (user abort, no fallback).
//   - Non-retriable errors (format) abort immediately.
//   - Retriable errors trigger fallback to next candidate.
//   - Success marks provider as good (resets cooldown).
//   - If all fail, returns aggregate error with all attempts.
func (fc *FallbackChain) Execute(
	ctx context.Context,
	candidates []FallbackCandidate,
	run func(ctx context.Context, provider, model string) (*LLMResponse, error),
) (*FallbackResult, error) {
	return fc.ExecuteCandidate(
		ctx,
		candidates,
		func(ctx context.Context, candidate FallbackCandidate) (*LLMResponse, error) {
			return run(ctx, candidate.Provider, candidate.Model)
		},
	)
}

// ExecuteCandidate runs the fallback chain and passes the complete candidate
// to the caller so model-list identity metadata remains available.
func (fc *FallbackChain) ExecuteCandidate(
	ctx context.Context,
	candidates []FallbackCandidate,
	run func(ctx context.Context, candidate FallbackCandidate) (*LLMResponse, error),
) (*FallbackResult, error) {
	return fc.ExecuteCandidateObserved(ctx, candidates, run, nil)
}

// ExecuteCandidateObserved reports every skipped, failed, and successful
// candidate without changing the compatibility Attempts projection.
func (fc *FallbackChain) ExecuteCandidateObserved(
	ctx context.Context,
	candidates []FallbackCandidate,
	run func(ctx context.Context, candidate FallbackCandidate) (*LLMResponse, error),
	observer FallbackAttemptObserver,
) (*FallbackResult, error) {
	if len(candidates) == 0 {
		return nil, fmt.Errorf("fallback: no candidates configured")
	}

	result := &FallbackResult{
		Attempts: make([]FallbackAttempt, 0, len(candidates)),
	}
	recordAttempt := func(attempt FallbackAttempt) {
		attempt.IdentityKey = candidatesIdentityKey(attempt, candidates)
		result.Attempts = append(result.Attempts, attempt)
		notifyFallbackObserver(observer, attempt)
	}
	recordSuccess := func(candidate FallbackCandidate, duration time.Duration) {
		if observer != nil {
			notifyFallbackObserver(observer, FallbackAttempt{
				Provider: candidate.Provider, Model: candidate.Model,
				IdentityKey: candidate.StableKey(), Duration: duration, Succeeded: true,
			})
		}
	}

	for i, candidate := range candidates {
		// Check context before each attempt.
		if ctx.Err() == context.Canceled {
			return nil, context.Canceled
		}

		// Check cooldown per stable candidate identity, not just provider/model.
		// This allows aliases and multi-key configs to fail over independently.
		cooldownKey := candidate.StableKey()
		if !fc.cooldown.IsAvailable(cooldownKey) {
			remaining := fc.cooldown.CooldownRemaining(cooldownKey)
			recordAttempt(FallbackAttempt{
				Provider: candidate.Provider,
				Model:    candidate.Model,
				Skipped:  true,
				Reason:   FailoverRateLimit,
				Error: fmt.Errorf(
					"%s in cooldown (%s remaining)",
					cooldownKey,
					remaining.Round(time.Second),
				),
			})
			continue
		}

		// Enforce per-candidate rate limit before calling the provider.
		// If this candidate is locally saturated, try other candidates first.
		if fc.rl != nil {
			if !fc.rl.TryAcquire(cooldownKey) {
				if i < len(candidates)-1 {
					recordAttempt(FallbackAttempt{
						Provider: candidate.Provider,
						Model:    candidate.Model,
						Skipped:  true,
						Reason:   FailoverRateLimit,
						Error:    fmt.Errorf("%s waiting for local rate limit token", cooldownKey),
					})
					continue
				}
				if waitErr := fc.rl.Wait(ctx, cooldownKey); waitErr != nil {
					recordAttempt(FallbackAttempt{
						Provider: candidate.Provider,
						Model:    candidate.Model,
						Skipped:  true,
						Reason:   FailoverRateLimit,
						Error:    waitErr,
					})
					return nil, waitErr
				}
			}
		}

		// Execute the run function.
		start := time.Now()
		resp, err := run(ctx, candidate)
		elapsed := time.Since(start)

		if err == nil {
			// Success.
			recordSuccess(candidate, elapsed)
			fc.cooldown.MarkSuccess(cooldownKey)
			result.Response = resp
			result.Provider = candidate.Provider
			result.Model = candidate.Model
			result.IdentityKey = candidate.StableKey()
			return result, nil
		}

		// Context cancellation: abort immediately, no fallback.
		if ctx.Err() == context.Canceled {
			recordAttempt(FallbackAttempt{
				Provider: candidate.Provider,
				Model:    candidate.Model,
				Error:    err,
				Duration: elapsed,
			})
			return nil, context.Canceled
		}

		// A nested fallback chain already accounted for the health of the
		// candidates it actually called. Record its exhaustion against this
		// route, but do not put the outer wrapper candidate into cooldown.
		var nestedExhausted *FallbackExhaustedError
		if errors.As(err, &nestedExhausted) {
			recordAttempt(FallbackAttempt{
				Provider: candidate.Provider,
				Model:    candidate.Model,
				Error:    err,
				Reason:   nestedExhausted.lastReason(),
				Duration: elapsed,
			})
			if i == len(candidates)-1 {
				return nil, &FallbackExhaustedError{Attempts: result.Attempts}
			}
			continue
		}

		// Classify the error.
		failErr := ClassifyError(err, candidate.Provider, candidate.Model)

		if failErr == nil {
			// Unclassifiable error: do not fallback, return immediately.
			recordAttempt(FallbackAttempt{
				Provider: candidate.Provider,
				Model:    candidate.Model,
				Error:    err,
				Duration: elapsed,
			})
			return nil, fmt.Errorf("fallback: unclassified error from %s/%s: %w",
				candidate.Provider, candidate.Model, err)
		}

		// Non-retriable error: abort immediately.
		if !failErr.IsRetriable() {
			recordAttempt(FallbackAttempt{
				Provider: candidate.Provider,
				Model:    candidate.Model,
				Error:    failErr,
				Reason:   failErr.Reason,
				Duration: elapsed,
			})
			return nil, failErr
		}

		// Retriable error: mark failure and continue to next candidate.
		fc.cooldown.MarkFailure(cooldownKey, failErr.Reason)
		recordAttempt(FallbackAttempt{
			Provider: candidate.Provider,
			Model:    candidate.Model,
			Error:    failErr,
			Reason:   failErr.Reason,
			Duration: elapsed,
		})

		// If this was the last candidate, return aggregate error.
		if i == len(candidates)-1 {
			return nil, &FallbackExhaustedError{Attempts: result.Attempts}
		}
	}

	// All candidates were skipped (all in cooldown).
	return nil, &FallbackExhaustedError{Attempts: result.Attempts}
}

func notifyFallbackObserver(observer FallbackAttemptObserver, attempt FallbackAttempt) {
	if observer == nil {
		return
	}
	defer func() { _ = recover() }()
	observer(attempt)
}

func candidatesIdentityKey(attempt FallbackAttempt, candidates []FallbackCandidate) string {
	for _, candidate := range candidates {
		if candidate.Provider == attempt.Provider && candidate.Model == attempt.Model {
			return candidate.StableKey()
		}
	}
	return ModelKey(attempt.Provider, attempt.Model)
}

// ExecuteImage runs the fallback chain for image/vision requests.
// Simpler than Execute: no cooldown checks (image endpoints have different rate limits).
// Image dimension/size errors abort immediately (non-retriable).
func (fc *FallbackChain) ExecuteImage(
	ctx context.Context,
	candidates []FallbackCandidate,
	run func(ctx context.Context, provider, model string) (*LLMResponse, error),
) (*FallbackResult, error) {
	if len(candidates) == 0 {
		return nil, fmt.Errorf("image fallback: no candidates configured")
	}

	result := &FallbackResult{
		Attempts: make([]FallbackAttempt, 0, len(candidates)),
	}

	for i, candidate := range candidates {
		if ctx.Err() == context.Canceled {
			return nil, context.Canceled
		}

		// Enforce per-candidate rate limit before calling the provider.
		// If this candidate is locally saturated, try other candidates first.
		imageKey := candidate.StableKey()
		if fc.rl != nil {
			if !fc.rl.TryAcquire(imageKey) {
				if i < len(candidates)-1 {
					result.Attempts = append(result.Attempts, FallbackAttempt{
						Provider: candidate.Provider,
						Model:    candidate.Model,
						Skipped:  true,
						Reason:   FailoverRateLimit,
						Error:    fmt.Errorf("%s waiting for local rate limit token", imageKey),
					})
					continue
				}
				if waitErr := fc.rl.Wait(ctx, imageKey); waitErr != nil {
					result.Attempts = append(result.Attempts, FallbackAttempt{
						Provider: candidate.Provider,
						Model:    candidate.Model,
						Skipped:  true,
						Reason:   FailoverRateLimit,
						Error:    waitErr,
					})
					return nil, waitErr
				}
			}
		}

		start := time.Now()
		resp, err := run(ctx, candidate.Provider, candidate.Model)
		elapsed := time.Since(start)

		if err == nil {
			result.Response = resp
			result.Provider = candidate.Provider
			result.Model = candidate.Model
			result.IdentityKey = candidate.StableKey()
			return result, nil
		}

		if ctx.Err() == context.Canceled {
			result.Attempts = append(result.Attempts, FallbackAttempt{
				Provider: candidate.Provider,
				Model:    candidate.Model,
				Error:    err,
				Duration: elapsed,
			})
			return nil, context.Canceled
		}

		// Image dimension/size errors are non-retriable.
		errMsg := strings.ToLower(err.Error())
		if IsImageDimensionError(errMsg) || IsImageSizeError(errMsg) {
			result.Attempts = append(result.Attempts, FallbackAttempt{
				Provider: candidate.Provider,
				Model:    candidate.Model,
				Error:    err,
				Reason:   FailoverFormat,
				Duration: elapsed,
			})
			return nil, &FailoverError{
				Reason:   FailoverFormat,
				Provider: candidate.Provider,
				Model:    candidate.Model,
				Wrapped:  err,
			}
		}

		// Any other error: record and try next.
		result.Attempts = append(result.Attempts, FallbackAttempt{
			Provider: candidate.Provider,
			Model:    candidate.Model,
			Error:    err,
			Duration: elapsed,
		})

		if i == len(candidates)-1 {
			return nil, &FallbackExhaustedError{Attempts: result.Attempts}
		}
	}

	return nil, &FallbackExhaustedError{Attempts: result.Attempts}
}

// FallbackExhaustedError indicates all fallback candidates were tried and failed.
type FallbackExhaustedError struct {
	Attempts []FallbackAttempt
}

func (e *FallbackExhaustedError) Error() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("fallback: all %d candidates failed:", len(e.Attempts)))
	for i, a := range e.Attempts {
		if a.Skipped {
			sb.WriteString(fmt.Sprintf("\n  [%d] %s/%s: skipped (cooldown)", i+1, a.Provider, a.Model))
		} else {
			sb.WriteString(fmt.Sprintf("\n  [%d] %s/%s: %v (reason=%s, %s)",
				i+1, a.Provider, a.Model, a.Error, a.Reason, a.Duration.Round(time.Millisecond)))
		}
	}
	return sb.String()
}

func (e *FallbackExhaustedError) lastReason() FailoverReason {
	for i := len(e.Attempts) - 1; i >= 0; i-- {
		if e.Attempts[i].Reason != "" {
			return e.Attempts[i].Reason
		}
	}
	return ""
}
