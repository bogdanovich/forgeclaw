package channels

import (
	"context"
	"errors"
	"math"
	"time"
)

// DeliveryStatus describes how much of a logical payload was confirmed.
type DeliveryStatus uint8

const (
	DeliveryStatusUnknown DeliveryStatus = iota
	DeliveryFailed
	DeliveryPartial
	DeliveryComplete
)

// DeliveryAcceptance describes whether the failed transport operation may
// have been accepted remotely.
type DeliveryAcceptance uint8

const (
	DeliveryAcceptanceUnknown DeliveryAcceptance = iota
	DeliveryRejected
	DeliveryAccepted
)

// DeliveryResult carries the transport-independent outcome of a delivery.
// A non-nil Remaining slice identifies payload that can be retried without
// replaying confirmed IDs. Acceptance records whether that retry may duplicate
// an unconfirmed operation.
type DeliveryResult[T any] struct {
	MessageIDs []string
	Status     DeliveryStatus
	Acceptance DeliveryAcceptance
	Remaining  []T
	RetryAfter time.Duration
	Attempts   int
	Err        error
}

func (r DeliveryResult[T]) Delivered() bool {
	return r.Status == DeliveryComplete && r.Err == nil
}

func (r DeliveryResult[T]) Ambiguous() bool {
	return r.Acceptance == DeliveryAcceptanceUnknown
}

func (r DeliveryResult[T]) MayHaveDelivered() bool {
	return r.Status == DeliveryPartial || r.Acceptance != DeliveryRejected
}

func (r DeliveryResult[T]) DefinitelyNotSent() bool {
	return r.Status == DeliveryFailed &&
		r.Acceptance == DeliveryRejected &&
		len(r.MessageIDs) == 0
}

// SuccessfulDelivery creates a confirmed complete delivery result.
func SuccessfulDelivery[T any](messageIDs []string) DeliveryResult[T] {
	return DeliveryResult[T]{
		MessageIDs: append([]string(nil), messageIDs...),
		Status:     DeliveryComplete,
		Acceptance: DeliveryAccepted,
	}
}

// RejectedDelivery creates a failure known to have occurred before remote
// acceptance.
func RejectedDelivery[T any](err error) DeliveryResult[T] {
	if err == nil {
		err = errors.New("delivery rejected")
	}
	return DeliveryResult[T]{
		Status:     DeliveryFailed,
		Acceptance: DeliveryRejected,
		Err:        err,
	}
}

// FailedDelivery maps a legacy channel result into the shared delivery
// contract. Remaining may be supplied only when the adapter knows the payload
// that has not completed.
func FailedDelivery[T any](
	messageIDs []string,
	remaining []T,
	retryAfter time.Duration,
	err error,
) DeliveryResult[T] {
	if err == nil {
		return SuccessfulDelivery[T](messageIDs)
	}

	status := DeliveryFailed
	if len(messageIDs) > 0 {
		status = DeliveryPartial
	}
	acceptance := DeliveryAcceptanceUnknown
	if deliveryFailureWasRejected(err) {
		acceptance = DeliveryRejected
	}

	return DeliveryResult[T]{
		MessageIDs: append([]string(nil), messageIDs...),
		Status:     status,
		Acceptance: acceptance,
		Remaining:  cloneDeliveryPayload(remaining),
		RetryAfter: retryAfter,
		Err:        err,
	}
}

func cloneDeliveryPayload[T any](payload []T) []T {
	if payload == nil {
		return nil
	}
	return append(make([]T, 0, len(payload)), payload...)
}

func deliveryFailureWasRejected(err error) bool {
	return errors.Is(err, ErrNotRunning) ||
		errors.Is(err, ErrSendFailed) ||
		errors.Is(err, ErrRateLimit)
}

// DeliveryRetryPolicy controls the shared transport retry coordinator.
type DeliveryRetryPolicy struct {
	MaxRetries     int
	RetryAmbiguous bool
	RateLimitDelay time.Duration
	BaseBackoff    time.Duration
	MaxBackoff     time.Duration
}

// DeliveryAttempt reports one completed transport call.
type DeliveryAttempt struct {
	Number   int
	Duration time.Duration
	Status   DeliveryStatus
	Err      error
}

// DeliverWithRetry retries a logical payload while preserving confirmed IDs
// and any adapter-provided remainder. Partial results without a known
// remainder stop immediately because replaying the original payload could
// duplicate confirmed delivery.
func DeliverWithRetry[T any](
	ctx context.Context,
	payload []T,
	policy DeliveryRetryPolicy,
	deliver func(context.Context, []T) DeliveryResult[T],
	observe func(DeliveryAttempt),
) DeliveryResult[T] {
	pending := cloneDeliveryPayload(payload)
	if len(pending) == 0 {
		return RejectedDelivery[T](errors.New("delivery payload is empty"))
	}
	var confirmedIDs []string
	var result DeliveryResult[T]
	acceptanceUnknown := false
	maxRetries := max(policy.MaxRetries, 0)

	for attempt := 0; attempt <= maxRetries; attempt++ {
		started := time.Now()
		result = deliver(ctx, pending)
		if !result.Delivered() && result.Err == nil {
			result.Err = errors.New("incomplete delivery result has no error")
		}
		acceptanceUnknown = acceptanceUnknown || result.Ambiguous()
		result.Attempts = attempt + 1
		confirmedIDs = append(confirmedIDs, result.MessageIDs...)
		if observe != nil {
			observe(DeliveryAttempt{
				Number:   attempt + 1,
				Duration: time.Since(started),
				Status:   result.Status,
				Err:      result.Err,
			})
		}

		if result.Delivered() {
			result.MessageIDs = confirmedIDs
			result.Remaining = nil
			return result
		}

		if result.Status == DeliveryPartial && result.Remaining == nil {
			break
		}
		if result.Remaining != nil {
			if len(result.Remaining) == 0 {
				break
			}
			pending = cloneDeliveryPayload(result.Remaining)
		}
		if !deliveryResultMayRetry(result, policy) || attempt == maxRetries {
			break
		}
		if err := waitForDeliveryRetry(ctx, result, policy, attempt); err != nil {
			result.Err = errors.Join(result.Err, err)
			break
		}
	}

	result.MessageIDs = confirmedIDs
	result.Attempts = max(result.Attempts, 1)
	if len(confirmedIDs) > 0 {
		result.Status = DeliveryPartial
	}
	if acceptanceUnknown {
		result.Acceptance = DeliveryAcceptanceUnknown
	}
	return result
}

func deliveryResultMayRetry[T any](result DeliveryResult[T], policy DeliveryRetryPolicy) bool {
	if result.Err == nil || errors.Is(result.Err, ErrNotRunning) || errors.Is(result.Err, ErrSendFailed) {
		return false
	}
	if result.Status == DeliveryPartial && result.Remaining == nil {
		return false
	}
	return !result.Ambiguous() || policy.RetryAmbiguous
}

func waitForDeliveryRetry[T any](
	ctx context.Context,
	result DeliveryResult[T],
	policy DeliveryRetryPolicy,
	attempt int,
) error {
	delay := result.RetryAfter
	if delay <= 0 && errors.Is(result.Err, ErrRateLimit) {
		delay = policy.RateLimitDelay
	}
	if delay <= 0 && policy.BaseBackoff > 0 {
		delay = time.Duration(float64(policy.BaseBackoff) * math.Pow(2, float64(attempt)))
		if policy.MaxBackoff > 0 {
			delay = min(delay, policy.MaxBackoff)
		}
	}
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
