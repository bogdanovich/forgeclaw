package channels

import (
	"errors"
	"fmt"
)

var (
	// ErrNotRunning indicates the channel is not running.
	// Manager will not retry.
	ErrNotRunning = errors.New("channel not running")

	// ErrRateLimit indicates the platform returned a rate-limit response (e.g. HTTP 429).
	// Manager will wait a fixed delay and retry.
	ErrRateLimit = errors.New("rate limited")

	// ErrTemporary indicates a transient failure (e.g. network timeout, 5xx).
	// Manager will use exponential backoff and retry.
	ErrTemporary = errors.New("temporary failure")

	// ErrSendFailed indicates a permanent failure (e.g. invalid chat ID, 4xx non-429).
	// Manager will not retry.
	ErrSendFailed = errors.New("send failed")
)

// DeliveryError reports whether a failed synchronous delivery may have reached
// the remote channel. Callers may safely retry only definite not-sent failures.
type DeliveryError struct {
	cause     error
	ambiguous bool
}

func (e *DeliveryError) Error() string {
	if e == nil || e.cause == nil {
		return "channel delivery failed"
	}
	return e.cause.Error()
}

func (e *DeliveryError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func newDeliveryError(cause error, ambiguous bool) error {
	if cause == nil {
		cause = fmt.Errorf("channel delivery failed")
	}
	return &DeliveryError{cause: cause, ambiguous: ambiguous}
}

// DefiniteNotSentDeliveryError marks an error produced before a remote channel
// may have accepted any part of the message.
func DefiniteNotSentDeliveryError(cause error) error {
	return newDeliveryError(cause, false)
}

// DeliveryDefinitelyNotSent returns true only when the channel manager knows
// that no channel send was accepted or may have been accepted.
func DeliveryDefinitelyNotSent(err error) bool {
	var deliveryErr *DeliveryError
	return errors.As(err, &deliveryErr) && !deliveryErr.ambiguous
}
