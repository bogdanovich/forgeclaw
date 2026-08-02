package channels

import (
	"fmt"
	"net/http"
	"time"
)

// ClassifySendError wraps a raw error with the appropriate sentinel based on
// an HTTP status code. Channels that perform HTTP API calls should use this
// in their Send path.
func ClassifySendError(statusCode int, rawErr error) error {
	switch {
	case statusCode == http.StatusTooManyRequests:
		return fmt.Errorf("%w: %w", ErrRateLimit, rawErr)
	case statusCode >= 500:
		return fmt.Errorf("%w: %w", ErrTemporary, rawErr)
	case statusCode >= 400:
		return fmt.Errorf("%w: %w", ErrSendFailed, rawErr)
	default:
		return rawErr
	}
}

// ClassifySendOutcome adds acceptance and retry metadata to an HTTP API error.
func ClassifySendOutcome(statusCode int, rawErr error, retryAfter time.Duration) error {
	classified := ClassifySendError(statusCode, rawErr)
	acceptance := DeliveryAcceptanceUnknown
	if statusCode == http.StatusTooManyRequests || (statusCode >= 400 && statusCode < 500) {
		acceptance = DeliveryRejected
	}
	return NewTransportOutcomeError(classified, acceptance, retryAfter)
}

// ClassifyNetError wraps a network/timeout error as ErrTemporary.
func ClassifyNetError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrTemporary, err)
}
