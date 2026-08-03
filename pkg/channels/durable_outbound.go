package channels

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/outbox"
)

// OutboundDeliveryStatus is the terminal durable channel outcome.
type OutboundDeliveryStatus uint8

const (
	OutboundDeliveryDelivered OutboundDeliveryStatus = iota + 1
	OutboundDeliveryDefinitelyFailed
	OutboundDeliveryAmbiguous
)

// OutboundDeliveryOutcome captures transport-independent terminal metadata.
type OutboundDeliveryOutcome struct {
	Status             OutboundDeliveryStatus
	PlatformMessageIDs []string
	RetryAfter         time.Time
	Err                error
}

func (m *Manager) persistDurableRejection(deliveryID string, cause error) error {
	deliveryID = strings.TrimSpace(deliveryID)
	if deliveryID == "" {
		return cause
	}
	if m == nil || m.outboundOutbox == nil {
		return errors.Join(cause, fmt.Errorf("durable delivery %q is unavailable", deliveryID))
	}
	outcome := outbox.Outcome{}
	if cause != nil {
		outcome.Error = cause.Error()
	}
	if persistErr := m.outboundOutbox.MarkDispatchRejected(deliveryID, outcome); persistErr != nil {
		return errors.Join(cause, persistErr)
	}
	return cause
}

func (m *Manager) beginDurableOutbound(deliveryID string) (bool, error) {
	deliveryID = strings.TrimSpace(deliveryID)
	if deliveryID == "" {
		return false, nil
	}
	if m == nil || m.outboundOutbox == nil {
		return true, fmt.Errorf("durable delivery %q is unavailable", deliveryID)
	}
	if err := m.outboundOutbox.BeginAttempt(deliveryID); err != nil {
		return true, fmt.Errorf("begin durable delivery %q: %w", deliveryID, err)
	}
	return true, nil
}

func (m *Manager) persistDurableOutbound(deliveryID string, outcome OutboundDeliveryOutcome) error {
	deliveryID = strings.TrimSpace(deliveryID)
	if deliveryID == "" {
		return nil
	}
	if m == nil || m.outboundOutbox == nil {
		return fmt.Errorf("persist durable delivery %q: coordinator is unavailable", deliveryID)
	}
	persisted := outbox.Outcome{
		PlatformMessageIDs: append([]string(nil), outcome.PlatformMessageIDs...),
		RetryAfter:         outcome.RetryAfter,
	}
	if outcome.Err != nil {
		persisted.Error = outcome.Err.Error()
	}
	var err error
	switch outcome.Status {
	case OutboundDeliveryDelivered:
		err = m.outboundOutbox.MarkDelivered(deliveryID, persisted)
	case OutboundDeliveryDefinitelyFailed:
		err = m.outboundOutbox.MarkDefinitelyFailed(deliveryID, persisted)
	case OutboundDeliveryAmbiguous:
		err = m.outboundOutbox.MarkAmbiguous(deliveryID, persisted)
	default:
		return fmt.Errorf("persist durable delivery %q: unsupported outcome %d", deliveryID, outcome.Status)
	}
	if err != nil {
		return fmt.Errorf("persist durable delivery %q: %w", deliveryID, err)
	}
	return nil
}

func durableOutcome[T any](result DeliveryResult[T], priorMessageIDs []string) OutboundDeliveryOutcome {
	messageIDs := append([]string(nil), priorMessageIDs...)
	messageIDs = append(messageIDs, result.MessageIDs...)
	outcome := OutboundDeliveryOutcome{
		PlatformMessageIDs: messageIDs,
		Err:                result.Err,
	}
	if result.RetryAfter > 0 {
		outcome.RetryAfter = time.Now().UTC().Add(result.RetryAfter)
	}
	switch {
	case result.Delivered():
		outcome.Status = OutboundDeliveryDelivered
	case len(priorMessageIDs) == 0 && result.DefinitelyNotSent():
		outcome.Status = OutboundDeliveryDefinitelyFailed
	default:
		outcome.Status = OutboundDeliveryAmbiguous
	}
	return outcome
}
