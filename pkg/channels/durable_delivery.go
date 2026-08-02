package channels

import "time"

// DurableDeliveryOutcome is the transport-independent result persisted by a
// durable owner after one logical outbound delivery.
type DurableDeliveryOutcome struct {
	MessageIDs       []string
	RetryAfter       time.Duration
	Err              error
	MayHaveDelivered bool
}

// DurableDeliveryLifecycle persists the crash boundary around a channel send.
// BeginDelivery must complete before any remote operation can accept the
// payload. CompleteDelivery records the resulting acceptance classification.
type DurableDeliveryLifecycle interface {
	BeginDelivery(deliveryID string) error
	CompleteDelivery(deliveryID string, outcome DurableDeliveryOutcome) error
}
