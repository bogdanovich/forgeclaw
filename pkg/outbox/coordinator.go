package outbox

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/channels"
)

// Coordinator admits outbound intents and persists channel delivery outcomes
// across multiple agent workspaces.
type Coordinator struct {
	mu         sync.Mutex
	stores     map[string]*Store
	owners     map[string]*Store
	dispatched map[string]bool
	now        func() time.Time
	open       func(string) (*Store, error)
}

// Admission reports whether the caller owns publication of a durable intent.
// A duplicate call can still be durably accepted without being dispatched a
// second time.
type Admission struct {
	Intent   Intent
	Dispatch bool
}

func NewCoordinator() *Coordinator {
	return &Coordinator{
		stores:     make(map[string]*Store),
		owners:     make(map[string]*Store),
		dispatched: make(map[string]bool),
		now:        time.Now,
		open:       Open,
	}
}

// AdmitMessage durably creates one text-message intent and returns its
// normalized payload with the stable delivery ID attached.
func (c *Coordinator) AdmitMessage(
	workspace string,
	identity Identity,
	msg bus.OutboundMessage,
) (Admission, error) {
	if c == nil {
		return Admission{}, errors.New("outbox coordinator is unavailable")
	}
	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		return Admission{}, errors.New("outbox workspace is required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	store, err := c.storeLocked(workspace)
	if err != nil {
		return Admission{}, err
	}
	intent, err := NewMessageIntent(identity, msg, c.now())
	if err != nil {
		return Admission{}, err
	}
	intent, err = store.Create(intent)
	if err != nil {
		return Admission{}, err
	}
	c.owners[intent.ID] = store
	dispatchable := intent.Status == StatusPending || intent.Status == StatusDefinitelyFailed
	dispatch := dispatchable && !c.dispatched[intent.ID]
	if dispatch {
		c.dispatched[intent.ID] = true
	}
	return Admission{Intent: intent, Dispatch: dispatch}, nil
}

// ReleaseAdmission returns an unsent pending intent to the coordinator after
// publication to the in-memory delivery bus was rejected.
func (c *Coordinator) ReleaseAdmission(deliveryID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	store, err := c.ownerLocked(deliveryID)
	if err != nil {
		return err
	}
	intent, err := store.Get(deliveryID)
	if err != nil {
		return err
	}
	if intent.Status != StatusPending {
		return fmt.Errorf("outbox intent %q is %q, not pending", deliveryID, intent.Status)
	}
	delete(c.dispatched, deliveryID)
	return nil
}

func (c *Coordinator) BeginDelivery(deliveryID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	store, err := c.ownerLocked(deliveryID)
	if err != nil {
		return err
	}
	_, err = store.BeginAttempt(deliveryID)
	return err
}

func (c *Coordinator) CompleteDelivery(
	deliveryID string,
	result channels.DurableDeliveryOutcome,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	store, err := c.ownerLocked(deliveryID)
	if err != nil {
		return err
	}
	outcome := Outcome{
		PlatformMessageIDs: append([]string(nil), result.MessageIDs...),
		Error:              errorString(result.Err),
	}
	if result.RetryAfter > 0 {
		outcome.RetryAfter = c.now().UTC().Add(result.RetryAfter)
	}
	switch {
	case result.Err == nil:
		_, err = store.MarkDelivered(deliveryID, outcome)
	case result.MayHaveDelivered:
		_, err = store.MarkAmbiguous(deliveryID, outcome)
	default:
		_, err = store.MarkDefinitelyFailed(deliveryID, outcome)
		if err == nil {
			delete(c.dispatched, deliveryID)
		}
	}
	return err
}

func (c *Coordinator) storeLocked(workspace string) (*Store, error) {
	if store := c.stores[workspace]; store != nil {
		return store, nil
	}
	store, err := c.open(workspace)
	if err != nil {
		return nil, fmt.Errorf("open outbox for %q: %w", workspace, err)
	}
	c.stores[workspace] = store
	return store, nil
}

func (c *Coordinator) ownerLocked(deliveryID string) (*Store, error) {
	deliveryID = strings.TrimSpace(deliveryID)
	if deliveryID == "" {
		return nil, errors.New("delivery ID is required")
	}
	store := c.owners[deliveryID]
	if store == nil {
		return nil, fmt.Errorf("outbox intent %q has no registered owner", deliveryID)
	}
	return store, nil
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
