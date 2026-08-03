package outbox

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/bus"
)

// Coordinator owns admission to one instance-wide outbox. Agent workspaces are
// record metadata, never independent stores or delivery-ID lookup scopes.
type Coordinator struct {
	mu         sync.Mutex
	store      *Store
	dispatched map[string]bool
	now        func() time.Time
}

// Admission reports the canonical durable intent and whether this process owns
// its next publication to the delivery bus.
type Admission struct {
	Intent   Intent
	Dispatch bool
}

// OpenCoordinator opens the canonical outbox beneath the MintClaw instance root.
func OpenCoordinator(instanceRoot string) (*Coordinator, error) {
	store, err := Open(instanceRoot)
	if err != nil {
		return nil, err
	}
	return newCoordinator(store), nil
}

func newCoordinator(store *Store) *Coordinator {
	return &Coordinator{
		store:      store,
		dispatched: make(map[string]bool),
		now:        time.Now,
	}
}

// AdmitMessage persists a text intent before transferring publication ownership.
func (c *Coordinator) AdmitMessage(
	ownerWorkspace string,
	identity Identity,
	msg bus.OutboundMessage,
) (Admission, error) {
	if c == nil || c.store == nil {
		return Admission{}, errors.New("outbox coordinator is unavailable")
	}

	intent, err := NewMessageIntent(ownerWorkspace, identity, msg, c.now())
	if err != nil {
		return Admission{}, err
	}
	return c.admit(intent)
}

// AdmitMedia persists a media intent before transferring publication ownership.
func (c *Coordinator) AdmitMedia(
	ownerWorkspace string,
	identity Identity,
	msg bus.OutboundMediaMessage,
) (Admission, error) {
	if c == nil || c.store == nil {
		return Admission{}, errors.New("outbox coordinator is unavailable")
	}

	intent, err := NewMediaIntent(ownerWorkspace, identity, msg, c.now())
	if err != nil {
		return Admission{}, err
	}
	return c.admit(intent)
}

// ReleaseAdmission returns an unsent intent after in-memory publication failed.
func (c *Coordinator) ReleaseAdmission(deliveryID string) error {
	if c == nil || c.store == nil {
		return errors.New("outbox coordinator is unavailable")
	}
	deliveryID = strings.TrimSpace(deliveryID)
	if deliveryID == "" {
		return errors.New("delivery ID is required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	intent, err := c.store.Get(deliveryID)
	if err != nil {
		return err
	}
	if intent.Status != StatusPending && intent.Status != StatusDefinitelyFailed {
		return fmt.Errorf("outbox intent %q is %q, not dispatchable", deliveryID, intent.Status)
	}
	delete(c.dispatched, deliveryID)
	return nil
}

func (c *Coordinator) admit(candidate Intent) (Admission, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	intent, err := c.store.Create(candidate)
	if err != nil {
		return Admission{}, err
	}
	dispatchable := intent.Status == StatusPending || intent.Status == StatusDefinitelyFailed
	dispatch := dispatchable && !c.dispatched[intent.ID]
	if dispatch {
		c.dispatched[intent.ID] = true
	}
	return Admission{Intent: intent, Dispatch: dispatch}, nil
}
