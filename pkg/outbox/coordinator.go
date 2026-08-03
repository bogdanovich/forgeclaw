package outbox

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/bus"
)

// Coordinator owns admission to one instance-wide outbox. Agent workspaces are
// record metadata, never independent stores or delivery-ID lookup scopes.
type Coordinator struct {
	mu         sync.Mutex
	store      *Store
	root       string
	leases     map[string]uint64
	publishing map[string]uint64
	published  map[string]bool
	attempting map[string]bool
	now        func() time.Time
	closed     bool
}

var coordinatorRoots = struct {
	sync.Mutex
	active map[string]bool
}{active: make(map[string]bool)}

var dispatchLeaseSequence atomic.Uint64

// DispatchLease identifies one in-process owner of publication. Its fields are
// private so callers can only return the exact lease issued with an admission.
type DispatchLease struct {
	deliveryID string
	generation uint64
}

// Admission reports the canonical durable intent and whether this process owns
// its next publication to the delivery bus.
type Admission struct {
	Intent   Intent
	Dispatch bool
	InFlight bool
	Lease    DispatchLease
}

// OpenCoordinator opens the canonical outbox beneath the MintClaw instance root.
func OpenCoordinator(instanceRoot string) (*Coordinator, error) {
	root, err := canonicalInstanceRoot(instanceRoot)
	if err != nil {
		return nil, err
	}
	coordinatorRoots.Lock()
	if coordinatorRoots.active[root] {
		coordinatorRoots.Unlock()
		return nil, fmt.Errorf("outbox coordinator for %q is already open", root)
	}
	coordinatorRoots.active[root] = true
	coordinatorRoots.Unlock()

	store, err := Open(root)
	if err != nil {
		coordinatorRoots.Lock()
		delete(coordinatorRoots.active, root)
		coordinatorRoots.Unlock()
		return nil, err
	}
	coordinator := newCoordinator(store)
	coordinator.root = root
	return coordinator, nil
}

func newCoordinator(store *Store) *Coordinator {
	return &Coordinator{
		store:      store,
		leases:     make(map[string]uint64),
		publishing: make(map[string]uint64),
		published:  make(map[string]bool),
		attempting: make(map[string]bool),
		now:        time.Now,
	}
}

// PrepareAdmission authorizes bus visibility while retaining the exact lease
// needed to roll back a failed publication.
func (c *Coordinator) PrepareAdmission(lease DispatchLease) error {
	if c == nil || c.store == nil {
		return errors.New("outbox coordinator is unavailable")
	}
	if lease.deliveryID == "" || lease.generation == 0 {
		return errors.New("dispatch lease is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.validateOpenLocked(); err != nil {
		return err
	}
	if c.leases[lease.deliveryID] != lease.generation {
		return fmt.Errorf("dispatch lease for %q is stale", lease.deliveryID)
	}
	if generation := c.publishing[lease.deliveryID]; generation != 0 && generation != lease.generation {
		return fmt.Errorf("publication lease for %q is stale", lease.deliveryID)
	}
	c.publishing[lease.deliveryID] = lease.generation
	c.published[lease.deliveryID] = true
	return nil
}

// CommitAdmission records successful transfer to the in-memory delivery bus.
func (c *Coordinator) CommitAdmission(lease DispatchLease) error {
	if c == nil || c.store == nil {
		return errors.New("outbox coordinator is unavailable")
	}
	if lease.deliveryID == "" || lease.generation == 0 {
		return errors.New("dispatch lease is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.validateOpenLocked(); err != nil {
		return err
	}
	if c.leases[lease.deliveryID] != lease.generation {
		return fmt.Errorf("dispatch lease for %q is stale", lease.deliveryID)
	}
	if c.publishing[lease.deliveryID] != lease.generation {
		return fmt.Errorf("publication lease for %q was not prepared", lease.deliveryID)
	}
	delete(c.publishing, lease.deliveryID)
	delete(c.leases, lease.deliveryID)
	return nil
}

// BeginAttempt persists the transport-call crash boundary for a published intent.
func (c *Coordinator) BeginAttempt(deliveryID string) error {
	if c == nil || c.store == nil {
		return errors.New("outbox coordinator is unavailable")
	}
	if err := validateID(deliveryID); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.validateOpenLocked(); err != nil {
		return err
	}
	if !c.published[deliveryID] {
		return fmt.Errorf("outbox intent %q is not published", deliveryID)
	}
	if c.attempting[deliveryID] {
		return fmt.Errorf("outbox intent %q already has an active delivery attempt", deliveryID)
	}
	if _, err := c.store.BeginAttempt(deliveryID); err != nil {
		return err
	}
	c.attempting[deliveryID] = true
	return nil
}

// MarkDispatchRejected records a published intent that could not reach an adapter.
func (c *Coordinator) MarkDispatchRejected(deliveryID string, outcome Outcome) error {
	return c.transitionPublished(deliveryID, false, func() error {
		_, err := c.store.MarkDispatchRejected(deliveryID, outcome)
		return err
	})
}

// MarkDelivered records confirmed remote acceptance and releases in-process ownership.
func (c *Coordinator) MarkDelivered(deliveryID string, outcome Outcome) error {
	return c.transitionPublished(deliveryID, true, func() error {
		_, err := c.store.MarkDelivered(deliveryID, outcome)
		return err
	})
}

// MarkDefinitelyFailed records a failure known to precede remote acceptance.
func (c *Coordinator) MarkDefinitelyFailed(deliveryID string, outcome Outcome) error {
	return c.transitionPublished(deliveryID, true, func() error {
		_, err := c.store.MarkDefinitelyFailed(deliveryID, outcome)
		return err
	})
}

// MarkAmbiguous records a transport outcome that must not be blindly retried.
func (c *Coordinator) MarkAmbiguous(deliveryID string, outcome Outcome) error {
	return c.transitionPublished(deliveryID, true, func() error {
		_, err := c.store.MarkAmbiguous(deliveryID, outcome)
		return err
	})
}

// Get returns the canonical durable intent for a delivery ID.
func (c *Coordinator) Get(deliveryID string) (Intent, error) {
	if c == nil || c.store == nil {
		return Intent{}, errors.New("outbox coordinator is unavailable")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.validateOpenLocked(); err != nil {
		return Intent{}, err
	}
	return c.store.Get(deliveryID)
}

func (c *Coordinator) transitionPublished(
	deliveryID string,
	requireAttempt bool,
	transition func() error,
) error {
	if c == nil || c.store == nil {
		return errors.New("outbox coordinator is unavailable")
	}
	if err := validateID(deliveryID); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.validateOpenLocked(); err != nil {
		return err
	}
	if !c.published[deliveryID] {
		return fmt.Errorf("outbox intent %q is not published", deliveryID)
	}
	if requireAttempt && !c.attempting[deliveryID] {
		return fmt.Errorf("outbox intent %q has no active delivery attempt", deliveryID)
	}
	if !requireAttempt && c.attempting[deliveryID] {
		return fmt.Errorf("outbox intent %q already has an active delivery attempt", deliveryID)
	}
	if err := transition(); err != nil {
		return err
	}
	delete(c.attempting, deliveryID)
	delete(c.published, deliveryID)
	return nil
}

// Close releases the process-wide ownership fence for this instance root.
func (c *Coordinator) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.root != "" {
		coordinatorRoots.Lock()
		delete(coordinatorRoots.active, c.root)
		coordinatorRoots.Unlock()
	}
	return nil
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
func (c *Coordinator) ReleaseAdmission(lease DispatchLease) error {
	if c == nil || c.store == nil {
		return errors.New("outbox coordinator is unavailable")
	}
	if lease.deliveryID == "" || lease.generation == 0 {
		return errors.New("dispatch lease is required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.validateOpenLocked(); err != nil {
		return err
	}
	if c.leases[lease.deliveryID] != lease.generation {
		return fmt.Errorf("dispatch lease for %q is stale", lease.deliveryID)
	}
	// The exact lease is sufficient proof that this caller owns the unsent
	// publication. Relinquish it even if the diagnostic record read fails, so a
	// later admission can retry instead of mistaking a stale lease for an owner.
	delete(c.leases, lease.deliveryID)
	delete(c.publishing, lease.deliveryID)
	delete(c.published, lease.deliveryID)
	intent, err := c.store.Get(lease.deliveryID)
	if err != nil {
		return err
	}
	if intent.Status != StatusPending && intent.Status != StatusDefinitelyFailed {
		return fmt.Errorf("outbox intent %q is %q, not dispatchable", lease.deliveryID, intent.Status)
	}
	return nil
}

func (c *Coordinator) admit(candidate Intent) (Admission, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.validateOpenLocked(); err != nil {
		return Admission{}, err
	}

	intent, err := c.store.Create(candidate)
	if err != nil {
		return Admission{}, err
	}
	dispatchable := intent.Status == StatusPending || intent.Status == StatusDefinitelyFailed
	_, leased := c.leases[intent.ID]
	published := c.published[intent.ID]
	dispatch := dispatchable && !leased && !published
	var lease DispatchLease
	if dispatch {
		generation := dispatchLeaseSequence.Add(1)
		c.leases[intent.ID] = generation
		lease = DispatchLease{deliveryID: intent.ID, generation: generation}
	}
	inFlight := dispatchable && leased
	return Admission{Intent: intent, Dispatch: dispatch, InFlight: inFlight, Lease: lease}, nil
}

func (c *Coordinator) validateOpenLocked() error {
	if c.closed {
		return errors.New("outbox coordinator is closed")
	}
	return nil
}

func canonicalInstanceRoot(instanceRoot string) (string, error) {
	instanceRoot = strings.TrimSpace(instanceRoot)
	if instanceRoot == "" {
		return "", errors.New("outbox instance root is required")
	}
	root, err := filepath.Abs(instanceRoot)
	if err != nil {
		return "", fmt.Errorf("resolve outbox instance root: %w", err)
	}
	if resolved, resolveErr := filepath.EvalSymlinks(root); resolveErr == nil {
		root = resolved
	}
	return filepath.Clean(root), nil
}
