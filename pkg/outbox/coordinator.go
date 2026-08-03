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
	mu        sync.Mutex
	store     *Store
	root      string
	leases    map[string]uint64
	published map[string]bool
	now       func() time.Time
	closed    bool
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
		store:     store,
		leases:    make(map[string]uint64),
		published: make(map[string]bool),
		now:       time.Now,
	}
}

// CommitAdmission records successful transfer to the in-memory delivery bus.
// It suppresses same-process replay while the durable channel outcome remains pending.
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
	delete(c.leases, lease.deliveryID)
	c.published[lease.deliveryID] = true
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
	return Admission{Intent: intent, Dispatch: dispatch, InFlight: dispatchable && leased, Lease: lease}, nil
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
