package nodes

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

const (
	DefaultChallengeTTL       = 30 * time.Second
	DefaultMaxChallenges      = 1024
	DefaultMaxPendingPairings = 128
)

var (
	ErrChallengeExpired = errors.New("node admission challenge expired")
	ErrChallengeUnknown = errors.New("node admission challenge is unknown or already used")
	ErrAdmissionBusy    = errors.New("node admission capacity reached")
	ErrAdmissionRevoked = errors.New("node identity is revoked")
)

type Challenge struct {
	Nonce       string `json:"nonce"`
	MinProtocol int    `json:"min_protocol"`
	MaxProtocol int    `json:"max_protocol"`
	ExpiresAt   int64  `json:"expires_at"`
}

type PendingPairing struct {
	Node          Snapshot `json:"node"`
	PublicKey     []byte   `json:"public_key"`
	RequestedRole string   `json:"requested_role"`
	RequestedAt   int64    `json:"requested_at"`
}

type PairingRegistry interface {
	Registry
	UpsertPending(PendingPairing) error
	Pending(ID) (PendingPairing, bool, error)
	Registration(ID) (Registration, bool, error)
}

type AdmissionResult struct {
	NodeID ID    `json:"node_id"`
	State  State `json:"state"`
}

// CommandApproval binds one command descriptor to the exact capability
// catalog approved for the connected node.
type CommandApproval struct {
	Descriptor  CommandDescriptor
	CatalogHash string
}

// Admission is a verified identity decision. Connected decisions become
// durable only through Connect, after the transport has claimed ownership.
type Admission struct {
	Result   AdmissionResult
	snapshot Snapshot
}

type AdmissionConfig struct {
	ChallengeTTL  time.Duration
	MaxChallenges int
	Random        io.Reader
	Now           func() time.Time
}

type Authenticator struct {
	registry      PairingRegistry
	ttl           time.Duration
	maxChallenges int
	random        io.Reader
	now           func() time.Time

	mu         sync.Mutex
	challenges map[string]time.Time
}

func NewAuthenticator(registry PairingRegistry, cfg AdmissionConfig) (*Authenticator, error) {
	if registry == nil {
		return nil, errors.New("node admission registry is required")
	}
	if cfg.ChallengeTTL <= 0 {
		cfg.ChallengeTTL = DefaultChallengeTTL
	}
	if cfg.MaxChallenges <= 0 {
		cfg.MaxChallenges = DefaultMaxChallenges
	}
	if cfg.Random == nil {
		cfg.Random = rand.Reader
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Authenticator{
		registry:      registry,
		ttl:           cfg.ChallengeTTL,
		maxChallenges: cfg.MaxChallenges,
		random:        cfg.Random,
		now:           cfg.Now,
		challenges:    make(map[string]time.Time),
	}, nil
}

func (auth *Authenticator) IssueChallenge() (Challenge, error) {
	now := auth.now()
	auth.mu.Lock()
	defer auth.mu.Unlock()
	auth.pruneExpiredLocked(now)
	if len(auth.challenges) >= auth.maxChallenges {
		return Challenge{}, ErrAdmissionBusy
	}
	nonceBytes := make([]byte, 32)
	if _, err := io.ReadFull(auth.random, nonceBytes); err != nil {
		return Challenge{}, fmt.Errorf("generate node admission challenge: %w", err)
	}
	nonce := base64.RawURLEncoding.EncodeToString(nonceBytes)
	if _, exists := auth.challenges[nonce]; exists {
		return Challenge{}, errors.New("generated duplicate node admission challenge")
	}
	expiresAt := now.Add(auth.ttl)
	auth.challenges[nonce] = expiresAt
	return Challenge{
		Nonce:       nonce,
		MinProtocol: ProtocolV1,
		MaxProtocol: ProtocolV1,
		ExpiresAt:   expiresAt.Unix(),
	}, nil
}

func (auth *Authenticator) Authenticate(proof IdentityProof) (Admission, error) {
	if err := auth.consumeChallenge(proof.Nonce); err != nil {
		return Admission{}, err
	}
	publicKey, err := proof.Verify()
	if err != nil {
		return Admission{}, err
	}
	now := auth.now().Unix()
	node := Snapshot{
		ID:              proof.NodeID,
		State:           StatePendingPairing,
		ProtocolVersion: ProtocolV1,
		Platform:        proof.Platform,
		Architecture:    proof.Architecture,
		SoftwareVersion: proof.ClientVersion,
		CatalogHash:     proof.CatalogHash,
		Catalog:         proof.Catalog,
		LastSeenAt:      now,
	}
	registration, exists, err := auth.registry.Registration(node.ID)
	if err != nil {
		return Admission{}, err
	}
	if !exists {
		if err := auth.persistPending(node, publicKey, proof, now); err != nil {
			return Admission{}, err
		}
		return Admission{Result: AdmissionResult{NodeID: node.ID, State: StatePendingPairing}}, nil
	}
	if !bytes.Equal(registration.PublicKey, publicKey) {
		return Admission{}, fmt.Errorf("%w: node public key changed", ErrInvalidNode)
	}
	if registration.Snapshot.State == StateRevoked {
		return Admission{}, ErrAdmissionRevoked
	}
	if registration.Snapshot.State == StatePendingPairing {
		if err := auth.persistPending(node, publicKey, proof, now); err != nil {
			return Admission{}, err
		}
		return Admission{Result: AdmissionResult{NodeID: node.ID, State: StatePendingPairing}}, nil
	}
	if registration.ApprovedAt <= 0 {
		return Admission{}, fmt.Errorf("%w: node identity is not approved", ErrInvalidNode)
	}
	node.State = StateConnected
	return Admission{
		Result:   AdmissionResult{NodeID: node.ID, State: StateConnected},
		snapshot: node,
	}, nil
}

func (auth *Authenticator) Connect(admission Admission) error {
	if admission.Result.State != StateConnected ||
		admission.snapshot.State != StateConnected ||
		admission.Result.NodeID != admission.snapshot.ID {
		return fmt.Errorf("%w: admission is not connectable", ErrInvalidNode)
	}
	return auth.registry.Upsert(admission.snapshot)
}

func (auth *Authenticator) Heartbeat(id ID) error {
	registration, exists, err := auth.registry.Registration(id)
	if err != nil {
		return err
	}
	if !exists || registration.Snapshot.State != StateConnected {
		return fmt.Errorf("%w: node %q has no active registration", ErrInvalidNode, id)
	}
	snapshot := registration.Snapshot
	snapshot.LastSeenAt = auth.now().Unix()
	return auth.registry.Upsert(snapshot)
}

func (auth *Authenticator) Disconnect(id ID, reason string) error {
	registration, exists, err := auth.registry.Registration(id)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%w: unknown node %q", ErrInvalidNode, id)
	}
	if registration.Snapshot.State == StateDisconnected || registration.Snapshot.State == StateRevoked {
		return nil
	}
	err = auth.registry.MarkDisconnected(id, Disconnect{
		Reason: reason,
		At:     auth.now().Unix(),
	})
	if err == nil {
		return nil
	}
	registration, exists, reloadErr := auth.registry.Registration(id)
	if reloadErr == nil && exists &&
		(registration.Snapshot.State == StateDisconnected || registration.Snapshot.State == StateRevoked) {
		return nil
	}
	return err
}

// ApprovedCommand resolves the durable operator-approved command surface for
// a currently connected node.
func (auth *Authenticator) ApprovedCommand(id ID, command string) (CommandApproval, error) {
	registration, exists, err := auth.registry.Registration(id)
	if err != nil {
		return CommandApproval{}, err
	}
	if !exists {
		return CommandApproval{}, fmt.Errorf("%w: unknown node %q", ErrInvalidNode, id)
	}
	descriptor, err := registration.ApprovedCommand(command)
	if err != nil {
		return CommandApproval{}, err
	}
	return CommandApproval{
		Descriptor:  descriptor,
		CatalogHash: registration.ApprovedCatalogHash,
	}, nil
}

func (auth *Authenticator) persistPending(node Snapshot, publicKey []byte, proof IdentityProof, now int64) error {
	return auth.registry.UpsertPending(PendingPairing{
		Node:          node,
		PublicKey:     publicKey,
		RequestedRole: proof.RequestedRole,
		RequestedAt:   now,
	})
}

// DiscardChallenge releases an issued challenge when its connection ends
// before a complete proof can be admitted.
func (auth *Authenticator) DiscardChallenge(nonce string) {
	auth.mu.Lock()
	defer auth.mu.Unlock()
	delete(auth.challenges, nonce)
}

func (auth *Authenticator) consumeChallenge(nonce string) error {
	now := auth.now()
	auth.mu.Lock()
	defer auth.mu.Unlock()
	expiresAt, exists := auth.challenges[nonce]
	if !exists {
		return ErrChallengeUnknown
	}
	delete(auth.challenges, nonce)
	if !now.Before(expiresAt) {
		return ErrChallengeExpired
	}
	return nil
}

func (auth *Authenticator) pruneExpiredLocked(now time.Time) {
	for nonce, expiresAt := range auth.challenges {
		if !now.Before(expiresAt) {
			delete(auth.challenges, nonce)
		}
	}
}
