package nodes

import (
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
}

type AdmissionResult struct {
	Node  Snapshot `json:"node"`
	State State    `json:"state"`
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

func (auth *Authenticator) Admit(proof IdentityProof) (AdmissionResult, error) {
	if err := auth.consumeChallenge(proof.Nonce); err != nil {
		return AdmissionResult{}, err
	}
	publicKey, err := proof.Verify()
	if err != nil {
		return AdmissionResult{}, err
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
	if err := auth.registry.UpsertPending(PendingPairing{
		Node:          node,
		PublicKey:     publicKey,
		RequestedRole: proof.RequestedRole,
		RequestedAt:   now,
	}); err != nil {
		return AdmissionResult{}, err
	}
	return AdmissionResult{Node: node, State: StatePendingPairing}, nil
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
