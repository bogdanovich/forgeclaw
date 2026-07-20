package nodes

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestAuthenticatorPersistsPendingPairingAndRejectsReplay(t *testing.T) {
	registry, err := NewFileRegistry(filepath.Join(t.TempDir(), "registry.json"), 4)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1000, 0)
	authenticator, err := NewAuthenticator(registry, AdmissionConfig{
		Random: bytes.NewReader(bytes.Repeat([]byte{1}, 64)),
		Now:    func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	challenge, err := authenticator.IssueChallenge()
	if err != nil {
		t.Fatal(err)
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := NewIdentityProof(
		privateKey, challenge.Nonce, ProtocolV1, ProtocolV1,
		"v0.1.0", "linux", "amd64", CapabilityCatalog{},
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := authenticator.Admit(proof)
	if err != nil {
		t.Fatal(err)
	}
	if result.State != StatePendingPairing {
		t.Fatalf("state = %q", result.State)
	}
	pending, exists, err := registry.Pending(proof.NodeID)
	if err != nil || !exists {
		t.Fatalf("Pending() = exists %v, error %v", exists, err)
	}
	if !bytes.Equal(pending.PublicKey, privateKey.Public().(ed25519.PublicKey)) {
		t.Fatal("pending public key does not match signer")
	}
	if _, err := authenticator.Admit(proof); !errors.Is(err, ErrChallengeUnknown) {
		t.Fatalf("replayed Admit() error = %v", err)
	}
}

func TestAuthenticatorConsumesInvalidProofChallenge(t *testing.T) {
	registry, err := NewFileRegistry(filepath.Join(t.TempDir(), "registry.json"), 4)
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := NewAuthenticator(registry, AdmissionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	challenge, err := authenticator.IssueChallenge()
	if err != nil {
		t.Fatal(err)
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := NewIdentityProof(
		privateKey, challenge.Nonce, ProtocolV1, ProtocolV1,
		"v0.1.0", "linux", "amd64", CapabilityCatalog{},
	)
	if err != nil {
		t.Fatal(err)
	}
	proof.Signature = "invalid"
	if _, err := authenticator.Admit(proof); !errors.Is(err, ErrInvalidIdentityProof) {
		t.Fatalf("Admit() error = %v", err)
	}
	if _, err := authenticator.Admit(proof); !errors.Is(err, ErrChallengeUnknown) {
		t.Fatalf("second Admit() error = %v", err)
	}
}

func TestAuthenticatorExpiresAndBoundsChallenges(t *testing.T) {
	registry, err := NewFileRegistry(filepath.Join(t.TempDir(), "registry.json"), 4)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1000, 0)
	authenticator, err := NewAuthenticator(registry, AdmissionConfig{
		ChallengeTTL:  time.Second,
		MaxChallenges: 1,
		Random:        bytes.NewReader(bytes.Repeat([]byte{2}, 96)),
		Now:           func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	challenge, err := authenticator.IssueChallenge()
	if err != nil {
		t.Fatal(err)
	}
	if _, issueErr := authenticator.IssueChallenge(); !errors.Is(issueErr, ErrAdmissionBusy) {
		t.Fatalf("second IssueChallenge() error = %v", issueErr)
	}
	now = now.Add(time.Second)
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := NewIdentityProof(
		privateKey, challenge.Nonce, ProtocolV1, ProtocolV1,
		"v0.1.0", "linux", "amd64", CapabilityCatalog{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authenticator.Admit(proof); !errors.Is(err, ErrChallengeExpired) {
		t.Fatalf("expired Admit() error = %v", err)
	}
	if _, err := authenticator.IssueChallenge(); err != nil {
		t.Fatalf("IssueChallenge() after expiry error = %v", err)
	}
}

func TestAuthenticatorDiscardChallengeReleasesCapacity(t *testing.T) {
	registry, err := NewFileRegistry(filepath.Join(t.TempDir(), "registry.json"), 4)
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := NewAuthenticator(registry, AdmissionConfig{MaxChallenges: 1})
	if err != nil {
		t.Fatal(err)
	}
	challenge, err := authenticator.IssueChallenge()
	if err != nil {
		t.Fatal(err)
	}
	authenticator.DiscardChallenge(challenge.Nonce)
	if _, err := authenticator.IssueChallenge(); err != nil {
		t.Fatalf("IssueChallenge() after discard error = %v", err)
	}
}
