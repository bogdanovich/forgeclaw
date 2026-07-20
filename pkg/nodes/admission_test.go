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
	admission, err := authenticator.Authenticate(proof)
	if err != nil {
		t.Fatal(err)
	}
	result := admission.Result
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
	if _, err := authenticator.Authenticate(proof); !errors.Is(err, ErrChallengeUnknown) {
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
	if _, err := authenticator.Authenticate(proof); !errors.Is(err, ErrInvalidIdentityProof) {
		t.Fatalf("Admit() error = %v", err)
	}
	if _, err := authenticator.Authenticate(proof); !errors.Is(err, ErrChallengeUnknown) {
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
	if _, err := authenticator.Authenticate(proof); !errors.Is(err, ErrChallengeExpired) {
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

func TestAuthenticatorReconnectsApprovedIdentityAndTracksLiveness(t *testing.T) {
	registry, err := NewFileRegistry(filepath.Join(t.TempDir(), "registry.json"), 4)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1000, 0)
	authenticator, err := NewAuthenticator(registry, AdmissionConfig{
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	first := admitTestIdentity(t, authenticator, privateKey)
	if first.State != StatePendingPairing {
		t.Fatalf("first admission state = %q", first.State)
	}
	if _, approveErr := registry.Approve(first.NodeID, PairingApproval{At: now.Unix()}); approveErr != nil {
		t.Fatal(approveErr)
	}
	now = now.Add(time.Second)
	second := admitTestIdentity(t, authenticator, privateKey)
	if second.State != StateConnected {
		t.Fatalf("approved admission state = %q", second.State)
	}

	now = now.Add(time.Second)
	if heartbeatErr := authenticator.Heartbeat(second.NodeID); heartbeatErr != nil {
		t.Fatal(heartbeatErr)
	}
	registration, exists, err := registry.Registration(second.NodeID)
	if err != nil || !exists {
		t.Fatalf("Registration() = exists %v, error %v", exists, err)
	}
	if registration.Snapshot.LastSeenAt != now.Unix() {
		t.Fatalf("last seen = %d, want %d", registration.Snapshot.LastSeenAt, now.Unix())
	}

	now = now.Add(time.Second)
	if disconnectErr := authenticator.Disconnect(second.NodeID, "test disconnect"); disconnectErr != nil {
		t.Fatal(disconnectErr)
	}
	registration, _, err = registry.Registration(second.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if registration.Snapshot.State != StateDisconnected ||
		registration.Snapshot.DisconnectReason != "test disconnect" {
		t.Fatalf("disconnected registration = %#v", registration)
	}
}

func TestAuthenticatorRejectsRevokedIdentityReconnect(t *testing.T) {
	registry, err := NewFileRegistry(filepath.Join(t.TempDir(), "registry.json"), 4)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1000, 0)
	authenticator, err := NewAuthenticator(registry, AdmissionConfig{
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	first := admitTestIdentity(t, authenticator, privateKey)
	if _, err := registry.Approve(first.NodeID, PairingApproval{At: now.Unix()}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Revoke(first.NodeID, Revocation{
		Reason: "test revocation",
		At:     now.Add(time.Second).Unix(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := authenticator.Heartbeat(first.NodeID); !errors.Is(err, ErrInvalidNode) {
		t.Fatalf("revoked Heartbeat() error = %v", err)
	}
	if _, err := admitTestIdentityResult(authenticator, privateKey); !errors.Is(err, ErrAdmissionRevoked) {
		t.Fatalf("revoked Admit() error = %v", err)
	}
}

func admitTestIdentity(
	t *testing.T,
	authenticator *Authenticator,
	privateKey ed25519.PrivateKey,
) AdmissionResult {
	t.Helper()
	result, err := admitTestIdentityResult(authenticator, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func admitTestIdentityResult(
	authenticator *Authenticator,
	privateKey ed25519.PrivateKey,
) (AdmissionResult, error) {
	challenge, err := authenticator.IssueChallenge()
	if err != nil {
		return AdmissionResult{}, err
	}
	proof, err := NewIdentityProof(
		privateKey, challenge.Nonce, ProtocolV1, ProtocolV1,
		"v0.1.0", "linux", "amd64", CapabilityCatalog{},
	)
	if err != nil {
		return AdmissionResult{}, err
	}
	admission, err := authenticator.Authenticate(proof)
	if err != nil {
		return AdmissionResult{}, err
	}
	if admission.Result.State == StateConnected {
		if err := authenticator.Connect(admission); err != nil {
			return AdmissionResult{}, err
		}
	}
	return admission.Result, nil
}
