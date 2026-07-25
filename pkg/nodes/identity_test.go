package nodes

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"
)

func TestIdentityProofRoundTripAndTamperDetection(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := NewIdentityProof(
		privateKey, "challenge", ProtocolV1, ProtocolV1,
		"v0.1.0", "linux", "amd64", CapabilityCatalog{},
	)
	if err != nil {
		t.Fatal(err)
	}
	publicKey, err := proof.Verify()
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if !publicKey.Equal(privateKey.Public()) {
		t.Fatal("verified public key does not match signer")
	}

	proof.Platform = "darwin"
	if _, err := proof.Verify(); !errors.Is(err, ErrInvalidIdentityProof) {
		t.Fatalf("tampered Verify() error = %v", err)
	}
}

func TestIdentityProofExecutionProfileRoundTripAndTamperDetection(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := NewIdentityProofWithExecutionProfile(
		privateKey,
		"challenge",
		ProtocolV2,
		ProtocolV2,
		"v0.1.0",
		"linux",
		"amd64",
		CapabilityCatalog{},
		ExecutionProfile{Executor: "local", PolicyRevision: "policy-1"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := proof.Verify(); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}

	proof.PolicyRevision = "policy-2"
	if _, err := proof.Verify(); !errors.Is(err, ErrInvalidIdentityProof) {
		t.Fatalf("tampered execution profile Verify() error = %v", err)
	}
}

func TestIdentityProofRejectsIncompleteExecutionProfile(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewIdentityProofWithExecutionProfile(
		privateKey,
		"challenge",
		ProtocolV2,
		ProtocolV2,
		"v0.1.0",
		"linux",
		"amd64",
		CapabilityCatalog{},
		ExecutionProfile{Executor: "local"},
	); !errors.Is(err, ErrInvalidInvocation) {
		t.Fatalf("incomplete execution profile error = %v", err)
	}
}

func TestDeriveIDIsStableAndKeyBound(t *testing.T) {
	firstPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	secondPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	firstID, err := DeriveID(firstPublic)
	if err != nil {
		t.Fatal(err)
	}
	repeatedID, err := DeriveID(firstPublic)
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := DeriveID(secondPublic)
	if err != nil {
		t.Fatal(err)
	}
	if firstID != repeatedID || firstID == secondID {
		t.Fatalf("derived ids = %q, %q, %q", firstID, repeatedID, secondID)
	}
}

func TestIdentityProofRejectsCatalogHashMismatch(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := NewIdentityProof(
		privateKey, "challenge", ProtocolV1, ProtocolV1,
		"v0.1.0", "linux", "amd64", CapabilityCatalog{},
	)
	if err != nil {
		t.Fatal(err)
	}
	proof.CatalogHash = "not-the-catalog-hash"
	if _, err := proof.Verify(); !errors.Is(err, ErrInvalidIdentityProof) {
		t.Fatalf("Verify() error = %v", err)
	}
}
