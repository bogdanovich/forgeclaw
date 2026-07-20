package nodes

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	MaxClientVersionLength = 128
	MaxPlatformLength      = 64
	MaxArchitectureLength  = 64
	MaxRoleLength          = 32
)

var ErrInvalidIdentityProof = errors.New("invalid node identity proof")

type IdentityProof struct {
	Nonce         string            `json:"nonce"`
	NodeID        ID                `json:"node_id"`
	PublicKey     string            `json:"public_key"`
	Signature     string            `json:"signature"`
	MinProtocol   int               `json:"min_protocol"`
	MaxProtocol   int               `json:"max_protocol"`
	ClientVersion string            `json:"client_version"`
	Platform      string            `json:"platform"`
	Architecture  string            `json:"architecture"`
	RequestedRole string            `json:"requested_role"`
	CatalogHash   string            `json:"catalog_hash"`
	Catalog       CapabilityCatalog `json:"catalog"`
}

type identityTranscript struct {
	Nonce         string `json:"nonce"`
	NodeID        ID     `json:"node_id"`
	PublicKey     string `json:"public_key"`
	MinProtocol   int    `json:"min_protocol"`
	MaxProtocol   int    `json:"max_protocol"`
	ClientVersion string `json:"client_version"`
	Platform      string `json:"platform"`
	Architecture  string `json:"architecture"`
	RequestedRole string `json:"requested_role"`
	CatalogHash   string `json:"catalog_hash"`
}

func DeriveID(publicKey ed25519.PublicKey) (ID, error) {
	if len(publicKey) != ed25519.PublicKeySize {
		return "", fmt.Errorf("%w: malformed public key", ErrInvalidIdentityProof)
	}
	sum := sha256.Sum256(publicKey)
	encoded := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(sum[:])
	return ID("node_" + strings.ToLower(encoded)), nil
}

func NewIdentityProof(
	privateKey ed25519.PrivateKey,
	nonce string,
	minProtocol, maxProtocol int,
	clientVersion, platform, architecture string,
	catalog CapabilityCatalog,
) (IdentityProof, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return IdentityProof{}, fmt.Errorf("%w: malformed private key", ErrInvalidIdentityProof)
	}
	publicKey := privateKey.Public().(ed25519.PublicKey)
	nodeID, err := DeriveID(publicKey)
	if err != nil {
		return IdentityProof{}, err
	}
	catalogHash, err := catalog.Hash()
	if err != nil {
		return IdentityProof{}, err
	}
	if catalog.Commands == nil {
		catalog.Commands = make([]CommandDescriptor, 0)
	}
	proof := IdentityProof{
		Nonce:         nonce,
		NodeID:        nodeID,
		PublicKey:     base64.RawURLEncoding.EncodeToString(publicKey),
		MinProtocol:   minProtocol,
		MaxProtocol:   maxProtocol,
		ClientVersion: clientVersion,
		Platform:      platform,
		Architecture:  architecture,
		RequestedRole: "companion",
		CatalogHash:   catalogHash,
		Catalog:       catalog,
	}
	transcript, err := proof.transcript()
	if err != nil {
		return IdentityProof{}, err
	}
	proof.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, transcript))
	return proof, nil
}

func (proof IdentityProof) Verify() (ed25519.PublicKey, error) {
	if err := proof.validateClaims(); err != nil {
		return nil, err
	}
	publicKey, err := base64.RawURLEncoding.DecodeString(proof.PublicKey)
	if err != nil || len(publicKey) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("%w: malformed public key", ErrInvalidIdentityProof)
	}
	derivedID, err := DeriveID(ed25519.PublicKey(publicKey))
	if err != nil || derivedID != proof.NodeID {
		return nil, fmt.Errorf("%w: node id does not match public key", ErrInvalidIdentityProof)
	}
	signature, err := base64.RawURLEncoding.DecodeString(proof.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return nil, fmt.Errorf("%w: malformed signature", ErrInvalidIdentityProof)
	}
	transcript, err := proof.transcript()
	if err != nil {
		return nil, err
	}
	if !ed25519.Verify(publicKey, transcript, signature) {
		return nil, fmt.Errorf("%w: signature verification failed", ErrInvalidIdentityProof)
	}
	return ed25519.PublicKey(publicKey), nil
}

func (proof IdentityProof) validateClaims() error {
	if proof.Nonce == "" || len(proof.Nonce) > MaxIDLength {
		return fmt.Errorf("%w: malformed nonce", ErrInvalidIdentityProof)
	}
	if err := proof.NodeID.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidIdentityProof, err)
	}
	if proof.MinProtocol <= 0 || proof.MaxProtocol < proof.MinProtocol ||
		proof.MinProtocol > ProtocolV1 || proof.MaxProtocol < ProtocolV1 {
		return fmt.Errorf("%w: incompatible protocol range", ErrInvalidIdentityProof)
	}
	if len(proof.ClientVersion) == 0 || len(proof.ClientVersion) > MaxClientVersionLength ||
		len(proof.Platform) == 0 || len(proof.Platform) > MaxPlatformLength ||
		len(proof.Architecture) == 0 || len(proof.Architecture) > MaxArchitectureLength ||
		proof.RequestedRole != "companion" || len(proof.RequestedRole) > MaxRoleLength {
		return fmt.Errorf("%w: malformed client claims", ErrInvalidIdentityProof)
	}
	if err := proof.Catalog.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidIdentityProof, err)
	}
	catalogHash, err := proof.Catalog.Hash()
	if err != nil || catalogHash != proof.CatalogHash {
		return fmt.Errorf("%w: catalog hash does not match catalog", ErrInvalidIdentityProof)
	}
	return nil
}

func (proof IdentityProof) transcript() ([]byte, error) {
	data, err := json.Marshal(identityTranscript{
		Nonce:         proof.Nonce,
		NodeID:        proof.NodeID,
		PublicKey:     proof.PublicKey,
		MinProtocol:   proof.MinProtocol,
		MaxProtocol:   proof.MaxProtocol,
		ClientVersion: proof.ClientVersion,
		Platform:      proof.Platform,
		Architecture:  proof.Architecture,
		RequestedRole: proof.RequestedRole,
		CatalogHash:   proof.CatalogHash,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: encode signature transcript: %v", ErrInvalidIdentityProof, err)
	}
	return append([]byte("forgeclaw-node-auth-v1\x00"), data...), nil
}
