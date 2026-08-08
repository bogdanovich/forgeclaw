package update

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestManifestSignAndOfflineVerify(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	manifestData, signatureData, err := Sign(validManifest(), privateKey)
	if err != nil {
		t.Fatalf("Sign() error = %v", err)
	}
	manifest, err := VerifyAt(manifestData, signatureData, TrustedKey{
		KeyID:     KeyID(publicKey),
		PublicKey: publicKey,
	}, time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if manifest.Release != "v1.2.3" || len(manifest.Artifacts) != ExpectedArtifactCount {
		t.Fatalf("verified manifest = %#v", manifest)
	}

	encodedPublicKey := base64.RawStdEncoding.EncodeToString(publicKey)
	parsed, err := ParsePublicKey(encodedPublicKey)
	if err != nil || parsed.KeyID != KeyID(publicKey) {
		t.Fatalf("ParsePublicKey() = %#v, %v", parsed, err)
	}
}

func TestVerifyRejectsWrongSignerAndChangedBytes(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	otherPublicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	manifestData, signatureData, err := Sign(validManifest(), privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Verify(manifestData, signatureData, TrustedKey{
		KeyID:     KeyID(otherPublicKey),
		PublicKey: otherPublicKey,
	}); !errors.Is(err, ErrUntrustedRelease) {
		t.Fatalf("wrong signer error = %v", err)
	}
	changed := append([]byte(nil), manifestData...)
	changed[len(changed)-2] ^= 1
	if _, err = Verify(changed, signatureData, TrustedKey{
		KeyID:     KeyID(publicKey),
		PublicKey: publicKey,
	}); !errors.Is(err, ErrUntrustedRelease) {
		t.Fatalf("changed manifest error = %v", err)
	}
}

func TestVerifyRejectsExpiredAndNotYetValidManifest(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	manifestData, signatureData, err := Sign(validManifest(), privateKey)
	if err != nil {
		t.Fatal(err)
	}
	trusted := TrustedKey{KeyID: KeyID(publicKey), PublicKey: publicKey}
	for name, now := range map[string]time.Time{
		"not yet valid": time.Date(2026, 8, 7, 18, 54, 0, 0, time.UTC),
		"expired":       time.Date(2026, 9, 6, 19, 0, 0, 0, time.UTC),
	} {
		t.Run(name, func(t *testing.T) {
			if _, verifyErr := VerifyAt(manifestData, signatureData, trusted, now); !errors.Is(
				verifyErr,
				ErrUntrustedRelease,
			) {
				t.Fatalf("VerifyAt() error = %v", verifyErr)
			}
		})
	}
}

func TestManifestValidationFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Manifest)
	}{
		{name: "prerelease on stable", mutate: func(manifest *Manifest) { manifest.Release = "v1.2.3-rc.1" }},
		{name: "stable on nightly", mutate: func(manifest *Manifest) { manifest.Channel = ChannelNightly }},
		{name: "non UTC time", mutate: func(manifest *Manifest) { manifest.PublishedAt = "2026-08-07T12:00:00-07:00" }},
		{name: "seconds", mutate: func(manifest *Manifest) { manifest.PublishedAt = "2026-08-07T19:00:01Z" }},
		{name: "expired before publication", mutate: func(manifest *Manifest) {
			manifest.ExpiresAt = "2026-08-07T18:00:00Z"
		}},
		{name: "excessive validity", mutate: func(manifest *Manifest) {
			manifest.ExpiresAt = "2027-08-07T19:00:00Z"
		}},
		{name: "coordinator API", mutate: func(manifest *Manifest) { manifest.CoordinatorAPI++ }},
		{name: "node protocol", mutate: func(manifest *Manifest) { manifest.NodeProtocol++ }},
		{name: "node config", mutate: func(manifest *Manifest) { manifest.NodeConfig++ }},
		{name: "missing tuple", mutate: func(manifest *Manifest) { manifest.Artifacts = manifest.Artifacts[:3] }},
		{name: "duplicate tuple", mutate: func(manifest *Manifest) { manifest.Artifacts[1] = manifest.Artifacts[0] }},
		{name: "wrong name", mutate: func(manifest *Manifest) { manifest.Artifacts[0].Name = "mintclaw.tar.gz" }},
		{name: "uppercase digest", mutate: func(manifest *Manifest) {
			manifest.Artifacts[0].SHA256 = strings.ToUpper(manifest.Artifacts[0].SHA256)
		}},
		{name: "oversized", mutate: func(manifest *Manifest) {
			manifest.Artifacts[0].Size = MaxNodeArtifactBytes + 1
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manifest := validManifest()
			test.mutate(&manifest)
			if err := manifest.Validate(); !errors.Is(err, ErrInvalidManifest) {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func TestParseManifestRejectsUnknownDuplicateTrailingAndOversizedInput(t *testing.T) {
	manifestData, err := json.Marshal(validManifest())
	if err != nil {
		t.Fatal(err)
	}
	unknown := append(manifestData[:len(manifestData)-1], []byte(`,"unexpected":true}`)...)
	for name, data := range map[string][]byte{
		"unknown":   unknown,
		"duplicate": []byte(`{"schema_version":1,"schema_version":1}`),
		"trailing":  append(append([]byte(nil), manifestData...), []byte(` {}`)...),
		"oversized": make([]byte, MaxManifestBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, parseErr := ParseManifest(data); !errors.Is(parseErr, ErrInvalidManifest) {
				t.Fatalf("ParseManifest() error = %v", parseErr)
			}
		})
	}
}

func validManifest() Manifest {
	digest := strings.Repeat("a", 64)
	return Manifest{
		SchemaVersion:             ManifestSchemaV1,
		Release:                   "v1.2.3",
		Channel:                   ChannelStable,
		PublishedAt:               "2026-08-07T19:00:00Z",
		ExpiresAt:                 "2026-09-06T19:00:00Z",
		MinimumCoordinatorVersion: "v1.0.0",
		CoordinatorAPI:            CurrentCoordinatorAPI,
		NodeProtocol:              CurrentNodeProtocol,
		NodeConfig:                CurrentNodeConfig,
		Artifacts: []Artifact{
			{
				Platform:     "darwin",
				Architecture: "amd64",
				Name:         "mintclaw-node_Darwin_x86_64.tar.gz",
				Size:         1,
				SHA256:       digest,
			},
			{
				Platform:     "darwin",
				Architecture: "arm64",
				Name:         "mintclaw-node_Darwin_arm64.tar.gz",
				Size:         1,
				SHA256:       digest,
			},
			{
				Platform:     "linux",
				Architecture: "amd64",
				Name:         "mintclaw-node_Linux_x86_64.tar.gz",
				Size:         1,
				SHA256:       digest,
			},
			{
				Platform:     "linux",
				Architecture: "arm64",
				Name:         "mintclaw-node_Linux_arm64.tar.gz",
				Size:         1,
				SHA256:       digest,
			},
		},
	}
}
