package update

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/nodes/internal/jsonstrict"
)

const (
	ManifestSchemaV1       = 1
	SignatureSchemaV1      = 1
	MaxManifestBytes       = 64 * 1024
	MaxSignatureBytes      = 4 * 1024
	MaxNodeArtifactBytes   = 128 * 1024 * 1024
	CurrentCoordinatorAPI  = 1
	CurrentNodeProtocol    = 1
	CurrentNodeConfig      = 1
	ExpectedArtifactCount  = 4
	minimumVersionMaxBytes = 64
)

var (
	ErrInvalidManifest  = errors.New("invalid node update manifest")
	ErrUntrustedRelease = errors.New("untrusted node update release")

	releasePattern = regexp.MustCompile(
		`^v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)` +
			`(?:-[0-9A-Za-z][0-9A-Za-z-]*(?:\.[0-9A-Za-z][0-9A-Za-z-]*)*)?$`,
	)
	artifactNamePattern = regexp.MustCompile(`^mintclaw-node_(Linux|Darwin)_(x86_64|arm64)\.tar\.gz$`)
)

type Channel string

const (
	ChannelStable  Channel = "stable"
	ChannelNightly Channel = "nightly"
)

type Artifact struct {
	Platform     string `json:"platform"`
	Architecture string `json:"architecture"`
	Name         string `json:"name"`
	Size         int64  `json:"size"`
	SHA256       string `json:"sha256"`
}

type Manifest struct {
	SchemaVersion             int        `json:"schema_version"`
	Release                   string     `json:"release"`
	Channel                   Channel    `json:"channel"`
	PublishedAt               string     `json:"published_at"`
	ExpiresAt                 string     `json:"expires_at"`
	MinimumCoordinatorVersion string     `json:"minimum_coordinator_version"`
	CoordinatorAPI            int        `json:"coordinator_api"`
	NodeProtocol              int        `json:"node_protocol"`
	NodeConfig                int        `json:"node_config"`
	Artifacts                 []Artifact `json:"artifacts"`
}

type Signature struct {
	SchemaVersion int    `json:"schema_version"`
	Algorithm     string `json:"algorithm"`
	KeyID         string `json:"key_id"`
	Value         string `json:"value"`
}

type TrustedKey struct {
	KeyID     string
	PublicKey ed25519.PublicKey
}

func (manifest Manifest) Validate() error {
	if manifest.SchemaVersion != ManifestSchemaV1 || !releasePattern.MatchString(manifest.Release) {
		return fmt.Errorf("%w: unsupported schema or malformed release", ErrInvalidManifest)
	}
	if manifest.Channel != ChannelStable && manifest.Channel != ChannelNightly {
		return fmt.Errorf("%w: unsupported channel", ErrInvalidManifest)
	}
	prerelease := strings.Contains(manifest.Release, "-")
	if (manifest.Channel == ChannelStable && prerelease) ||
		(manifest.Channel == ChannelNightly && !prerelease) {
		return fmt.Errorf("%w: release does not match channel", ErrInvalidManifest)
	}
	publishedAt, err := parseManifestTime(manifest.PublishedAt)
	if err != nil {
		return fmt.Errorf("%w: published_at must have minute precision", ErrInvalidManifest)
	}
	expiresAt, err := parseManifestTime(manifest.ExpiresAt)
	if err != nil || !expiresAt.After(publishedAt) || expiresAt.Sub(publishedAt) > 90*24*time.Hour {
		return fmt.Errorf("%w: expires_at is outside the admitted window", ErrInvalidManifest)
	}
	if len(manifest.MinimumCoordinatorVersion) > minimumVersionMaxBytes ||
		!releasePattern.MatchString(manifest.MinimumCoordinatorVersion) ||
		manifest.CoordinatorAPI != CurrentCoordinatorAPI ||
		manifest.NodeProtocol != CurrentNodeProtocol ||
		manifest.NodeConfig != CurrentNodeConfig {
		return fmt.Errorf("%w: incompatible release contract", ErrInvalidManifest)
	}
	if len(manifest.Artifacts) != ExpectedArtifactCount {
		return fmt.Errorf("%w: release must contain every admitted platform tuple", ErrInvalidManifest)
	}
	priorTuple := ""
	seen := make(map[string]struct{}, len(manifest.Artifacts))
	for _, artifact := range manifest.Artifacts {
		if err := artifact.Validate(); err != nil {
			return err
		}
		tuple := artifact.Platform + "/" + artifact.Architecture
		if _, duplicate := seen[tuple]; duplicate || (priorTuple != "" && tuple <= priorTuple) {
			return fmt.Errorf("%w: artifact tuples are duplicate or unsorted", ErrInvalidManifest)
		}
		seen[tuple] = struct{}{}
		priorTuple = tuple
	}
	for _, tuple := range []string{"darwin/amd64", "darwin/arm64", "linux/amd64", "linux/arm64"} {
		if _, found := seen[tuple]; !found {
			return fmt.Errorf("%w: missing artifact tuple %s", ErrInvalidManifest, tuple)
		}
	}
	return nil
}

func (artifact Artifact) Validate() error {
	if artifact.Platform != "linux" && artifact.Platform != "darwin" {
		return fmt.Errorf("%w: unsupported artifact platform", ErrInvalidManifest)
	}
	if artifact.Architecture != "amd64" && artifact.Architecture != "arm64" {
		return fmt.Errorf("%w: unsupported artifact architecture", ErrInvalidManifest)
	}
	expectedName := "mintclaw-node_" + map[string]string{"linux": "Linux", "darwin": "Darwin"}[artifact.Platform] + "_" +
		map[string]string{"amd64": "x86_64", "arm64": "arm64"}[artifact.Architecture] + ".tar.gz"
	if artifact.Name != filepath.Base(artifact.Name) || artifact.Name != expectedName ||
		!artifactNamePattern.MatchString(artifact.Name) {
		return fmt.Errorf("%w: artifact name does not match its platform tuple", ErrInvalidManifest)
	}
	if artifact.Size <= 0 || artifact.Size > MaxNodeArtifactBytes {
		return fmt.Errorf("%w: artifact size is outside the admitted bound", ErrInvalidManifest)
	}
	if !validDigest(artifact.SHA256) {
		return fmt.Errorf("%w: malformed artifact digest", ErrInvalidManifest)
	}
	return nil
}

func ParseManifest(data []byte) (Manifest, error) {
	var manifest Manifest
	if err := decodeStrict(data, MaxManifestBytes, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	if err := manifest.Validate(); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func Verify(manifestData, signatureData []byte, trusted TrustedKey) (Manifest, error) {
	return VerifyAt(manifestData, signatureData, trusted, time.Now())
}

func VerifyAt(
	manifestData, signatureData []byte,
	trusted TrustedKey,
	now time.Time,
) (Manifest, error) {
	if len(trusted.PublicKey) != ed25519.PublicKeySize || trusted.KeyID != KeyID(trusted.PublicKey) {
		return Manifest{}, fmt.Errorf("%w: malformed trusted key", ErrUntrustedRelease)
	}
	var signature Signature
	if err := decodeStrict(signatureData, MaxSignatureBytes, &signature); err != nil {
		return Manifest{}, fmt.Errorf("%w: malformed signature envelope", ErrUntrustedRelease)
	}
	if signature.SchemaVersion != SignatureSchemaV1 || signature.Algorithm != "ed25519" ||
		signature.KeyID != trusted.KeyID {
		return Manifest{}, fmt.Errorf("%w: signature identity mismatch", ErrUntrustedRelease)
	}
	value, err := base64.RawStdEncoding.DecodeString(signature.Value)
	if err != nil || len(value) != ed25519.SignatureSize ||
		!ed25519.Verify(trusted.PublicKey, manifestData, value) {
		return Manifest{}, fmt.Errorf("%w: signature verification failed", ErrUntrustedRelease)
	}
	manifest, err := ParseManifest(manifestData)
	if err != nil {
		return Manifest{}, err
	}
	publishedAt, _ := parseManifestTime(manifest.PublishedAt)
	expiresAt, _ := parseManifestTime(manifest.ExpiresAt)
	if now.Before(publishedAt.Add(-5*time.Minute)) || !now.Before(expiresAt) {
		return Manifest{}, fmt.Errorf("%w: manifest is not currently valid", ErrUntrustedRelease)
	}
	return manifest, nil
}

func Sign(manifest Manifest, privateKey ed25519.PrivateKey) ([]byte, []byte, error) {
	if err := manifest.Validate(); err != nil {
		return nil, nil, err
	}
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, nil, errors.New("valid Ed25519 private key is required")
	}
	manifestData, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, nil, fmt.Errorf("encode node update manifest: %w", err)
	}
	manifestData = append(manifestData, '\n')
	publicKey := privateKey.Public().(ed25519.PublicKey)
	signature := Signature{
		SchemaVersion: SignatureSchemaV1,
		Algorithm:     "ed25519",
		KeyID:         KeyID(publicKey),
		Value:         base64.RawStdEncoding.EncodeToString(ed25519.Sign(privateKey, manifestData)),
	}
	signatureData, err := json.MarshalIndent(signature, "", "  ")
	if err != nil {
		return nil, nil, fmt.Errorf("encode node update signature: %w", err)
	}
	return manifestData, append(signatureData, '\n'), nil
}

func KeyID(publicKey ed25519.PublicKey) string {
	sum := sha256.Sum256(publicKey)
	return hex.EncodeToString(sum[:])
}

func ParsePublicKey(value string) (TrustedKey, error) {
	decoded, err := base64.RawStdEncoding.DecodeString(value)
	if err != nil || len(decoded) != ed25519.PublicKeySize {
		return TrustedKey{}, errors.New("public key must be an unpadded base64 Ed25519 key")
	}
	publicKey := ed25519.PublicKey(decoded)
	return TrustedKey{KeyID: KeyID(publicKey), PublicKey: publicKey}, nil
}

func SortArtifacts(artifacts []Artifact) {
	sort.Slice(artifacts, func(left, right int) bool {
		leftTuple := artifacts[left].Platform + "/" + artifacts[left].Architecture
		rightTuple := artifacts[right].Platform + "/" + artifacts[right].Architecture
		return leftTuple < rightTuple
	})
}

func decodeStrict(data []byte, maximum int, destination any) error {
	if len(data) == 0 || len(data) > maximum {
		return errors.New("document exceeds its size bound")
	}
	if _, err := jsonstrict.Decode(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("document contains trailing data")
	}
	return nil
}

func validDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
}

func parseManifestTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil || parsed.IsZero() || parsed.Second() != 0 || parsed.Nanosecond() != 0 {
		return time.Time{}, errors.New("time must have minute precision")
	}
	_, offset := parsed.Zone()
	if offset != 0 {
		return time.Time{}, errors.New("time must be UTC")
	}
	return parsed, nil
}
