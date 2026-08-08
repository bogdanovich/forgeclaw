package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	nodeupdate "github.com/bogdanovich/mintclaw/pkg/nodes/update"
)

const (
	signingKeyEnvironment = "MINTCLAW_NODE_RELEASE_SIGNING_KEY"
	publicKeyEnvironment  = "MINTCLAW_NODE_RELEASE_PUBLIC_KEY"
)

type artifactFlags []string

func (values *artifactFlags) String() string { return strings.Join(*values, ",") }

func (values *artifactFlags) Set(value string) error {
	*values = append(*values, value)
	return nil
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "mintclaw-node-release:", err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	if len(arguments) != 1 {
		return errors.New("usage: mintclaw-node-release <keygen|sign|verify>")
	}
	if arguments[0] == "keygen" {
		return generateSigningKeyFiles()
	}
	if arguments[0] == "verify" {
		return verifyReleaseFiles()
	}
	if arguments[0] != "sign" {
		return errors.New("usage: mintclaw-node-release <keygen|sign|verify>")
	}
	release := strings.TrimSpace(os.Getenv("MINTCLAW_NODE_RELEASE"))
	channel := nodeupdate.Channel(strings.TrimSpace(os.Getenv("MINTCLAW_NODE_RELEASE_CHANNEL")))
	publishedAt := strings.TrimSpace(os.Getenv("MINTCLAW_NODE_RELEASE_PUBLISHED_AT"))
	minimumCoordinator := strings.TrimSpace(os.Getenv("MINTCLAW_NODE_MIN_COORDINATOR_VERSION"))
	manifestPath := strings.TrimSpace(os.Getenv("MINTCLAW_NODE_MANIFEST_PATH"))
	signaturePath := strings.TrimSpace(os.Getenv("MINTCLAW_NODE_SIGNATURE_PATH"))
	if manifestPath == "" {
		manifestPath = "mintclaw-node-manifest.json"
	}
	if signaturePath == "" {
		signaturePath = "mintclaw-node-manifest.sig"
	}
	artifacts, err := loadArtifactFlags(os.Getenv("MINTCLAW_NODE_RELEASE_ARTIFACTS"))
	if err != nil {
		return err
	}
	privateKey, err := signingKeyFromEnvironment()
	if err != nil {
		return err
	}
	publishedTime, err := time.Parse(time.RFC3339, publishedAt)
	if err != nil {
		return errors.New("MINTCLAW_NODE_RELEASE_PUBLISHED_AT must be RFC3339")
	}
	manifest := nodeupdate.Manifest{
		SchemaVersion:             nodeupdate.ManifestSchemaV1,
		Release:                   release,
		Channel:                   channel,
		PublishedAt:               publishedAt,
		ExpiresAt:                 publishedTime.Add(90 * 24 * time.Hour).UTC().Format(time.RFC3339),
		MinimumCoordinatorVersion: minimumCoordinator,
		CoordinatorAPI:            nodeupdate.CurrentCoordinatorAPI,
		NodeProtocol:              nodeupdate.CurrentNodeProtocol,
		NodeConfig:                nodeupdate.CurrentNodeConfig,
		Artifacts:                 artifacts,
	}
	nodeupdate.SortArtifacts(manifest.Artifacts)
	manifestData, signatureData, err := nodeupdate.Sign(manifest, privateKey)
	if err != nil {
		return err
	}
	if err = writeExclusive(manifestPath, manifestData, 0o644); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	if err = writeExclusive(signaturePath, signatureData, 0o644); err != nil {
		_ = os.Remove(manifestPath)
		return fmt.Errorf("write signature: %w", err)
	}
	return nil
}

func generateSigningKeyFiles() error {
	privatePath := strings.TrimSpace(os.Getenv("MINTCLAW_NODE_PRIVATE_KEY_PATH"))
	publicPath := strings.TrimSpace(os.Getenv("MINTCLAW_NODE_PUBLIC_KEY_PATH"))
	if privatePath == "" || publicPath == "" || filepath.Clean(privatePath) == filepath.Clean(publicPath) {
		return errors.New("distinct private and public key output paths are required")
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return fmt.Errorf("generate Ed25519 release key: %w", err)
	}
	privateData := []byte(base64.RawStdEncoding.EncodeToString(privateKey.Seed()) + "\n")
	publicData := []byte(base64.RawStdEncoding.EncodeToString(publicKey) + "\n")
	if err = writeExclusive(privatePath, privateData, 0o600); err != nil {
		return fmt.Errorf("write private key: %w", err)
	}
	if err = writeExclusive(publicPath, publicData, 0o644); err != nil {
		_ = os.Remove(privatePath)
		return fmt.Errorf("write public key: %w", err)
	}
	return nil
}

func verifyReleaseFiles() error {
	manifestPath := strings.TrimSpace(os.Getenv("MINTCLAW_NODE_MANIFEST_PATH"))
	signaturePath := strings.TrimSpace(os.Getenv("MINTCLAW_NODE_SIGNATURE_PATH"))
	if manifestPath == "" {
		manifestPath = "mintclaw-node-manifest.json"
	}
	if signaturePath == "" {
		signaturePath = "mintclaw-node-manifest.sig"
	}
	trustedKey, err := nodeupdate.ParsePublicKey(strings.TrimSpace(os.Getenv(publicKeyEnvironment)))
	if err != nil {
		return fmt.Errorf("%s: %w", publicKeyEnvironment, err)
	}
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	signatureData, err := os.ReadFile(signaturePath)
	if err != nil {
		return fmt.Errorf("read signature: %w", err)
	}
	if _, err = nodeupdate.Verify(manifestData, signatureData, trustedKey); err != nil {
		return err
	}
	return nil
}

func loadArtifactFlags(raw string) ([]nodeupdate.Artifact, error) {
	var values artifactFlags
	parser := flag.NewFlagSet("artifacts", flag.ContinueOnError)
	parser.SetOutput(io.Discard)
	parser.Var(&values, "artifact", "platform/architecture/path")
	fields := strings.Fields(raw)
	arguments := make([]string, 0, len(fields)*2)
	for _, field := range fields {
		arguments = append(arguments, "--artifact", field)
	}
	if err := parser.Parse(arguments); err != nil {
		return nil, errors.New("invalid release artifact list")
	}
	artifacts := make([]nodeupdate.Artifact, 0, len(values))
	for _, value := range values {
		parts := strings.SplitN(value, "/", 3)
		if len(parts) != 3 || parts[2] == "" {
			return nil, errors.New("artifact must use platform/architecture/path")
		}
		artifact, err := inspectArtifact(parts[0], parts[1], parts[2])
		if err != nil {
			return nil, err
		}
		artifacts = append(artifacts, artifact)
	}
	return artifacts, nil
}

func inspectArtifact(platform, architecture, path string) (nodeupdate.Artifact, error) {
	file, err := os.Open(path)
	if err != nil {
		return nodeupdate.Artifact{}, fmt.Errorf("open artifact: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nodeupdate.Artifact{}, errors.New("artifact must be a regular file")
	}
	if info.Size() <= 0 || info.Size() > nodeupdate.MaxNodeArtifactBytes {
		return nodeupdate.Artifact{}, errors.New("artifact size is outside the admitted bound")
	}
	digest := sha256.New()
	if _, err = io.Copy(digest, file); err != nil {
		return nodeupdate.Artifact{}, fmt.Errorf("hash artifact: %w", err)
	}
	artifact := nodeupdate.Artifact{
		Platform:     platform,
		Architecture: architecture,
		Name:         filepath.Base(path),
		Size:         info.Size(),
		SHA256:       hex.EncodeToString(digest.Sum(nil)),
	}
	if err = artifact.Validate(); err != nil {
		return nodeupdate.Artifact{}, err
	}
	return artifact, nil
}

func signingKeyFromEnvironment() (ed25519.PrivateKey, error) {
	encoded := strings.TrimSpace(os.Getenv(signingKeyEnvironment))
	seed, err := base64.RawStdEncoding.DecodeString(encoded)
	if err != nil || len(seed) != ed25519.SeedSize {
		return nil, errors.New(signingKeyEnvironment + " must contain an unpadded base64 Ed25519 seed")
	}
	return ed25519.NewKeyFromSeed(seed), nil
}

func writeExclusive(path string, data []byte, mode os.FileMode) error {
	if path == "" || filepath.Clean(path) == "." {
		return errors.New("output path is required")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		_ = os.Remove(path)
		return err
	}
	if closeErr != nil {
		_ = os.Remove(path)
		return closeErr
	}
	return nil
}
