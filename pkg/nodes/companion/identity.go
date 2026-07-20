package companion

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/sipeed/picoclaw/pkg/nodes"
)

const identityFileVersion = 1

type Identity struct {
	ID         nodes.ID
	PrivateKey ed25519.PrivateKey
}

type identityDocument struct {
	Version int      `json:"version"`
	NodeID  nodes.ID `json:"node_id"`
	Seed    string   `json:"seed"`
}

func LoadOrCreateIdentity(stateDir string) (Identity, error) {
	if err := ensurePrivateDirectory(stateDir); err != nil {
		return Identity{}, err
	}
	path := filepath.Join(stateDir, "identity.json")
	identity, err := loadIdentity(path)
	if err == nil {
		return identity, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return Identity{}, err
	}

	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return Identity{}, fmt.Errorf("generate node identity: %w", err)
	}
	identity, err = identityFromPrivateKey(privateKey)
	if err != nil {
		return Identity{}, err
	}
	document := identityDocument{
		Version: identityFileVersion,
		NodeID:  identity.ID,
		Seed:    base64.RawURLEncoding.EncodeToString(privateKey.Seed()),
	}
	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return Identity{}, fmt.Errorf("encode node identity: %w", err)
	}
	data = append(data, '\n')
	if err := publishIdentityFile(path, data, os.Link); errors.Is(err, os.ErrExist) {
		return loadIdentity(path)
	} else if err != nil {
		return Identity{}, err
	}
	return identity, nil
}

func publishIdentityFile(
	path string,
	data []byte,
	link func(string, string) error,
) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".identity-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary node identity: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if chmodErr := temporary.Chmod(0o600); chmodErr != nil {
		temporary.Close()
		return fmt.Errorf("secure temporary node identity: %w", chmodErr)
	}
	if _, copyErr := io.Copy(temporary, bytes.NewReader(data)); copyErr != nil {
		temporary.Close()
		return fmt.Errorf("write temporary node identity: %w", copyErr)
	}
	if syncErr := temporary.Sync(); syncErr != nil {
		temporary.Close()
		return fmt.Errorf("sync temporary node identity: %w", syncErr)
	}
	if closeErr := temporary.Close(); closeErr != nil {
		return fmt.Errorf("close temporary node identity: %w", closeErr)
	}
	if linkErr := link(temporaryPath, path); linkErr != nil {
		return fmt.Errorf("publish node identity: %w", linkErr)
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("open node state directory for sync: %w", err)
	}
	defer directory.Close()
	if syncErr := directory.Sync(); syncErr != nil {
		return fmt.Errorf("sync node state directory: %w", syncErr)
	}
	return nil
}

func loadIdentity(path string) (Identity, error) {
	file, err := os.Open(path)
	if err != nil {
		return Identity{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return Identity{}, fmt.Errorf("stat node identity: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return Identity{}, errors.New("node identity file must not be accessible by group or other users")
	}
	data, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil {
		return Identity{}, fmt.Errorf("read node identity: %w", err)
	}
	if len(data) > 4096 {
		return Identity{}, errors.New("node identity file exceeds size limit")
	}
	var document identityDocument
	if decodeErr := decodeStrictJSON(data, &document); decodeErr != nil {
		return Identity{}, fmt.Errorf("decode node identity: %w", decodeErr)
	}
	if document.Version != identityFileVersion {
		return Identity{}, fmt.Errorf("unsupported node identity version %d", document.Version)
	}
	seed, err := base64.RawURLEncoding.DecodeString(document.Seed)
	if err != nil || len(seed) != ed25519.SeedSize {
		return Identity{}, errors.New("node identity contains a malformed seed")
	}
	identity, err := identityFromPrivateKey(ed25519.NewKeyFromSeed(seed))
	if err != nil {
		return Identity{}, err
	}
	if identity.ID != document.NodeID {
		return Identity{}, errors.New("node identity ID does not match its private key")
	}
	return identity, nil
}

func identityFromPrivateKey(privateKey ed25519.PrivateKey) (Identity, error) {
	publicKey, ok := privateKey.Public().(ed25519.PublicKey)
	if !ok {
		return Identity{}, errors.New("node identity has an unsupported private key")
	}
	id, err := nodes.DeriveID(publicKey)
	if err != nil {
		return Identity{}, err
	}
	return Identity{ID: id, PrivateKey: privateKey}, nil
}

func ensurePrivateDirectory(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("create node state directory: %w", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat node state directory: %w", err)
	}
	if !info.IsDir() {
		return errors.New("node state path is not a directory")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return errors.New("node state directory must not be accessible by group or other users")
	}
	return nil
}
