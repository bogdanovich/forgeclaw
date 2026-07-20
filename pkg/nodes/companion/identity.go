package companion

import (
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
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		return loadIdentity(path)
	}
	if err != nil {
		return Identity{}, fmt.Errorf("create node identity: %w", err)
	}
	removeOnFailure := true
	defer func() {
		if removeOnFailure {
			_ = file.Close()
			_ = os.Remove(path)
		}
	}()
	if _, err := file.Write(data); err != nil {
		return Identity{}, fmt.Errorf("write node identity: %w", err)
	}
	if err := file.Sync(); err != nil {
		return Identity{}, fmt.Errorf("sync node identity: %w", err)
	}
	if err := file.Close(); err != nil {
		return Identity{}, fmt.Errorf("close node identity: %w", err)
	}
	removeOnFailure = false
	return identity, nil
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
