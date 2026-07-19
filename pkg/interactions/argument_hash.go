package interactions

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const argumentHashKeySize = 32

// HashArguments returns a restart-stable fingerprint without persisting raw
// tool arguments. The workspace-local key prevents guessing low-entropy
// secrets from hashes stored in interaction records.
func HashArguments(workspace string, arguments map[string]any) (string, error) {
	key, err := loadOrCreateArgumentHashKey(workspace)
	if err != nil {
		return "", err
	}
	canonical, err := json.Marshal(arguments)
	if err != nil {
		return "", fmt.Errorf("canonicalize approval arguments: %w", err)
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(canonical)
	return hex.EncodeToString(mac.Sum(nil)), nil
}

func argumentHashKeyPath(workspace string) string {
	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		return ""
	}
	return filepath.Join(workspace, "state", "interaction_hmac.key")
}

func loadOrCreateArgumentHashKey(workspace string) ([]byte, error) {
	path := argumentHashKeyPath(workspace)
	if path == "" {
		return nil, fmt.Errorf("approval argument hash requires a workspace")
	}
	if key, err := readArgumentHashKey(path); err == nil {
		return key, nil
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create approval hash key directory: %w", err)
	}
	key := make([]byte, argumentHashKeySize)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("generate approval hash key: %w", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err == nil {
		if _, writeErr := file.Write(key); writeErr != nil {
			_ = file.Close()
			_ = os.Remove(path)
			return nil, fmt.Errorf("write approval hash key: %w", writeErr)
		}
		if syncErr := file.Sync(); syncErr != nil {
			_ = file.Close()
			_ = os.Remove(path)
			return nil, fmt.Errorf("sync approval hash key: %w", syncErr)
		}
		if closeErr := file.Close(); closeErr != nil {
			return nil, fmt.Errorf("close approval hash key: %w", closeErr)
		}
		directory, openErr := os.Open(filepath.Dir(path))
		if openErr != nil {
			return nil, fmt.Errorf("open approval hash key directory: %w", openErr)
		}
		syncErr := directory.Sync()
		closeErr := directory.Close()
		if syncErr != nil {
			return nil, fmt.Errorf("sync approval hash key directory: %w", syncErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close approval hash key directory: %w", closeErr)
		}
		return key, nil
	}
	if !os.IsExist(err) {
		return nil, fmt.Errorf("create approval hash key: %w", err)
	}
	return readArgumentHashKey(path)
}

func readArgumentHashKey(path string) ([]byte, error) {
	key, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(key) != argumentHashKeySize {
		return nil, fmt.Errorf("approval hash key has invalid length")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return nil, fmt.Errorf("restrict approval hash key permissions: %w", err)
	}
	return key, nil
}
