package nodes

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
)

func validateAnchoredName(name string) error {
	if name == "" ||
		name == "." ||
		name == ".." ||
		filepath.Base(name) != name ||
		strings.ContainsAny(name, `/\`) {
		return errors.New("anchored file name must be one path component")
	}
	return nil
}

func randomAnchoredTempName() (string, error) {
	var suffix [16]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", err
	}
	return ".node-terminals-" + hex.EncodeToString(suffix[:]) + ".tmp", nil
}
