package companion

import (
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

func buildTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	fingerprint, err := parseCertificateFingerprint(cfg.CertificateSHA256)
	if err != nil {
		return nil, err
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.CAFile != "" {
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		pemData, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read node gateway CA file: %w", err)
		}
		if !roots.AppendCertsFromPEM(pemData) {
			return nil, errors.New("node gateway CA file contains no certificates")
		}
		tlsConfig.RootCAs = roots
	}
	if len(fingerprint) == 0 {
		return tlsConfig, nil
	}

	if cfg.CAFile == "" {
		// Exact out-of-band pinning replaces chain verification, but not the
		// certificate validity window. There is no user-configurable insecure mode.
		tlsConfig.InsecureSkipVerify = true //nolint:gosec // verified by exact SHA-256 pin below
	}
	tlsConfig.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			return errors.New("node gateway presented no certificate")
		}
		leaf := state.PeerCertificates[0]
		digest := sha256.Sum256(leaf.Raw)
		if subtle.ConstantTimeCompare(digest[:], fingerprint) != 1 {
			return errors.New("node gateway certificate fingerprint mismatch")
		}
		if cfg.CAFile == "" {
			now := time.Now()
			if now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) {
				return errors.New("node gateway pinned certificate is outside its validity window")
			}
		}
		return nil
	}
	return tlsConfig, nil
}

func parseCertificateFingerprint(value string) ([]byte, error) {
	value = strings.TrimSpace(strings.ToLower(value))
	value = strings.TrimPrefix(value, "sha256:")
	value = strings.ReplaceAll(value, ":", "")
	if value == "" {
		return nil, nil
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return nil, errors.New("certificate_sha256 must be a 32-byte SHA-256 fingerprint")
	}
	return decoded, nil
}
