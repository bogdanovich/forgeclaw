package companion

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	GatewayPath               = "/nodes/v1/ws"
	DefaultMinReconnectDelay  = time.Second
	DefaultMaxReconnectDelay  = 30 * time.Second
	DefaultPendingRetryDelay  = 30 * time.Second
	DefaultHandshakeTimeout   = 15 * time.Second
	DefaultGatewayLiveness    = 90 * time.Second
	MaxCompanionConfigFileLen = 1024 * 1024
)

type TLSConfig struct {
	CAFile            string `json:"ca_file,omitempty"`
	CertificateSHA256 string `json:"certificate_sha256,omitempty"`
}

type ReconnectConfig struct {
	MinDelaySeconds     int `json:"min_delay_seconds,omitempty"`
	MaxDelaySeconds     int `json:"max_delay_seconds,omitempty"`
	PendingDelaySeconds int `json:"pending_delay_seconds,omitempty"`
}

type Config struct {
	GatewayURL             string          `json:"gateway_url"`
	StateDir               string          `json:"state_dir,omitempty"`
	AllowLoopbackPlaintext bool            `json:"allow_loopback_plaintext,omitempty"`
	TLS                    TLSConfig       `json:"tls,omitempty"`
	Reconnect              ReconnectConfig `json:"reconnect,omitempty"`

	minReconnectDelay time.Duration
	maxReconnectDelay time.Duration
	pendingRetryDelay time.Duration
}

func LoadConfig(path string) (Config, error) {
	path, err := expandHome(path)
	if err != nil {
		return Config{}, err
	}
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open node config: %w", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, MaxCompanionConfigFileLen+1))
	decoder.DisallowUnknownFields()
	var cfg Config
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode node config: %w", err)
	}
	if err := ensureConfigEOF(decoder); err != nil {
		return Config{}, err
	}
	if info, err := file.Stat(); err != nil {
		return Config{}, fmt.Errorf("stat node config: %w", err)
	} else if info.Size() > MaxCompanionConfigFileLen {
		return Config{}, errors.New("node config exceeds size limit")
	}
	return cfg.Normalize(filepath.Dir(path))
}

func (cfg Config) Normalize(baseDir string) (Config, error) {
	endpoint, err := normalizeGatewayURL(cfg.GatewayURL, cfg.AllowLoopbackPlaintext)
	if err != nil {
		return Config{}, err
	}
	cfg.GatewayURL = endpoint

	if strings.TrimSpace(cfg.StateDir) == "" {
		home, homeErr := os.UserHomeDir()
		if homeErr != nil {
			return Config{}, fmt.Errorf("resolve home directory: %w", homeErr)
		}
		cfg.StateDir = filepath.Join(home, ".picoclaw-node")
	} else {
		cfg.StateDir, err = resolveConfigPath(baseDir, cfg.StateDir)
		if err != nil {
			return Config{}, fmt.Errorf("resolve node state directory: %w", err)
		}
	}
	if strings.TrimSpace(cfg.TLS.CAFile) != "" {
		cfg.TLS.CAFile, err = resolveConfigPath(baseDir, cfg.TLS.CAFile)
		if err != nil {
			return Config{}, fmt.Errorf("resolve node CA file: %w", err)
		}
	}
	if _, fingerprintErr := parseCertificateFingerprint(cfg.TLS.CertificateSHA256); fingerprintErr != nil {
		return Config{}, fingerprintErr
	}

	cfg.minReconnectDelay, err = durationSeconds(
		cfg.Reconnect.MinDelaySeconds,
		DefaultMinReconnectDelay,
		"minimum reconnect delay",
	)
	if err != nil {
		return Config{}, err
	}
	cfg.maxReconnectDelay, err = durationSeconds(
		cfg.Reconnect.MaxDelaySeconds,
		DefaultMaxReconnectDelay,
		"maximum reconnect delay",
	)
	if err != nil {
		return Config{}, err
	}
	cfg.pendingRetryDelay, err = durationSeconds(
		cfg.Reconnect.PendingDelaySeconds,
		DefaultPendingRetryDelay,
		"pending retry delay",
	)
	if err != nil {
		return Config{}, err
	}
	if cfg.maxReconnectDelay < cfg.minReconnectDelay {
		return Config{}, errors.New("maximum reconnect delay must not be shorter than minimum reconnect delay")
	}
	return cfg, nil
}

func normalizeGatewayURL(raw string, allowLoopbackPlaintext bool) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("gateway_url must be an absolute WebSocket URL without credentials, query, or fragment")
	}
	if parsed.Path == "" || parsed.Path == "/" {
		parsed.Path = GatewayPath
	}
	if parsed.Path != GatewayPath {
		return "", fmt.Errorf("gateway_url path must be %q", GatewayPath)
	}
	switch strings.ToLower(parsed.Scheme) {
	case "wss":
		parsed.Scheme = "wss"
	case "ws":
		if !allowLoopbackPlaintext || !isLoopbackHost(parsed.Hostname()) {
			return "", errors.New("plaintext gateway_url requires explicit loopback-only opt-in")
		}
		parsed.Scheme = "ws"
	default:
		return "", errors.New("gateway_url must use wss://")
	}
	return parsed.String(), nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func resolveConfigPath(baseDir, value string) (string, error) {
	expanded, err := expandHome(strings.TrimSpace(value))
	if err != nil {
		return "", err
	}
	if !filepath.IsAbs(expanded) {
		expanded = filepath.Join(baseDir, expanded)
	}
	return filepath.Clean(expanded), nil
}

func expandHome(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "~" || strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home directory: %w", err)
		}
		if path == "~" {
			return home, nil
		}
		return filepath.Join(home, strings.TrimPrefix(path, "~/")), nil
	}
	return path, nil
}

func durationSeconds(value int, fallback time.Duration, label string) (time.Duration, error) {
	if value == 0 {
		return fallback, nil
	}
	if value < 0 || value > int((24*time.Hour)/time.Second) {
		return 0, fmt.Errorf("%s must be between 1 second and 24 hours", label)
	}
	return time.Duration(value) * time.Second, nil
}

func ensureConfigEOF(decoder *json.Decoder) error {
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("node config contains multiple JSON values")
		}
		return fmt.Errorf("decode node config: %w", err)
	}
	return nil
}

func decodeStrictJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return ensureConfigEOF(decoder)
}
