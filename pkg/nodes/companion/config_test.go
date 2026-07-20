package companion

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestConfigNormalizesSecureEndpointAndPaths(t *testing.T) {
	baseDir := t.TempDir()
	cfg, err := (Config{
		GatewayURL: "wss://gateway.example",
		StateDir:   "state",
		TLS:        TLSConfig{CAFile: "gateway-ca.pem"},
	}).Normalize(baseDir)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GatewayURL != "wss://gateway.example"+GatewayPath {
		t.Fatalf("GatewayURL = %q", cfg.GatewayURL)
	}
	if cfg.StateDir != filepath.Join(baseDir, "state") || cfg.TLS.CAFile != filepath.Join(baseDir, "gateway-ca.pem") {
		t.Fatalf("normalized paths = %q, %q", cfg.StateDir, cfg.TLS.CAFile)
	}
	if cfg.minReconnectDelay != DefaultMinReconnectDelay ||
		cfg.maxReconnectDelay != DefaultMaxReconnectDelay ||
		cfg.pendingRetryDelay != DefaultPendingRetryDelay {
		t.Fatalf(
			"normalized reconnect delays = %v, %v, %v",
			cfg.minReconnectDelay,
			cfg.maxReconnectDelay,
			cfg.pendingRetryDelay,
		)
	}
}

func TestConfigRejectsUnsafePlaintextEndpoints(t *testing.T) {
	tests := []Config{
		{GatewayURL: "ws://gateway.example"},
		{GatewayURL: "ws://127.0.0.1:3210"},
		{GatewayURL: "ws://gateway.example", AllowLoopbackPlaintext: true},
	}
	for _, cfg := range tests {
		if _, err := cfg.Normalize(t.TempDir()); err == nil {
			t.Fatalf("Normalize(%q) accepted unsafe plaintext", cfg.GatewayURL)
		}
	}
	allowed := Config{GatewayURL: "ws://127.0.0.1:3210", AllowLoopbackPlaintext: true}
	if _, err := allowed.Normalize(t.TempDir()); err != nil {
		t.Fatalf("explicit loopback plaintext rejected: %v", err)
	}
}

func TestLoadConfigRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	data := []byte(`{"gateway_url":"wss://gateway.example","unknown":true}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("LoadConfig() accepted an unknown field")
	}
}

func TestConfigReconnectBounds(t *testing.T) {
	cfg := Config{
		GatewayURL: "wss://gateway.example",
		Reconnect: ReconnectConfig{
			MinDelaySeconds: 10,
			MaxDelaySeconds: 5,
		},
	}
	if _, err := cfg.Normalize(t.TempDir()); err == nil {
		t.Fatal("Normalize() accepted inverted reconnect bounds")
	}
	cfg.Reconnect.MaxDelaySeconds = int((24*time.Hour)/time.Second) + 1
	if _, err := cfg.Normalize(t.TempDir()); err == nil {
		t.Fatal("Normalize() accepted excessive reconnect delay")
	}
}
