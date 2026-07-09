package config

import (
	"encoding/json"
	"testing"
)

func TestGatewaySafeRestartConfigParsing(t *testing.T) {
	raw := []byte(`{
		"version": 3,
		"gateway": {
			"safe_restart": {
				"enabled": true,
				"service_manager": "systemd-user",
				"service": "picoclaw-main.service",
				"drain_timeout_seconds": 120,
				"force_after_timeout": true
			}
		}
	}`)

	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	got := cfg.Gateway.SafeRestart
	if !got.Enabled {
		t.Fatal("safe restart should be enabled")
	}
	if got.EffectiveServiceManager() != "systemd-user" {
		t.Fatalf("service manager = %q", got.EffectiveServiceManager())
	}
	if got.EffectiveService() != "picoclaw-main.service" {
		t.Fatalf("service = %q", got.EffectiveService())
	}
	if got.EffectiveDrainTimeoutSeconds() != 120 {
		t.Fatalf("drain timeout = %d", got.EffectiveDrainTimeoutSeconds())
	}
	if !got.ForceAfterTimeout {
		t.Fatal("force_after_timeout should be true")
	}
}

func TestGatewaySafeRestartDefaultDisabled(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Gateway.SafeRestart.Enabled {
		t.Fatal("safe restart should be disabled by default")
	}
	if cfg.Gateway.SafeRestart.EffectiveDrainTimeoutSeconds() != 300 {
		t.Fatalf("default drain timeout = %d", cfg.Gateway.SafeRestart.EffectiveDrainTimeoutSeconds())
	}
}
