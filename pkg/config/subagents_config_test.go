package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig_SubagentsSessionModelOverrideMode(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(configPath, []byte(`{
		"version": 1,
		"agents": {
			"defaults": {
				"workspace": "/tmp/test",
				"model_name": "gpt-5.4",
				"max_tokens": 8192,
				"summarize_message_threshold": 20,
				"summarize_token_percent": 75,
				"subturn": {},
				"subagents": {
					"session_model_override_mode": "fallback_only",
					"model": {
						"primary": "gemini-flash-lite",
						"fallbacks": ["deepseek"]
					}
				}
			},
			"list": [{
				"id": "coding",
				"subagents": {
					"session_model_override_mode": "ignore"
				}
			}]
		}
	}`), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.Agents.Defaults.Subagents == nil {
		t.Fatal("defaults.subagents = nil")
	}
	if cfg.Agents.Defaults.Subagents.SessionModelOverrideMode != "fallback_only" {
		t.Fatalf(
			"defaults.subagents.session_model_override_mode = %q, want fallback_only",
			cfg.Agents.Defaults.Subagents.SessionModelOverrideMode,
		)
	}
	if cfg.Agents.Defaults.Subagents.Model == nil ||
		cfg.Agents.Defaults.Subagents.Model.Primary != "gemini-flash-lite" {
		t.Fatalf(
			"defaults.subagents.model = %#v, want gemini-flash-lite",
			cfg.Agents.Defaults.Subagents.Model,
		)
	}
	if cfg.Agents.List[0].Subagents == nil || cfg.Agents.List[0].Subagents.SessionModelOverrideMode != "ignore" {
		t.Fatalf("agent subagents override = %#v, want ignore", cfg.Agents.List[0].Subagents)
	}
}
