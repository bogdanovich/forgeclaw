package config

import (
	"encoding/json"
	"os"
	"strings"

	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/netbind"
)

const DefaultGatewayLogLevel = "warn"

type GatewayConfig struct {
	Host      string `json:"host"                env:"PICOCLAW_GATEWAY_HOST"`
	Port      int    `json:"port"                env:"PICOCLAW_GATEWAY_PORT"`
	HotReload bool   `json:"hot_reload"          env:"PICOCLAW_GATEWAY_HOT_RELOAD"`
	LogLevel  string `json:"log_level,omitempty" env:"PICOCLAW_LOG_LEVEL"`

	SafeRestart GatewaySafeRestartConfig `json:"safe_restart,omitempty"`
	Deploy      GatewayDeployConfig      `json:"deploy,omitempty"`
}

type GatewayDeployConfig struct {
	Enabled        bool     `json:"enabled,omitempty"`
	Group          string   `json:"group,omitempty"`
	Command        string   `json:"command,omitempty"`
	DefaultTarget  string   `json:"default_target,omitempty"`
	AllowedTargets []string `json:"allowed_targets,omitempty"`
	TimeoutSeconds int      `json:"timeout_seconds,omitempty"`
}

func (c GatewayDeployConfig) EffectiveTimeoutSeconds() int {
	if c.TimeoutSeconds > 0 {
		return c.TimeoutSeconds
	}
	return 600
}

type GatewaySafeRestartConfig struct {
	Enabled             bool   `json:"enabled,omitempty"`
	ServiceManager      string `json:"service_manager,omitempty"`
	Service             string `json:"service,omitempty"`
	DrainTimeoutSeconds int    `json:"drain_timeout_seconds,omitempty"`
	ForceAfterTimeout   bool   `json:"force_after_timeout,omitempty"`
}

func (c GatewaySafeRestartConfig) EffectiveDrainTimeoutSeconds() int {
	if c.DrainTimeoutSeconds > 0 {
		return c.DrainTimeoutSeconds
	}
	return 300
}

func (c GatewaySafeRestartConfig) EffectiveServiceManager() string {
	return strings.TrimSpace(c.ServiceManager)
}

func (c GatewaySafeRestartConfig) EffectiveService() string {
	return strings.TrimSpace(c.Service)
}

func canonicalGatewayLogLevel(level logger.LogLevel) string {
	switch level {
	case logger.DEBUG:
		return "debug"
	case logger.INFO:
		return "info"
	case logger.WARN:
		return "warn"
	case logger.ERROR:
		return "error"
	case logger.FATAL:
		return "fatal"
	default:
		return DefaultGatewayLogLevel
	}
}

func normalizeGatewayLogLevel(logLevel string) string {
	if level, ok := logger.ParseLevel(logLevel); ok {
		return canonicalGatewayLogLevel(level)
	}
	return DefaultGatewayLogLevel
}

// EffectiveGatewayLogLevel returns the normalized runtime log level from a loaded config.
// Invalid or empty values fall back to the package default.
func EffectiveGatewayLogLevel(cfg *Config) string {
	if cfg == nil {
		return DefaultGatewayLogLevel
	}
	return normalizeGatewayLogLevel(cfg.Gateway.LogLevel)
}

func resolveGatewayHostFromEnv(baseHost string) (string, error) {
	envHost, ok := os.LookupEnv(EnvGatewayHost)
	if !ok {
		return normalizeGatewayHostInput(baseHost)
	}

	envHost = strings.TrimSpace(envHost)
	if envHost == "" {
		return normalizeGatewayHostInput(baseHost)
	}

	return normalizeGatewayHostInput(envHost)
}

func normalizeGatewayHostInput(host string) (string, error) {
	host = strings.TrimSpace(host)
	if host == "" {
		host = strings.TrimSpace(DefaultConfig().Gateway.Host)
	}
	if host == "" {
		host = "localhost"
	}
	return netbind.NormalizeHostInput(host)
}

// ResolveGatewayLogLevel reads the configured gateway log level without triggering
// the full config loader, so startup code can apply logging before config load logs run.
// The PICOCLAW_LOG_LEVEL environment variable overrides the file value.
func ResolveGatewayLogLevel(path string) string {
	cfg := struct {
		Gateway GatewayConfig `json:"gateway"`
	}{
		Gateway: GatewayConfig{LogLevel: DefaultGatewayLogLevel},
	}

	data, err := os.ReadFile(path)
	if err == nil {
		if err := json.Unmarshal(data, &cfg); err != nil {
			logger.WarnCF("config", "failed to parse gateway config, using defaults", map[string]any{
				"path":  path,
				"error": err.Error(),
			})
		}
	}

	if envLevel := os.Getenv("PICOCLAW_LOG_LEVEL"); envLevel != "" {
		cfg.Gateway.LogLevel = envLevel
	}

	return normalizeGatewayLogLevel(cfg.Gateway.LogLevel)
}
