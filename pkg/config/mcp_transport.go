package config

import (
	"fmt"
	"strings"
)

// MCPSessionLossReplay controls whether MintClaw replays a tool call after
// reconnecting an MCP server whose session was lost during that call.
type MCPSessionLossReplay string

const (
	// MCPSessionLossReplayOnce preserves the historical behavior: reconnect
	// and invoke the same tool call once on the replacement session.
	MCPSessionLossReplayOnce MCPSessionLossReplay = "once"
	// MCPSessionLossReplayNever reconnects for future calls but reports the
	// interrupted call as uncertain without invoking it again.
	MCPSessionLossReplayNever MCPSessionLossReplay = "never"
)

// NormalizeMCPTransportType canonicalizes MCP transport names used in config.
// "http" is MintClaw's streamable HTTP request-response mode, and
// "streamable-http" is accepted as an explicit alias for the same transport.
func NormalizeMCPTransportType(transport string) string {
	normalized := strings.ToLower(strings.TrimSpace(transport))

	switch normalized {
	case "streamable-http", "streamable_http", "streamablehttp":
		return "http"
	default:
		return normalized
	}
}

// EffectiveMCPTransportType returns the normalized configured transport, or the
// inferred default when the config leaves Type empty.
func EffectiveMCPTransportType(server MCPServerConfig) string {
	if transport := NormalizeMCPTransportType(server.Type); transport != "" {
		return transport
	}
	if server.URL != "" {
		return "sse"
	}
	if server.Command != "" {
		return "stdio"
	}
	return ""
}

// NormalizeMCPSessionLossReplay canonicalizes configured replay policy names.
func NormalizeMCPSessionLossReplay(policy MCPSessionLossReplay) MCPSessionLossReplay {
	return MCPSessionLossReplay(strings.ToLower(strings.TrimSpace(string(policy))))
}

// EffectiveMCPSessionLossReplay returns the configured policy or the
// backward-compatible default when it is omitted.
func EffectiveMCPSessionLossReplay(server MCPServerConfig) MCPSessionLossReplay {
	if policy := NormalizeMCPSessionLossReplay(server.SessionLossReplay); policy != "" {
		return policy
	}
	return MCPSessionLossReplayOnce
}

// ValidateMCPSessionLossReplay rejects policies the MCP manager cannot enforce.
func ValidateMCPSessionLossReplay(server MCPServerConfig) error {
	switch EffectiveMCPSessionLossReplay(server) {
	case MCPSessionLossReplayOnce, MCPSessionLossReplayNever:
		return nil
	default:
		return fmt.Errorf(
			"unsupported MCP session_loss_replay %q (supported: once, never)",
			server.SessionLossReplay,
		)
	}
}
