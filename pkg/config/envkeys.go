// MintClaw - Ultra-lightweight personal AI agent
// License: MIT
//
// Copyright (c) 2026 PicoClaw contributors

package config

import (
	"os"
	"path/filepath"

	"github.com/bogdanovich/mintclaw/pkg"
)

// Runtime environment variable keys for the mintclaw process.
// These control the location of files and binaries at runtime and are read
// directly via os.Getenv / os.LookupEnv. All mintclaw-specific keys use the
// MINTCLAW_ prefix. Reference these constants instead of inline string
// literals to keep all supported knobs visible in one place and to prevent
// typos.
const (
	// EnvHome overrides the base directory for all mintclaw data
	// (config, workspace, skills, auth store, …).
	// Default: ~/.mintclaw
	EnvHome = "MINTCLAW_HOME"

	// EnvConfig overrides the full path to the JSON config file.
	// Default: $MINTCLAW_HOME/config.json
	EnvConfig = "MINTCLAW_CONFIG"

	// EnvBuiltinSkills overrides the directory from which built-in
	// skills are loaded.
	// Default: <cwd>/skills
	EnvBuiltinSkills = "MINTCLAW_BUILTIN_SKILLS"

	// EnvBinary overrides the path to the mintclaw executable.
	// Used by the web launcher when spawning the gateway subprocess.
	// Default: resolved from the same directory as the current executable.
	EnvBinary = "MINTCLAW_BINARY"

	// EnvGatewayHost overrides the host address for the gateway server.
	// Default: "localhost"
	EnvGatewayHost = "MINTCLAW_GATEWAY_HOST"
)

func GetHome() string {
	homePath, _ := os.UserHomeDir()
	if mintclawHome := os.Getenv(EnvHome); mintclawHome != "" {
		homePath = mintclawHome
	} else if homePath != "" {
		homePath = filepath.Join(homePath, pkg.DefaultMintClawHome)
	}
	if homePath == "" {
		homePath = "."
	}
	return homePath
}
