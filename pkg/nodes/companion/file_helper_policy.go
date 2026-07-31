package companion

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

const MaxFileHelperConfigBytes = 1024 * 1024

type FileHelperServiceConfig struct {
	SocketPath string       `json:"socket_path"`
	StateDir   string       `json:"state_dir"`
	AllowedUID uint32       `json:"allowed_uid"`
	AllowedGID uint32       `json:"allowed_gid"`
	Profiles   FilePolicies `json:"node_file_policies"`

	normalized bool
}

func NormalizeFileHelperServiceConfig(
	config FileHelperServiceConfig,
	baseDir string,
) (FileHelperServiceConfig, error) {
	config.SocketPath = strings.TrimSpace(config.SocketPath)
	config.StateDir = strings.TrimSpace(config.StateDir)
	if config.SocketPath == "" || config.StateDir == "" {
		return FileHelperServiceConfig{}, errors.New("file helper socket and state directory are required")
	}
	var err error
	config.SocketPath, err = resolveConfigPath(baseDir, config.SocketPath)
	if err != nil || !validFileHelperServicePath(config.SocketPath) ||
		config.SocketPath == string(filepath.Separator) {
		return FileHelperServiceConfig{}, errors.New("file helper socket path is invalid")
	}
	config.StateDir, err = resolveConfigPath(baseDir, config.StateDir)
	if err != nil || !validFileHelperServicePath(config.StateDir) ||
		config.StateDir == string(filepath.Separator) {
		return FileHelperServiceConfig{}, errors.New("file helper state directory is invalid")
	}
	if config.AllowedUID == 0 || config.AllowedGID == 0 {
		return FileHelperServiceConfig{}, errors.New("file helper companion peer must be unprivileged")
	}
	config.Profiles, err = normalizeFilePolicies(config.Profiles, baseDir)
	if err != nil {
		return FileHelperServiceConfig{}, fmt.Errorf("validate file helper policies: %w", err)
	}
	if len(config.Profiles) != 1 || !HasEnabledFilePolicy(config.Profiles) {
		return FileHelperServiceConfig{}, errors.New("file helper requires exactly one enabled profile")
	}
	for _, profile := range config.Profiles {
		if profile.Approval.Read != FileApprovalRequired ||
			profile.Approval.Write != FileApprovalRequired {
			return FileHelperServiceConfig{}, errors.New(
				"file helper read and write authority require trusted approval",
			)
		}
	}
	config.normalized = true
	return config, nil
}

func validFileHelperServicePath(value string) bool {
	return filepath.IsAbs(value) &&
		filepath.Clean(value) == value &&
		len(value) <= MaxFilePathBytes &&
		utf8.ValidString(value) &&
		strings.IndexFunc(value, unicode.IsControl) < 0
}

func (config FileHelperServiceConfig) Descriptors() ([]nodes.CommandDescriptor, error) {
	if !config.normalized {
		return nil, errors.New("file helper configuration is not normalized")
	}
	return fileCapabilityDescriptors(config.Profiles)
}

func fileHelperServiceDigest(config FileHelperServiceConfig) (string, error) {
	if !config.normalized {
		return "", errors.New("file helper configuration is not normalized")
	}
	binding := struct {
		Version    int          `json:"version"`
		SocketPath string       `json:"socket_path"`
		StateDir   string       `json:"state_dir"`
		AllowedUID uint32       `json:"allowed_uid"`
		AllowedGID uint32       `json:"allowed_gid"`
		Profiles   FilePolicies `json:"node_file_policies"`
	}{
		Version:    FileHelperProtocolVersion,
		SocketPath: config.SocketPath,
		StateDir:   config.StateDir,
		AllowedUID: config.AllowedUID,
		AllowedGID: config.AllowedGID,
		Profiles:   config.Profiles,
	}
	data, err := json.Marshal(binding)
	if err != nil {
		return "", fmt.Errorf("encode file helper service authority: %w", err)
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}
