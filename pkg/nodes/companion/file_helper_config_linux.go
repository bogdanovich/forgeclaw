//go:build linux

package companion

import (
	"errors"
	"path/filepath"
	"strings"
)

func normalizeFileHelperClientConfig(
	config *FileHelperClientConfig,
	baseDir string,
) (*FileHelperClientConfig, error) {
	if config == nil {
		return nil, nil
	}
	if !config.Enabled {
		if strings.TrimSpace(config.SocketPath) != "" {
			return nil, errors.New("disabled helper cannot configure a socket")
		}
		return nil, nil
	}
	if strings.TrimSpace(config.SocketPath) == "" {
		return nil, errors.New("enabled helper requires a socket")
	}
	socketPath, err := resolveConfigPath(baseDir, config.SocketPath)
	if err != nil || !validFileHelperServicePath(socketPath) ||
		socketPath == string(filepath.Separator) {
		return nil, errors.New("helper socket path is invalid")
	}
	return &FileHelperClientConfig{Enabled: true, SocketPath: socketPath}, nil
}
