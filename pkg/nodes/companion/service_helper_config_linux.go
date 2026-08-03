//go:build linux

package companion

import (
	"errors"
	"path/filepath"
	"strings"
)

func normalizeServiceHelperClientConfig(
	config *ServiceHelperClientConfig,
	baseDir string,
) (*ServiceHelperClientConfig, error) {
	if config == nil {
		return nil, nil
	}
	if !config.Enabled {
		if strings.TrimSpace(config.SocketPath) != "" {
			return nil, errors.New("disabled service helper cannot configure a socket")
		}
		return nil, nil
	}
	if strings.TrimSpace(config.SocketPath) == "" {
		return nil, errors.New("enabled service helper requires a socket")
	}
	socketPath, err := resolveConfigPath(baseDir, config.SocketPath)
	if err != nil || !validFileHelperServicePath(socketPath) ||
		socketPath == string(filepath.Separator) {
		return nil, errors.New("service helper socket path is invalid")
	}
	return &ServiceHelperClientConfig{Enabled: true, SocketPath: socketPath}, nil
}
