//go:build !linux

package companion

import "errors"

func normalizeFileHelperClientConfig(
	config *FileHelperClientConfig,
	_ string,
) (*FileHelperClientConfig, error) {
	if config == nil || (!config.Enabled && config.SocketPath == "") {
		return nil, nil
	}
	return nil, errors.New("privileged file helper is supported only on Linux")
}
