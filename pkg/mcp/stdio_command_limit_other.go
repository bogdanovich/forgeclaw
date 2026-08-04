//go:build !linux && !darwin

package mcp

import (
	"context"
	"errors"
	"os/exec"
)

func stdioCommand(
	ctx context.Context,
	command string,
	args []string,
	fileSizeLimitBytes int64,
) (*exec.Cmd, error) {
	if fileSizeLimitBytes > 0 {
		return nil, errors.New("stdio file size limits are unsupported on this platform")
	}
	return exec.CommandContext(ctx, command, args...), nil
}
