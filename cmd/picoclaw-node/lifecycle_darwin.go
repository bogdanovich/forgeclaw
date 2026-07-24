//go:build darwin

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
)

func newPlatformServiceLifecycle(system bool) (serviceLifecycle, error) {
	plistDir := "/Library/LaunchDaemons"
	domains := []string{"system"}
	if !system {
		uid := os.Getuid()
		home, err := resolveLaunchdUserHome(uid, user.LookupId)
		if err != nil {
			return nil, err
		}
		plistDir = filepath.Join(home, "Library", "LaunchAgents")
		domains = []string{fmt.Sprintf("gui/%d", uid), fmt.Sprintf("user/%d", uid)}
	}
	return &launchdLifecycle{
		system:   system,
		plistDir: plistDir,
		domains:  domains,
		run:      runLaunchctl,
	}, nil
}

func validatePlatformServiceAction(action string) error {
	if action == "status" || action == "install" {
		return nil
	}
	return fmt.Errorf("launchd %s is not implemented yet", action)
}

func runLaunchctl(ctx context.Context, args ...string) (launchdRunResult, error) {
	command := exec.CommandContext(ctx, "/bin/launchctl", args...)
	output, err := command.CombinedOutput()
	result := launchdRunResult{Output: strings.TrimSpace(string(output))}
	if err == nil {
		return result, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return launchdRunResult{}, fmt.Errorf("run launchctl: %w", ctxErr)
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		result.ExitCode = exitErr.ExitCode()
		return result, nil
	}
	return launchdRunResult{}, fmt.Errorf("run launchctl: %w", err)
}
