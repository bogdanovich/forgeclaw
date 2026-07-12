package gateway

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

type LaunchdServiceRestarter struct{}

var launchctlCommandContext = exec.CommandContext

func (LaunchdServiceRestarter) DispatchRestart(ctx context.Context, target string) RestartDispatchResult {
	cmd := launchctlCommandContext(ctx, "launchctl", "kickstart", "-k", target)
	if output, err := cmd.CombinedOutput(); err != nil {
		return failedRestartDispatch(fmt.Errorf(
			"launchctl kickstart %s failed: %w: %s",
			target, err, strings.TrimSpace(string(output)),
		))
	}
	return RestartDispatchResult{Outcome: RestartDispatchAccepted}
}

func validateLaunchdServiceTarget(target string) error {
	target = strings.TrimSpace(target)
	if target == "" || strings.ContainsAny(target, "\\\x00\r\n\t ") {
		return fmt.Errorf("invalid launchd service target %q", target)
	}
	parts := strings.Split(target, "/")
	if len(parts) != 3 || (parts[0] != "gui" && parts[0] != "user" && parts[0] != "system") || parts[2] == "" {
		return fmt.Errorf("invalid launchd service target %q", target)
	}
	return nil
}
