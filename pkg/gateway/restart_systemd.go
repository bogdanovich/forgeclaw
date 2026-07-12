package gateway

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

type SystemdUserServiceRestarter struct{}

var systemctlCommandContext = exec.CommandContext

func (SystemdUserServiceRestarter) DispatchRestart(ctx context.Context, service string) RestartDispatchResult {
	cmd := systemctlCommandContext(ctx, "systemctl", "--user", "restart", "--no-block", service)
	if output, err := cmd.CombinedOutput(); err != nil {
		wrapped := fmt.Errorf(
			"systemctl --user restart %s failed: %w: %s",
			service, err, strings.TrimSpace(string(output)),
		)
		if cmd.ProcessState != nil && cmd.ProcessState.ExitCode() == -1 {
			return RestartDispatchResult{Outcome: RestartDispatchIndeterminate, Err: wrapped}
		}
		return failedRestartDispatch(wrapped)
	}
	return RestartDispatchResult{Outcome: RestartDispatchAccepted}
}
