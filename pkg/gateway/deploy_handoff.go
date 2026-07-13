package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"

	"github.com/sipeed/picoclaw/pkg/config"
)

// DeployHandoffLauncher starts a worker outside the gateway service cgroup.
// It is optional: platforms without a compatible supervisor keep synchronous
// deploy behavior.
type DeployHandoffLauncher interface {
	Launch(ctx context.Context, runner *DeployRunner, target string, origin RestartOrigin) error
}

func newDeployHandoffLauncher(cfg config.GatewaySafeRestartConfig) DeployHandoffLauncher {
	if runtime.GOOS != "linux" || cfg.EffectiveServiceManager() != "systemd-user" {
		return nil
	}
	return systemdUserDeployHandoffLauncher{}
}

type systemdUserDeployHandoffLauncher struct{}

var systemdRunCommandContext = exec.CommandContext

func (systemdUserDeployHandoffLauncher) Launch(
	ctx context.Context,
	runner *DeployRunner,
	target string,
	origin RestartOrigin,
) error {
	if runner == nil {
		return fmt.Errorf("deploy runner is nil")
	}
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve picoclaw executable: %w", err)
	}
	originJSON, err := json.Marshal(origin)
	if err != nil {
		return fmt.Errorf("encode deploy origin: %w", err)
	}
	unitName := deployHandoffUnitName(runner.cfg.Group)
	args := []string{
		"--user", "--collect", "--quiet", "--unit", unitName,
		executable, "gateway", "deploy-worker",
		"--command", runner.cfg.Command,
		"--group", runner.cfg.Group,
		"--workspace", runner.workspace,
		"--service", runner.service,
		"--target", target,
		"--timeout-seconds", strconv.Itoa(runner.cfg.EffectiveTimeoutSeconds()),
		"--origin", base64.RawURLEncoding.EncodeToString(originJSON),
	}
	cmd := systemdRunCommandContext(ctx, "systemd-run", args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("start detached deploy worker: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func deployHandoffUnitName(group string) string {
	hash := sha256.Sum256([]byte(group))
	return fmt.Sprintf("picoclaw-deploy-%x", hash[:8])
}
