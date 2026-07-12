package gateway

import (
	"fmt"
	"runtime"

	"github.com/sipeed/picoclaw/pkg/config"
)

var restartRuntimeGOOS = runtime.GOOS

func newConfiguredServiceRestarter(cfg config.GatewaySafeRestartConfig) (ServiceRestarter, error) {
	manager := cfg.EffectiveServiceManager()
	service := cfg.EffectiveService()
	switch manager {
	case "systemd-user":
		if restartRuntimeGOOS != "linux" {
			return nil, fmt.Errorf("service manager %q requires linux, running on %s", manager, restartRuntimeGOOS)
		}
		if err := validateSystemdUserService(service); err != nil {
			return nil, err
		}
		return SystemdUserServiceRestarter{}, nil
	case "launchd":
		if restartRuntimeGOOS != "darwin" {
			return nil, fmt.Errorf("service manager %q requires darwin, running on %s", manager, restartRuntimeGOOS)
		}
		if err := validateLaunchdServiceTarget(service); err != nil {
			return nil, err
		}
		return LaunchdServiceRestarter{}, nil
	case "windows-scm":
		return nil, fmt.Errorf(
			"service manager %q is unsupported: safe self-restart requires an external supervisor helper",
			manager,
		)
	default:
		return nil, fmt.Errorf("unsupported safe restart service manager %q", manager)
	}
}
