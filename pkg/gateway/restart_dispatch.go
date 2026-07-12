package gateway

import (
	"fmt"

	"github.com/sipeed/picoclaw/pkg/config"
)

type RestartDispatchOutcome string

const (
	RestartDispatchAccepted      RestartDispatchOutcome = "accepted"
	RestartDispatchIndeterminate RestartDispatchOutcome = "indeterminate"
	RestartDispatchFailed        RestartDispatchOutcome = "failed"
)

type RestartDispatchResult struct {
	Outcome RestartDispatchOutcome
	Err     error
}

func failedRestartDispatch(err error) RestartDispatchResult {
	return RestartDispatchResult{Outcome: RestartDispatchFailed, Err: err}
}

func newConfiguredServiceRestarter(cfg config.GatewaySafeRestartConfig) (ServiceRestarter, error) {
	manager := cfg.EffectiveServiceManager()
	if manager != "systemd-user" {
		return nil, fmt.Errorf("unsupported safe restart service manager %q", manager)
	}
	if err := validateSystemdUserService(cfg.EffectiveService()); err != nil {
		return nil, err
	}
	return SystemdUserServiceRestarter{}, nil
}
