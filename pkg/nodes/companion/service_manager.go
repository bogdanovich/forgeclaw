package companion

import (
	"context"
	"errors"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

type ServiceStatusRequest struct {
	Profile string
	Service string
}

type ServiceStatus struct {
	Service     string `json:"service"`
	LoadState   string `json:"load_state"`
	ActiveState string `json:"active_state"`
	Substate    string `json:"substate"`
	Enabled     string `json:"enabled"`
	ObservedAt  int64  `json:"observed_at"`
	Code        string `json:"code,omitempty"`
}

type ServiceLogRequest struct {
	Profile      string
	Service      string
	Entries      int
	SinceSeconds int
}

type ServiceLogRecord struct {
	Timestamp int64  `json:"timestamp"`
	Severity  string `json:"severity,omitempty"`
	Message   string `json:"message"`
}

type ServiceLogs struct {
	Service   string             `json:"service"`
	Records   []ServiceLogRecord `json:"records"`
	Truncated bool               `json:"truncated"`
}

type ServiceActionRequest struct {
	Profile string
	Service string
	Action  nodes.ServiceAction
}

type ServiceActionResult struct {
	Service    string              `json:"service"`
	Action     nodes.ServiceAction `json:"action"`
	State      string              `json:"state"`
	AcceptedAt int64               `json:"accepted_at,omitempty"`
	Status     *ServiceStatus      `json:"status,omitempty"`
	Code       string              `json:"code,omitempty"`
}

func (result ServiceActionResult) validateTerminal() error {
	if err := (nodes.Alias(result.Service)).Validate(); err != nil || !result.Action.Valid() {
		return errors.New("service action terminal identity is invalid")
	}
	if result.Status != nil && result.Status.Service != result.Service {
		return errors.New("service action terminal status binding is invalid")
	}
	switch result.State {
	case "completed":
		if result.AcceptedAt <= 0 || result.Status == nil || result.Code != "" ||
			!serviceActionStatusMatches(result.Action, *result.Status) {
			return errors.New("completed service action lacks terminal proof")
		}
	case "unknown":
		if result.AcceptedAt <= 0 || result.Code == "" {
			return errors.New("unknown service action lacks acceptance evidence")
		}
	case "failed", "canceled":
		if result.AcceptedAt != 0 || result.Status != nil || result.Code == "" {
			return errors.New("pre-acceptance service action terminal is invalid")
		}
	default:
		return errors.New("service action terminal state is invalid")
	}
	return nil
}

func serviceActionStatusMatches(action nodes.ServiceAction, status ServiceStatus) bool {
	if status.Code != "" {
		return false
	}
	switch action {
	case nodes.ServiceActionStart, nodes.ServiceActionRestart, nodes.ServiceActionReload:
		return status.ActiveState == "active"
	case nodes.ServiceActionStop:
		return status.ActiveState == "inactive"
	case nodes.ServiceActionEnable:
		return status.Enabled == "enabled"
	case nodes.ServiceActionDisable:
		return status.Enabled == "disabled"
	default:
		return false
	}
}

// serviceActionAcceptor atomically marks the manager acceptance boundary. A
// false return means cancellation won before manager access and the action
// must not be attempted.
type serviceActionAcceptor func() bool

// ServiceManager is the narrow system-manager boundary used by typed service
// commands. Implementations resolve the exact profile and model-safe service
// aliases locally and must not accept raw units, flags, environment, or shell
// input.
type ServiceManager interface {
	Descriptors() []nodes.CommandDescriptor
	Status(context.Context, ServiceStatusRequest) (ServiceStatus, error)
	Logs(context.Context, ServiceLogRequest) (ServiceLogs, error)
}

// ServiceController is implemented only by the privileged helper's exact
// system-manager backend. It adds no arbitrary argv, unit, or environment
// surface to ServiceManager.
type ServiceController interface {
	ServiceManager
	Action(context.Context, ServiceActionRequest) (ServiceActionResult, error)
}

type serviceActionExecutor interface {
	ServiceManager
	executeAction(context.Context, ServiceActionRequest, serviceActionAcceptor) (ServiceActionResult, error)
}

type ServiceManagerError struct {
	Code string
}

func (failure *ServiceManagerError) Error() string {
	return "service manager request failed: " + failure.Code
}
