package companion

import (
	"context"

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
