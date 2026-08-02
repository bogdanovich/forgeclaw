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

// ServiceManager is the narrow system-manager boundary used by typed service
// commands. Implementations resolve the exact profile and model-safe service
// aliases locally and must not accept raw units, flags, environment, or shell
// input.
type ServiceManager interface {
	Descriptors() []nodes.CommandDescriptor
	Status(context.Context, ServiceStatusRequest) (ServiceStatus, error)
	Logs(context.Context, ServiceLogRequest) (ServiceLogs, error)
}

type ServiceManagerError struct {
	Code string
}

func (failure *ServiceManagerError) Error() string {
	return "service manager request failed: " + failure.Code
}
