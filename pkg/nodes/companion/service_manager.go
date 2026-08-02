package companion

import (
	"context"
)

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
// commands. Implementations resolve model-safe aliases locally and must not
// accept raw units, flags, environment, or shell input.
type ServiceManager interface {
	Status(context.Context, string) (ServiceStatus, error)
	Logs(context.Context, ServiceLogRequest) (ServiceLogs, error)
}
