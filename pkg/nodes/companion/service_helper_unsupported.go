//go:build !linux

package companion

import (
	"context"
	"errors"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

type ServiceHelperClient struct{}

func normalizeServiceHelperClientConfig(
	config *ServiceHelperClientConfig,
	_ string,
) (*ServiceHelperClientConfig, error) {
	if config == nil || (!config.Enabled && config.SocketPath == "") {
		return nil, nil
	}
	return nil, errors.New("privileged service helper is supported only on Linux")
}

func NewServiceHelperClient(context.Context, string) (*ServiceHelperClient, error) {
	return nil, errors.New("privileged service helper is supported only on Linux")
}

func (*ServiceHelperClient) Descriptors() []nodes.CommandDescriptor { return nil }

func (*ServiceHelperClient) Status(context.Context, ServiceStatusRequest) (ServiceStatus, error) {
	return ServiceStatus{}, errors.New("privileged service helper is supported only on Linux")
}

func (*ServiceHelperClient) Logs(context.Context, ServiceLogRequest) (ServiceLogs, error) {
	return ServiceLogs{}, errors.New("privileged service helper is supported only on Linux")
}

func (*ServiceHelperClient) Action(context.Context, ServiceActionRequest) (ServiceActionResult, error) {
	return ServiceActionResult{}, errors.New("privileged service helper is supported only on Linux")
}

func (*ServiceHelperClient) Close() error { return nil }
