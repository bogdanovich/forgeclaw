//go:build !linux

package companion

import "errors"

func NewSystemdServiceManager(ServicePolicies) (ServiceManager, error) {
	return nil, errors.New("systemd service reads are supported only on Linux")
}
