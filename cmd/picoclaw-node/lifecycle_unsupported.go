//go:build !linux && !darwin

package main

import (
	"errors"
)

func newPlatformServiceLifecycle(bool) (serviceLifecycle, error) {
	return nil, errors.New("service lifecycle is not implemented on this platform")
}

func validatePlatformServiceAction(string) error {
	return errors.New("service lifecycle is not implemented on this platform")
}
