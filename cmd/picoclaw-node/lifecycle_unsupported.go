//go:build !linux

package main

import (
	"errors"
)

func newPlatformServiceLifecycle(bool) (serviceLifecycle, error) {
	return nil, errors.New("service lifecycle is not implemented on this platform")
}
