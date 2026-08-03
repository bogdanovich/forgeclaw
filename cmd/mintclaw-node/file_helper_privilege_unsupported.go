//go:build !linux

package main

import (
	"errors"

	"github.com/bogdanovich/mintclaw/pkg/nodes/companion"
)

func validateFileHelperProcessIdentity(cfg companion.Config) error {
	if cfg.FileHelper != nil || cfg.ServiceHelper != nil {
		return errors.New("node companion privileged helper authority is supported only on Linux")
	}
	return nil
}
