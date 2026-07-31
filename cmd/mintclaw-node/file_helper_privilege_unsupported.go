//go:build !linux

package main

import (
	"errors"

	"github.com/bogdanovich/mintclaw/pkg/nodes/companion"
)

func validateFileHelperProcessIdentity(cfg companion.Config) error {
	if cfg.FileHelper != nil {
		return errors.New("node companion file helper authority is supported only on Linux")
	}
	return nil
}
