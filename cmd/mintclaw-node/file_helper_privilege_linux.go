//go:build linux

package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/bogdanovich/mintclaw/pkg/nodes/companion"
)

const maxProcessStatusBytes = 64 * 1024

var requiredCapabilityFields = [...]string{"CapPrm", "CapEff", "CapAmb"}

func validateFileHelperProcessIdentity(cfg companion.Config) error {
	if cfg.FileHelper == nil {
		return nil
	}
	statusFile, err := os.Open("/proc/self/status")
	if err != nil {
		return fmt.Errorf("inspect node companion process capabilities: %w", err)
	}
	defer statusFile.Close()
	status, err := io.ReadAll(io.LimitReader(statusFile, maxProcessStatusBytes+1))
	if err != nil {
		return fmt.Errorf("inspect node companion process capabilities: %w", err)
	}
	if len(status) > maxProcessStatusBytes {
		return errors.New("node companion process status exceeds the inspection limit")
	}
	return validateFileHelperProcessIdentityStatus(cfg, os.Geteuid(), status)
}

func validateFileHelperProcessIdentityStatus(
	cfg companion.Config,
	effectiveUID int,
	status []byte,
) error {
	if cfg.FileHelper == nil {
		return nil
	}
	if effectiveUID == 0 {
		return errors.New("node companion with file helper authority must remain unprivileged")
	}
	capabilities := make(map[string]uint64, len(requiredCapabilityFields))
	scanner := bufio.NewScanner(bytes.NewReader(status))
	for scanner.Scan() {
		name, value, found := strings.Cut(scanner.Text(), ":")
		if !found {
			continue
		}
		for _, required := range requiredCapabilityFields {
			if name != required {
				continue
			}
			if _, duplicate := capabilities[name]; duplicate {
				return fmt.Errorf("node companion process status repeats %s", name)
			}
			parsed, err := strconv.ParseUint(strings.TrimSpace(value), 16, 64)
			if err != nil {
				return fmt.Errorf("node companion process status has invalid %s", name)
			}
			capabilities[name] = parsed
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("inspect node companion process capabilities: %w", err)
	}
	for _, name := range requiredCapabilityFields {
		value, found := capabilities[name]
		if !found {
			return fmt.Errorf("node companion process status is missing %s", name)
		}
		if value != 0 {
			return fmt.Errorf("node companion with file helper authority must not retain %s capabilities", name)
		}
	}
	return nil
}
