//go:build linux

package main

import (
	"strings"
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/nodes/companion"
)

func TestFileHelperAuthorityRejectsRootCompanion(t *testing.T) {
	config := companion.Config{
		FileHelper: &companion.FileHelperClientConfig{Enabled: true, SocketPath: "/run/helper.sock"},
	}
	zeroCapabilities := []byte(
		"CapInh:\t0000000000000000\n" +
			"CapPrm:\t0000000000000000\n" +
			"CapEff:\t0000000000000000\n" +
			"CapAmb:\t0000000000000000\n",
	)
	if err := validateFileHelperProcessIdentityStatus(config, 0, zeroCapabilities); err == nil {
		t.Fatal("root-run full companion was accepted with helper authority")
	}
	if err := validateFileHelperProcessIdentityStatus(config, 1000, zeroCapabilities); err != nil {
		t.Fatalf("unprivileged companion rejected: %v", err)
	}
	if err := validateFileHelperProcessIdentityStatus(companion.Config{}, 0, nil); err != nil {
		t.Fatalf("unrelated root-run companion behavior changed: %v", err)
	}
}

func TestFileHelperAuthorityRejectsLinuxCapabilities(t *testing.T) {
	config := companion.Config{
		FileHelper: &companion.FileHelperClientConfig{Enabled: true, SocketPath: "/run/helper.sock"},
	}
	for _, field := range requiredCapabilityFields {
		t.Run(field, func(t *testing.T) {
			status := "CapInh:\t0000000000000000\n" +
				"CapPrm:\t0000000000000000\n" +
				"CapEff:\t0000000000000000\n" +
				"CapAmb:\t0000000000000000\n"
			status = strings.Replace(status, field+":\t0000000000000000", field+":\t0000000000000001", 1)
			if err := validateFileHelperProcessIdentityStatus(config, 1000, []byte(status)); err == nil {
				t.Fatalf("nonzero %s was accepted", field)
			}
		})
	}
}

func TestFileHelperAuthorityRejectsMalformedCapabilityStatus(t *testing.T) {
	config := companion.Config{
		FileHelper: &companion.FileHelperClientConfig{Enabled: true, SocketPath: "/run/helper.sock"},
	}
	for name, status := range map[string]string{
		"missing":   "CapInh:\t0\nCapPrm:\t0\nCapEff:\t0\n",
		"invalid":   "CapInh:\t0\nCapPrm:\t0\nCapEff:\tnot-hex\nCapAmb:\t0\n",
		"duplicate": "CapInh:\t0\nCapPrm:\t0\nCapEff:\t0\nCapEff:\t0\nCapAmb:\t0\n",
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateFileHelperProcessIdentityStatus(config, 1000, []byte(status)); err == nil {
				t.Fatalf("%s capability status was accepted", name)
			}
		})
	}
}
