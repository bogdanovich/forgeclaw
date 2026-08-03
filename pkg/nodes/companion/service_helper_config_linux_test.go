//go:build linux

package companion

import (
	"path/filepath"
	"testing"
)

func TestCompanionServiceHelperConfigIsExplicitAndExclusive(t *testing.T) {
	base := t.TempDir()
	config, err := (Config{
		GatewayURL: "wss://gateway.example",
		ServiceHelper: &ServiceHelperClientConfig{
			Enabled: true, SocketPath: "service-helper.sock",
		},
	}).Normalize(base)
	if err != nil {
		t.Fatal(err)
	}
	if config.ServiceHelper == nil ||
		config.ServiceHelper.SocketPath != filepath.Join(base, "service-helper.sock") {
		t.Fatalf("normalized service helper = %#v", config.ServiceHelper)
	}
	profile := servicePolicyFixture()
	if _, err := (Config{
		GatewayURL: "wss://gateway.example",
		ServiceHelper: &ServiceHelperClientConfig{
			Enabled: true, SocketPath: "service-helper.sock",
		},
		ServicePolicies: ServicePolicies{"server-services": profile},
	}).Normalize(base); err == nil {
		t.Fatal("companion accepted two service authority sources")
	}
}
