//go:build !linux

package companion

import "testing"

func TestCompanionServiceHelperConfigFailsClosedOutsideLinux(t *testing.T) {
	_, err := (Config{
		GatewayURL: "wss://gateway.example",
		ServiceHelper: &ServiceHelperClientConfig{
			Enabled: true, SocketPath: "/run/mintclaw/service-helper.sock",
		},
	}).Normalize(t.TempDir())
	if err == nil {
		t.Fatal("non-Linux companion accepted privileged service helper")
	}
}
