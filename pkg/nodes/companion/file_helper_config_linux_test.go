//go:build linux

package companion

import (
	"path/filepath"
	"testing"
)

func TestCompanionFileHelperConfigIsExplicitAndLinuxOnly(t *testing.T) {
	base := t.TempDir()
	config, err := (Config{
		GatewayURL: "wss://gateway.example",
		FileHelper: &FileHelperClientConfig{
			Enabled:    true,
			SocketPath: "helper.sock",
		},
	}).Normalize(base)
	if err != nil {
		t.Fatal(err)
	}
	if config.FileHelper == nil ||
		config.FileHelper.SocketPath != filepath.Join(base, "helper.sock") {
		t.Fatalf("normalized file helper = %#v", config.FileHelper)
	}
	for _, helper := range []*FileHelperClientConfig{
		{Enabled: true},
		{Enabled: false, SocketPath: "helper.sock"},
	} {
		if _, err := (Config{
			GatewayURL: "wss://gateway.example",
			FileHelper: helper,
		}).Normalize(base); err == nil {
			t.Fatalf("unsafe file helper config was accepted: %#v", helper)
		}
	}
}
