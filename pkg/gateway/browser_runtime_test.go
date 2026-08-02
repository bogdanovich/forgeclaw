package gateway

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/browser"
	"github.com/bogdanovich/mintclaw/pkg/config"
)

func TestBrowserRuntimeDisabledDoesNotOwnState(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = t.TempDir()
	runtime, err := newBrowserRuntime(context.Background(), cfg)
	if err != nil || runtime != nil {
		t.Fatalf("newBrowserRuntime() = %+v, %v; want disabled", runtime, err)
	}
	services := &services{}
	if err = setupBrowserRuntime(context.Background(), cfg, services); err != nil || services.Browser != nil {
		t.Fatalf("setupBrowserRuntime() error = %v, runtime = %+v", err, services.Browser)
	}
}

func TestBrowserRuntimeOwnsAndReleasesDurableStore(t *testing.T) {
	root := t.TempDir()
	cfg := gatewayBrowserConfig(root)
	runtime, err := newBrowserRuntime(context.Background(), cfg)
	if err != nil {
		t.Fatalf("newBrowserRuntime() error = %v", err)
	}
	if runtime.Broker() == nil {
		t.Fatal("newBrowserRuntime() broker = nil")
	}
	if _, secondErr := newBrowserRuntime(context.Background(), cfg); !errors.Is(secondErr, browser.ErrStoreOwned) {
		t.Fatalf("second newBrowserRuntime() error = %v, want ErrStoreOwned", secondErr)
	}
	if err = runtime.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err = runtime.Close(context.Background()); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	reopened, err := newBrowserRuntime(context.Background(), cfg)
	if err != nil {
		t.Fatalf("newBrowserRuntime() after Close error = %v", err)
	}
	if err = reopened.Close(context.Background()); err != nil {
		t.Fatalf("reopened Close() error = %v", err)
	}
}

func TestSetupBrowserRuntimeRequiresServicesOwner(t *testing.T) {
	if err := setupBrowserRuntime(context.Background(), config.DefaultConfig(), nil); err == nil {
		t.Fatal("setupBrowserRuntime() error = nil")
	}
}

func TestBrowserSweepIntervalUsesShortestAuthorityLifetime(t *testing.T) {
	if got := browserSweepInterval(config.BrowserLimitsConfig{
		IdleSeconds: 20, SessionSeconds: 60, PreparedSeconds: 5,
	}); got != 5*time.Second {
		t.Fatalf("browserSweepInterval() = %v, want 5s", got)
	}
	if got := browserSweepInterval(config.BrowserLimitsConfig{
		IdleSeconds: 600, SessionSeconds: 3600, PreparedSeconds: 300,
	}); got != 30*time.Second {
		t.Fatalf("browserSweepInterval() = %v, want 30s cap", got)
	}
}

func gatewayBrowserConfig(workspace string) *config.Config {
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = workspace
	cfg.Tools.MCP.Servers["playwright"] = config.MCPServerConfig{
		Enabled: false, Command: "npx", Type: "stdio",
		SessionLossReplay: config.MCPSessionLossReplayNever,
		ExclusiveLockFile: filepath.Join(workspace, "playwright.lock"),
	}
	cfg.Tools.Browser = config.BrowserToolsConfig{
		Enabled: true,
		Agents:  []string{"browser"},
		Targets: map[string]config.BrowserTargetConfig{
			config.BrowserDefaultTarget: {
				Enabled: true, Driver: config.BrowserDriverPlaywrightMCP, DriverServer: "playwright",
				Profiles: map[string]config.BrowserProfileConfig{
					config.BrowserDefaultProfile: {
						Enabled: true, Mode: config.BrowserProfileManaged, DryRun: true,
						AllowedOrigins: []string{"https://example.com"},
					},
				},
			},
		},
	}
	return cfg
}
