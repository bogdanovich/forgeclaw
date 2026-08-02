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
	services := &services{Browser: runtime}
	if err = closeBrowserRuntime(context.Background(), services); err != nil {
		t.Fatalf("closeBrowserRuntime() error = %v", err)
	}
	if services.Browser != nil {
		t.Fatal("closeBrowserRuntime() retained closed runtime")
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

func TestBrowserRuntimeRetainsOwnershipUntilWorkerShutdownSucceeds(t *testing.T) {
	root := t.TempDir()
	cfg := gatewayBrowserConfig(root)
	storePath := filepath.Join(root, "state", "browser", browserStateFile)
	store, err := browser.NewFileStore(storePath, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	worker := &gatewayTestBrowserWorker{closeErr: errors.New("still running")}
	broker, err := browser.NewBroker(cfg, store, &gatewayTestBrowserFactory{worker: worker})
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	owner := browser.Owner{
		ActorID: "actor_1", AgentID: "browser", SessionKey: "session_1", ExecutionID: "execution_1",
	}
	if _, err = broker.Open(context.Background(), browser.OpenRequest{
		Owner: owner, Target: config.BrowserDefaultTarget, Profile: config.BrowserDefaultProfile,
	}); err != nil {
		store.Close()
		t.Fatal(err)
	}
	runtime := &browserRuntime{broker: broker, store: store}
	services := &services{Browser: runtime}
	if err = closeBrowserRuntime(context.Background(), services); err == nil || services.Browser != runtime {
		t.Fatalf("first closeBrowserRuntime() error = %v, runtime = %+v", err, services.Browser)
	}
	if _, openErr := browser.NewFileStore(storePath, 0, 0); !errors.Is(openErr, browser.ErrStoreOwned) {
		t.Fatalf("store after failed shutdown error = %v, want ErrStoreOwned", openErr)
	}
	worker.closeErr = nil
	if err = closeBrowserRuntime(context.Background(), services); err != nil || services.Browser != nil {
		t.Fatalf("retry closeBrowserRuntime() error = %v, runtime = %+v", err, services.Browser)
	}
	if worker.closeCalls != 2 {
		t.Fatalf("worker close calls = %d, want 2", worker.closeCalls)
	}
	reopened, err := browser.NewFileStore(storePath, 0, 0)
	if err != nil {
		t.Fatalf("reopen store after successful retry error = %v", err)
	}
	reopened.Close()
}

func TestChannelStartupRollbackReleasesBrowserStore(t *testing.T) {
	root := t.TempDir()
	cfg := gatewayBrowserConfig(root)
	runtime, err := newBrowserRuntime(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	services := &services{Browser: runtime}
	if err = rollbackBrowserRuntime(services); err != nil || services.Browser != nil {
		t.Fatalf("rollbackBrowserRuntime() error = %v, runtime = %+v", err, services.Browser)
	}
	reopened, err := newBrowserRuntime(context.Background(), cfg)
	if err != nil {
		t.Fatalf("newBrowserRuntime() after startup rollback error = %v", err)
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

type gatewayTestBrowserWorker struct {
	closeErr   error
	closeCalls int
}

func (*gatewayTestBrowserWorker) Status(context.Context) (browser.WorkerStatus, error) {
	return browser.WorkerReady, nil
}

func (worker *gatewayTestBrowserWorker) Close(context.Context) error {
	worker.closeCalls++
	return worker.closeErr
}

type gatewayTestBrowserFactory struct {
	worker browser.Worker
}

func (factory *gatewayTestBrowserFactory) Open(
	context.Context,
	browser.WorkerOpenRequest,
) (browser.WorkerOpenResult, error) {
	return browser.WorkerOpenResult{Owner: factory.worker}, nil
}
