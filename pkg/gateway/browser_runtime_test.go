package gateway

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
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
		ActorID: "actor_1", AgentID: browser.OpaqueAgentID("browser"),
		SessionKey: "session_1", ExecutionID: "execution_1",
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
	disabled := config.DefaultConfig()
	disabled.Agents.Defaults.Workspace = root
	if replaceErr := setupBrowserRuntime(context.Background(), disabled, services); replaceErr == nil ||
		services.Browser != runtime {
		t.Fatalf("disabled replacement error = %v, runtime = %+v", replaceErr, services.Browser)
	}
	otherWorkspace := gatewayBrowserConfig(t.TempDir())
	if replaceErr := setupBrowserRuntime(context.Background(), otherWorkspace, services); replaceErr == nil ||
		services.Browser != runtime {
		t.Fatalf("workspace replacement error = %v, runtime = %+v", replaceErr, services.Browser)
	}
	worker.closeErr = nil
	if err = closeBrowserRuntime(context.Background(), services); err != nil || services.Browser != nil {
		t.Fatalf("retry closeBrowserRuntime() error = %v, runtime = %+v", err, services.Browser)
	}
	if closeCalls := worker.closeCalls.Load(); closeCalls != 2 {
		t.Fatalf("worker close calls = %d, want 2", closeCalls)
	}
	reopened, err := browser.NewFileStore(storePath, 0, 0)
	if err != nil {
		t.Fatalf("reopen store after successful retry error = %v", err)
	}
	reopened.Close()
}

func TestBrowserRuntimeCloseHonorsCallerDeadlineAndRetainsOwnership(t *testing.T) {
	root := t.TempDir()
	cfg := gatewayBrowserConfig(root)
	storePath := filepath.Join(root, "state", "browser", browserStateFile)
	store, err := browser.NewFileStore(storePath, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	worker := &gatewayTestBrowserWorker{waitForContextCalls: 1}
	broker, err := browser.NewBroker(cfg, store, &gatewayTestBrowserFactory{worker: worker})
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	owner := browser.Owner{
		ActorID: "actor_1", AgentID: browser.OpaqueAgentID("browser"),
		SessionKey: "session_1", ExecutionID: "execution_1",
	}
	if _, err = broker.Open(context.Background(), browser.OpenRequest{
		Owner: owner, Target: config.BrowserDefaultTarget, Profile: config.BrowserDefaultProfile,
	}); err != nil {
		store.Close()
		t.Fatal(err)
	}
	runtime := &browserRuntime{broker: broker, store: store}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	started := time.Now()
	err = runtime.Close(ctx)
	cancel()
	if err == nil || time.Since(started) > time.Second {
		t.Fatalf("deadline Close() error = %v, elapsed = %v", err, time.Since(started))
	}
	if _, openErr := browser.NewFileStore(storePath, 0, 0); !errors.Is(openErr, browser.ErrStoreOwned) {
		t.Fatalf("store after deadline error = %v, want ErrStoreOwned", openErr)
	}
	if err = runtime.Close(context.Background()); err != nil {
		t.Fatalf("Close() retry error = %v", err)
	}
	reopened, err := browser.NewFileStore(storePath, 0, 0)
	if err != nil {
		t.Fatalf("reopen store after retry error = %v", err)
	}
	reopened.Close()
}

func TestBrowserRuntimeCloseDeadlineBoundsActiveToolLeaseWait(t *testing.T) {
	services := &services{Browser: &browserRuntime{}}
	services.browserMu.RLock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	started := time.Now()
	err := closeBrowserRuntime(ctx, services)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
		t.Fatalf("close while tool is active error = %v, elapsed = %v", err, time.Since(started))
	}
	if services.Browser == nil {
		t.Fatal("close while tool is active discarded runtime ownership")
	}
	services.browserMu.RUnlock()
}

func TestBrowserRuntimeCloseDeadlineBoundsSweepWait(t *testing.T) {
	root := t.TempDir()
	storePath := filepath.Join(root, "state", "browser", browserStateFile)
	store, err := browser.NewFileStore(storePath, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	_, cancelSweep := context.WithCancel(context.Background())
	runtime := &browserRuntime{store: store, cancel: cancelSweep, done: done}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	started := time.Now()
	err = runtime.Close(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
		t.Fatalf("deadline Close() error = %v, elapsed = %v", err, time.Since(started))
	}
	if _, openErr := browser.NewFileStore(storePath, 0, 0); !errors.Is(openErr, browser.ErrStoreOwned) {
		t.Fatalf("store while sweep is retained error = %v, want ErrStoreOwned", openErr)
	}
	close(done)
	if err = runtime.Close(context.Background()); err != nil {
		t.Fatalf("Close() after sweep exit error = %v", err)
	}
	reopened, err := browser.NewFileStore(storePath, 0, 0)
	if err != nil {
		t.Fatalf("reopen store after sweep exit error = %v", err)
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
	closeErr            error
	closeCalls          atomic.Int32
	waitForContextCalls int32
}

func (*gatewayTestBrowserWorker) Status(context.Context) (browser.WorkerStatus, error) {
	return browser.WorkerReady, nil
}

func (worker *gatewayTestBrowserWorker) Close(ctx context.Context) error {
	call := worker.closeCalls.Add(1)
	if call <= worker.waitForContextCalls {
		<-ctx.Done()
		return ctx.Err()
	}
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
