package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/agent"
	"github.com/bogdanovich/mintclaw/pkg/browser"
	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/logger"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/tools"
)

const browserStateFile = "browser.json"

// browserRuntime is the gateway-owned lifetime boundary for browser workers
// and their durable ledger. The broker is never shared across reloads because
// its policy revision and private worker ownership are immutable snapshots.
type browserRuntime struct {
	broker         *browser.Broker
	store          *browser.FileStore
	policyRevision string
	cancel         context.CancelFunc
	done           chan struct{}

	stopOnce sync.Once
	closeMu  sync.Mutex
	shutdown chan error
	closed   bool
}

func newBrowserRuntime(
	ctx context.Context,
	cfg *config.Config,
	recovery ...browser.DownloadRecoveryVerifier,
) (*browserRuntime, error) {
	if cfg == nil || !cfg.Tools.Browser.Enabled {
		return nil, nil
	}
	policyRevision, err := cfg.Tools.Browser.PolicyRevision()
	if err != nil {
		return nil, errors.New("browser policy unavailable")
	}
	store, err := browser.NewFileStore(
		filepath.Join(cfg.WorkspacePath(), "state", "browser", browserStateFile),
		0,
		0,
	)
	if err != nil {
		if errors.Is(err, browser.ErrStoreOwned) {
			return nil, fmt.Errorf("browser state unavailable: %w", browser.ErrStoreOwned)
		}
		return nil, errors.New("browser state unavailable")
	}
	factory, err := browser.NewPlaywrightWorkerFactory(cfg)
	if err != nil {
		store.Close()
		return nil, errors.New("browser driver unavailable")
	}
	broker, err := browser.NewBroker(cfg, store, factory)
	if err != nil {
		store.Close()
		return nil, errors.New("browser policy unavailable")
	}
	runtime := &browserRuntime{broker: broker, store: store, policyRevision: policyRevision}
	if err = broker.Recover(ctx, recovery...); err != nil {
		store.Close()
		return nil, errors.New("browser recovery unavailable")
	}
	if err = broker.Sweep(ctx); err != nil {
		store.Close()
		return nil, errors.New("browser state sweep unavailable")
	}
	sweepCtx, cancel := context.WithCancel(context.Background())
	runtime.cancel = cancel
	runtime.done = make(chan struct{})
	go runtime.sweep(sweepCtx, browserSweepInterval(cfg.Tools.Browser.Limits.Effective()))
	return runtime, nil
}

func browserSweepInterval(limits config.BrowserLimitsConfig) time.Duration {
	interval := 30 * time.Second
	for _, seconds := range []int{limits.IdleSeconds, limits.SessionSeconds, limits.PreparedSeconds} {
		candidate := time.Duration(seconds) * time.Second
		if candidate > 0 && candidate < interval {
			interval = candidate
		}
	}
	return interval
}

func (runtime *browserRuntime) sweep(ctx context.Context, interval time.Duration) {
	defer close(runtime.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := runtime.broker.Sweep(ctx); err != nil && ctx.Err() == nil {
				logger.WarnCF("browser", "Browser state sweep failed", map[string]any{
					"reason": "state_unavailable",
				})
			}
		}
	}
}

func (runtime *browserRuntime) Broker() *browser.Broker {
	if runtime == nil {
		return nil
	}
	return runtime.broker
}

func (runtime *browserRuntime) Close(ctx context.Context) error {
	if runtime == nil {
		return nil
	}
	runtime.closeMu.Lock()
	defer runtime.closeMu.Unlock()
	if runtime.closed {
		return nil
	}
	runtime.stopOnce.Do(func() {
		if runtime.cancel != nil {
			runtime.cancel()
		}
	})
	if runtime.done != nil {
		select {
		case <-runtime.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for runtime.broker != nil {
		if runtime.shutdown == nil {
			runtime.shutdown = make(chan error, 1)
			shutdown := runtime.shutdown
			go func() { shutdown <- runtime.broker.Shutdown(ctx) }()
		}
		select {
		case err := <-runtime.shutdown:
			runtime.shutdown = nil
			if err == nil {
				runtime.broker = nil
				break
			}
			if (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) &&
				ctx.Err() == nil {
				continue
			}
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if runtime.store != nil {
		runtime.store.Close()
	}
	runtime.closed = true
	return nil
}

func setupBrowserRuntime(ctx context.Context, cfg *config.Config, runningServices *services) error {
	if runningServices == nil {
		return errors.New("browser runtime requires gateway services")
	}
	runningServices.browserMu.Lock()
	defer runningServices.browserMu.Unlock()
	if runningServices.Browser != nil {
		return errors.New("previous browser runtime still owns resources")
	}
	policyRevision, policyErr := cfg.Tools.Browser.PolicyRevision()
	if policyErr != nil && cfg.Tools.Browser.Enabled {
		return errors.New("browser policy unavailable")
	}
	recoverySource := &gatewayBrowserToolSource{
		services: runningServices, policyRevision: policyRevision, workspace: cfg.WorkspacePath(),
		limits: cfg.Tools.Browser.Limits.Effective(),
	}
	runtime, err := newBrowserRuntime(ctx, cfg, recoverySource.committedBrowserDownload)
	if err != nil {
		return err
	}
	runningServices.Browser = runtime
	return nil
}

type gatewayBrowserToolSource struct {
	services            *services
	policyRevision      string
	workspace           string
	screenshotRetention time.Duration
	screenshotCopy      browserScreenshotCopyFunc
	limits              config.BrowserLimitsConfig
	downloadAvailable   bool
}

func (source *gatewayBrowserToolSource) Available() bool {
	if source == nil || source.services == nil {
		return false
	}
	source.services.browserMu.RLock()
	defer source.services.browserMu.RUnlock()
	runtime := source.services.Browser
	return runtime != nil && runtime.policyRevision == source.policyRevision && runtime.Broker() != nil
}

func (source *gatewayBrowserToolSource) ProfileAvailability(
	ctx context.Context,
	target string,
	profile string,
) (browser.ProfileAvailability, error) {
	return withGatewayBrowserBroker(
		ctx,
		source,
		func(ctx context.Context, broker *browser.Broker) (browser.ProfileAvailability, error) {
			return broker.ProfileAvailability(ctx, target, profile)
		},
	)
}

func (source *gatewayBrowserToolSource) PassiveTargetDiagnostics(
	ctx context.Context,
	target string,
	profiles []string,
) (tools.BrowserTargetDiagnostics, error) {
	return withGatewayBrowserBroker(
		ctx,
		source,
		func(ctx context.Context, broker *browser.Broker) (tools.BrowserTargetDiagnostics, error) {
			uploadAvailable := source.ArtifactTransferAvailable()
			result := tools.BrowserTargetDiagnostics{
				Profiles:   make(map[string]browser.PassiveReadiness, len(profiles)),
				Screenshot: source.ScreenshotAvailable(),
				Upload:     uploadAvailable,
				Download:   uploadAvailable && source.DownloadAvailable(),
			}
			for _, profile := range profiles {
				readiness, err := broker.PassiveReadiness(ctx, target, profile)
				if err != nil {
					return tools.BrowserTargetDiagnostics{}, err
				}
				result.Profiles[profile] = readiness
			}
			return result, nil
		},
	)
}

func withGatewayBrowserBroker[T any](
	ctx context.Context,
	source *gatewayBrowserToolSource,
	operation func(context.Context, *browser.Broker) (T, error),
) (T, error) {
	var zero T
	if source == nil || source.services == nil || operation == nil {
		return zero, browser.ErrWorkerUnavailable
	}
	source.services.browserMu.RLock()
	defer source.services.browserMu.RUnlock()
	runtime := source.services.Browser
	if runtime == nil || runtime.Broker() == nil {
		return zero, browser.ErrWorkerUnavailable
	}
	if runtime.policyRevision != source.policyRevision {
		return zero, browser.ErrDenied
	}
	return operation(ctx, runtime.Broker())
}

func (source *gatewayBrowserToolSource) Open(
	ctx context.Context,
	request browser.OpenRequest,
) (browser.Session, error) {
	return withGatewayBrowserBroker(
		ctx,
		source,
		func(ctx context.Context, broker *browser.Broker) (browser.Session, error) {
			return broker.Open(ctx, request)
		},
	)
}

func (source *gatewayBrowserToolSource) Status(
	ctx context.Context,
	owner browser.Owner,
	sessionID string,
) (browser.Session, error) {
	return withGatewayBrowserBroker(
		ctx,
		source,
		func(ctx context.Context, broker *browser.Broker) (browser.Session, error) {
			return broker.Status(ctx, owner, sessionID)
		},
	)
}

func (source *gatewayBrowserToolSource) Close(
	ctx context.Context,
	owner browser.Owner,
	sessionID string,
) (browser.Session, error) {
	return withGatewayBrowserBroker(
		ctx,
		source,
		func(ctx context.Context, broker *browser.Broker) (browser.Session, error) {
			return broker.Close(ctx, owner, sessionID)
		},
	)
}

func (source *gatewayBrowserToolSource) Observe(
	ctx context.Context,
	owner browser.Owner,
	sessionID string,
	tabID string,
) (browser.Observation, error) {
	return withGatewayBrowserBroker(
		ctx,
		source,
		func(ctx context.Context, broker *browser.Broker) (browser.Observation, error) {
			return broker.Observe(ctx, owner, sessionID, tabID)
		},
	)
}

func (source *gatewayBrowserToolSource) PrepareAction(
	ctx context.Context,
	request browser.PrepareActionRequest,
) (browser.Preparation, error) {
	if request.Action.Kind == browser.ActionDownload {
		recovered, recoverErr := withGatewayBrowserBroker(
			ctx, source,
			func(ctx context.Context, broker *browser.Broker) (browser.Preparation, error) {
				preparation, found, err := broker.RecoverableDownloadPreparation(ctx, request)
				if err != nil || !found {
					return browser.Preparation{}, err
				}
				return preparation, nil
			},
		)
		if recoverErr == nil && recovered.Action.ID != "" {
			_, artifactFound, artifactErr := source.lookupBrowserDownload(
				ctx, request.Owner, request.RequestID, request.SessionID, request.Action.Deliver,
			)
			if artifactErr != nil {
				return browser.Preparation{}, artifactErr
			}
			if artifactFound {
				return recovered, nil
			}
		}
		if recoverErr != nil && !errors.Is(recoverErr, browser.ErrNotFound) {
			return browser.Preparation{}, recoverErr
		}
	}
	if request.Action.Kind == browser.ActionUpload {
		binding, err := source.resolveBrowserUpload(ctx, request)
		if err != nil {
			return browser.Preparation{}, err
		}
		request.Upload = &binding
	}
	return withGatewayBrowserBroker(
		ctx,
		source,
		func(ctx context.Context, broker *browser.Broker) (browser.Preparation, error) {
			return broker.PrepareAction(ctx, request)
		},
	)
}

func (source *gatewayBrowserToolSource) ExecuteAction(
	ctx context.Context,
	owner browser.Owner,
	preparedID string,
	approval *browser.ApprovalBinding,
) (browser.Invocation, error) {
	return withGatewayBrowserBroker(
		ctx,
		source,
		func(ctx context.Context, broker *browser.Broker) (browser.Invocation, error) {
			prepared, err := broker.PreparedAction(ctx, owner, preparedID)
			if err != nil {
				return browser.Invocation{}, err
			}
			var artifact *browser.DownloadArtifact
			if prepared.Action.Kind == browser.ActionDownload {
				retained, found, lookupErr := source.lookupBrowserDownload(
					ctx, owner, prepared.RequestID, prepared.SessionID, prepared.Action.Deliver,
				)
				if lookupErr != nil {
					return browser.Invocation{}, lookupErr
				}
				if found {
					terminal, marshalErr := json.Marshal(map[string]any{"status": "completed", "artifact": retained})
					if marshalErr != nil {
						return browser.Invocation{}, marshalErr
					}
					recovered, recoverErr := broker.RecoverAcceptedDownload(ctx, owner, preparedID, terminal)
					if recoverErr == nil {
						recovered.Download = &retained
						return recovered, nil
					}
					if !errors.Is(recoverErr, browser.ErrConflict) {
						return recovered, recoverErr
					}
					artifact = &retained
				}
			}
			invocation, executeErr := broker.ExecuteActionWithDownloadSink(
				ctx,
				owner,
				preparedID,
				approval,
				func(sinkCtx context.Context, action browser.PreparedAction, download browser.DriverDownload) (json.RawMessage, error) {
					retained, retainErr := source.retainBrowserDownload(sinkCtx, action, download)
					if retainErr != nil {
						return nil, retainErr
					}
					artifact = &retained
					return json.Marshal(map[string]any{"status": "completed", "artifact": retained})
				},
			)
			if executeErr == nil && prepared.Action.Kind == browser.ActionDownload {
				if artifact == nil {
					retained, found, lookupErr := source.lookupBrowserDownload(
						ctx, owner, prepared.RequestID, prepared.SessionID, prepared.Action.Deliver,
					)
					if lookupErr != nil {
						return invocation, lookupErr
					}
					if found {
						artifact = &retained
					}
				}
				invocation.Download = artifact
			}
			return invocation, executeErr
		},
	)
}

func setupBrowserTools(cfg *config.Config, agentLoop *agent.AgentLoop, runningServices *services) error {
	if cfg == nil || agentLoop == nil || runningServices == nil {
		return nil
	}
	sourceFor := func(reloadCfg *config.Config) (*gatewayBrowserToolSource, error) {
		if reloadCfg == nil {
			return nil, errors.New("browser tool policy is unavailable")
		}
		policyRevision, err := reloadCfg.Tools.Browser.PolicyRevision()
		if err != nil {
			return nil, errors.New("browser tool policy is unavailable")
		}
		return &gatewayBrowserToolSource{
			services: runningServices, policyRevision: policyRevision,
			workspace: reloadCfg.WorkspacePath(),
			screenshotRetention: browserScreenshotRetention(
				reloadCfg.Tools.Browser.Limits.Effective().RetentionSecs,
			),
			limits:            reloadCfg.Tools.Browser.Limits.Effective(),
			downloadAvailable: browser.PlaywrightDownloadAvailable(reloadCfg),
		}, nil
	}
	factories := map[string]agent.RuntimeToolFactory{
		"browser_targets": func(reloadCfg *config.Config) (tools.Tool, error) {
			source, err := sourceFor(reloadCfg)
			if err != nil {
				return nil, err
			}
			return tools.NewBrowserTargetsTool(reloadCfg, source), nil
		},
		"browser_session": func(reloadCfg *config.Config) (tools.Tool, error) {
			source, err := sourceFor(reloadCfg)
			if err != nil {
				return nil, err
			}
			return tools.NewBrowserSessionTool(reloadCfg, source), nil
		},
		"browser_observe": func(reloadCfg *config.Config) (tools.Tool, error) {
			source, err := sourceFor(reloadCfg)
			if err != nil {
				return nil, err
			}
			return tools.NewBrowserObserveTool(reloadCfg, source), nil
		},
		"browser_act": func(reloadCfg *config.Config) (tools.Tool, error) {
			source, err := sourceFor(reloadCfg)
			if err != nil {
				return nil, err
			}
			return tools.NewBrowserActTool(reloadCfg, source), nil
		},
	}
	for _, name := range []string{"browser_targets", "browser_session", "browser_observe", "browser_act"} {
		if err := agentLoop.RegisterRuntimeTool(name, factories[name]); err != nil {
			return err
		}
	}
	return nil
}

func browserScreenshotRetention(seconds int) time.Duration {
	retention := time.Duration(seconds) * time.Second
	if retention <= 0 || retention > nodes.MaxGatewayTransferLifetime {
		return nodes.MaxGatewayTransferLifetime
	}
	return retention
}
