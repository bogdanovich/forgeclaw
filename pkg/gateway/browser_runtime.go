package gateway

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/browser"
	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/logger"
)

const browserStateFile = "browser.json"

// browserRuntime is the gateway-owned lifetime boundary for browser workers
// and their durable ledger. The broker is never shared across reloads because
// its policy revision and private worker ownership are immutable snapshots.
type browserRuntime struct {
	broker *browser.Broker
	store  *browser.FileStore
	cancel context.CancelFunc
	done   chan struct{}

	closeOnce sync.Once
	closeErr  error
}

func newBrowserRuntime(ctx context.Context, cfg *config.Config) (*browserRuntime, error) {
	if cfg == nil || !cfg.Tools.Browser.Enabled {
		return nil, nil
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
	runtime := &browserRuntime{broker: broker, store: store}
	if err = broker.Recover(ctx); err != nil {
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
	runtime.closeOnce.Do(func() {
		if runtime.cancel != nil {
			runtime.cancel()
		}
		if runtime.done != nil {
			<-runtime.done
		}
		if runtime.broker != nil {
			runtime.closeErr = runtime.broker.Shutdown(ctx)
		}
		if runtime.store != nil {
			runtime.store.Close()
		}
	})
	return runtime.closeErr
}

func setupBrowserRuntime(ctx context.Context, cfg *config.Config, runningServices *services) error {
	if runningServices == nil {
		return errors.New("browser runtime requires gateway services")
	}
	runtime, err := newBrowserRuntime(ctx, cfg)
	if err != nil {
		return err
	}
	runningServices.Browser = runtime
	return nil
}
