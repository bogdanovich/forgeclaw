package gateway

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/tools"
)

const (
	restartStatusPending   = "pending"
	restartStatusRunning   = "running"
	restartStatusFailed    = "failed"
	restartStatusSucceeded = "succeeded"

	defaultRestartPollInterval = 250 * time.Millisecond
)

type ServiceRestarter interface {
	RestartService(ctx context.Context, service string) error
}

type SystemdUserServiceRestarter struct{}

func (SystemdUserServiceRestarter) RestartService(ctx context.Context, service string) error {
	if err := validateSystemdUserService(service); err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, "systemctl", "--user", "restart", service)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("systemctl --user restart %s failed: %w: %s", service, err, strings.TrimSpace(string(output)))
	}
	return nil
}

type RestartController struct {
	cfg              config.GatewaySafeRestartConfig
	source           RestartPreflightSource
	store            *RestartSentinelStore
	restarter        ServiceRestarter
	pollInterval     time.Duration
	preflightTimeout time.Duration
	preflightOptions RestartPreflightOptions
	now              func() time.Time
	mu               sync.Mutex
}

type RestartControllerOptions struct {
	Config           config.GatewaySafeRestartConfig
	Source           RestartPreflightSource
	Store            *RestartSentinelStore
	Restarter        ServiceRestarter
	PollInterval     time.Duration
	PreflightTimeout time.Duration
	PreflightOptions RestartPreflightOptions
	Now              func() time.Time
}

type RestartRequest struct {
	Origin RestartOrigin
	Reason string
}

type RestartRequestResult struct {
	Service          string           `json:"service"`
	Status           string           `json:"status"`
	AlreadyScheduled bool             `json:"already_scheduled,omitempty"`
	ForcedAfterDrain bool             `json:"forced_after_drain,omitempty"`
	Preflight        RestartPreflight `json:"preflight"`
}

func NewRestartController(opts RestartControllerOptions) (*RestartController, error) {
	if !opts.Config.Enabled {
		return nil, errors.New("safe restart is disabled")
	}
	if opts.Source == nil {
		return nil, errors.New("restart preflight source is required")
	}
	if opts.Store == nil {
		return nil, errors.New("restart sentinel store is required")
	}
	manager := opts.Config.EffectiveServiceManager()
	if manager != "systemd-user" {
		return nil, fmt.Errorf("unsupported safe restart service manager %q", manager)
	}
	restarter := opts.Restarter
	if restarter == nil {
		restarter = SystemdUserServiceRestarter{}
	}
	service := opts.Config.EffectiveService()
	if err := validateConfiguredRestartService(manager, service); err != nil {
		return nil, err
	}
	pollInterval := opts.PollInterval
	if pollInterval <= 0 {
		pollInterval = defaultRestartPollInterval
	}
	preflightTimeout := opts.PreflightTimeout
	if preflightTimeout <= 0 {
		preflightTimeout = DefaultRestartPreflightTimeout
	}
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &RestartController{
		cfg:              opts.Config,
		source:           opts.Source,
		store:            opts.Store,
		restarter:        restarter,
		pollInterval:     pollInterval,
		preflightTimeout: preflightTimeout,
		preflightOptions: opts.PreflightOptions,
		now:              now,
	}, nil
}

func (c *RestartController) RequestRestart(
	ctx context.Context,
	req RestartRequest,
) (RestartRequestResult, error) {
	if c == nil {
		return RestartRequestResult{}, errors.New("restart controller is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if existing, err := c.store.Read(); err == nil {
		if existing.Kind == "restart" &&
			(existing.Status == restartStatusPending || existing.Status == restartStatusRunning) {
			return RestartRequestResult{
				Service:          existing.RequestedService,
				Status:           existing.Status,
				AlreadyScheduled: true,
				Preflight:        existing.Preflight,
			}, nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return RestartRequestResult{}, fmt.Errorf("read existing restart sentinel: %w", err)
	}

	service := c.cfg.EffectiveService()
	preflight := c.collectPreflight(ctx)
	sentinel := RestartSentinel{
		Kind:             "restart",
		Status:           restartStatusPending,
		RequestedService: service,
		Origin:           req.Origin,
		RequestedAt:      c.now(),
		UpdatedAt:        c.now(),
		Reason:           strings.TrimSpace(req.Reason),
		Preflight:        preflight,
	}
	if err := c.store.Write(sentinel); err != nil {
		return RestartRequestResult{}, err
	}

	go c.runRestart(context.WithoutCancel(ctx), sentinel, preflight)

	return RestartRequestResult{
		Service:   service,
		Status:    restartStatusPending,
		Preflight: preflight,
	}, nil
}

func (c *RestartController) runRestart(
	ctx context.Context,
	sentinel RestartSentinel,
	preflight RestartPreflight,
) {
	if ctx == nil {
		ctx = context.Background()
	}
	service := sentinel.RequestedService
	forced, latestPreflight, err := c.waitUntilSafe(ctx, preflight)
	if err != nil {
		sentinel.Status = restartStatusFailed
		sentinel.UpdatedAt = c.now()
		sentinel.Preflight = latestPreflight
		_ = c.store.Write(sentinel)
		return
	}

	sentinel.Status = restartStatusRunning
	sentinel.UpdatedAt = c.now()
	sentinel.Preflight = latestPreflight
	if err := c.store.Write(sentinel); err != nil {
		return
	}

	if err := c.restarter.RestartService(ctx, service); err != nil {
		sentinel.Status = restartStatusFailed
		sentinel.UpdatedAt = c.now()
		_ = c.store.Write(sentinel)
		return
	}

	sentinel.UpdatedAt = c.now()
	sentinel.ForcedAfterDrain = forced
	_ = c.store.Write(sentinel)
}

func (c *RestartController) collectPreflight(ctx context.Context) RestartPreflight {
	opts := c.preflightOptions
	opts.Now = c.now
	opts.Timeout = c.preflightTimeout
	return CollectRestartPreflight(ctx, c.source, opts)
}

func (c *RestartController) waitUntilSafe(
	ctx context.Context,
	initial RestartPreflight,
) (bool, RestartPreflight, error) {
	if !initial.HasActiveWork() {
		return false, initial, nil
	}
	timeout := time.Duration(c.cfg.EffectiveDrainTimeoutSeconds()) * time.Second
	deadline := c.now().Add(timeout)
	pollInterval := c.pollInterval
	if pollInterval <= 0 {
		pollInterval = defaultRestartPollInterval
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	latest := initial
	for {
		select {
		case <-ctx.Done():
			return false, latest, ctx.Err()
		case <-ticker.C:
			latest = c.collectPreflight(ctx)
			if !latest.HasActiveWork() {
				return false, latest, nil
			}
			if !c.now().Before(deadline) {
				if c.cfg.ForceAfterTimeout {
					return true, latest, nil
				}
				return false, latest, errors.New("restart deferred because active work did not drain before timeout")
			}
		}
	}
}

type GatewayRestartTool struct {
	controller *RestartController
}

func NewGatewayRestartTool(controller *RestartController) *GatewayRestartTool {
	return &GatewayRestartTool{controller: controller}
}

func (t *GatewayRestartTool) Name() string {
	return "gateway_restart"
}

func (t *GatewayRestartTool) Description() string {
	return "Safely restart the configured gateway service. The service manager and service name come only from gateway.safe_restart config."
}

func (t *GatewayRestartTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"reason": map[string]any{
				"type":        "string",
				"description": "Optional operator-visible reason for requesting the restart.",
			},
		},
	}
}

func (t *GatewayRestartTool) Execute(ctx context.Context, args map[string]any) *tools.ToolResult {
	if t == nil || t.controller == nil {
		return tools.ErrorResult("gateway restart failed: restart controller is not configured")
	}
	reason, _ := args["reason"].(string)
	result, err := t.controller.RequestRestart(ctx, RestartRequest{
		Origin: RestartOrigin{
			Channel:    tools.ToolChannel(ctx),
			ChatID:     tools.ToolChatID(ctx),
			SessionKey: tools.ToolSessionKey(ctx),
		},
		Reason: reason,
	})
	if err != nil {
		return tools.ErrorResult(fmt.Sprintf("gateway restart failed: %v", err)).WithError(err)
	}
	if result.AlreadyScheduled {
		message := fmt.Sprintf("Gateway restart for %s is already %s.", result.Service, result.Status)
		return tools.UserResult(message).WithImmediateDelivery()
	}
	message := fmt.Sprintf("Gateway restart scheduled for %s. It will run after active work drains.", result.Service)
	return tools.UserResult(message).WithImmediateDelivery()
}

func validateConfiguredRestartService(manager, service string) error {
	if strings.TrimSpace(service) == "" {
		return errors.New("safe restart service is required")
	}
	if manager == "systemd-user" {
		return validateSystemdUserService(service)
	}
	return nil
}

func validateSystemdUserService(service string) error {
	service = strings.TrimSpace(service)
	if service == "" {
		return errors.New("systemd user service is required")
	}
	if strings.ContainsAny(service, "/\\\x00\r\n\t ") || !isSafeSystemdServiceName(service) {
		return fmt.Errorf("invalid systemd user service %q", service)
	}
	if !strings.HasSuffix(service, ".service") {
		return fmt.Errorf("systemd user service %q must end with .service", service)
	}
	return nil
}

func isSafeSystemdServiceName(service string) bool {
	for _, r := range service {
		if r >= 'a' && r <= 'z' {
			continue
		}
		if r >= 'A' && r <= 'Z' {
			continue
		}
		if r >= '0' && r <= '9' {
			continue
		}
		switch r {
		case '.', '-', '_', '@', ':':
			continue
		default:
			return false
		}
	}
	return true
}
