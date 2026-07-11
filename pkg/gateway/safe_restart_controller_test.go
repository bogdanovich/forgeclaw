package gateway

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/tools"
)

type restartSourceSequence struct {
	mu      sync.Mutex
	pending [][]bus.InboundMessage
	stats   bus.MessageBusStats
}

func (s *restartSourceSequence) Stats() bus.MessageBusStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

func (s *restartSourceSequence) PendingInboundSpool(context.Context) ([]bus.InboundMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.pending) == 0 {
		return nil, nil
	}
	next := s.pending[0]
	if len(s.pending) > 1 {
		s.pending = s.pending[1:]
	}
	return next, nil
}

type fakeServiceRestarter struct {
	mu       sync.Mutex
	services []string
	err      error
	called   chan string
}

func (r *fakeServiceRestarter) RestartService(_ context.Context, service string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.services = append(r.services, service)
	if r.called != nil {
		select {
		case r.called <- service:
		default:
		}
	}
	return r.err
}

func (r *fakeServiceRestarter) calledWith(service string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.services) == 1 && r.services[0] == service
}

func (r *fakeServiceRestarter) waitCalledWith(t *testing.T, service string) {
	t.Helper()
	if r.called == nil {
		t.Fatal("fake restarter called channel is nil")
	}
	select {
	case got := <-r.called:
		if got != service {
			t.Fatalf("RestartService called with %q, want %q", got, service)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("RestartService was not called with %q", service)
	}
}

func testRestartConfig() config.GatewaySafeRestartConfig {
	return config.GatewaySafeRestartConfig{
		Enabled:             true,
		ServiceManager:      "systemd-user",
		Service:             "picoclaw-main.service",
		DrainTimeoutSeconds: 1,
		ForceAfterTimeout:   true,
	}
}

func knownPreflightOptions() RestartPreflightOptions {
	return RestartPreflightOptions{
		ActiveTurnsAvailable: true,
		CronJobsAvailable:    true,
	}
}

func TestNewRestartControllerRejectsDisabledConfig(t *testing.T) {
	store, err := NewRestartSentinelStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewRestartSentinelStore() error = %v", err)
	}
	_, err = NewRestartController(RestartControllerOptions{
		Config: config.GatewaySafeRestartConfig{},
		Source: &restartSourceSequence{},
		Store:  store,
	})
	if err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("NewRestartController() error = %v, want disabled", err)
	}
}

func TestNewRestartControllerRejectsUnsupportedManager(t *testing.T) {
	store, err := NewRestartSentinelStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewRestartSentinelStore() error = %v", err)
	}
	cfg := testRestartConfig()
	cfg.ServiceManager = "launchd"
	_, err = NewRestartController(RestartControllerOptions{
		Config: cfg,
		Source: &restartSourceSequence{},
		Store:  store,
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("NewRestartController() error = %v, want unsupported", err)
	}
}

func TestNewRestartControllerValidatesSystemdService(t *testing.T) {
	store, err := NewRestartSentinelStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewRestartSentinelStore() error = %v", err)
	}
	cfg := testRestartConfig()
	cfg.Service = "picoclaw-main.service;rm"
	_, err = NewRestartController(RestartControllerOptions{
		Config:    cfg,
		Source:    &restartSourceSequence{},
		Store:     store,
		Restarter: &fakeServiceRestarter{},
	})
	if err == nil || !strings.Contains(err.Error(), "invalid systemd user service") {
		t.Fatalf("NewRestartController() error = %v, want invalid service", err)
	}
}

func TestRestartControllerSafePathWritesSentinelAndRestarts(t *testing.T) {
	store, err := NewRestartSentinelStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewRestartSentinelStore() error = %v", err)
	}
	restarter := &fakeServiceRestarter{called: make(chan string, 1)}
	controller, err := NewRestartController(RestartControllerOptions{
		Config:           testRestartConfig(),
		Source:           &restartSourceSequence{},
		Store:            store,
		Restarter:        restarter,
		PreflightOptions: knownPreflightOptions(),
	})
	if err != nil {
		t.Fatalf("NewRestartController() error = %v", err)
	}

	result, err := controller.RequestRestart(context.Background(), RestartRequest{
		Origin: RestartOrigin{Channel: "telegram", ChatID: "chat-1", SessionKey: "s1"},
		Reason: "test restart",
	})
	if err != nil {
		t.Fatalf("RequestRestart() error = %v", err)
	}
	if result.Status != restartStatusPending {
		t.Fatalf("status = %q, want %q", result.Status, restartStatusPending)
	}
	restarter.waitCalledWith(t, "picoclaw-main.service")
	waitForRestartSentinelStatus(t, store, restartStatusRunning)
	if !restarter.calledWith("picoclaw-main.service") {
		t.Fatalf("restarter calls = %#v, want configured service", restarter.services)
	}
	sentinel, err := store.Read()
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if sentinel.Status != restartStatusRunning {
		t.Fatalf("sentinel status = %q, want %q", sentinel.Status, restartStatusRunning)
	}
	if sentinel.Origin.ChatID != "chat-1" {
		t.Fatalf("sentinel origin = %#v", sentinel.Origin)
	}
}

func TestRestartControllerDefersUntilIdle(t *testing.T) {
	store, err := NewRestartSentinelStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewRestartSentinelStore() error = %v", err)
	}
	source := &restartSourceSequence{
		pending: [][]bus.InboundMessage{{{SpoolID: "pending"}}, nil},
	}
	restarter := &fakeServiceRestarter{called: make(chan string, 1)}
	controller, err := NewRestartController(RestartControllerOptions{
		Config:           testRestartConfig(),
		Source:           source,
		Store:            store,
		Restarter:        restarter,
		PollInterval:     time.Millisecond,
		PreflightOptions: knownPreflightOptions(),
	})
	if err != nil {
		t.Fatalf("NewRestartController() error = %v", err)
	}

	result, err := controller.RequestRestart(context.Background(), RestartRequest{})
	if err != nil {
		t.Fatalf("RequestRestart() error = %v", err)
	}
	if result.ForcedAfterDrain {
		t.Fatal("restart should not force when work drains")
	}
	restarter.waitCalledWith(t, "picoclaw-main.service")
	if !restarter.calledWith("picoclaw-main.service") {
		t.Fatalf("restarter calls = %#v, want configured service", restarter.services)
	}
}

func TestRestartControllerForcesAfterDrainTimeout(t *testing.T) {
	store, err := NewRestartSentinelStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewRestartSentinelStore() error = %v", err)
	}
	now := time.Date(2026, 7, 9, 1, 0, 0, 0, time.UTC)
	restarter := &fakeServiceRestarter{called: make(chan string, 1)}
	controller, err := NewRestartController(RestartControllerOptions{
		Config: testRestartConfig(),
		Source: &restartSourceSequence{
			pending: [][]bus.InboundMessage{{{SpoolID: "pending"}}},
		},
		Store:            store,
		Restarter:        restarter,
		PollInterval:     time.Millisecond,
		PreflightOptions: knownPreflightOptions(),
		Now: func() time.Time {
			now = now.Add(2 * time.Second)
			return now
		},
	})
	if err != nil {
		t.Fatalf("NewRestartController() error = %v", err)
	}

	result, err := controller.RequestRestart(context.Background(), RestartRequest{})
	if err != nil {
		t.Fatalf("RequestRestart() error = %v", err)
	}
	if result.Status != restartStatusPending {
		t.Fatalf("status = %q, want %q", result.Status, restartStatusPending)
	}
	restarter.waitCalledWith(t, "picoclaw-main.service")
	waitForRestartSentinelForcedAfterDrain(t, store)
	sentinel, err := store.Read()
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if !sentinel.ForcedAfterDrain {
		t.Fatal("restart should force after drain timeout")
	}
}

func TestRestartControllerFailsWhenDrainTimesOutWithoutForce(t *testing.T) {
	store, err := NewRestartSentinelStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewRestartSentinelStore() error = %v", err)
	}
	cfg := testRestartConfig()
	cfg.ForceAfterTimeout = false
	now := time.Date(2026, 7, 9, 1, 0, 0, 0, time.UTC)
	restarter := &fakeServiceRestarter{called: make(chan string, 1)}
	controller, err := NewRestartController(RestartControllerOptions{
		Config: cfg,
		Source: &restartSourceSequence{
			pending: [][]bus.InboundMessage{{{SpoolID: "pending"}}},
		},
		Store:            store,
		Restarter:        restarter,
		PollInterval:     time.Millisecond,
		PreflightOptions: knownPreflightOptions(),
		Now: func() time.Time {
			now = now.Add(2 * time.Second)
			return now
		},
	})
	if err != nil {
		t.Fatalf("NewRestartController() error = %v", err)
	}

	result, err := controller.RequestRestart(context.Background(), RestartRequest{})
	if err != nil {
		t.Fatalf("RequestRestart() error = %v", err)
	}
	if result.Status != restartStatusPending {
		t.Fatalf("status = %q, want %q", result.Status, restartStatusPending)
	}
	waitForRestartSentinelStatus(t, store, restartStatusFailed)
	sentinel, readErr := store.Read()
	if readErr != nil {
		t.Fatalf("Read() error = %v", readErr)
	}
	if sentinel.Status != restartStatusFailed {
		t.Fatalf("sentinel status = %q, want %q", sentinel.Status, restartStatusFailed)
	}
}

func TestGatewayRestartToolReportsControllerErrors(t *testing.T) {
	tool := NewGatewayRestartTool(nil)

	ctx := tools.WithToolContext(context.Background(), "telegram", "chat-1")
	got := tool.Execute(ctx, map[string]any{"reason": "test"})
	if !got.IsError {
		t.Fatal("tool result should be an error")
	}
	if !strings.Contains(got.ForLLM, "not configured") {
		t.Fatalf("tool result = %q, want not configured", got.ForLLM)
	}
}

func TestRestartControllerBackgroundFailureWritesFailedSentinel(t *testing.T) {
	store, err := NewRestartSentinelStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewRestartSentinelStore() error = %v", err)
	}
	restarter := &fakeServiceRestarter{
		err:    errors.New("boom"),
		called: make(chan string, 1),
	}
	controller, err := NewRestartController(RestartControllerOptions{
		Config:           testRestartConfig(),
		Source:           &restartSourceSequence{},
		Store:            store,
		Restarter:        restarter,
		PollInterval:     time.Millisecond,
		PreflightOptions: knownPreflightOptions(),
		Now:              func() time.Time { return time.Date(2026, 7, 9, 1, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("NewRestartController() error = %v", err)
	}

	result, err := controller.RequestRestart(context.Background(), RestartRequest{})
	if err != nil {
		t.Fatalf("RequestRestart() error = %v", err)
	}
	if result.Status != restartStatusPending {
		t.Fatalf("status = %q, want %q", result.Status, restartStatusPending)
	}
	restarter.waitCalledWith(t, "picoclaw-main.service")
	waitForRestartSentinelStatus(t, store, restartStatusFailed)
}

func TestRestartControllerDoesNotScheduleOverlappingRestart(t *testing.T) {
	store, err := NewRestartSentinelStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewRestartSentinelStore() error = %v", err)
	}
	restarter := &fakeServiceRestarter{called: make(chan string, 2)}
	controller, err := NewRestartController(RestartControllerOptions{
		Config:           testRestartConfig(),
		Source:           &restartSourceSequence{pending: [][]bus.InboundMessage{{{SpoolID: "pending"}}}},
		Store:            store,
		Restarter:        restarter,
		PollInterval:     time.Hour,
		PreflightOptions: knownPreflightOptions(),
	})
	if err != nil {
		t.Fatalf("NewRestartController() error = %v", err)
	}

	first, err := controller.RequestRestart(context.Background(), RestartRequest{Reason: "first"})
	if err != nil {
		t.Fatalf("first RequestRestart() error = %v", err)
	}
	if first.AlreadyScheduled {
		t.Fatal("first restart unexpectedly reported already scheduled")
	}
	second, err := controller.RequestRestart(context.Background(), RestartRequest{Reason: "second"})
	if err != nil {
		t.Fatalf("second RequestRestart() error = %v", err)
	}
	if !second.AlreadyScheduled {
		t.Fatal("second restart should report the existing pending restart")
	}
	sentinel, err := store.Read()
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if sentinel.Reason != "first" {
		t.Fatalf("sentinel reason = %q, want first request to remain canonical", sentinel.Reason)
	}
	select {
	case <-restarter.called:
		t.Fatal("restart should still be waiting for the original request to drain")
	default:
	}
}

func waitForRestartSentinelStatus(t *testing.T, store *RestartSentinelStore, status string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		sentinel, err := store.Read()
		if err == nil && sentinel.Status == status {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	sentinel, err := store.Read()
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	t.Fatalf("sentinel status = %q, want %q", sentinel.Status, status)
}

func waitForRestartSentinelForcedAfterDrain(t *testing.T, store *RestartSentinelStore) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		sentinel, err := store.Read()
		if err == nil && sentinel.ForcedAfterDrain {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	sentinel, err := store.Read()
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	t.Fatalf("forced_after_drain = %v, want true", sentinel.ForcedAfterDrain)
}
