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
}

func (r *fakeServiceRestarter) RestartService(_ context.Context, service string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.services = append(r.services, service)
	return r.err
}

func (r *fakeServiceRestarter) calledWith(service string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.services) == 1 && r.services[0] == service
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
	restarter := &fakeServiceRestarter{}
	controller, err := NewRestartController(RestartControllerOptions{
		Config:    testRestartConfig(),
		Source:    &restartSourceSequence{},
		Store:     store,
		Restarter: restarter,
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
	if result.Status != restartStatusSucceeded {
		t.Fatalf("status = %q, want %q", result.Status, restartStatusSucceeded)
	}
	if !restarter.calledWith("picoclaw-main.service") {
		t.Fatalf("restarter calls = %#v, want configured service", restarter.services)
	}
	sentinel, err := store.Read()
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if sentinel.Status != restartStatusSucceeded {
		t.Fatalf("sentinel status = %q, want %q", sentinel.Status, restartStatusSucceeded)
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
	restarter := &fakeServiceRestarter{}
	controller, err := NewRestartController(RestartControllerOptions{
		Config:       testRestartConfig(),
		Source:       source,
		Store:        store,
		Restarter:    restarter,
		PollInterval: time.Millisecond,
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
	controller, err := NewRestartController(RestartControllerOptions{
		Config: testRestartConfig(),
		Source: &restartSourceSequence{
			pending: [][]bus.InboundMessage{{{SpoolID: "pending"}}},
		},
		Store:        store,
		Restarter:    &fakeServiceRestarter{},
		PollInterval: time.Millisecond,
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
	if !result.ForcedAfterDrain {
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
	controller, err := NewRestartController(RestartControllerOptions{
		Config: cfg,
		Source: &restartSourceSequence{
			pending: [][]bus.InboundMessage{{{SpoolID: "pending"}}},
		},
		Store:        store,
		Restarter:    &fakeServiceRestarter{},
		PollInterval: time.Millisecond,
		Now: func() time.Time {
			now = now.Add(2 * time.Second)
			return now
		},
	})
	if err != nil {
		t.Fatalf("NewRestartController() error = %v", err)
	}

	_, err = controller.RequestRestart(context.Background(), RestartRequest{})
	if err == nil || !strings.Contains(err.Error(), "did not drain") {
		t.Fatalf("RequestRestart() error = %v, want drain timeout", err)
	}
	sentinel, readErr := store.Read()
	if readErr != nil {
		t.Fatalf("Read() error = %v", readErr)
	}
	if sentinel.Status != restartStatusFailed {
		t.Fatalf("sentinel status = %q, want %q", sentinel.Status, restartStatusFailed)
	}
}

func TestGatewayRestartToolReportsControllerErrors(t *testing.T) {
	tool := NewGatewayRestartTool(&RestartController{
		cfg: testRestartConfig(),
		restarter: &fakeServiceRestarter{
			err: errors.New("boom"),
		},
		source: &restartSourceSequence{},
		store:  mustRestartSentinelStore(t),
		now:    func() time.Time { return time.Date(2026, 7, 9, 1, 0, 0, 0, time.UTC) },
	})

	ctx := tools.WithToolContext(context.Background(), "telegram", "chat-1")
	got := tool.Execute(ctx, map[string]any{"reason": "test"})
	if !got.IsError {
		t.Fatal("tool result should be an error")
	}
	if !strings.Contains(got.ForLLM, "boom") {
		t.Fatalf("tool result = %q, want boom", got.ForLLM)
	}
}

func mustRestartSentinelStore(t *testing.T) *RestartSentinelStore {
	t.Helper()
	store, err := NewRestartSentinelStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewRestartSentinelStore() error = %v", err)
	}
	return store
}
