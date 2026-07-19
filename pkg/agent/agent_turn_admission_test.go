package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
)

func TestAcquireAgentTurnSerializesConfiguredAgent(t *testing.T) {
	al := &AgentLoop{
		agentTurnAdmissions: &agentTurnAdmissionController{
			limits:  map[string]int{"browser": 1},
			active:  make(map[string]int),
			changed: make(chan struct{}),
		},
	}

	firstCtx, releaseFirst, err := al.acquireAgentTurn(context.Background(), "browser")
	if err != nil {
		t.Fatalf("first acquireAgentTurn() error = %v", err)
	}
	defer releaseFirst()

	// Nested turns inherit the admission and must not deadlock on the same agent.
	_, releaseNested, err := al.acquireAgentTurn(firstCtx, "browser")
	if err != nil {
		t.Fatalf("nested acquireAgentTurn() error = %v", err)
	}
	releaseNested()

	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, _, err = al.acquireAgentTurn(waitCtx, "browser")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked acquireAgentTurn() error = %v, want deadline exceeded", err)
	}
}

func TestAcquireAgentTurnAllowsUnconfiguredAgent(t *testing.T) {
	al := &AgentLoop{
		agentTurnAdmissions: &agentTurnAdmissionController{
			limits:  map[string]int{"browser": 1},
			active:  make(map[string]int),
			changed: make(chan struct{}),
		},
	}

	_, release, err := al.acquireAgentTurn(context.Background(), "main")
	if err != nil {
		t.Fatalf("acquireAgentTurn() error = %v", err)
	}
	release()
}

func TestAgentTurnAdmissionReloadPreservesActiveTurns(t *testing.T) {
	controller := &agentTurnAdmissionController{
		limits:  make(map[string]int),
		active:  make(map[string]int),
		changed: make(chan struct{}),
	}
	release, err := controller.acquire(context.Background(), "browser")
	if err != nil {
		t.Fatalf("initial acquire() error = %v", err)
	}

	controller.update(&AgentRegistry{agents: map[string]*AgentInstance{
		"browser": {ID: "browser", MaxParallelTurns: 1},
	}})

	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = controller.acquire(waitCtx, "browser")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("acquire() after reload error = %v, want deadline exceeded", err)
	}

	release()
	nextRelease, err := controller.acquire(context.Background(), "browser")
	if err != nil {
		t.Fatalf("acquire() after release error = %v", err)
	}
	nextRelease()
}

func TestReloadProviderAndConfigRefreshesAgentTurnAdmissions(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = t.TempDir()
	cfg.Agents.List = []config.AgentConfig{{ID: "browser", Default: true}}
	al := NewAgentLoop(cfg, bus.NewMessageBus(), &mockProvider{})
	defer al.Close()

	_, release, err := al.acquireAgentTurn(context.Background(), "browser")
	if err != nil {
		t.Fatalf("initial acquireAgentTurn() error = %v", err)
	}
	defer release()

	reloaded := config.DefaultConfig()
	reloaded.Agents.Defaults.Workspace = cfg.Agents.Defaults.Workspace
	reloaded.Agents.List = []config.AgentConfig{{
		ID:               "browser",
		Default:          true,
		MaxParallelTurns: 1,
	}}
	err = al.ReloadProviderAndConfig(context.Background(), &mockProvider{}, reloaded)
	if err != nil {
		t.Fatalf("ReloadProviderAndConfig() error = %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, _, err = al.acquireAgentTurn(waitCtx, "browser")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("acquireAgentTurn() after reload error = %v, want deadline exceeded", err)
	}
}

func TestInheritAgentTurnAdmissionsDetachesCancellation(t *testing.T) {
	source, cancel := context.WithCancel(context.WithValue(
		context.Background(),
		agentTurnAdmissionsKey{},
		map[string]struct{}{"browser": {}},
	))
	detached := inheritAgentTurnAdmissions(context.Background(), source)
	cancel()

	if err := detached.Err(); err != nil {
		t.Fatalf("detached context error = %v", err)
	}
	admissions, ok := detached.Value(agentTurnAdmissionsKey{}).(map[string]struct{})
	if !ok {
		t.Fatal("detached context has no admissions")
	}
	if _, ok := admissions["browser"]; !ok {
		t.Fatal("detached context did not inherit browser admission")
	}
}
