package agent

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAcquireAgentTurnSerializesConfiguredAgent(t *testing.T) {
	al := &AgentLoop{
		agentTurnSems: map[string]chan struct{}{"browser": make(chan struct{}, 1)},
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
		agentTurnSems: map[string]chan struct{}{"browser": make(chan struct{}, 1)},
	}

	_, release, err := al.acquireAgentTurn(context.Background(), "main")
	if err != nil {
		t.Fatalf("acquireAgentTurn() error = %v", err)
	}
	release()
}
