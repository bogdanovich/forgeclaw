package gateway

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
)

func TestReportRestartHandoffRecoversAndDeliversOnce(t *testing.T) {
	store, err := NewRestartSentinelStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(-time.Minute)
	if writeErr := store.Write(RestartSentinel{
		Status:           restartStatusRunning,
		RequestedService: "picoclaw-main.service",
		Origin:           RestartOrigin{Channel: "telegram", ChatID: "chat-1", SessionKey: "session-1"},
		RequestedAt:      now,
		UpdatedAt:        now,
	}); writeErr != nil {
		t.Fatal(writeErr)
	}
	messageBus := bus.NewMessageBus()
	reportRestartHandoff(context.Background(), messageBus, store)

	select {
	case message := <-messageBus.OutboundChan():
		if message.Channel != "telegram" || message.ChatID != "chat-1" || message.SessionKey != "session-1" {
			t.Fatalf("outbound message = %#v", message)
		}
		if !strings.Contains(message.Content, "Gateway is back") || !strings.Contains(message.Content, "succeeded") {
			t.Fatalf("continuation = %q", message.Content)
		}
	case <-time.After(time.Second):
		t.Fatal("restart continuation was not published")
	}

	sentinel, err := store.Read()
	if err != nil {
		t.Fatal(err)
	}
	if sentinel.Status != restartStatusSucceeded || sentinel.ContinuationSentAt.IsZero() {
		t.Fatalf("sentinel = %#v", sentinel)
	}
	reportRestartHandoff(context.Background(), messageBus, store)
	select {
	case duplicate := <-messageBus.OutboundChan():
		t.Fatalf("unexpected duplicate continuation: %#v", duplicate)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestReportDeployHandoffShowsFailureAndDoesNotDuplicate(t *testing.T) {
	store, err := NewDeploySentinelStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := store.Write(DeploySentinel{
		Kind:        "deploy",
		Status:      "failed",
		Group:       "picoclaw-local",
		Target:      "all",
		Command:     "/opt/deploy.sh",
		OutputTail:  "health check failed",
		ExitCode:    7,
		Origin:      RestartOrigin{Channel: "slack", ChatID: "C123", SessionKey: "session-2"},
		RequestedAt: now,
		UpdatedAt:   now,
	}); err != nil {
		t.Fatal(err)
	}
	messageBus := bus.NewMessageBus()
	reportDeployHandoff(context.Background(), messageBus, store)

	select {
	case message := <-messageBus.OutboundChan():
		if !strings.Contains(message.Content, "failed") || !strings.Contains(message.Content, "exit code 7") {
			t.Fatalf("continuation = %q", message.Content)
		}
	case <-time.After(time.Second):
		t.Fatal("deploy continuation was not published")
	}
	status := formatGatewayHandoffStatus(nil, store)
	if !strings.Contains(status, "health check failed") || !strings.Contains(status, "Continuation sent: true") {
		t.Fatalf("status = %q", status)
	}
	reportDeployHandoff(context.Background(), messageBus, store)
	select {
	case duplicate := <-messageBus.OutboundChan():
		t.Fatalf("unexpected duplicate continuation: %#v", duplicate)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestReportDeployHandoffLeavesUndeliverableOriginPending(t *testing.T) {
	store, err := NewDeploySentinelStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if writeErr := store.Write(DeploySentinel{Kind: "deploy", Status: "succeeded"}); writeErr != nil {
		t.Fatal(writeErr)
	}
	reportDeployHandoff(context.Background(), bus.NewMessageBus(), store)
	sentinel, err := store.Read()
	if err != nil {
		t.Fatal(err)
	}
	if !sentinel.ContinuationSentAt.IsZero() {
		t.Fatalf("undeliverable sentinel marked sent: %#v", sentinel)
	}
}
