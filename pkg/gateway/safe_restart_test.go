package gateway

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
)

type restartPreflightSourceStub struct {
	stats   bus.MessageBusStats
	pending []bus.InboundMessage
	err     error
	delay   time.Duration
}

func (s restartPreflightSourceStub) Stats() bus.MessageBusStats {
	return s.stats
}

func (s restartPreflightSourceStub) PendingInboundSpool(context.Context) ([]bus.InboundMessage, error) {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	return s.pending, s.err
}

func TestCollectRestartPreflightReportsBoundedState(t *testing.T) {
	now := time.Date(2026, 7, 9, 1, 2, 3, 0, time.UTC)
	source := restartPreflightSourceStub{
		stats: bus.MessageBusStats{
			Outbound:      bus.StreamStats{Depth: 2, Capacity: 64},
			OutboundMedia: bus.StreamStats{Depth: 1, Capacity: 64},
		},
		pending: []bus.InboundMessage{{SpoolID: "a"}, {SpoolID: "b"}},
	}

	got := CollectRestartPreflight(context.Background(), source, RestartPreflightOptions{
		Now: func() time.Time { return now },
	})

	if !got.CheckedAt.Equal(now) {
		t.Fatalf("checked_at = %v, want %v", got.CheckedAt, now)
	}
	if got.PendingInbound != 2 {
		t.Fatalf("pending inbound = %d, want 2", got.PendingInbound)
	}
	if got.OutboundDepth != 2 {
		t.Fatalf("outbound depth = %d, want 2", got.OutboundDepth)
	}
	if got.OutboundMediaDepth != 1 {
		t.Fatalf("outbound media depth = %d, want 1", got.OutboundMediaDepth)
	}
	if !got.ActiveTurnsUnavailable {
		t.Fatal("active turns should be marked unavailable in this PR")
	}
	if !got.CronJobsUnavailable {
		t.Fatal("cron jobs should be marked unavailable in this PR")
	}
	if !got.HasActiveWork() {
		t.Fatal("preflight should report active work")
	}
}

func TestCollectRestartPreflightCapturesPendingInboundError(t *testing.T) {
	source := restartPreflightSourceStub{err: errors.New("spool unavailable")}

	got := CollectRestartPreflight(context.Background(), source, RestartPreflightOptions{})

	if got.PendingInboundError != "spool unavailable" {
		t.Fatalf("pending inbound error = %q", got.PendingInboundError)
	}
}

func TestRestartPreflightTreatsUnknownStateAsActiveWork(t *testing.T) {
	got := RestartPreflight{
		ActiveTurnsUnavailable: true,
		CronJobsUnavailable:    true,
	}

	if !got.HasActiveWork() {
		t.Fatal("unknown active-turn or cron state should be treated as unsafe active work")
	}
	got = RestartPreflight{PendingInboundError: "spool unavailable"}
	if !got.HasActiveWork() {
		t.Fatal("pending inbound errors should be treated as unsafe active work")
	}
	got = RestartPreflight{PendingInboundTimedOut: true}
	if !got.HasActiveWork() {
		t.Fatal("pending inbound timeout should be treated as unsafe active work")
	}
}

func TestCollectRestartPreflightTimesOutPendingInbound(t *testing.T) {
	source := restartPreflightSourceStub{delay: 50 * time.Millisecond}

	got := CollectRestartPreflight(context.Background(), source, RestartPreflightOptions{
		Timeout: time.Millisecond,
	})

	if !got.PendingInboundTimedOut {
		t.Fatal("pending inbound preflight should time out")
	}
	if got.PendingInboundError == "" {
		t.Fatal("pending inbound timeout should be reported")
	}
}

func TestRestartSentinelStoreWriteReadClear(t *testing.T) {
	store, err := NewRestartSentinelStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewRestartSentinelStore() error = %v", err)
	}
	requestedAt := time.Date(2026, 7, 9, 2, 3, 4, 0, time.UTC)
	want := RestartSentinel{
		Status:           "pending",
		RequestedService: "picoclaw-main.service",
		Origin: RestartOrigin{
			Channel:    "telegram",
			ChatID:     "chat-1",
			SessionKey: "telegram:chat-1",
		},
		RequestedAt: requestedAt,
		UpdatedAt:   requestedAt,
		Reason:      "operator requested restart",
		Preflight: RestartPreflight{
			CheckedAt:      requestedAt,
			PendingInbound: 1,
		},
	}

	if writeErr := store.Write(want); writeErr != nil {
		t.Fatalf("Write() error = %v", writeErr)
	}
	got, err := store.Read()
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if got.Kind != "restart" {
		t.Fatalf("kind = %q, want restart", got.Kind)
	}
	if got.Status != want.Status {
		t.Fatalf("status = %q, want %q", got.Status, want.Status)
	}
	if got.RequestedService != want.RequestedService {
		t.Fatalf("requested service = %q, want %q", got.RequestedService, want.RequestedService)
	}
	if got.Origin != want.Origin {
		t.Fatalf("origin = %#v, want %#v", got.Origin, want.Origin)
	}
	if !got.RequestedAt.Equal(requestedAt) {
		t.Fatalf("requested at = %v, want %v", got.RequestedAt, requestedAt)
	}
	if got.Preflight.PendingInbound != 1 {
		t.Fatalf("preflight pending inbound = %d, want 1", got.Preflight.PendingInbound)
	}

	if err := store.Clear(); err != nil {
		t.Fatalf("Clear() error = %v", err)
	}
	if _, err := store.Read(); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Read() after Clear() error = %v, want os.ErrNotExist", err)
	}
}

func TestRestartSentinelStoreMarksInterruptedRestartComplete(t *testing.T) {
	store, err := NewRestartSentinelStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewRestartSentinelStore() error = %v", err)
	}
	requestedAt := time.Date(2026, 7, 9, 2, 3, 4, 0, time.UTC)
	updatedAt := requestedAt.Add(time.Minute)
	if writeErr := store.Write(RestartSentinel{
		Status:           "running",
		RequestedService: "picoclaw-main.service",
		RequestedAt:      requestedAt,
		UpdatedAt:        requestedAt,
	}); writeErr != nil {
		t.Fatalf("Write() error = %v", writeErr)
	}

	got, changed, err := store.MarkInterruptedRestartComplete(updatedAt)
	if err != nil {
		t.Fatalf("MarkInterruptedRestartComplete() error = %v", err)
	}
	if !changed {
		t.Fatal("running restart sentinel should be recovered")
	}
	if got.Status != "succeeded" {
		t.Fatalf("status = %q, want succeeded", got.Status)
	}
	if !got.UpdatedAt.Equal(updatedAt) {
		t.Fatalf("updated_at = %v, want %v", got.UpdatedAt, updatedAt)
	}
}
