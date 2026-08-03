package channels

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/outbox"
)

type durableTextChannel struct {
	mockChannel
	messageIDs []string
	err        error
	sends      int
	send       func() ([]string, error)
}

func (c *durableTextChannel) Send(context.Context, bus.OutboundMessage) ([]string, error) {
	c.sends++
	if c.send != nil {
		return c.send()
	}
	return append([]string(nil), c.messageIDs...), c.err
}

type durableMediaChannel struct {
	mockMediaChannel
	messageIDs []string
	err        error
	sends      int
}

type durableTypedTextChannel struct {
	durableTextChannel
	result DeliveryResult[bus.OutboundMessage]
	cancel context.CancelFunc
	typed  int
}

func (c *durableTypedTextChannel) SendMessageResult(
	context.Context,
	[]bus.OutboundMessage,
) DeliveryResult[bus.OutboundMessage] {
	c.typed++
	if c.cancel != nil {
		c.cancel()
	}
	return c.result
}

func (c *durableMediaChannel) SendMedia(context.Context, bus.OutboundMediaMessage) ([]string, error) {
	c.sends++
	return append([]string(nil), c.messageIDs...), c.err
}

func TestDurableQueuedMessagePersistsTypedChannelOutcome(t *testing.T) {
	tests := []struct {
		name       string
		messageIDs []string
		err        error
		wantStatus outbox.Status
	}{
		{
			name:       "delivered",
			messageIDs: []string{"platform-1"},
			wantStatus: outbox.StatusDelivered,
		},
		{
			name:       "definitely failed",
			err:        fmt.Errorf("invalid destination: %w", ErrSendFailed),
			wantStatus: outbox.StatusDefinitelyFailed,
		},
		{
			name:       "ambiguous",
			err:        fmt.Errorf("transport timeout: %w", ErrTemporary),
			wantStatus: outbox.StatusAmbiguous,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coordinator := openDurableTestCoordinator(t)
			msg := admitDurableTestMessage(t, coordinator, "source-"+tt.name)
			channel := &durableTextChannel{messageIDs: tt.messageIDs, err: tt.err}
			manager := newTestManager()
			manager.outboundOutbox = coordinator
			manager.deliverQueuedMessage(t.Context(), "test", &channelWorker{
				ch: channel, limiter: rate.NewLimiter(rate.Inf, 1),
			}, msg)

			if channel.sends != 1 {
				t.Fatalf("adapter sends = %d, want 1", channel.sends)
			}
			intent, err := coordinator.Get(msg.DeliveryID)
			if err != nil {
				t.Fatalf("Get() error = %v", err)
			}
			if intent.Status != tt.wantStatus || intent.Attempts != 1 {
				t.Fatalf("intent = %+v, want status %q and one attempt", intent, tt.wantStatus)
			}
			if !slices.Equal(intent.PlatformMessageIDs, tt.messageIDs) {
				t.Fatalf("platform IDs = %v, want %v", intent.PlatformMessageIDs, tt.messageIDs)
			}
		})
	}
}

func TestDurableMessageDispatchPersistsOutcomeBeforeReportingCompletion(t *testing.T) {
	coordinator := openDurableTestCoordinator(t)
	msg := admitDurableTestMessage(t, coordinator, "source-dispatch")
	channel := &durableTextChannel{messageIDs: []string{"platform-dispatch-1"}}
	manager := newTestManager()
	manager.outboundOutbox = coordinator
	manager.channels["test"] = channel
	if err := manager.StartAll(t.Context()); err != nil {
		t.Fatalf("StartAll() error = %v", err)
	}
	t.Cleanup(func() { _ = manager.StopAll(context.Background()) })
	if err := manager.bus.PublishOutbound(t.Context(), msg); err != nil {
		t.Fatalf("PublishOutbound() error = %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		intent, err := coordinator.Get(msg.DeliveryID)
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if intent.Status == outbox.StatusDelivered {
			if !slices.Equal(intent.PlatformMessageIDs, []string{"platform-dispatch-1"}) {
				t.Fatalf("platform IDs = %v", intent.PlatformMessageIDs)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("intent did not reach delivered: %+v", intent)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if channel.sends != 1 {
		t.Fatalf("adapter sends = %d, want 1", channel.sends)
	}
}

func TestDurableDispatchRejectionPersistsDefinitelyFailed(t *testing.T) {
	coordinator := openDurableTestCoordinator(t)
	msg := admitDurableTestMessageForChannel(t, coordinator, "source-unknown-channel", "missing")
	manager := newTestManager()
	manager.outboundOutbox = coordinator
	if err := manager.StartAll(t.Context()); err != nil {
		t.Fatalf("StartAll() error = %v", err)
	}
	t.Cleanup(func() { _ = manager.StopAll(context.Background()) })
	if err := manager.bus.PublishOutbound(t.Context(), msg); err != nil {
		t.Fatalf("PublishOutbound() error = %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		intent, err := coordinator.Get(msg.DeliveryID)
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if intent.Status == outbox.StatusDefinitelyFailed {
			if !strings.Contains(intent.LastError, "channel missing not found") {
				t.Fatalf("rejection error = %q", intent.LastError)
			}
			if intent.Attempts != 0 {
				t.Fatalf("adapter attempts = %d, want 0", intent.Attempts)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("intent did not reach definitely_failed: %+v", intent)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestDurableQueuedMediaPersistsPartialDeliveryAsAmbiguous(t *testing.T) {
	coordinator := openDurableTestCoordinator(t)
	msg := admitDurableTestMedia(t, coordinator, "source-media-partial")
	channel := &durableMediaChannel{
		messageIDs: []string{"platform-media-1"},
		err:        fmt.Errorf("second attachment rejected: %w", ErrSendFailed),
	}
	manager := newTestManager()
	manager.outboundOutbox = coordinator
	manager.deliverQueuedMedia(t.Context(), "test", &channelWorker{
		ch: channel, limiter: rate.NewLimiter(rate.Inf, 1),
	}, msg)

	if channel.sends != 1 {
		t.Fatalf("adapter sends = %d, want 1", channel.sends)
	}
	intent, err := coordinator.Get(msg.DeliveryID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if intent.Status != outbox.StatusAmbiguous ||
		!slices.Equal(intent.PlatformMessageIDs, []string{"platform-media-1"}) {
		t.Fatalf("media intent = %+v", intent)
	}
}

func TestDurableQueuedMessageDoesNotCallAdapterWhenAttemptCannotPersist(t *testing.T) {
	coordinator := openDurableTestCoordinator(t)
	msg := admitDurableTestMessage(t, coordinator, "source-begin-failure")
	if err := coordinator.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	channel := &durableTextChannel{}
	manager := newTestManager()
	manager.outboundOutbox = coordinator
	manager.deliverQueuedMessage(t.Context(), "test", &channelWorker{
		ch: channel, limiter: rate.NewLimiter(rate.Inf, 1),
	}, msg)
	if channel.sends != 0 {
		t.Fatalf("adapter sends = %d, want 0", channel.sends)
	}
}

func TestDurableQueuedMessageFailsClosedWithoutCoordinator(t *testing.T) {
	channel := &durableTextChannel{}
	manager := newTestManager()
	manager.deliverQueuedMessage(t.Context(), "test", &channelWorker{
		ch: channel, limiter: rate.NewLimiter(rate.Inf, 1),
	}, testOutboundMessage(bus.OutboundMessage{
		Channel:    "test",
		ChatID:     "chat-1",
		Content:    "must not send",
		DeliveryID: "out_missing_coordinator",
	}))
	if channel.sends != 0 {
		t.Fatalf("adapter sends = %d, want 0", channel.sends)
	}
}

func TestDurableQueuedMediaFailsClosedWithoutCoordinator(t *testing.T) {
	channel := &durableMediaChannel{}
	manager := newTestManager()
	manager.deliverQueuedMedia(t.Context(), "test", &channelWorker{
		ch: channel, limiter: rate.NewLimiter(rate.Inf, 1),
	}, testOutboundMediaMessage(bus.OutboundMediaMessage{
		Channel:    "test",
		ChatID:     "chat-1",
		DeliveryID: "out_missing_coordinator",
		Parts:      []bus.MediaPart{{Type: "image", Ref: "media://one"}},
	}))
	if channel.sends != 0 {
		t.Fatalf("adapter sends = %d, want 0", channel.sends)
	}
}

func TestQueuedMessageWithoutDeliveryIDPreservesLegacySend(t *testing.T) {
	channel := &durableTextChannel{messageIDs: []string{"platform-legacy-1"}}
	manager := newTestManager()
	manager.deliverQueuedMessage(t.Context(), "test", &channelWorker{
		ch: channel, limiter: rate.NewLimiter(rate.Inf, 1),
	}, testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "chat-1",
		Content: "legacy",
	}))
	if channel.sends != 1 {
		t.Fatalf("adapter sends = %d, want 1", channel.sends)
	}
}

func TestDurableOutcomePersistenceFailureLeavesAttemptForReconciliation(t *testing.T) {
	root := t.TempDir()
	coordinator, err := outbox.OpenCoordinator(root)
	if err != nil {
		t.Fatalf("OpenCoordinator() error = %v", err)
	}
	msg := admitDurableTestMessage(t, coordinator, "source-outcome-persist-failure")
	channel := &durableTextChannel{send: func() ([]string, error) {
		if closeErr := coordinator.Close(); closeErr != nil {
			t.Fatalf("Close() error = %v", closeErr)
		}
		return []string{"platform-uncertain-1"}, nil
	}}
	manager := newTestManager()
	manager.outboundOutbox = coordinator
	manager.deliverQueuedMessage(t.Context(), "test", &channelWorker{
		ch: channel, limiter: rate.NewLimiter(rate.Inf, 1),
	}, msg)
	if channel.sends != 1 {
		t.Fatalf("adapter sends = %d, want 1", channel.sends)
	}

	reopened, err := outbox.OpenCoordinator(root)
	if err != nil {
		t.Fatalf("reopen coordinator: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	intent, err := reopened.Get(msg.DeliveryID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if intent.Status != outbox.StatusAttempting || intent.Attempts != 1 ||
		len(intent.PlatformMessageIDs) != 0 {
		t.Fatalf("intent after outcome persistence failure = %+v", intent)
	}
}

func TestDurableOutcomeCarriesRetryAfter(t *testing.T) {
	before := time.Now().UTC().Add(20 * time.Second)
	outcome := durableOutcome(DeliveryResult[string]{
		Status:     DeliveryFailed,
		Acceptance: DeliveryRejected,
		RetryAfter: 30 * time.Second,
		Err:        ErrRateLimit,
	}, nil)
	if outcome.Status != OutboundDeliveryDefinitelyFailed || outcome.RetryAfter.Before(before) {
		t.Fatalf("outcome = %+v", outcome)
	}
}

func TestDurableQueuedMessagePersistsTypedAdapterRetryAfter(t *testing.T) {
	coordinator := openDurableTestCoordinator(t)
	msg := admitDurableTestMessage(t, coordinator, "source-typed-retry-after")
	ctx, cancel := context.WithCancel(t.Context())
	channel := &durableTypedTextChannel{
		result: DeliveryResult[bus.OutboundMessage]{
			Status:     DeliveryFailed,
			Acceptance: DeliveryRejected,
			RetryAfter: 30 * time.Second,
			Err:        ErrRateLimit,
		},
		cancel: cancel,
	}
	manager := newTestManager()
	manager.outboundOutbox = coordinator
	before := time.Now().UTC().Add(20 * time.Second)
	manager.deliverQueuedMessage(ctx, "test", &channelWorker{
		ch: channel, limiter: rate.NewLimiter(rate.Inf, 1),
	}, msg)

	if channel.typed != 1 || channel.sends != 0 {
		t.Fatalf("typed sends = %d, legacy sends = %d; want 1 and 0", channel.typed, channel.sends)
	}
	intent, err := coordinator.Get(msg.DeliveryID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if intent.Status != outbox.StatusDefinitelyFailed || intent.RetryAfter.Before(before) {
		t.Fatalf("typed retry outcome = %+v", intent)
	}
}

func openDurableTestCoordinator(t *testing.T) *outbox.Coordinator {
	t.Helper()
	coordinator, err := outbox.OpenCoordinator(t.TempDir())
	if err != nil {
		t.Fatalf("OpenCoordinator() error = %v", err)
	}
	t.Cleanup(func() { _ = coordinator.Close() })
	return coordinator
}

func admitDurableTestMessage(
	t *testing.T,
	coordinator *outbox.Coordinator,
	sourceID string,
) bus.OutboundMessage {
	return admitDurableTestMessageForChannel(t, coordinator, sourceID, "test")
}

func admitDurableTestMessageForChannel(
	t *testing.T,
	coordinator *outbox.Coordinator,
	sourceID string,
	channel string,
) bus.OutboundMessage {
	t.Helper()
	admission, err := coordinator.AdmitMessage("/agents/main", outbox.Identity{
		SourceID: sourceID,
		Channel:  channel,
		ChatID:   "chat-1",
	}, testOutboundMessage(bus.OutboundMessage{Channel: channel, ChatID: "chat-1", Content: "hello"}))
	if err != nil || !admission.Dispatch {
		t.Fatalf("AdmitMessage() = %+v, %v", admission, err)
	}
	commitDurableTestAdmission(t, coordinator, admission.Lease)
	return *admission.Intent.Message
}

func admitDurableTestMedia(
	t *testing.T,
	coordinator *outbox.Coordinator,
	sourceID string,
) bus.OutboundMediaMessage {
	t.Helper()
	admission, err := coordinator.AdmitMedia("/agents/main", outbox.Identity{
		SourceID: sourceID,
		Channel:  "test",
		ChatID:   "chat-1",
	}, testOutboundMediaMessage(bus.OutboundMediaMessage{
		Channel: "test",
		ChatID:  "chat-1",
		Parts:   []bus.MediaPart{{Type: "image", Ref: "media://one"}},
	}))
	if err != nil || !admission.Dispatch {
		t.Fatalf("AdmitMedia() = %+v, %v", admission, err)
	}
	commitDurableTestAdmission(t, coordinator, admission.Lease)
	return *admission.Intent.Media
}

func commitDurableTestAdmission(
	t *testing.T,
	coordinator *outbox.Coordinator,
	lease outbox.DispatchLease,
) {
	t.Helper()
	if err := coordinator.PrepareAdmission(lease); err != nil {
		t.Fatalf("PrepareAdmission() error = %v", err)
	}
	if err := coordinator.CommitAdmission(lease); err != nil {
		t.Fatalf("CommitAdmission() error = %v", err)
	}
}

func TestDurableDeliveryErrorPreservesCause(t *testing.T) {
	outcome := durableOutcome(RejectedDelivery[string](ErrSendFailed), nil)
	if !errors.Is(outcome.Err, ErrSendFailed) {
		t.Fatalf("outcome error = %v", outcome.Err)
	}
}
