package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/outbox"
)

func TestGatewayOutboundReconcilerPublishesCanonicalTextAndMedia(t *testing.T) {
	root := t.TempDir()
	first := openGatewayRecoveryCoordinator(t, root)
	message, err := first.AdmitMessage(
		"/agents/main",
		gatewayRecoveryIdentity("message", 0),
		bus.OutboundMessage{Content: "canonical response"},
	)
	if err != nil {
		t.Fatalf("AdmitMessage() error = %v", err)
	}
	media, err := first.AdmitMedia(
		"/agents/media",
		gatewayRecoveryIdentity("media", 1),
		bus.OutboundMediaMessage{Parts: []bus.MediaPart{{
			Type:    "image",
			Ref:     "media://canonical",
			Caption: "canonical caption",
		}}},
	)
	if err != nil {
		t.Fatalf("AdmitMedia() error = %v", err)
	}
	closeGatewayRecoveryCoordinator(t, first)

	second := openGatewayRecoveryCoordinator(t, root)
	t.Cleanup(func() { _ = second.Close() })
	admissions, err := second.Recover()
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	msgBus := bus.NewMessageBus()
	t.Cleanup(msgBus.Close)
	reconciler, err := startGatewayOutboundReconciler(t.Context(), second, msgBus, admissions, nil, "")
	if err != nil {
		t.Fatalf("startGatewayOutboundReconciler() error = %v", err)
	}
	t.Cleanup(reconciler.stop)

	recoveredMessage := <-msgBus.OutboundChan()
	if recoveredMessage.DeliveryID != message.Intent.ID || recoveredMessage.Content != "canonical response" ||
		recoveredMessage.Channel != "telegram" || recoveredMessage.ChatID != "chat-message" {
		t.Fatalf("recovered message = %#v", recoveredMessage)
	}
	recoveredMedia := <-msgBus.OutboundMediaChan()
	if recoveredMedia.DeliveryID != media.Intent.ID || recoveredMedia.Channel != "telegram" ||
		recoveredMedia.ChatID != "chat-media" || len(recoveredMedia.Parts) != 1 ||
		recoveredMedia.Parts[0].Ref != "media://canonical" ||
		recoveredMedia.Parts[0].Caption != "canonical caption" {
		t.Fatalf("recovered media = %#v", recoveredMedia)
	}
	for _, deliveryID := range []string{message.Intent.ID, media.Intent.ID} {
		if err := second.BeginAttempt(deliveryID); err != nil {
			t.Fatalf("BeginAttempt(%q) error = %v", deliveryID, err)
		}
		if err := second.MarkDelivered(deliveryID, outbox.Outcome{}); err != nil {
			t.Fatalf("MarkDelivered(%q) error = %v", deliveryID, err)
		}
	}
}

func TestGatewayOutboundReconcilerHonorsPersistedRetryDeadline(t *testing.T) {
	root := t.TempDir()
	first := openGatewayRecoveryCoordinator(t, root)
	admission, err := first.AdmitMessage(
		"/agents/main",
		gatewayRecoveryIdentity("delayed", 0),
		bus.OutboundMessage{Content: "retry after deadline"},
	)
	if err != nil {
		t.Fatalf("AdmitMessage() error = %v", err)
	}
	commitGatewayRecoveryAdmission(t, first, admission)
	if err := first.BeginAttempt(admission.Intent.ID); err != nil {
		t.Fatalf("BeginAttempt() error = %v", err)
	}
	retryAt := time.Now().UTC().Add(time.Second)
	if err := first.MarkDefinitelyFailed(admission.Intent.ID, outbox.Outcome{
		RetryAfter: retryAt,
		Error:      "rate limited",
	}); err != nil {
		t.Fatalf("MarkDefinitelyFailed() error = %v", err)
	}
	closeGatewayRecoveryCoordinator(t, first)

	second := openGatewayRecoveryCoordinator(t, root)
	t.Cleanup(func() { _ = second.Close() })
	admissions, err := second.Recover()
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	msgBus := bus.NewMessageBus()
	t.Cleanup(msgBus.Close)
	reconciler, err := startGatewayOutboundReconciler(t.Context(), second, msgBus, admissions, nil, "")
	if err != nil {
		t.Fatalf("startGatewayOutboundReconciler() error = %v", err)
	}
	t.Cleanup(reconciler.stop)

	select {
	case msg := <-msgBus.OutboundChan():
		t.Fatalf("recovered message published before retry deadline: %#v", msg)
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case msg := <-msgBus.OutboundChan():
		if msg.DeliveryID != admission.Intent.ID {
			t.Fatalf("recovered delivery ID = %q, want %q", msg.DeliveryID, admission.Intent.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("recovered message was not published after retry deadline")
	}
}

func TestGatewayOutboundReconcilerReleasesUnpublishedAdmission(t *testing.T) {
	root := t.TempDir()
	first := openGatewayRecoveryCoordinator(t, root)
	admission, err := first.AdmitMessage(
		"/agents/main",
		gatewayRecoveryIdentity("closed-bus", 0),
		bus.OutboundMessage{Content: "retry next startup"},
	)
	if err != nil {
		t.Fatalf("AdmitMessage() error = %v", err)
	}
	closeGatewayRecoveryCoordinator(t, first)

	second := openGatewayRecoveryCoordinator(t, root)
	admissions, err := second.Recover()
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	msgBus := bus.NewMessageBus()
	msgBus.Close()
	if _, err := startGatewayOutboundReconciler(t.Context(), second, msgBus, admissions, nil, ""); err == nil {
		t.Fatal("startGatewayOutboundReconciler() succeeded with a closed bus")
	}
	closeGatewayRecoveryCoordinator(t, second)

	third := openGatewayRecoveryCoordinator(t, root)
	t.Cleanup(func() { _ = third.Close() })
	recovered, err := third.Recover()
	if err != nil {
		t.Fatalf("Recover() after publication failure error = %v", err)
	}
	if len(recovered) != 1 || recovered[0].Intent.ID != admission.Intent.ID {
		t.Fatalf("Recover() after publication failure = %#v", recovered)
	}
}

func TestGatewayOutboundReconcilerShutdownReleasesDelayedAdmission(t *testing.T) {
	root := t.TempDir()
	first := openGatewayRecoveryCoordinator(t, root)
	admission, err := first.AdmitMessage(
		"/agents/main",
		gatewayRecoveryIdentity("shutdown", 0),
		bus.OutboundMessage{Content: "retry after restart"},
	)
	if err != nil {
		t.Fatalf("AdmitMessage() error = %v", err)
	}
	commitGatewayRecoveryAdmission(t, first, admission)
	if err := first.BeginAttempt(admission.Intent.ID); err != nil {
		t.Fatalf("BeginAttempt() error = %v", err)
	}
	if err := first.MarkDefinitelyFailed(admission.Intent.ID, outbox.Outcome{
		RetryAfter: time.Now().UTC().Add(time.Hour),
		Error:      "rate limited",
	}); err != nil {
		t.Fatalf("MarkDefinitelyFailed() error = %v", err)
	}
	closeGatewayRecoveryCoordinator(t, first)

	second := openGatewayRecoveryCoordinator(t, root)
	admissions, err := second.Recover()
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	msgBus := bus.NewMessageBus()
	t.Cleanup(msgBus.Close)
	reconciler, err := startGatewayOutboundReconciler(context.Background(), second, msgBus, admissions, nil, "")
	if err != nil {
		t.Fatalf("startGatewayOutboundReconciler() error = %v", err)
	}
	reconciler.stop()
	closeGatewayRecoveryCoordinator(t, second)

	third := openGatewayRecoveryCoordinator(t, root)
	t.Cleanup(func() { _ = third.Close() })
	recovered, err := third.Recover()
	if err != nil {
		t.Fatalf("Recover() after shutdown error = %v", err)
	}
	if len(recovered) != 1 || recovered[0].Intent.ID != admission.Intent.ID {
		t.Fatalf("Recover() after shutdown = %#v", recovered)
	}
}

func gatewayRecoveryIdentity(source string, ordinal int) outbox.Identity {
	return outbox.Identity{
		SourceID:   source,
		Ordinal:    ordinal,
		Channel:    "telegram",
		ChatID:     "chat-" + source,
		SessionKey: "agent:main:telegram:chat-" + source,
	}
}

func openGatewayRecoveryCoordinator(t *testing.T, root string) *outbox.Coordinator {
	t.Helper()
	coordinator, err := outbox.OpenCoordinator(root)
	if err != nil {
		t.Fatalf("OpenCoordinator() error = %v", err)
	}
	return coordinator
}

func closeGatewayRecoveryCoordinator(t *testing.T, coordinator *outbox.Coordinator) {
	t.Helper()
	if err := coordinator.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func commitGatewayRecoveryAdmission(t *testing.T, coordinator *outbox.Coordinator, admission outbox.Admission) {
	t.Helper()
	if err := coordinator.PrepareAdmission(admission.Lease); err != nil {
		t.Fatalf("PrepareAdmission() error = %v", err)
	}
	if err := coordinator.CommitAdmission(admission.Lease); err != nil {
		t.Fatalf("CommitAdmission() error = %v", err)
	}
}
