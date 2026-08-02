package outbox

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/channels"
)

func TestCoordinatorPersistsDeliveredOutcome(t *testing.T) {
	workspace := t.TempDir()
	coordinator := NewCoordinator()
	coordinator.now = func() time.Time {
		return time.Date(2026, time.August, 2, 14, 0, 0, 0, time.UTC)
	}
	identity := testIdentity()
	admission, err := coordinator.AdmitMessage(workspace, identity, bus.OutboundMessage{
		Context:    bus.InboundContext{Channel: identity.Channel, ChatID: identity.ChatID},
		SessionKey: identity.SessionKey,
		Content:    "durable response",
	})
	if err != nil {
		t.Fatalf("AdmitMessage() error = %v", err)
	}
	intent := admission.Intent
	if !admission.Dispatch {
		t.Fatal("first admission did not acquire dispatch ownership")
	}
	if intent.Status != StatusPending || intent.Message == nil || intent.Message.DeliveryID != intent.ID {
		t.Fatalf("AdmitMessage() intent = %#v", intent)
	}
	if beginErr := coordinator.BeginDelivery(intent.ID); beginErr != nil {
		t.Fatalf("BeginDelivery() error = %v", beginErr)
	}
	if completionErr := coordinator.CompleteDelivery(intent.ID, channels.DurableDeliveryOutcome{
		MessageIDs: []string{"remote-1"},
		RetryAfter: 30 * time.Second,
	}); completionErr != nil {
		t.Fatalf("CompleteDelivery() error = %v", completionErr)
	}

	store, err := Open(filepath.Clean(workspace))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	got, err := store.Get(intent.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Status != StatusDelivered || len(got.PlatformMessageIDs) != 1 ||
		got.PlatformMessageIDs[0] != "remote-1" {
		t.Fatalf("persisted outcome = %#v", got)
	}
	wantRetryAfter := coordinator.now().Add(30 * time.Second)
	if !got.RetryAfter.Equal(wantRetryAfter) {
		t.Fatalf("RetryAfter = %v, want %v", got.RetryAfter, wantRetryAfter)
	}
}

func TestCoordinatorDispatchLeasePreventsDuplicatePublication(t *testing.T) {
	workspace := t.TempDir()
	coordinator := NewCoordinator()
	identity := testIdentity()
	msg := bus.OutboundMessage{
		Context:    bus.InboundContext{Channel: identity.Channel, ChatID: identity.ChatID},
		SessionKey: identity.SessionKey,
		Content:    "deliver once",
	}

	first, err := coordinator.AdmitMessage(workspace, identity, msg)
	if err != nil {
		t.Fatalf("AdmitMessage(first) error = %v", err)
	}
	second, err := coordinator.AdmitMessage(workspace, identity, msg)
	if err != nil {
		t.Fatalf("AdmitMessage(second) error = %v", err)
	}
	if !first.Dispatch || second.Dispatch || first.Intent.ID != second.Intent.ID {
		t.Fatalf("dispatch leases = first %+v second %+v", first, second)
	}

	if releaseErr := coordinator.ReleaseAdmission(first.Intent.ID); releaseErr != nil {
		t.Fatalf("ReleaseAdmission() error = %v", releaseErr)
	}
	third, err := coordinator.AdmitMessage(workspace, identity, msg)
	if err != nil {
		t.Fatalf("AdmitMessage(third) error = %v", err)
	}
	if !third.Dispatch {
		t.Fatal("released pending admission was not dispatchable")
	}
}

func TestCoordinatorClassifiesAmbiguousAndRejectedOutcomes(t *testing.T) {
	tests := []struct {
		name           string
		mayHaveReached bool
		wantStatus     Status
	}{
		{name: "ambiguous", mayHaveReached: true, wantStatus: StatusAmbiguous},
		{name: "rejected", mayHaveReached: false, wantStatus: StatusDefinitelyFailed},
	}
	for ordinal, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coordinator := NewCoordinator()
			identity := testIdentity()
			identity.Ordinal = ordinal
			admission, err := coordinator.AdmitMessage(t.TempDir(), identity, bus.OutboundMessage{
				Context:    bus.InboundContext{Channel: identity.Channel, ChatID: identity.ChatID},
				SessionKey: identity.SessionKey,
				Content:    test.name,
			})
			if err != nil {
				t.Fatalf("AdmitMessage() error = %v", err)
			}
			intent := admission.Intent
			if beginErr := coordinator.BeginDelivery(intent.ID); beginErr != nil {
				t.Fatalf("BeginDelivery() error = %v", beginErr)
			}
			if completionErr := coordinator.CompleteDelivery(intent.ID, channels.DurableDeliveryOutcome{
				Err:              errors.New("transport failed"),
				MayHaveDelivered: test.mayHaveReached,
			}); completionErr != nil {
				t.Fatalf("CompleteDelivery() error = %v", completionErr)
			}
			got, err := coordinator.owners[intent.ID].Get(intent.ID)
			if err != nil {
				t.Fatalf("Get() error = %v", err)
			}
			if got.Status != test.wantStatus || got.LastError != "transport failed" {
				t.Fatalf("outcome = %#v, want status %q", got, test.wantStatus)
			}
		})
	}
}

func TestCoordinatorRetriesOnlyDefinitelyRejectedTerminalOutcome(t *testing.T) {
	tests := []struct {
		name           string
		mayHaveReached bool
		wantDispatch   bool
	}{
		{name: "ambiguous", mayHaveReached: true, wantDispatch: false},
		{name: "rejected", mayHaveReached: false, wantDispatch: true},
	}
	for ordinal, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coordinator := NewCoordinator()
			workspace := t.TempDir()
			identity := testIdentity()
			identity.Ordinal = ordinal
			msg := bus.OutboundMessage{
				Context:    bus.InboundContext{Channel: identity.Channel, ChatID: identity.ChatID},
				SessionKey: identity.SessionKey,
				Content:    test.name,
			}
			first, err := coordinator.AdmitMessage(workspace, identity, msg)
			if err != nil {
				t.Fatalf("AdmitMessage(first) error = %v", err)
			}
			if beginErr := coordinator.BeginDelivery(first.Intent.ID); beginErr != nil {
				t.Fatalf("BeginDelivery() error = %v", beginErr)
			}
			if completionErr := coordinator.CompleteDelivery(
				first.Intent.ID,
				channels.DurableDeliveryOutcome{
					Err:              errors.New("transport failed"),
					MayHaveDelivered: test.mayHaveReached,
				},
			); completionErr != nil {
				t.Fatalf("CompleteDelivery() error = %v", completionErr)
			}
			second, err := coordinator.AdmitMessage(workspace, identity, msg)
			if err != nil {
				t.Fatalf("AdmitMessage(second) error = %v", err)
			}
			if second.Dispatch != test.wantDispatch {
				t.Fatalf("second dispatch = %v, want %v", second.Dispatch, test.wantDispatch)
			}
			if test.wantDispatch {
				if beginErr := coordinator.BeginDelivery(second.Intent.ID); beginErr != nil {
					t.Fatalf("BeginDelivery(retry) error = %v", beginErr)
				}
			}
		})
	}
}
