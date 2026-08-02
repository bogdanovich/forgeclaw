package outbox

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/channels"
	"github.com/bogdanovich/mintclaw/pkg/fileutil"
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

func TestCoordinatorReleasesDefinitelyRejectedRetryAdmission(t *testing.T) {
	coordinator := NewCoordinator()
	workspace := t.TempDir()
	identity := testIdentity()
	msg := bus.OutboundMessage{
		Context:    bus.InboundContext{Channel: identity.Channel, ChatID: identity.ChatID},
		SessionKey: identity.SessionKey,
		Content:    "retry after rejection",
	}
	first, err := coordinator.AdmitMessage(workspace, identity, msg)
	if err != nil {
		t.Fatalf("AdmitMessage(first) error = %v", err)
	}
	if beginErr := coordinator.BeginDelivery(first.Intent.ID); beginErr != nil {
		t.Fatalf("BeginDelivery() error = %v", beginErr)
	}
	if completionErr := coordinator.CompleteDelivery(first.Intent.ID, channels.DurableDeliveryOutcome{
		Err: errors.New("rejected"),
	}); completionErr != nil {
		t.Fatalf("CompleteDelivery() error = %v", completionErr)
	}
	retry, err := coordinator.AdmitMessage(workspace, identity, msg)
	if err != nil || !retry.Dispatch {
		t.Fatalf("AdmitMessage(retry) = %+v, %v", retry, err)
	}
	if releaseErr := coordinator.ReleaseAdmission(retry.Intent.ID); releaseErr != nil {
		t.Fatalf("ReleaseAdmission(retry) error = %v", releaseErr)
	}
	again, err := coordinator.AdmitMessage(workspace, identity, msg)
	if err != nil || !again.Dispatch {
		t.Fatalf("AdmitMessage(after bus failure) = %+v, %v", again, err)
	}
}

func TestCoordinatorReadmitsCanonicalPayloadAfterBusRejection(t *testing.T) {
	coordinator := NewCoordinator()
	workspace := t.TempDir()
	identity := testIdentity()
	message := func(content string) bus.OutboundMessage {
		return bus.OutboundMessage{
			Context:    bus.InboundContext{Channel: identity.Channel, ChatID: identity.ChatID},
			SessionKey: identity.SessionKey,
			Content:    content,
		}
	}
	first, err := coordinator.AdmitMessage(workspace, identity, message("original response"))
	if err != nil || !first.Dispatch {
		t.Fatalf("AdmitMessage(first) = %+v, %v", first, err)
	}
	if releaseErr := coordinator.ReleaseAdmission(first.Intent.ID); releaseErr != nil {
		t.Fatalf("ReleaseAdmission(first) error = %v", releaseErr)
	}

	replayed, err := coordinator.AdmitMessage(workspace, identity, message("regenerated response"))
	if err != nil || !replayed.Dispatch {
		t.Fatalf("AdmitMessage(replay) = %+v, %v", replayed, err)
	}
	if replayed.Intent.Message == nil || replayed.Intent.Message.Content != "original response" {
		t.Fatalf("replayed canonical intent = %#v", replayed.Intent.Message)
	}
	duplicate, err := coordinator.AdmitMessage(workspace, identity, message("third response"))
	if err != nil || duplicate.Dispatch {
		t.Fatalf("AdmitMessage(duplicate) = %+v, %v, want no second dispatch", duplicate, err)
	}
	if duplicate.Intent.Message == nil || duplicate.Intent.Message.Content != "original response" {
		t.Fatalf("duplicate canonical intent = %#v", duplicate.Intent.Message)
	}
}

func TestCoordinatorReconcilesCommittedBeginAttempt(t *testing.T) {
	coordinator := NewCoordinator()
	identity := testIdentity()
	admission, err := coordinator.AdmitMessage(t.TempDir(), identity, bus.OutboundMessage{
		Context:    bus.InboundContext{Channel: identity.Channel, ChatID: identity.ChatID},
		SessionKey: identity.SessionKey,
		Content:    "committed begin",
	})
	if err != nil {
		t.Fatalf("AdmitMessage() error = %v", err)
	}
	store := coordinator.owners[admission.Intent.ID]
	originalWrite := store.writeAtomic
	writes := 0
	store.writeAtomic = func(path string, data []byte, mode os.FileMode) error {
		if writeErr := originalWrite(path, data, mode); writeErr != nil {
			return writeErr
		}
		writes++
		if writes == 1 {
			return &fileutil.CommittedWriteError{Err: errors.New("directory sync")}
		}
		return nil
	}

	if beginErr := coordinator.BeginDelivery(admission.Intent.ID); beginErr != nil {
		t.Fatalf("BeginDelivery() error = %v", beginErr)
	}
	got, err := store.Get(admission.Intent.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Status != StatusAttempting || got.Attempts != 1 || writes != 2 {
		t.Fatalf("reconciled begin = %#v, writes %d", got, writes)
	}
}

func TestCoordinatorReleasesLeaseAfterUncommittedBeginFailure(t *testing.T) {
	coordinator := NewCoordinator()
	workspace := t.TempDir()
	identity := testIdentity()
	msg := bus.OutboundMessage{
		Context:    bus.InboundContext{Channel: identity.Channel, ChatID: identity.ChatID},
		SessionKey: identity.SessionKey,
		Content:    "failed begin",
	}
	admission, err := coordinator.AdmitMessage(workspace, identity, msg)
	if err != nil {
		t.Fatalf("AdmitMessage() error = %v", err)
	}
	store := coordinator.owners[admission.Intent.ID]
	originalWrite := store.writeAtomic
	store.writeAtomic = func(string, []byte, os.FileMode) error {
		return errors.New("disk unavailable")
	}
	if beginErr := coordinator.BeginDelivery(admission.Intent.ID); beginErr == nil {
		t.Fatal("BeginDelivery() succeeded")
	}
	store.writeAtomic = originalWrite
	retry, err := coordinator.AdmitMessage(workspace, identity, msg)
	if err != nil || !retry.Dispatch {
		t.Fatalf("AdmitMessage(retry) = %+v, %v", retry, err)
	}
}
