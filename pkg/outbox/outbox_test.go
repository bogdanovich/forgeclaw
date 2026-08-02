package outbox

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/bus"
)

func TestDeliveryIDIsStableForLogicalMessage(t *testing.T) {
	identity := testIdentity()
	first, err := DeliveryID(identity)
	if err != nil {
		t.Fatalf("DeliveryID() error = %v", err)
	}
	second, err := DeliveryID(identity)
	if err != nil {
		t.Fatalf("DeliveryID() second error = %v", err)
	}
	if first != second {
		t.Fatalf("DeliveryID() = %q, want stable %q", second, first)
	}

	identity.Ordinal++
	different, err := DeliveryID(identity)
	if err != nil {
		t.Fatalf("DeliveryID() changed ordinal error = %v", err)
	}
	if different == first {
		t.Fatal("DeliveryID() did not distinguish message ordinal")
	}
}

func TestStorePersistsDeliveryLifecycle(t *testing.T) {
	store := openTestStore(t)
	created := createTestIntent(t, store, "response")

	attempting, err := store.BeginAttempt(created.ID)
	if err != nil {
		t.Fatalf("BeginAttempt() error = %v", err)
	}
	if attempting.Status != StatusAttempting || attempting.Attempts != 1 {
		t.Fatalf("BeginAttempt() = status %q attempts %d", attempting.Status, attempting.Attempts)
	}

	retryAt := time.Date(2026, time.August, 2, 12, 30, 0, 0, time.UTC)
	delivered, err := store.MarkDelivered(created.ID, Outcome{
		PlatformMessageIDs: []string{"platform-1", "platform-2"},
		RetryAfter:         retryAt,
	})
	if err != nil {
		t.Fatalf("MarkDelivered() error = %v", err)
	}
	if delivered.Status != StatusDelivered {
		t.Fatalf("MarkDelivered() status = %q, want %q", delivered.Status, StatusDelivered)
	}
	if len(delivered.PlatformMessageIDs) != 2 || delivered.RetryAfter != retryAt {
		t.Fatalf("MarkDelivered() metadata = %#v", delivered)
	}

	reopened, err := Open(filepath.Dir(filepath.Dir(store.dir)))
	if err != nil {
		t.Fatalf("Open() reopened error = %v", err)
	}
	loaded, err := reopened.Get(created.ID)
	if err != nil {
		t.Fatalf("Get() reopened error = %v", err)
	}
	if loaded.Status != StatusDelivered || loaded.Attempts != 1 {
		t.Fatalf("Get() reopened = status %q attempts %d", loaded.Status, loaded.Attempts)
	}
}

func TestRecoverRetriesOnlyPendingAndMarksInterruptedAttemptAmbiguous(t *testing.T) {
	store := openTestStore(t)
	pending := createTestIntent(t, store, "pending")
	attempting := createTestIntentWithOrdinal(t, store, "attempting", 1)
	if _, err := store.BeginAttempt(attempting.ID); err != nil {
		t.Fatalf("BeginAttempt() error = %v", err)
	}
	delivered := createTestIntentWithOrdinal(t, store, "delivered", 2)
	if _, err := store.BeginAttempt(delivered.ID); err != nil {
		t.Fatalf("BeginAttempt(delivered) error = %v", err)
	}
	if _, err := store.MarkDelivered(delivered.ID, Outcome{}); err != nil {
		t.Fatalf("MarkDelivered() error = %v", err)
	}

	recovered, err := store.Recover()
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if len(recovered) != 1 || recovered[0].ID != pending.ID {
		t.Fatalf("Recover() = %#v, want only pending %q", recovered, pending.ID)
	}

	interrupted, err := store.Get(attempting.ID)
	if err != nil {
		t.Fatalf("Get(interrupted) error = %v", err)
	}
	if interrupted.Status != StatusAmbiguous {
		t.Fatalf("interrupted status = %q, want %q", interrupted.Status, StatusAmbiguous)
	}
	if interrupted.LastError == "" {
		t.Fatal("interrupted attempt did not record recovery reason")
	}

	recoveredAgain, err := store.Recover()
	if err != nil {
		t.Fatalf("Recover() second error = %v", err)
	}
	if len(recoveredAgain) != 1 || recoveredAgain[0].ID != pending.ID {
		t.Fatalf("Recover() second = %#v, want only pending %q", recoveredAgain, pending.ID)
	}
}

func TestStoreRejectsInvalidTransitions(t *testing.T) {
	store := openTestStore(t)
	intent := createTestIntent(t, store, "response")

	if _, err := store.MarkDelivered(intent.ID, Outcome{}); err == nil {
		t.Fatal("MarkDelivered() from pending succeeded")
	}
	if _, err := store.BeginAttempt(intent.ID); err != nil {
		t.Fatalf("BeginAttempt() error = %v", err)
	}
	if _, err := store.MarkAmbiguous(intent.ID, Outcome{Error: "timeout"}); err != nil {
		t.Fatalf("MarkAmbiguous() error = %v", err)
	}
	if _, err := store.BeginAttempt(intent.ID); err == nil {
		t.Fatal("BeginAttempt() retried an ambiguous intent")
	}
}

func TestCreateIsIdempotentButRejectsPayloadConflict(t *testing.T) {
	store := openTestStore(t)
	intent := newTestIntent(t, "response", 0)
	if _, err := store.Create(intent); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := store.Create(intent); err != nil {
		t.Fatalf("Create() duplicate error = %v", err)
	}

	conflict := intent
	conflict.Message = cloneMessage(intent.Message)
	conflict.Message.Content = "different response"
	if _, err := store.Create(conflict); err == nil {
		t.Fatal("Create() accepted conflicting payload for stable identity")
	}
}

func TestCreateDoesNotAdmitFailedPersistence(t *testing.T) {
	store := openTestStore(t)
	store.writeAtomic = func(string, []byte, os.FileMode) error {
		return errors.New("disk unavailable")
	}
	intent := newTestIntent(t, "response", 0)
	if _, err := store.Create(intent); err == nil {
		t.Fatal("Create() succeeded after persistence failure")
	}
	if _, err := os.Stat(store.recordPath(intent.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("record exists after failed persistence: %v", err)
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	store.now = func() time.Time {
		return time.Date(2026, time.August, 2, 12, 0, 0, 0, time.UTC)
	}
	return store
}

func createTestIntent(t *testing.T, store *Store, content string) Intent {
	t.Helper()
	return createTestIntentWithOrdinal(t, store, content, 0)
}

func createTestIntentWithOrdinal(t *testing.T, store *Store, content string, ordinal int) Intent {
	t.Helper()
	intent := newTestIntent(t, content, ordinal)
	created, err := store.Create(intent)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	return created
}

func newTestIntent(t *testing.T, content string, ordinal int) Intent {
	t.Helper()
	identity := testIdentity()
	identity.Ordinal = ordinal
	intent, err := NewMessageIntent(identity, bus.OutboundMessage{
		Context: bus.InboundContext{
			Channel:  "telegram",
			ChatID:   "chat-1",
			SenderID: "user-1",
		},
		SessionKey: identity.SessionKey,
		Content:    content,
	}, time.Date(2026, time.August, 2, 11, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("NewMessageIntent() error = %v", err)
	}
	return intent
}

func testIdentity() Identity {
	return Identity{
		SourceID:   "spool-123",
		Kind:       KindMessage,
		Channel:    "telegram",
		ChatID:     "chat-1",
		SessionKey: "agent:main:telegram:chat-1",
	}
}

func cloneMessage(msg *bus.OutboundMessage) *bus.OutboundMessage {
	if msg == nil {
		return nil
	}
	cloned := *msg
	return &cloned
}
