package outbox

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/fileutil"
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

func TestRecoverRetriesSafeIntentsAndMarksInterruptedAttemptAmbiguous(t *testing.T) {
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
	definitelyFailed := createTestIntentWithOrdinal(t, store, "definitely failed", 3)
	if _, err := store.BeginAttempt(definitelyFailed.ID); err != nil {
		t.Fatalf("BeginAttempt(definitely failed) error = %v", err)
	}
	retryAt := time.Now().UTC().Add(time.Minute)
	if _, err := store.MarkDefinitelyFailed(definitelyFailed.ID, Outcome{
		RetryAfter: retryAt,
		Error:      "rate limited",
	}); err != nil {
		t.Fatalf("MarkDefinitelyFailed() error = %v", err)
	}
	ambiguous := createTestIntentWithOrdinal(t, store, "ambiguous", 4)
	if _, err := store.BeginAttempt(ambiguous.ID); err != nil {
		t.Fatalf("BeginAttempt(ambiguous) error = %v", err)
	}
	if _, err := store.MarkAmbiguous(ambiguous.ID, Outcome{Error: "timeout"}); err != nil {
		t.Fatalf("MarkAmbiguous() error = %v", err)
	}

	recovered, err := store.Recover()
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if len(recovered) != 2 {
		t.Fatalf("Recover() = %#v, want pending and definitely failed", recovered)
	}
	recoveredByID := map[string]Intent{recovered[0].ID: recovered[0], recovered[1].ID: recovered[1]}
	if recoveredByID[pending.ID].Status != StatusPending ||
		recoveredByID[definitelyFailed.ID].RetryAfter != retryAt ||
		recoveredByID[definitelyFailed.ID].LastError != "rate limited" {
		t.Fatalf("recovered safe intents = %#v", recovered)
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
	if len(recoveredAgain) != 2 {
		t.Fatalf("Recover() second = %#v, want pending and definitely failed", recoveredAgain)
	}
	recoveredAgainByID := map[string]Intent{
		recoveredAgain[0].ID: recoveredAgain[0],
		recoveredAgain[1].ID: recoveredAgain[1],
	}
	if recoveredAgainByID[pending.ID].Status != StatusPending ||
		recoveredAgainByID[definitelyFailed.ID].Status != StatusDefinitelyFailed {
		t.Fatalf("Recover() second = %#v, want pending and definitely failed", recoveredAgain)
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

func TestDefinitelyFailedIntentCanBeginRetry(t *testing.T) {
	store := openTestStore(t)
	intent := createTestIntent(t, store, "response")
	if _, err := store.BeginAttempt(intent.ID); err != nil {
		t.Fatalf("BeginAttempt(first) error = %v", err)
	}
	retryAt := time.Date(2026, time.August, 2, 12, 30, 0, 0, time.UTC)
	if _, err := store.MarkDefinitelyFailed(intent.ID, Outcome{
		RetryAfter: retryAt,
		Error:      "rejected",
	}); err != nil {
		t.Fatalf("MarkDefinitelyFailed() error = %v", err)
	}

	retrying, err := store.BeginAttempt(intent.ID)
	if err != nil {
		t.Fatalf("BeginAttempt(retry) error = %v", err)
	}
	if retrying.Status != StatusAttempting || retrying.Attempts != 2 ||
		!retrying.RetryAfter.IsZero() || retrying.LastError != "" {
		t.Fatalf("BeginAttempt(retry) = %#v", retrying)
	}
}

func TestCreateKeepsCanonicalIntentAcrossReplayChanges(t *testing.T) {
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
	conflict.OwnerWorkspace = "/agents/rerouted"
	conflict.Identity.Channel = "slack"
	conflict.Identity.ChatID = "other-chat"
	conflict.Identity.SessionKey = "agent:other:slack:other-chat"
	conflict.Message.Channel = "slack"
	conflict.Message.ChatID = "other-chat"
	conflict.Message.Context.Channel = "slack"
	conflict.Message.Context.ChatID = "other-chat"
	conflict.Message.SessionKey = conflict.Identity.SessionKey
	conflict.Message.Scope = &bus.OutboundScope{Channel: "slack"}
	replayed, err := store.Create(conflict)
	if err != nil {
		t.Fatalf("Create() replay error = %v", err)
	}
	if replayed.OwnerWorkspace != intent.OwnerWorkspace || replayed.Identity != intent.Identity ||
		replayed.Message.Content != intent.Message.Content {
		t.Fatalf("Create() replay = %#v, want canonical %#v", replayed, intent)
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

func TestOpenRejectsUnconfirmedDirectoryDurability(t *testing.T) {
	workspace := t.TempDir()
	wantErr := &fileutil.CommittedWriteError{Err: errors.New("directory sync failed")}
	store, err := open(workspace, func(root, relativePath string, perm os.FileMode) error {
		if root != workspace || relativePath != filepath.Join("state", "outbox") || perm != 0o700 {
			t.Fatalf("mkdir durable args = root %q relative %q perm %#o", root, relativePath, perm)
		}
		return wantErr
	})
	if store != nil || !errors.Is(err, wantErr) {
		t.Fatalf("open() = store %#v error %v, want nil and %v", store, err, wantErr)
	}
}

func TestTerminalTransitionReconfirmsCommittedWriteError(t *testing.T) {
	store := openTestStore(t)
	intent := createTestIntent(t, store, "response")
	if _, err := store.BeginAttempt(intent.ID); err != nil {
		t.Fatalf("BeginAttempt() error = %v", err)
	}

	originalWrite := store.writeAtomic
	writes := 0
	store.writeAtomic = func(path string, data []byte, perm os.FileMode) error {
		writes++
		if err := originalWrite(path, data, perm); err != nil {
			return err
		}
		if writes == 1 {
			return &fileutil.CommittedWriteError{Err: errors.New("directory sync failed")}
		}
		return nil
	}
	outcome := Outcome{PlatformMessageIDs: []string{"platform-1"}}
	if _, err := store.MarkDelivered(intent.ID, outcome); err == nil {
		t.Fatal("MarkDelivered() did not report uncertain durability")
	}
	if _, err := store.MarkDelivered(intent.ID, outcome); err != nil {
		t.Fatalf("MarkDelivered() reconfirm error = %v", err)
	}
	if writes != 2 {
		t.Fatalf("terminal writes = %d, want 2", writes)
	}
}

func TestRecoverReconfirmsCommittedAmbiguousWrite(t *testing.T) {
	store := openTestStore(t)
	intent := createTestIntent(t, store, "response")
	if _, err := store.BeginAttempt(intent.ID); err != nil {
		t.Fatalf("BeginAttempt() error = %v", err)
	}

	originalWrite := store.writeAtomic
	writes := 0
	store.writeAtomic = func(path string, data []byte, perm os.FileMode) error {
		writes++
		if err := originalWrite(path, data, perm); err != nil {
			return err
		}
		if writes == 1 {
			return &fileutil.CommittedWriteError{Err: errors.New("directory sync failed")}
		}
		return nil
	}
	if _, err := store.Recover(); err == nil {
		t.Fatal("Recover() did not report uncertain durability")
	}
	if recovered, err := store.Recover(); err != nil {
		t.Fatalf("Recover() reconfirm error = %v", err)
	} else if len(recovered) != 0 {
		t.Fatalf("Recover() reconfirm returned pending intents: %#v", recovered)
	}
	if writes != 2 {
		t.Fatalf("recovery writes = %d, want 2", writes)
	}
}

func TestConstructorsBindPayloadRouteToIdentity(t *testing.T) {
	identity := testIdentity()
	message, err := NewMessageIntent("/agents/main", identity, bus.OutboundMessage{
		Channel: "wrong", ChatID: "wrong", SessionKey: "wrong",
		Context: bus.InboundContext{Channel: "wrong", ChatID: "wrong"},
		Scope:   &bus.OutboundScope{Channel: "wrong"},
		Content: "response",
	}, time.Now())
	if err != nil {
		t.Fatalf("NewMessageIntent() error = %v", err)
	}
	assertMessageRouteMatchesIdentity(t, identity, *message.Message)

	media, err := NewMediaIntent("/agents/main", identity, bus.OutboundMediaMessage{
		Channel: "wrong", ChatID: "wrong", SessionKey: "wrong",
		Context: bus.InboundContext{Channel: "wrong", ChatID: "wrong"},
		Scope:   &bus.OutboundScope{Channel: "wrong"},
		Parts:   []bus.MediaPart{{Type: "image", Ref: "media://image"}},
	}, time.Now())
	if err != nil {
		t.Fatalf("NewMediaIntent() error = %v", err)
	}
	assertMediaRouteMatchesIdentity(t, identity, *media.Media)
}

func TestCreateRejectsPayloadRouteMismatch(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Intent)
	}{
		{name: "message mirror", mutate: func(intent *Intent) { intent.Message.Channel = "wrong" }},
		{name: "message context", mutate: func(intent *Intent) { intent.Message.Context.ChatID = "wrong" }},
		{name: "message session", mutate: func(intent *Intent) { intent.Message.SessionKey = "wrong" }},
		{name: "message scope", mutate: func(intent *Intent) {
			intent.Message.Scope = &bus.OutboundScope{Channel: "wrong"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := openTestStore(t)
			intent := newTestIntent(t, "response", 0)
			tc.mutate(&intent)
			if _, err := store.Create(intent); err == nil {
				t.Fatal("Create() accepted mismatched message route")
			}
		})
	}

	store := openTestStore(t)
	identity := testIdentity()
	intent, err := NewMediaIntent("/agents/main", identity, bus.OutboundMediaMessage{
		Parts: []bus.MediaPart{{Type: "image", Ref: "media://image"}},
	}, time.Now())
	if err != nil {
		t.Fatalf("NewMediaIntent() error = %v", err)
	}
	intent.Media.Context.Channel = "wrong"
	if _, err := store.Create(intent); err == nil {
		t.Fatal("Create() accepted mismatched media route")
	}
}

func TestCreateRequiresCleanPendingState(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Intent)
	}{
		{name: "status", mutate: func(intent *Intent) { intent.Status = StatusDelivered }},
		{name: "attempts", mutate: func(intent *Intent) { intent.Attempts = 1 }},
		{name: "message IDs", mutate: func(intent *Intent) { intent.PlatformMessageIDs = []string{"id"} }},
		{name: "retry after", mutate: func(intent *Intent) { intent.RetryAfter = time.Now() }},
		{name: "error", mutate: func(intent *Intent) { intent.LastError = "failed" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := openTestStore(t)
			intent := newTestIntent(t, "response", 0)
			tc.mutate(&intent)
			if _, err := store.Create(intent); err == nil {
				t.Fatal("Create() accepted non-pending lifecycle state")
			}
		})
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
	intent, err := NewMessageIntent("/agents/main", identity, bus.OutboundMessage{
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

func assertMessageRouteMatchesIdentity(t *testing.T, identity Identity, msg bus.OutboundMessage) {
	t.Helper()
	if msg.Channel != identity.Channel || msg.Context.Channel != identity.Channel ||
		msg.ChatID != identity.ChatID || msg.Context.ChatID != identity.ChatID ||
		msg.SessionKey != identity.SessionKey || msg.Scope == nil || msg.Scope.Channel != identity.Channel {
		t.Fatalf("message route = %#v, want identity %#v", msg, identity)
	}
}

func assertMediaRouteMatchesIdentity(t *testing.T, identity Identity, msg bus.OutboundMediaMessage) {
	t.Helper()
	if msg.Channel != identity.Channel || msg.Context.Channel != identity.Channel ||
		msg.ChatID != identity.ChatID || msg.Context.ChatID != identity.ChatID ||
		msg.SessionKey != identity.SessionKey || msg.Scope == nil || msg.Scope.Channel != identity.Channel {
		t.Fatalf("media route = %#v, want identity %#v", msg, identity)
	}
}
