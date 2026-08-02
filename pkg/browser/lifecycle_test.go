package browser

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/fileutil"
)

func TestFileStorePersistsAndExclusivelyOwnsState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "browser.json")
	store, err := NewFileStore(path, 0, 0)
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}
	if _, err = NewFileStore(path, 0, 0); !errors.Is(err, ErrStoreOwned) {
		t.Fatalf("second NewFileStore() error = %v, want ErrStoreOwned", err)
	}

	broker := lifecycleTestBroker(t, admittedBrowserConfig(), store, &fakeWorkerFactory{})
	session, err := broker.Open(context.Background(), OpenRequest{
		Owner: testOwner(), Target: "gateway", Profile: "managed",
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	store.Close()
	info, err := os.Stat(path)
	if err != nil || (runtime.GOOS != "windows" && info.Mode().Perm() != 0o600) {
		t.Fatalf("state file = %v, %v; want mode 0600", info, err)
	}

	reopened, err := NewFileStore(path, 0, 0)
	if err != nil {
		t.Fatalf("reopen FileStore error = %v", err)
	}
	t.Cleanup(reopened.Close)
	if err = store.CreateSession(
		context.Background(),
		testOpeningSession(testOwner()),
	); !errors.Is(
		err,
		ErrStoreClosed,
	) {
		t.Fatalf("closed store CreateSession() error = %v, want ErrStoreClosed", err)
	}
	stored, err := reopened.GetSession(context.Background(), session.ID)
	if err != nil || stored != session {
		t.Fatalf("persisted session = %+v, %v; want %+v", stored, err, session)
	}
}

func TestFileStoreRollsBackRejectedBoundedWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "browser.json")
	store, err := NewFileStore(path, 0, 256)
	if err != nil {
		t.Fatal(err)
	}
	session := testOpeningSession(testOwner())
	if err = store.CreateSession(context.Background(), session); !errors.Is(err, ErrStoreFull) {
		t.Fatalf("CreateSession() error = %v, want ErrStoreFull", err)
	}
	if _, err = store.GetSession(context.Background(), session.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("live GetSession() error = %v, want ErrNotFound", err)
	}
	store.Close()
	reopened, err := NewFileStore(path, 0, 256)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err = reopened.GetSession(context.Background(), session.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("reopened GetSession() error = %v, want ErrNotFound", err)
	}
}

func TestFileStoreRejectsUnsafeOrCorruptState(t *testing.T) {
	validEmpty := `{"version":1,"sessions":{},"invocations":{}}`
	for _, test := range []struct {
		name    string
		content string
		mode    os.FileMode
		bytes   int
	}{
		{name: "unknown field", content: `{"version":1,"sessions":{},"invocations":{},"secret":"x"}`, mode: 0o600},
		{name: "duplicate field", content: `{"version":1,"version":1,"sessions":{},"invocations":{}}`, mode: 0o600},
		{name: "trailing value", content: validEmpty + `{}`, mode: 0o600},
		{name: "insecure permissions", content: validEmpty, mode: 0o644},
		{name: "oversized", content: validEmpty, mode: 0o600, bytes: 32},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.Chmod(root, 0o700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "browser.json")
			if err := os.WriteFile(path, []byte(test.content), test.mode); err != nil {
				t.Fatal(err)
			}
			maxBytes := 0
			if test.bytes != 0 {
				maxBytes = test.bytes
			}
			store, err := NewFileStore(path, 0, maxBytes)
			if store != nil {
				store.Close()
			}
			if err == nil {
				t.Fatal("NewFileStore() error = nil")
			}
		})
	}
}

func TestBrokerRecoverMarksSessionLostAndAcceptedInvocationUnknown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "browser.json")
	store, err := NewFileStore(path, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	broker := lifecycleTestBroker(t, admittedBrowserConfig(), store, &fakeWorkerFactory{})
	owner := testOwner()
	session, err := broker.Open(context.Background(), OpenRequest{Owner: owner, Target: "gateway", Profile: "managed"})
	if err != nil {
		t.Fatal(err)
	}
	invocation := preparedInvocation(session, "invocation_restart")
	if err = store.CreateInvocation(context.Background(), invocation); err != nil {
		t.Fatal(err)
	}
	invocation.State = InvocationAccepted
	invocation.AcceptedAt = invocation.UpdatedAt + 1
	invocation.UpdatedAt = invocation.AcceptedAt
	invocation.Revision++
	if err = store.UpdateInvocation(context.Background(), 1, invocation); err != nil {
		t.Fatal(err)
	}
	store.Close() // Simulate gateway process exit after durable acceptance.

	reopened, err := NewFileStore(path, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Close)
	recovered := lifecycleTestBroker(t, admittedBrowserConfig(), reopened, &fakeWorkerFactory{})
	recovered.newID = randomID
	if err = recovered.Recover(context.Background()); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	gotSession, _ := reopened.GetSession(context.Background(), session.ID)
	gotInvocation, _ := reopened.GetInvocation(context.Background(), invocation.ID)
	if gotSession.State != SessionLost || gotSession.SafeFailure != "gateway_restarted" {
		t.Fatalf("recovered session = %+v", gotSession)
	}
	if gotInvocation.State != InvocationUnknown || gotInvocation.SafeFailure != "gateway_restarted" {
		t.Fatalf("recovered invocation = %+v", gotInvocation)
	}
	if err = recovered.Recover(context.Background()); err != nil {
		t.Fatalf("idempotent Recover() error = %v", err)
	}
	if _, err = recovered.Open(
		context.Background(),
		OpenRequest{Owner: owner, Target: "gateway", Profile: "managed"},
	); err != nil {
		t.Fatalf("Open() after recovery error = %v", err)
	}
}

func TestBrokerOpenReconcilesCommittedReadyWarning(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "browser.json")
	store, err := NewFileStore(path, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	originalWrite := store.writeFile
	writes := 0
	store.writeFile = func(path string, data []byte, mode os.FileMode) error {
		writes++
		if writeErr := originalWrite(path, data, mode); writeErr != nil {
			return writeErr
		}
		if writes == 2 {
			return &fileutil.CommittedWriteError{Err: errors.New("directory sync failed")}
		}
		return nil
	}
	factory := &fakeWorkerFactory{}
	broker := lifecycleTestBroker(t, admittedBrowserConfig(), store, factory)
	session, err := broker.Open(
		context.Background(),
		OpenRequest{Owner: testOwner(), Target: "gateway", Profile: "managed"},
	)
	if err == nil || session.State != SessionLost || factory.workers[0].closed != 1 {
		t.Fatalf("Open() = %+v, %v; worker = %+v", session, err, factory.workers[0])
	}
	stored, getErr := store.GetSession(context.Background(), session.ID)
	if getErr != nil || stored.State != SessionLost {
		t.Fatalf("stored session = %+v, %v", stored, getErr)
	}
	if _, err = broker.Open(
		context.Background(),
		OpenRequest{Owner: testOwner(), Target: "gateway", Profile: "managed"},
	); err != nil {
		t.Fatalf("Open() after committed warning reconciliation error = %v", err)
	}
}

func TestBrokerSweepEnforcesIdleAndAbsoluteExpiry(t *testing.T) {
	for _, test := range []struct {
		name    string
		advance time.Duration
	}{
		{name: "idle", advance: 6 * time.Second},
		{name: "absolute", advance: 21 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := admittedBrowserConfig()
			root.Tools.Browser.Limits.IdleSeconds = 5
			root.Tools.Browser.Limits.SessionSeconds = 20
			root.Tools.Browser.Limits.PreparedSeconds = 5
			factory := &fakeWorkerFactory{}
			broker := lifecycleTestBroker(t, root, NewMemoryStore(), factory)
			now := time.Unix(1_000, 0).UTC()
			broker.now = func() time.Time { return now }
			session, err := broker.Open(
				context.Background(),
				OpenRequest{Owner: testOwner(), Target: "gateway", Profile: "managed"},
			)
			if err != nil {
				t.Fatal(err)
			}
			now = now.Add(test.advance)
			if err = broker.Sweep(context.Background()); err != nil {
				t.Fatalf("Sweep() error = %v", err)
			}
			expired, err := broker.Status(context.Background(), testOwner(), session.ID)
			if err != nil || expired.State != SessionExpired || factory.workers[0].closed != 1 {
				t.Fatalf("expired session = %+v, %v; worker = %+v", expired, err, factory.workers[0])
			}
		})
	}
}

func TestBrokerTouchRenewsIdleButNotAbsoluteLifetime(t *testing.T) {
	root := admittedBrowserConfig()
	root.Tools.Browser.Limits.IdleSeconds = 5
	root.Tools.Browser.Limits.SessionSeconds = 10
	root.Tools.Browser.Limits.PreparedSeconds = 5
	broker := lifecycleTestBroker(t, root, NewMemoryStore(), &fakeWorkerFactory{})
	now := time.Unix(2_000, 0).UTC()
	broker.now = func() time.Time { return now }
	session, err := broker.Open(
		context.Background(),
		OpenRequest{Owner: testOwner(), Target: "gateway", Profile: "managed"},
	)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(4 * time.Second)
	touched, err := broker.Touch(context.Background(), testOwner(), session.ID)
	if err != nil || touched.ExpiresAt != session.ExpiresAt {
		t.Fatalf("Touch() = %+v, %v", touched, err)
	}
	now = now.Add(4 * time.Second)
	if err = broker.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	ready, _ := broker.Status(context.Background(), testOwner(), session.ID)
	if ready.State != SessionReady {
		t.Fatalf("session after renewed idle = %+v", ready)
	}
	now = now.Add(3 * time.Second)
	if err = broker.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	expired, _ := broker.Status(context.Background(), testOwner(), session.ID)
	if expired.State != SessionExpired {
		t.Fatalf("session after absolute expiry = %+v", expired)
	}
}

func TestBrokerExecutePreparedPersistsAcceptanceAndReturnsTerminalResultIdempotently(t *testing.T) {
	store := NewMemoryStore()
	broker := lifecycleTestBroker(t, admittedBrowserConfig(), store, &fakeWorkerFactory{})
	owner := testOwner()
	session, err := broker.Open(context.Background(), OpenRequest{Owner: owner, Target: "gateway", Profile: "managed"})
	if err != nil {
		t.Fatal(err)
	}
	invocation := preparedInvocation(session, "invocation_once")
	if err = store.CreateInvocation(context.Background(), invocation); err != nil {
		t.Fatal(err)
	}
	calls := 0
	execute := func(context.Context) (json.RawMessage, error) {
		calls++
		return json.RawMessage(`{"ok":true}`), nil
	}
	first, err := broker.ExecutePrepared(context.Background(), owner, invocation.ID, invocation.ActionHash, execute)
	if err != nil || first.State != InvocationSucceeded || calls != 1 {
		t.Fatalf("first ExecutePrepared() = %+v, %v; calls = %d", first, err, calls)
	}
	second, err := broker.ExecutePrepared(context.Background(), owner, invocation.ID, invocation.ActionHash, execute)
	if err != nil || second.State != InvocationSucceeded || calls != 1 ||
		string(second.TerminalResult) != `{"ok":true}` {
		t.Fatalf("second ExecutePrepared() = %+v, %v; calls = %d", second, err, calls)
	}
}

func TestBrokerExecutePreparedNeverReplaysAcceptedInvocation(t *testing.T) {
	store := NewMemoryStore()
	broker := lifecycleTestBroker(t, admittedBrowserConfig(), store, &fakeWorkerFactory{})
	owner := testOwner()
	session, err := broker.Open(context.Background(), OpenRequest{Owner: owner, Target: "gateway", Profile: "managed"})
	if err != nil {
		t.Fatal(err)
	}
	invocation := preparedInvocation(session, "invocation_accepted")
	if err = store.CreateInvocation(context.Background(), invocation); err != nil {
		t.Fatal(err)
	}
	invocation.State = InvocationAccepted
	invocation.Revision = 2
	invocation.AcceptedAt = invocation.UpdatedAt + 1
	invocation.UpdatedAt = invocation.AcceptedAt
	if err = store.UpdateInvocation(context.Background(), 1, invocation); err != nil {
		t.Fatal(err)
	}
	calls := 0
	got, err := broker.ExecutePrepared(
		context.Background(),
		owner,
		invocation.ID,
		invocation.ActionHash,
		func(context.Context) (json.RawMessage, error) {
			calls++
			return json.RawMessage(`{"unexpected":true}`), nil
		},
	)
	if err != nil || got.State != InvocationUnknown || calls != 0 {
		t.Fatalf("ExecutePrepared() = %+v, %v; calls = %d", got, err, calls)
	}
}

func TestBrokerExecutePreparedDoesNotDispatchBeforeDurableAcceptance(t *testing.T) {
	store := &failInvocationAcceptanceStore{MemoryStore: NewMemoryStore()}
	broker := lifecycleTestBroker(t, admittedBrowserConfig(), store, &fakeWorkerFactory{})
	owner := testOwner()
	session, err := broker.Open(context.Background(), OpenRequest{Owner: owner, Target: "gateway", Profile: "managed"})
	if err != nil {
		t.Fatal(err)
	}
	invocation := preparedInvocation(session, "invocation_persist_failure")
	if err = store.CreateInvocation(context.Background(), invocation); err != nil {
		t.Fatal(err)
	}
	calls := 0
	_, err = broker.ExecutePrepared(
		context.Background(),
		owner,
		invocation.ID,
		invocation.ActionHash,
		func(context.Context) (json.RawMessage, error) {
			calls++
			return json.RawMessage(`{"unexpected":true}`), nil
		},
	)
	if !errors.Is(err, ErrStale) || calls != 0 {
		t.Fatalf("ExecutePrepared() error = %v, calls = %d; want durable failure and no dispatch", err, calls)
	}
}

func TestBrokerExecutePreparedDoesNotDispatchAfterCommittedAcceptanceWarning(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "browser.json")
	store, err := NewFileStore(path, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	originalWrite := store.writeFile
	writes := 0
	store.writeFile = func(path string, data []byte, mode os.FileMode) error {
		writes++
		if writeErr := originalWrite(path, data, mode); writeErr != nil {
			return writeErr
		}
		if writes == 4 {
			return &fileutil.CommittedWriteError{Err: errors.New("directory sync failed")}
		}
		return nil
	}
	broker := lifecycleTestBroker(t, admittedBrowserConfig(), store, &fakeWorkerFactory{})
	owner := testOwner()
	session, err := broker.Open(context.Background(), OpenRequest{Owner: owner, Target: "gateway", Profile: "managed"})
	if err != nil {
		t.Fatal(err)
	}
	invocation := preparedInvocation(session, "invocation_committed_acceptance")
	if err = store.CreateInvocation(context.Background(), invocation); err != nil {
		t.Fatal(err)
	}
	calls := 0
	execute := func(context.Context) (json.RawMessage, error) {
		calls++
		return json.RawMessage(`{"unexpected":true}`), nil
	}
	if _, err = broker.ExecutePrepared(
		context.Background(),
		owner,
		invocation.ID,
		invocation.ActionHash,
		execute,
	); !fileutil.IsCommittedWriteError(
		err,
	) {
		t.Fatalf("first ExecutePrepared() error = %v, want committed write warning", err)
	}
	if calls != 0 {
		t.Fatalf("executor calls after committed acceptance warning = %d", calls)
	}
	got, err := broker.ExecutePrepared(context.Background(), owner, invocation.ID, invocation.ActionHash, execute)
	if err != nil || got.State != InvocationUnknown || calls != 0 {
		t.Fatalf("second ExecutePrepared() = %+v, %v; calls = %d", got, err, calls)
	}
}

func TestBrokerExecutePreparedCancellationBoundary(t *testing.T) {
	for _, test := range []struct {
		name           string
		cancelBefore   bool
		wantState      InvocationState
		wantAcceptedAt bool
	}{
		{name: "before acceptance", cancelBefore: true, wantState: InvocationCanceled},
		{name: "after acceptance", wantState: InvocationUnknown, wantAcceptedAt: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := NewMemoryStore()
			broker := lifecycleTestBroker(t, admittedBrowserConfig(), store, &fakeWorkerFactory{})
			owner := testOwner()
			session, err := broker.Open(
				context.Background(),
				OpenRequest{Owner: owner, Target: "gateway", Profile: "managed"},
			)
			if err != nil {
				t.Fatal(err)
			}
			invocation := preparedInvocation(session, "invocation_cancel_"+strings.ReplaceAll(test.name, " ", "_"))
			if err = store.CreateInvocation(context.Background(), invocation); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			if test.cancelBefore {
				cancel()
			}
			calls := 0
			got, err := broker.ExecutePrepared(
				ctx,
				owner,
				invocation.ID,
				invocation.ActionHash,
				func(context.Context) (json.RawMessage, error) {
					calls++
					cancel()
					return nil, context.Canceled
				},
			)
			if err != nil || got.State != test.wantState || (got.AcceptedAt != 0) != test.wantAcceptedAt {
				t.Fatalf("ExecutePrepared() = %+v, %v", got, err)
			}
			wantCalls := 1
			if test.cancelBefore {
				wantCalls = 0
			}
			if calls != wantCalls {
				t.Fatalf("executor calls = %d, want %d", calls, wantCalls)
			}
		})
	}
}

func TestBrokerExecutePreparedUsesConfiguredDeadline(t *testing.T) {
	root := admittedBrowserConfig()
	root.Tools.Browser.Limits.ActionSeconds = 1
	store := NewMemoryStore()
	broker := lifecycleTestBroker(t, root, store, &fakeWorkerFactory{})
	owner := testOwner()
	session, err := broker.Open(context.Background(), OpenRequest{Owner: owner, Target: "gateway", Profile: "managed"})
	if err != nil {
		t.Fatal(err)
	}
	invocation := preparedInvocation(session, "invocation_deadline")
	if err = store.CreateInvocation(context.Background(), invocation); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	got, err := broker.ExecutePrepared(
		context.Background(),
		owner,
		invocation.ID,
		invocation.ActionHash,
		func(ctx context.Context) (json.RawMessage, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	)
	if err != nil || got.State != InvocationUnknown || got.AcceptedAt == 0 {
		t.Fatalf("ExecutePrepared() = %+v, %v", got, err)
	}
	if elapsed := time.Since(started); elapsed < 900*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("configured action deadline elapsed = %s", elapsed)
	}
}

func TestBrokerCloseMarksAcceptedInvocationUnknown(t *testing.T) {
	store := NewMemoryStore()
	broker := lifecycleTestBroker(t, admittedBrowserConfig(), store, &fakeWorkerFactory{})
	owner := testOwner()
	session, err := broker.Open(context.Background(), OpenRequest{Owner: owner, Target: "gateway", Profile: "managed"})
	if err != nil {
		t.Fatal(err)
	}
	invocation := preparedInvocation(session, "invocation_close")
	if err = store.CreateInvocation(context.Background(), invocation); err != nil {
		t.Fatal(err)
	}
	invocation.State = InvocationAccepted
	invocation.Revision++
	invocation.AcceptedAt = invocation.UpdatedAt + 1
	invocation.UpdatedAt = invocation.AcceptedAt
	if err = store.UpdateInvocation(context.Background(), 1, invocation); err != nil {
		t.Fatal(err)
	}
	if _, err = broker.Close(context.Background(), owner, session.ID); err != nil {
		t.Fatal(err)
	}
	stored, err := store.GetInvocation(context.Background(), invocation.ID)
	if err != nil || stored.State != InvocationUnknown || stored.SafeFailure != "session_closed" {
		t.Fatalf("invocation after close = %+v, %v", stored, err)
	}
}

func TestBrokerSweepPrunesOnlyExpiredTerminalInvocations(t *testing.T) {
	root := admittedBrowserConfig()
	root.Tools.Browser.Limits.RetentionSecs = 5
	store := NewMemoryStore()
	broker := lifecycleTestBroker(t, root, store, &fakeWorkerFactory{})
	now := time.Unix(3_000, 0).UTC()
	broker.now = func() time.Time { return now }
	owner := testOwner()
	session, err := broker.Open(context.Background(), OpenRequest{Owner: owner, Target: "gateway", Profile: "managed"})
	if err != nil {
		t.Fatal(err)
	}
	invocation := preparedInvocation(session, "invocation_retention")
	if err = store.CreateInvocation(context.Background(), invocation); err != nil {
		t.Fatal(err)
	}
	invocation.State = InvocationCanceled
	invocation.Revision++
	invocation.UpdatedAt = invocation.CreatedAt + 1
	invocation.CompletedAt = invocation.UpdatedAt
	invocation.SafeFailure = "canceled"
	if err = store.UpdateInvocation(context.Background(), 1, invocation); err != nil {
		t.Fatal(err)
	}
	now = time.Unix(0, invocation.CompletedAt).Add(6 * time.Second)
	if err = broker.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = store.GetInvocation(context.Background(), invocation.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetInvocation() after retention error = %v, want ErrNotFound", err)
	}
}

func TestBrokerPolicyChangeInvalidatesSession(t *testing.T) {
	factory := &fakeWorkerFactory{}
	broker := lifecycleTestBroker(t, admittedBrowserConfig(), NewMemoryStore(), factory)
	session, err := broker.Open(
		context.Background(),
		OpenRequest{Owner: testOwner(), Target: "gateway", Profile: "managed"},
	)
	if err != nil {
		t.Fatal(err)
	}
	broker.policyRevision = strings.Repeat("b", 64)
	got, err := broker.Status(context.Background(), testOwner(), session.ID)
	if err != nil || got.State != SessionLost || got.SafeFailure != "policy_changed" || factory.workers[0].closed != 1 {
		t.Fatalf("Status() = %+v, %v; worker = %+v", got, err, factory.workers[0])
	}
}

func lifecycleTestBroker(t *testing.T, cfg *config.Config, store Store, factory WorkerFactory) *Broker {
	t.Helper()
	broker, err := NewBroker(cfg, store, factory)
	if err != nil {
		t.Fatal(err)
	}
	counter := 0
	broker.newID = func() (string, error) {
		counter++
		return "lifecycle_session_" + string(rune('a'+counter)), nil
	}
	return broker
}

func preparedInvocation(session Session, id string) Invocation {
	created := session.UpdatedAt + 1
	return Invocation{
		ID: id, SessionID: session.ID, Owner: session.Owner,
		ActionHash: strings.Repeat("a", 64), Effect: EffectRead,
		State: InvocationPrepared, Revision: 1, CreatedAt: created,
		UpdatedAt: created, ExpiresAt: created + int64(time.Minute),
	}
}

type failInvocationAcceptanceStore struct {
	*MemoryStore
}

func (store *failInvocationAcceptanceStore) UpdateInvocation(
	ctx context.Context,
	expected uint64,
	next Invocation,
) error {
	if next.State == InvocationAccepted {
		return ErrStale
	}
	return store.MemoryStore.UpdateInvocation(ctx, expected, next)
}
