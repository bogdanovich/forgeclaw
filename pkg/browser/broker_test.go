package browser

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/config"
)

type fakeWorker struct {
	closeErr            error
	closed              int
	rejectRepeatedClose bool
	status              WorkerStatus
	statusErr           error
	statusCalls         int
}

func (worker *fakeWorker) Status(context.Context) (WorkerStatus, error) {
	worker.statusCalls++
	return worker.status, worker.statusErr
}

func (worker *fakeWorker) Close(context.Context) error {
	worker.closed++
	if worker.rejectRepeatedClose && worker.closed > 1 {
		return errors.New("worker close is not idempotent")
	}
	return worker.closeErr
}

type fakeWorkerFactory struct {
	mu             sync.Mutex
	openErr        error
	cleanupWorker  *fakeWorker
	requests       []WorkerOpenRequest
	workers        []*fakeWorker
	readiness      DriverReadiness
	readinessCalls int
}

func (factory *fakeWorkerFactory) PassiveReadiness() DriverReadiness {
	factory.readinessCalls++
	if factory.readiness.Status != "" {
		return factory.readiness
	}
	return configuredDriverReadiness()
}

type failNextSessionUpdateStore struct {
	*MemoryStore
	failNext  bool
	failAfter int
	failState SessionState
}

func (store *failNextSessionUpdateStore) UpdateSession(
	ctx context.Context,
	expected uint64,
	next Session,
) error {
	if store.failAfter > 0 {
		store.failAfter--
		if store.failAfter == 0 {
			return ErrStale
		}
	}
	if store.failNext || (store.failState != "" && next.State == store.failState) {
		store.failNext = false
		store.failState = ""
		return ErrStale
	}
	return store.MemoryStore.UpdateSession(ctx, expected, next)
}

func (factory *fakeWorkerFactory) Open(
	_ context.Context,
	request WorkerOpenRequest,
) (WorkerOpenResult, error) {
	factory.mu.Lock()
	defer factory.mu.Unlock()
	factory.requests = append(factory.requests, request)
	if factory.openErr != nil {
		var cleanup Worker
		if factory.cleanupWorker != nil {
			cleanup = factory.cleanupWorker
		}
		return WorkerOpenResult{Owner: cleanup}, factory.openErr
	}
	worker := &fakeWorker{status: WorkerReady}
	factory.workers = append(factory.workers, worker)
	return WorkerOpenResult{Owner: worker}, nil
}

func TestBrokerOpenAndCloseSession(t *testing.T) {
	store := NewMemoryStore()
	factory := &fakeWorkerFactory{}
	broker := newTestBroker(t, admittedBrowserConfig(), store, factory)
	owner := testOwner()

	session, err := broker.Open(context.Background(), OpenRequest{
		Owner: owner, Target: "gateway", Profile: "managed",
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if session.State != SessionReady || !session.DryRun || session.Revision != 2 {
		t.Fatalf("Open() session = %+v", session)
	}
	if len(factory.requests) != 1 || factory.requests[0].SessionID != session.ID ||
		factory.requests[0].Limits.Sessions != 1 {
		t.Fatalf("worker requests = %+v", factory.requests)
	}

	status, err := broker.Status(context.Background(), owner, session.ID)
	if err != nil || status != session {
		t.Fatalf("Status() = %+v, %v; want %+v", status, err, session)
	}
	closed, err := broker.Close(context.Background(), owner, session.ID)
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if closed.State != SessionClosed || closed.Revision != 4 || factory.workers[0].closed != 1 {
		t.Fatalf("Close() session = %+v, worker = %+v", closed, factory.workers[0])
	}
	again, err := broker.Close(context.Background(), owner, session.ID)
	if err != nil || again != closed || factory.workers[0].closed != 1 {
		t.Fatalf("second Close() = %+v, %v; worker = %+v", again, err, factory.workers[0])
	}
}

func TestBrokerProfileAvailabilityIsReadOnly(t *testing.T) {
	store := NewMemoryStore()
	broker := newTestBroker(t, admittedBrowserConfig(), store, &fakeWorkerFactory{})
	ready, err := broker.ProfileAvailability(context.Background(), "gateway", "managed")
	if err != nil || ready != (ProfileAvailability{Status: "ready"}) {
		t.Fatalf("initial availability = %#v, %v", ready, err)
	}
	session, err := broker.Open(context.Background(), OpenRequest{
		Owner: testOwner(), Target: "gateway", Profile: "managed",
	})
	if err != nil {
		t.Fatal(err)
	}
	busy, err := broker.ProfileAvailability(context.Background(), "gateway", "managed")
	if err != nil || busy != (ProfileAvailability{Status: "busy", Reason: "profile_busy"}) {
		t.Fatalf("leased availability = %#v, %v", busy, err)
	}
	stored, err := store.GetSession(context.Background(), session.ID)
	if err != nil || stored.Revision != session.Revision || stored.LastActivityAt != session.LastActivityAt {
		t.Fatalf("availability changed session = %#v, %v; want %#v", stored, err, session)
	}
	if _, err = broker.ProfileAvailability(context.Background(), "unknown", "managed"); !errors.Is(err, ErrDenied) {
		t.Fatalf("unknown target availability error = %v", err)
	}
}

func TestBrokerPassiveReadinessDoesNotProbeOrRenewWorker(t *testing.T) {
	store := NewMemoryStore()
	factory := &fakeWorkerFactory{readiness: DriverReadiness{
		Status: ReadinessReady, Driver: ReadinessReady, Browser: ReadinessReady,
		Proxy: ReadinessReady, Compatibility: CompatibilityCompatible,
	}}
	broker := newTestBroker(t, admittedBrowserConfig(), store, factory)
	initial, err := broker.PassiveReadiness(context.Background(), "gateway", "managed")
	if err != nil || initial.Status != ReadinessReady || !initial.Passive ||
		initial.Profile.Status != ReadinessReady || factory.readinessCalls != 1 ||
		len(factory.requests) != 0 {
		t.Fatalf("initial readiness = %#v, %v; factory = %#v", initial, err, factory)
	}
	session, err := broker.Open(context.Background(), OpenRequest{
		Owner: testOwner(), Target: "gateway", Profile: "managed",
	})
	if err != nil {
		t.Fatal(err)
	}
	busy, err := broker.PassiveReadiness(context.Background(), "gateway", "managed")
	if err != nil || busy.Status != ReadinessBusy || busy.Code != "profile_busy" ||
		busy.Action != "wait_or_close_session" || factory.workers[0].statusCalls != 0 {
		t.Fatalf("busy readiness = %#v, %v; worker = %#v", busy, err, factory.workers[0])
	}
	stored, err := store.GetSession(context.Background(), session.ID)
	if err != nil || stored.Revision != session.Revision || stored.LastActivityAt != session.LastActivityAt {
		t.Fatalf("readiness changed session = %#v, %v; want %#v", stored, err, session)
	}
	delete(broker.slots, session.ID)
	degraded, err := broker.PassiveReadiness(context.Background(), "gateway", "managed")
	if err != nil || degraded.Status != ReadinessDegraded || degraded.Code != "recovery_required" ||
		degraded.Action != "close_or_recover_session" || factory.workers[0].statusCalls != 0 {
		t.Fatalf("degraded readiness = %#v, %v", degraded, err)
	}
}

func TestRandomIDAlwaysUsesValidUniqueSessionIdentifiers(t *testing.T) {
	seen := make(map[string]struct{}, 10_000)
	for range 10_000 {
		id, err := randomID()
		if err != nil {
			t.Fatalf("randomID() error = %v", err)
		}
		if !strings.HasPrefix(id, "session_") || !validIdentifier(id) {
			t.Fatalf("randomID() = %q, want a valid session identifier", id)
		}
		if _, exists := seen[id]; exists {
			t.Fatalf("randomID() produced duplicate %q", id)
		}
		seen[id] = struct{}{}
	}
}

func TestBrokerCloseRetainsWorkerAndLeaseUntilCleanupSucceeds(t *testing.T) {
	store := NewMemoryStore()
	factory := &fakeWorkerFactory{}
	broker := newTestBroker(t, admittedBrowserConfig(), store, factory)
	owner := testOwner()
	session, err := broker.Open(context.Background(), OpenRequest{
		Owner: owner, Target: "gateway", Profile: "managed",
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	worker := factory.workers[0]
	worker.closeErr = errors.New("secret cleanup failure")
	if _, err = broker.Close(context.Background(), owner, session.ID); !errors.Is(err, ErrWorkerUnavailable) ||
		strings.Contains(err.Error(), "secret") {
		t.Fatalf("Close() cleanup error = %v", err)
	}
	stored, err := store.GetSession(context.Background(), session.ID)
	if err != nil || stored.State != SessionClosing || worker.closed != 1 {
		t.Fatalf("stored session after close failure = %+v, %v; worker = %+v", stored, err, worker)
	}
	other := owner
	other.ExecutionID = "execution_2"
	if _, err = broker.Open(context.Background(), OpenRequest{
		Owner: other, Target: "gateway", Profile: "managed",
	}); !errors.Is(err, ErrBusy) {
		t.Fatalf("Open() while close remains pending error = %v, want ErrBusy", err)
	}
	worker.closeErr = nil
	closed, err := broker.Close(context.Background(), owner, session.ID)
	if err != nil || closed.State != SessionClosed || worker.closed != 2 {
		t.Fatalf("Close() cleanup retry = %+v, %v; worker = %+v", closed, err, worker)
	}
}

func TestBrokerCloseRetriesOnlyTerminalPersistenceAfterCleanup(t *testing.T) {
	store := &failNextSessionUpdateStore{MemoryStore: NewMemoryStore()}
	factory := &fakeWorkerFactory{}
	broker := newTestBroker(t, admittedBrowserConfig(), store, factory)
	owner := testOwner()
	session, err := broker.Open(context.Background(), OpenRequest{
		Owner: owner, Target: "gateway", Profile: "managed",
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	worker := factory.workers[0]
	worker.rejectRepeatedClose = true
	store.failState = SessionClosed
	if _, err = broker.Close(context.Background(), owner, session.ID); !errors.Is(err, ErrStale) {
		t.Fatalf("Close() persistence error = %v, want ErrStale", err)
	}
	stored, err := store.GetSession(context.Background(), session.ID)
	if err != nil || stored.State != SessionClosing || worker.closed != 1 {
		t.Fatalf("stored session after persistence failure = %+v, %v; worker = %+v", stored, err, worker)
	}
	closed, err := broker.Close(context.Background(), owner, session.ID)
	if err != nil || closed.State != SessionClosed || worker.closed != 1 {
		t.Fatalf("Close() persistence retry = %+v, %v; worker = %+v", closed, err, worker)
	}
}

func TestBrokerDeniesUnadmittedAuthorityBeforeWorkerOpen(t *testing.T) {
	tests := []struct {
		name   string
		cfg    *config.Config
		mutate func(*OpenRequest)
	}{
		{name: "disabled", cfg: config.DefaultConfig()},
		{
			name: "agent",
			cfg:  admittedBrowserConfig(),
			mutate: func(request *OpenRequest) {
				request.Owner.AgentID = "main"
			},
		},
		{
			name: "target",
			cfg:  admittedBrowserConfig(),
			mutate: func(request *OpenRequest) {
				request.Target = "companion"
			},
		},
		{
			name: "profile",
			cfg:  admittedBrowserConfig(),
			mutate: func(request *OpenRequest) {
				request.Profile = "attached"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			factory := &fakeWorkerFactory{}
			broker := newTestBroker(t, test.cfg, NewMemoryStore(), factory)
			request := OpenRequest{Owner: testOwner(), Target: "gateway", Profile: "managed"}
			if test.mutate != nil {
				test.mutate(&request)
			}
			if _, err := broker.Open(context.Background(), request); !errors.Is(err, ErrDenied) {
				t.Fatalf("Open() error = %v, want ErrDenied", err)
			}
			if len(factory.requests) != 0 {
				t.Fatalf("worker opened for denied request: %+v", factory.requests)
			}
		})
	}
}

func TestBrokerRejectsSecondProfileSessionBeforeWorkerOpen(t *testing.T) {
	store := NewMemoryStore()
	factory := &fakeWorkerFactory{}
	broker := newTestBroker(t, admittedBrowserConfig(), store, factory)
	request := OpenRequest{Owner: testOwner(), Target: "gateway", Profile: "managed"}
	if _, err := broker.Open(context.Background(), request); err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	request.Owner.ExecutionID = "execution_2"
	if _, err := broker.Open(context.Background(), request); !errors.Is(err, ErrBusy) {
		t.Fatalf("second Open() error = %v, want ErrBusy", err)
	}
	if len(factory.requests) != 1 {
		t.Fatalf("worker opens = %d, want 1", len(factory.requests))
	}
}

func TestBrokerPersistsSafeLostStateWhenWorkerOpenFails(t *testing.T) {
	store := NewMemoryStore()
	factory := &fakeWorkerFactory{openErr: errors.New("secret executable path")}
	broker := newTestBroker(t, admittedBrowserConfig(), store, factory)

	session, err := broker.Open(context.Background(), OpenRequest{
		Owner: testOwner(), Target: "gateway", Profile: "managed",
	})
	if !errors.Is(err, ErrWorkerUnavailable) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("Open() error = %v, want bounded ErrWorkerUnavailable", err)
	}
	if session.State != SessionLost || session.SafeFailure != "worker_unavailable" {
		t.Fatalf("Open() lost session = %+v", session)
	}
	stored, getErr := store.GetSession(context.Background(), session.ID)
	if getErr != nil || stored != session {
		t.Fatalf("stored session = %+v, %v; want %+v", stored, getErr, session)
	}
	if strings.Contains(stored.SafeFailure, "secret") {
		t.Fatalf("stored safe failure leaked worker error: %q", stored.SafeFailure)
	}
}

func TestBrokerRetainsFailedOpenCleanupUntilCloseRetrySucceeds(t *testing.T) {
	store := NewMemoryStore()
	cleanup := &fakeWorker{closeErr: errors.New("secret cleanup failure")}
	factory := &fakeWorkerFactory{
		openErr: errors.New("secret startup failure"), cleanupWorker: cleanup,
	}
	broker := newTestBroker(t, admittedBrowserConfig(), store, factory)
	owner := testOwner()

	session, err := broker.Open(context.Background(), OpenRequest{
		Owner: owner, Target: "gateway", Profile: "managed",
	})
	if !errors.Is(err, ErrWorkerUnavailable) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("Open() error = %v, want bounded ErrWorkerUnavailable", err)
	}
	if session.State != SessionClosing || session.SafeFailure != "" || cleanup.closed != 1 {
		t.Fatalf("Open() session = %+v, cleanup closes = %d", session, cleanup.closed)
	}
	stored, getErr := store.GetSession(context.Background(), session.ID)
	if getErr != nil || stored != session {
		t.Fatalf("stored session = %+v, %v; want %+v", stored, getErr, session)
	}

	owner.ExecutionID = "execution_2"
	if _, err = broker.Open(context.Background(), OpenRequest{
		Owner: owner, Target: "gateway", Profile: "managed",
	}); !errors.Is(err, ErrBusy) {
		t.Fatalf("Open() while startup cleanup is pending error = %v, want ErrBusy", err)
	}
	if len(factory.requests) != 1 {
		t.Fatalf("worker opens = %d, want 1", len(factory.requests))
	}

	cleanup.closeErr = nil
	owner.ExecutionID = "execution_1"
	lost, err := broker.Close(context.Background(), owner, session.ID)
	if err != nil || lost.State != SessionLost || lost.SafeFailure != "worker_unavailable" ||
		cleanup.closed != 2 {
		t.Fatalf("Close() retry = %+v, %v; cleanup closes = %d", lost, err, cleanup.closed)
	}
}

func TestBrokerRetainsFailedOpenCleanupWhenClosingPersistenceFails(t *testing.T) {
	store := &failNextSessionUpdateStore{MemoryStore: NewMemoryStore(), failState: SessionClosing}
	cleanup := &fakeWorker{closeErr: errors.New("secret cleanup failure")}
	factory := &fakeWorkerFactory{
		openErr: errors.New("secret startup failure"), cleanupWorker: cleanup,
	}
	broker := newTestBroker(t, admittedBrowserConfig(), store, factory)
	owner := testOwner()

	session, err := broker.Open(context.Background(), OpenRequest{
		Owner: owner, Target: "gateway", Profile: "managed",
	})
	if !errors.Is(err, ErrWorkerUnavailable) || !errors.Is(err, ErrStale) || session.ID == "" ||
		session.State != SessionOpening || cleanup.closed != 1 {
		t.Fatalf("Open() = %+v, %v; cleanup closes = %d", session, err, cleanup.closed)
	}
	stored, getErr := store.GetSession(context.Background(), session.ID)
	if getErr != nil || stored != session {
		t.Fatalf("stored session = %+v, %v; want %+v", stored, getErr, session)
	}

	cleanup.closeErr = nil
	lost, err := broker.Close(context.Background(), owner, session.ID)
	if err != nil || lost.State != SessionLost || lost.SafeFailure != "worker_unavailable" ||
		cleanup.closed != 2 {
		t.Fatalf("Close() retry = %+v, %v; cleanup closes = %d", lost, err, cleanup.closed)
	}
}

func TestBrokerRetainsCleanupCompleteSlotWhenClosingPersistenceFails(t *testing.T) {
	store := &failNextSessionUpdateStore{MemoryStore: NewMemoryStore(), failState: SessionClosing}
	cleanup := &fakeWorker{}
	factory := &fakeWorkerFactory{
		openErr: errors.New("secret startup failure"), cleanupWorker: cleanup,
	}
	broker := newTestBroker(t, admittedBrowserConfig(), store, factory)
	owner := testOwner()

	session, err := broker.Open(context.Background(), OpenRequest{
		Owner: owner, Target: "gateway", Profile: "managed",
	})
	if !errors.Is(err, ErrWorkerUnavailable) || !errors.Is(err, ErrStale) || session.ID == "" ||
		session.State != SessionOpening || cleanup.closed != 1 {
		t.Fatalf("Open() = %+v, %v; cleanup closes = %d", session, err, cleanup.closed)
	}

	lost, err := broker.Close(context.Background(), owner, session.ID)
	if err != nil || lost.State != SessionLost || lost.SafeFailure != "worker_unavailable" ||
		cleanup.closed != 1 {
		t.Fatalf("Close() transition retry = %+v, %v; cleanup closes = %d", lost, err, cleanup.closed)
	}
}

func TestBrokerCleansWorkerAndPersistsLossWhenReadyPersistenceFails(t *testing.T) {
	store := &failNextSessionUpdateStore{MemoryStore: NewMemoryStore(), failState: SessionReady}
	factory := &fakeWorkerFactory{}
	broker := newTestBroker(t, admittedBrowserConfig(), store, factory)
	session, err := broker.Open(context.Background(), OpenRequest{
		Owner: testOwner(), Target: "gateway", Profile: "managed",
	})
	if !errors.Is(err, ErrStale) || !errors.Is(err, ErrWorkerUnavailable) {
		t.Fatalf("Open() persistence error = %v, want ErrStale and ErrWorkerUnavailable", err)
	}
	worker := factory.workers[0]
	if session.State != SessionLost || session.SafeFailure != "worker_unavailable" || worker.closed != 1 {
		t.Fatalf("Open() recovered session = %+v, worker = %+v", session, worker)
	}
	stored, getErr := store.GetSession(context.Background(), session.ID)
	if getErr != nil || stored != session {
		t.Fatalf("stored recovered session = %+v, %v; want %+v", stored, getErr, session)
	}
}

func TestBrokerDoesNotRevealForeignSession(t *testing.T) {
	broker := newTestBroker(t, admittedBrowserConfig(), NewMemoryStore(), &fakeWorkerFactory{})
	session, err := broker.Open(context.Background(), OpenRequest{
		Owner: testOwner(), Target: "gateway", Profile: "managed",
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	other := testOwner()
	other.ActorID = "other_actor"
	if _, err = broker.Status(context.Background(), other, session.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Status() foreign error = %v, want ErrNotFound", err)
	}
	if _, err = broker.Close(context.Background(), other, session.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Close() foreign error = %v, want ErrNotFound", err)
	}
}

func TestBrokerStatusPersistsLiveWorkerLossAndReleasesProfile(t *testing.T) {
	store := NewMemoryStore()
	factory := &fakeWorkerFactory{}
	broker := newTestBroker(t, admittedBrowserConfig(), store, factory)
	owner := testOwner()
	session, err := broker.Open(context.Background(), OpenRequest{
		Owner: owner, Target: "gateway", Profile: "managed",
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	factory.workers[0].status = WorkerLost
	lost, err := broker.Status(context.Background(), owner, session.ID)
	if err != nil {
		t.Fatalf("Status() error = %v", err)
	}
	if lost.State != SessionLost || lost.SafeFailure != "worker_lost" ||
		factory.workers[0].closed != 1 {
		t.Fatalf("Status() lost session = %+v", lost)
	}
	stored, err := store.GetSession(context.Background(), session.ID)
	if err != nil || stored != lost {
		t.Fatalf("stored lost session = %+v, %v; want %+v", stored, err, lost)
	}
	owner.ExecutionID = "execution_2"
	if _, err = broker.Open(context.Background(), OpenRequest{
		Owner: owner, Target: "gateway", Profile: "managed",
	}); err != nil {
		t.Fatalf("Open() after worker loss error = %v", err)
	}
	if len(factory.requests) != 2 {
		t.Fatalf("worker opens = %d, want 2", len(factory.requests))
	}
}

func TestBrokerStatusRetainsWorkerAndProfileWhenLossCleanupFails(t *testing.T) {
	store := NewMemoryStore()
	factory := &fakeWorkerFactory{}
	broker := newTestBroker(t, admittedBrowserConfig(), store, factory)
	owner := testOwner()
	session, err := broker.Open(context.Background(), OpenRequest{
		Owner: owner, Target: "gateway", Profile: "managed",
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	worker := factory.workers[0]
	worker.status = WorkerLost
	worker.closeErr = errors.New("secret cleanup failure")
	if _, err = broker.Status(context.Background(), owner, session.ID); !errors.Is(err, ErrWorkerUnavailable) ||
		strings.Contains(err.Error(), "secret") {
		t.Fatalf("Status() cleanup error = %v", err)
	}
	stored, err := store.GetSession(context.Background(), session.ID)
	if err != nil || stored.State != SessionReady || worker.closed != 1 {
		t.Fatalf("stored session after cleanup failure = %+v, %v; worker = %+v", stored, err, worker)
	}
	owner.ExecutionID = "execution_2"
	if _, err = broker.Open(context.Background(), OpenRequest{
		Owner: owner, Target: "gateway", Profile: "managed",
	}); !errors.Is(err, ErrBusy) {
		t.Fatalf("Open() while cleanup remains pending error = %v, want ErrBusy", err)
	}
	worker.closeErr = nil
	lost, err := broker.Status(context.Background(), testOwner(), session.ID)
	if err != nil || lost.State != SessionLost || worker.closed != 2 {
		t.Fatalf("Status() cleanup retry = %+v, %v; worker = %+v", lost, err, worker)
	}
}

func TestBrokerStatusRetainsCleanupPathWhenLostPersistenceFails(t *testing.T) {
	store := &failNextSessionUpdateStore{MemoryStore: NewMemoryStore()}
	factory := &fakeWorkerFactory{}
	broker := newTestBroker(t, admittedBrowserConfig(), store, factory)
	owner := testOwner()
	session, err := broker.Open(context.Background(), OpenRequest{
		Owner: owner, Target: "gateway", Profile: "managed",
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	worker := factory.workers[0]
	worker.status = WorkerLost
	worker.rejectRepeatedClose = true
	store.failNext = true
	if _, err = broker.Status(context.Background(), owner, session.ID); !errors.Is(err, ErrStale) {
		t.Fatalf("Status() persistence error = %v, want ErrStale", err)
	}
	stored, err := store.GetSession(context.Background(), session.ID)
	if err != nil || stored.State != SessionReady || worker.closed != 1 {
		t.Fatalf("stored session after persistence failure = %+v, %v; worker = %+v", stored, err, worker)
	}
	lost, err := broker.Status(context.Background(), owner, session.ID)
	if err != nil || lost.State != SessionLost || worker.closed != 1 {
		t.Fatalf("Status() persistence retry = %+v, %v; worker = %+v", lost, err, worker)
	}
}

func TestBrokerClosePreservesPendingLossAfterStatusPersistenceFails(t *testing.T) {
	store := &failNextSessionUpdateStore{MemoryStore: NewMemoryStore()}
	factory := &fakeWorkerFactory{}
	broker := newTestBroker(t, admittedBrowserConfig(), store, factory)
	owner := testOwner()
	session, err := broker.Open(context.Background(), OpenRequest{
		Owner: owner, Target: "gateway", Profile: "managed",
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	worker := factory.workers[0]
	worker.status = WorkerLost
	worker.rejectRepeatedClose = true
	store.failNext = true
	if _, err = broker.Status(context.Background(), owner, session.ID); !errors.Is(err, ErrStale) {
		t.Fatalf("Status() persistence error = %v, want ErrStale", err)
	}
	lost, err := broker.Close(context.Background(), owner, session.ID)
	if err != nil || lost.State != SessionLost || lost.SafeFailure != "worker_lost" ||
		worker.closed != 1 {
		t.Fatalf("Close() pending loss retry = %+v, %v; worker = %+v", lost, err, worker)
	}
}

func TestBrokerStatusRedactsWorkerFailure(t *testing.T) {
	store := NewMemoryStore()
	factory := &fakeWorkerFactory{}
	broker := newTestBroker(t, admittedBrowserConfig(), store, factory)
	owner := testOwner()
	session, err := broker.Open(context.Background(), OpenRequest{
		Owner: owner, Target: "gateway", Profile: "managed",
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	factory.workers[0].statusErr = errors.New("secret driver endpoint")
	lost, err := broker.Status(context.Background(), owner, session.ID)
	if err != nil {
		t.Fatalf("Status() error = %v", err)
	}
	if lost.State != SessionLost || lost.SafeFailure != "worker_unavailable" ||
		strings.Contains(lost.SafeFailure, "secret") {
		t.Fatalf("Status() lost session = %+v", lost)
	}
}

func TestBrokerStatusCancellationDoesNotLoseSession(t *testing.T) {
	store := NewMemoryStore()
	factory := &fakeWorkerFactory{}
	broker := newTestBroker(t, admittedBrowserConfig(), store, factory)
	owner := testOwner()
	session, err := broker.Open(context.Background(), OpenRequest{
		Owner: owner, Target: "gateway", Profile: "managed",
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	factory.workers[0].statusErr = context.Canceled
	if _, err = broker.Status(ctx, owner, session.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("Status() canceled error = %v", err)
	}
	stored, err := store.GetSession(context.Background(), session.ID)
	if err != nil || stored.State != SessionReady {
		t.Fatalf("stored session after canceled status = %+v, %v", stored, err)
	}
}

func TestBrokerSnapshotsValidatedAuthority(t *testing.T) {
	root := admittedBrowserConfig()
	broker := newTestBroker(t, root, NewMemoryStore(), &fakeWorkerFactory{})
	root.Tools.Browser.Agents[0] = "main"
	target := root.Tools.Browser.Targets["gateway"]
	profile := target.Profiles["managed"]
	profile.Enabled = false
	profile.AllowedOrigins[0] = "https://changed.example"
	target.Profiles["managed"] = profile
	root.Tools.Browser.Targets["gateway"] = target

	session, err := broker.Open(context.Background(), OpenRequest{
		Owner: testOwner(), Target: "gateway", Profile: "managed",
	})
	if err != nil {
		t.Fatalf("Open() after source config mutation error = %v", err)
	}
	if session.State != SessionReady || len(session.PolicyRevision) != 64 {
		t.Fatalf("Open() session = %+v", session)
	}
}

func TestNewBrokerRejectsInvalidRootConfiguration(t *testing.T) {
	root := admittedBrowserConfig()
	server := root.Tools.MCP.Servers["playwright"]
	server.Enabled = true
	root.Tools.MCP.Servers["playwright"] = server
	if _, err := NewBroker(root, NewMemoryStore(), &fakeWorkerFactory{}); err == nil {
		t.Fatal("NewBroker() invalid config error = nil")
	}
}

func TestMemoryStoreInvocationAcceptanceAndTerminalState(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	owner := testOwner()
	session := createReadySession(t, store, owner)
	invocation := Invocation{
		ID: "invocation_1", SessionID: session.ID, Owner: owner,
		ActionHash: strings.Repeat("a", 64), Effect: EffectLocalEdit,
		State: InvocationPrepared, Revision: 1, CreatedAt: 100,
		UpdatedAt: 100, ExpiresAt: 1000,
	}
	if err := store.CreateInvocation(ctx, invocation); err != nil {
		t.Fatalf("CreateInvocation() error = %v", err)
	}
	accepted := invocation
	accepted.State = InvocationAccepted
	accepted.AcceptedAt = 200
	accepted.UpdatedAt = 200
	accepted.Revision = 2
	if err := store.UpdateInvocation(ctx, 1, accepted); err != nil {
		t.Fatalf("accept invocation error = %v", err)
	}
	result := json.RawMessage(`{"url":"https://example.com"}`)
	succeeded := accepted
	succeeded.State = InvocationSucceeded
	succeeded.UpdatedAt = 300
	succeeded.CompletedAt = 300
	succeeded.Revision = 3
	succeeded.TerminalResult = result
	if err := store.UpdateInvocation(ctx, 2, succeeded); err != nil {
		t.Fatalf("complete invocation error = %v", err)
	}
	result[2] = 'X'
	stored, err := store.GetInvocation(ctx, invocation.ID)
	if err != nil {
		t.Fatalf("GetInvocation() error = %v", err)
	}
	if string(stored.TerminalResult) != `{"url":"https://example.com"}` {
		t.Fatalf("terminal result = %s", stored.TerminalResult)
	}

	replayed := succeeded
	replayed.Revision = 4
	if err = store.UpdateInvocation(ctx, 3, replayed); !errors.Is(err, ErrConflict) {
		t.Fatalf("terminal redispatch update error = %v, want ErrConflict", err)
	}
}

func TestMemoryStoreRejectsStaleOrMutatedTransition(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	owner := testOwner()
	session := createReadySession(t, store, owner)
	stale := session
	stale.State = SessionClosing
	stale.Revision = 4
	stale.UpdatedAt++
	if err := store.UpdateSession(ctx, 2, stale); !errors.Is(err, ErrStale) {
		t.Fatalf("UpdateSession() stale error = %v, want ErrStale", err)
	}
	mutated := session
	mutated.Owner.ActorID = "other_actor"
	mutated.State = SessionClosing
	mutated.Revision = 3
	mutated.UpdatedAt++
	if err := store.UpdateSession(ctx, 2, mutated); !errors.Is(err, ErrConflict) {
		t.Fatalf("UpdateSession() mutated error = %v, want ErrConflict", err)
	}
}

func TestMemoryStoreAllowsCancellationBeforeAcceptance(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	owner := testOwner()
	session := createReadySession(t, store, owner)
	invocation := Invocation{
		ID: "invocation_1", SessionID: session.ID, Owner: owner,
		ActionHash: strings.Repeat("a", 64), Effect: EffectExternalCommit,
		State: InvocationPrepared, Revision: 1, CreatedAt: 100,
		UpdatedAt: 100, ExpiresAt: 1000,
	}
	if err := store.CreateInvocation(ctx, invocation); err != nil {
		t.Fatalf("CreateInvocation() error = %v", err)
	}
	canceled := invocation
	canceled.State = InvocationCanceled
	canceled.SafeFailure = "approval_expired"
	canceled.UpdatedAt = 200
	canceled.CompletedAt = 200
	canceled.Revision = 2
	if err := store.UpdateInvocation(ctx, 1, canceled); err != nil {
		t.Fatalf("cancel prepared invocation error = %v", err)
	}
}

func TestMemoryStoreRejectsCancellationAfterAcceptance(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	owner := testOwner()
	session := createReadySession(t, store, owner)
	invocation := Invocation{
		ID: "invocation_1", SessionID: session.ID, Owner: owner,
		ActionHash: strings.Repeat("a", 64), Effect: EffectExternalCommit,
		State: InvocationPrepared, Revision: 1, CreatedAt: 100,
		UpdatedAt: 100, ExpiresAt: 1000,
	}
	if err := store.CreateInvocation(ctx, invocation); err != nil {
		t.Fatalf("CreateInvocation() error = %v", err)
	}
	accepted := invocation
	accepted.State = InvocationAccepted
	accepted.AcceptedAt = 200
	accepted.UpdatedAt = 200
	accepted.Revision = 2
	if err := store.UpdateInvocation(ctx, 1, accepted); err != nil {
		t.Fatalf("accept invocation error = %v", err)
	}
	canceled := accepted
	canceled.State = InvocationCanceled
	canceled.SafeFailure = "cancellation_requested"
	canceled.UpdatedAt = 300
	canceled.CompletedAt = 300
	canceled.Revision = 3
	if err := store.UpdateInvocation(ctx, 2, canceled); err == nil {
		t.Fatal("accepted to canceled update error = nil")
	}
	stored, err := store.GetInvocation(ctx, invocation.ID)
	if err != nil || stored.State != InvocationAccepted {
		t.Fatalf("stored invocation after rejected cancel = %+v, %v", stored, err)
	}
}

func TestMemoryStoreRequiresCanonicalEntryStates(t *testing.T) {
	ctx := context.Background()
	owner := testOwner()
	ready := testOpeningSession(owner)
	ready.State = SessionReady
	if err := NewMemoryStore().CreateSession(ctx, ready); !errors.Is(err, ErrConflict) {
		t.Fatalf("CreateSession() ready error = %v, want ErrConflict", err)
	}
	wrongRevision := testOpeningSession(owner)
	wrongRevision.Revision = 2
	if err := NewMemoryStore().CreateSession(ctx, wrongRevision); !errors.Is(err, ErrConflict) {
		t.Fatalf("CreateSession() revision error = %v, want ErrConflict", err)
	}

	store := NewMemoryStore()
	session := createReadySession(t, store, owner)
	accepted := Invocation{
		ID: "invocation_1", SessionID: session.ID, Owner: owner,
		ActionHash: strings.Repeat("a", 64), Effect: EffectRead,
		State: InvocationAccepted, Revision: 1, CreatedAt: 100,
		UpdatedAt: 100, AcceptedAt: 100, ExpiresAt: 1000,
	}
	if err := store.CreateInvocation(ctx, accepted); !errors.Is(err, ErrConflict) {
		t.Fatalf("CreateInvocation() accepted error = %v, want ErrConflict", err)
	}
}

func TestBrokerShutdownClosesLiveSessionsExactlyOnce(t *testing.T) {
	store := NewMemoryStore()
	factory := &fakeWorkerFactory{}
	broker := newTestBroker(t, admittedBrowserConfig(), store, factory)
	owner := testOwner()
	session, err := broker.Open(context.Background(), OpenRequest{
		Owner: owner, Target: config.BrowserDefaultTarget, Profile: config.BrowserDefaultProfile,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err = broker.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	closed, err := store.GetSession(context.Background(), session.ID)
	if err != nil || closed.State != SessionClosed {
		t.Fatalf("session after Shutdown() = %+v, %v", closed, err)
	}
	if len(factory.workers) != 1 || factory.workers[0].closed != 1 {
		t.Fatalf("workers after Shutdown() = %+v", factory.workers)
	}
	if err = broker.Shutdown(context.Background()); err != nil || factory.workers[0].closed != 1 {
		t.Fatalf("second Shutdown() error = %v, closes = %d", err, factory.workers[0].closed)
	}
}

func newTestBroker(
	t *testing.T,
	cfg *config.Config,
	store Store,
	factory WorkerFactory,
) *Broker {
	t.Helper()
	broker, err := NewBroker(cfg, store, factory)
	if err != nil {
		t.Fatalf("NewBroker() error = %v", err)
	}
	now := time.Unix(100, 0).UTC()
	broker.now = func() time.Time {
		now = now.Add(time.Nanosecond)
		return now
	}
	idCounter := 0
	broker.newID = func() (string, error) {
		idCounter++
		return fmt.Sprintf("browser_session_%d", idCounter), nil
	}
	return broker
}

func admittedBrowserConfig() *config.Config {
	root := config.DefaultConfig()
	root.Tools.MCP.Servers["playwright"] = config.MCPServerConfig{
		Enabled: false, Command: "npx", Type: "stdio",
		SessionLossReplay: config.MCPSessionLossReplayNever,
		ExclusiveLockFile: "/var/lib/mintclaw/playwright.lock",
	}
	root.Tools.Browser = config.BrowserToolsConfig{
		Enabled: true,
		Agents:  []string{"browser"},
		Targets: map[string]config.BrowserTargetConfig{
			"gateway": {
				Enabled: true, Driver: config.BrowserDriverPlaywrightMCP,
				DriverServer: "playwright",
				Profiles: map[string]config.BrowserProfileConfig{
					"managed": {
						Enabled: true, Mode: config.BrowserProfileManaged, DryRun: true,
						AllowedOrigins: []string{"https://example.com"},
					},
				},
			},
		},
	}
	return root
}

func testOwner() Owner {
	return Owner{
		ActorID: "actor_1", AgentID: OpaqueAgentID("browser"),
		SessionKey: "telegram_chat_1", ExecutionID: "execution_1",
	}
}

func testOpeningSession(owner Owner) Session {
	return Session{
		ID: "browser_session_1", Owner: owner, Target: "gateway", Profile: "managed",
		State: SessionOpening, DryRun: true, PolicyRevision: "b1_v1",
		ControllerGeneration: 1, TabID: "tab_primary", Revision: 1, CreatedAt: 1,
		UpdatedAt: 1, LastActivityAt: 1, ExpiresAt: 1000,
	}
}

func createReadySession(t *testing.T, store *MemoryStore, owner Owner) Session {
	t.Helper()
	ctx := context.Background()
	session := testOpeningSession(owner)
	if err := store.CreateSession(ctx, session); err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}
	session.State = SessionReady
	session.Revision = 2
	session.UpdatedAt = 2
	session.LastActivityAt = 2
	if err := store.UpdateSession(ctx, 1, session); err != nil {
		t.Fatalf("ready session update error = %v", err)
	}
	return session
}
