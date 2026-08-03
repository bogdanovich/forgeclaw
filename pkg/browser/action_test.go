package browser

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/fileutil"
)

type actionTestWorker struct {
	observation    DriverObservation
	observeErr     error
	resolveElement DriverElement
	resolveOrigin  string
	resolveErr     error
	resolveCalls   int
	actions        []DriverAction
	onExecute      func(DriverAction)
	closed         int
}

func (worker *actionTestWorker) Status(context.Context) (WorkerStatus, error) {
	return WorkerReady, nil
}

func (worker *actionTestWorker) Close(context.Context) error {
	worker.closed++
	return nil
}

func (worker *actionTestWorker) Observe(context.Context) (DriverObservation, error) {
	return worker.observation, worker.observeErr
}

func (worker *actionTestWorker) Resolve(context.Context, string) (DriverElement, string, error) {
	worker.resolveCalls++
	if worker.resolveErr != nil {
		return DriverElement{}, "", worker.resolveErr
	}
	return worker.resolveElement, worker.resolveOrigin, nil
}

func (worker *actionTestWorker) Execute(_ context.Context, action DriverAction) error {
	worker.actions = append(worker.actions, action)
	if worker.onExecute != nil {
		worker.onExecute(action)
	}
	return nil
}

func (worker *actionTestWorker) CatalogRevision() string {
	return strings.Repeat("c", 64)
}

type actionTestFactory struct {
	worker *actionTestWorker
}

func (factory *actionTestFactory) Open(
	context.Context,
	WorkerOpenRequest,
) (WorkerOpenResult, error) {
	return WorkerOpenResult{Owner: factory.worker}, nil
}

func TestBrokerObservationScopesOpaqueReferencesToFreshGeneration(t *testing.T) {
	broker, worker, session := openActionTestBroker(t, NewMemoryStore())
	owner := testOwner()
	first, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(first.Snapshot, "ref=e1") || !strings.Contains(first.Snapshot, "ref=ref_") {
		t.Fatalf("model-visible snapshot contains an unscoped driver ref: %q", first.Snapshot)
	}
	firstRef := onlyVisibleRef(t, first.Snapshot)
	second, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
	if err != nil {
		t.Fatal(err)
	}
	if second.SnapshotGeneration != first.SnapshotGeneration+1 ||
		onlyVisibleRef(t, second.Snapshot) == firstRef {
		t.Fatalf("observations did not rotate authority: first=%+v second=%+v", first, second)
	}
	_, err = broker.PrepareAction(context.Background(), PrepareActionRequest{
		Owner: owner, RequestID: "request_old", SessionID: session.ID, TabID: session.TabID,
		SnapshotID: first.SnapshotID, SnapshotGeneration: first.SnapshotGeneration,
		Action: Action{Kind: ActionFill, Ref: firstRef, Value: "Ada"},
	})
	if !errors.Is(err, ErrStale) {
		t.Fatalf("PrepareAction() stale generation error = %v, want ErrStale", err)
	}
	if len(worker.actions) != 0 {
		t.Fatalf("stale preparation dispatched actions: %+v", worker.actions)
	}
}

func TestBrokerBlankSnapshotAuthorizesOnlyAllowedNavigation(t *testing.T) {
	store := NewMemoryStore()
	broker, worker, session := openActionTestBroker(t, store)
	owner := testOwner()
	worker.observation = DriverObservation{URL: initialBlankOrigin, Origin: initialBlankOrigin}

	blank, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
	if err != nil {
		t.Fatal(err)
	}
	if blank.URL != initialBlankOrigin || blank.Origin != initialBlankOrigin || blank.Snapshot != "" ||
		blank.SnapshotID == "" || blank.SnapshotGeneration != 1 {
		t.Fatalf("blank observation = %+v", blank)
	}
	_, err = broker.PrepareAction(context.Background(), PrepareActionRequest{
		Owner: owner, RequestID: "request_blank_scroll", SessionID: session.ID, TabID: session.TabID,
		SnapshotID: blank.SnapshotID, SnapshotGeneration: blank.SnapshotGeneration,
		Action: Action{Kind: ActionScroll, Direction: "down", Amount: 1},
	})
	if !errors.Is(err, ErrDenied) {
		t.Fatalf("PrepareAction(blank scroll) error = %v, want ErrDenied", err)
	}

	prepared, err := broker.PrepareAction(context.Background(), PrepareActionRequest{
		Owner: owner, RequestID: "request_blank_navigate", SessionID: session.ID, TabID: session.TabID,
		SnapshotID: blank.SnapshotID, SnapshotGeneration: blank.SnapshotGeneration,
		Action: Action{Kind: ActionNavigate, URL: "https://example.com/form"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Action.CurrentOrigin != initialBlankOrigin ||
		prepared.Action.DestinationOrigin != "https://example.com" {
		t.Fatalf("prepared blank navigation = %+v", prepared.Action)
	}
	worker.onExecute = func(DriverAction) {
		worker.observation = driverObservationFixture(
			DriverElement{Target: "e1", Role: "textbox", Name: "Name"},
		)
	}
	invocation, err := broker.ExecuteAction(context.Background(), owner, prepared.Action.ID, nil)
	if err != nil || invocation.State != InvocationSucceeded {
		t.Fatalf("ExecuteAction(blank navigation) = %+v, %v", invocation, err)
	}
	if len(worker.actions) != 1 || worker.actions[0].Kind != DriverNavigate ||
		worker.actions[0].URL != "https://example.com/form" {
		t.Fatalf("blank navigation driver actions = %+v", worker.actions)
	}
	observed, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
	if err != nil || observed.Origin != "https://example.com" || observed.SnapshotGeneration != 2 {
		t.Fatalf("Observe(after blank navigation) = %+v, %v", observed, err)
	}
}

func TestBrokerRejectsNonEmptyBlankDriverObservation(t *testing.T) {
	dialog := &DialogObservation{Type: "alert", Message: "unexpected"}
	tests := []struct {
		name        string
		observation DriverObservation
	}{
		{name: "different URL", observation: DriverObservation{URL: "about:srcdoc", Origin: initialBlankOrigin}},
		{
			name: "blank URL with HTTP origin",
			observation: DriverObservation{
				URL: initialBlankOrigin, Origin: "https://example.com",
			},
		},
		{
			name:        "title",
			observation: DriverObservation{URL: initialBlankOrigin, Origin: initialBlankOrigin, Title: "Blank"},
		},
		{
			name:        "snapshot",
			observation: DriverObservation{URL: initialBlankOrigin, Origin: initialBlankOrigin, Snapshot: "text"},
		},
		{
			name: "element",
			observation: DriverObservation{
				URL:      initialBlankOrigin,
				Origin:   initialBlankOrigin,
				Elements: []DriverElement{{Target: "e1"}},
			},
		},
		{
			name:        "dialog",
			observation: DriverObservation{URL: initialBlankOrigin, Origin: initialBlankOrigin, PendingDialog: dialog},
		},
		{
			name:        "truncated",
			observation: DriverObservation{URL: initialBlankOrigin, Origin: initialBlankOrigin, Truncated: true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			broker, worker, session := openActionTestBroker(t, NewMemoryStore())
			worker.observation = test.observation
			_, err := broker.Observe(context.Background(), testOwner(), session.ID, session.TabID)
			if !errors.Is(err, ErrDriverIncompatible) {
				t.Fatalf("Observe() error = %v, want ErrDriverIncompatible", err)
			}
		})
	}
}

func TestBrokerRejectsBlankMutationAtActionObservationBoundaries(t *testing.T) {
	for _, test := range []struct {
		name    string
		execute bool
	}{
		{name: "prepare"},
		{name: "execute", execute: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			broker, worker, session := openActionTestBroker(t, NewMemoryStore())
			owner := testOwner()
			worker.observation = DriverObservation{URL: initialBlankOrigin, Origin: initialBlankOrigin}
			blank, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
			if err != nil {
				t.Fatal(err)
			}
			request := PrepareActionRequest{
				Owner: owner, RequestID: "request_blank_mutation", SessionID: session.ID,
				TabID: session.TabID, SnapshotID: blank.SnapshotID,
				SnapshotGeneration: blank.SnapshotGeneration,
				Action:             Action{Kind: ActionNavigate, URL: "https://example.com/form"},
			}
			if test.execute {
				prepared, prepareErr := broker.PrepareAction(context.Background(), request)
				if prepareErr != nil {
					t.Fatal(prepareErr)
				}
				worker.observation.Title = "mutated"
				_, err = broker.ExecuteAction(context.Background(), owner, prepared.Action.ID, nil)
			} else {
				worker.observation.Origin = "https://example.com"
				_, err = broker.PrepareAction(context.Background(), request)
			}
			if !errors.Is(err, ErrDriverIncompatible) {
				t.Fatalf("action boundary error = %v, want ErrDriverIncompatible", err)
			}
			if len(worker.actions) != 0 {
				t.Fatalf("malformed blank observation dispatched actions: %+v", worker.actions)
			}
		})
	}
}

func TestBrokerObservationDeniesPrivateDNSResolutionAndClosesSession(t *testing.T) {
	store := NewMemoryStore()
	broker, worker, session := openActionTestBroker(t, store)
	broker.lookupIP = func(context.Context, string, string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("169.254.169.254")}, nil
	}
	if _, err := broker.Observe(
		context.Background(), testOwner(), session.ID, session.TabID,
	); !errors.Is(err, ErrDenied) {
		t.Fatalf("Observe() private DNS error = %v, want ErrDenied", err)
	}
	stored, err := store.GetSession(context.Background(), session.ID)
	if err != nil || stored.State != SessionLost || stored.SafeFailure != "network_denied" || worker.closed != 1 {
		t.Fatalf("session after private DNS = %+v, %v; worker = %+v", stored, err, worker)
	}
}

func TestBrokerPublicWebOriginPolicyAllowsPublicSyntaxButRejectsPrivateLiterals(t *testing.T) {
	root := admittedBrowserConfig()
	target := root.Tools.Browser.Targets[config.BrowserDefaultTarget]
	profile := target.Profiles[config.BrowserDefaultProfile]
	profile.NetworkMode = config.BrowserNetworkPublicWeb
	profile.AllowedOrigins = nil
	target.Profiles[config.BrowserDefaultProfile] = profile
	root.Tools.Browser.Targets[config.BrowserDefaultTarget] = target
	broker := &Broker{config: root.Tools.Browser}
	session := Session{Target: config.BrowserDefaultTarget, Profile: config.BrowserDefaultProfile}
	if !broker.originAllowed(session, "https://public.example") {
		t.Fatal("public_web rejected a normalized public origin")
	}
	for _, denied := range []string{
		"http://127.0.0.1", "http://169.254.169.254", "https://public.example:443",
	} {
		if broker.originAllowed(session, denied) {
			t.Fatalf("public_web allowed non-public or non-canonical origin %q", denied)
		}
	}
}

func TestBrokerAnyHTTPObservationAdmitsPrivateOrigin(t *testing.T) {
	root := admittedBrowserConfig()
	target := root.Tools.Browser.Targets[config.BrowserDefaultTarget]
	profile := target.Profiles[config.BrowserDefaultProfile]
	profile.NetworkMode = config.BrowserNetworkAnyHTTP
	profile.AllowedOrigins = nil
	target.Profiles[config.BrowserDefaultProfile] = profile
	root.Tools.Browser.Targets[config.BrowserDefaultTarget] = target

	store := NewMemoryStore()
	broker, worker, session := openActionTestBrokerWithConfig(t, root, store)
	broker.lookupIP = func(context.Context, string, string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("169.254.169.254")}, nil
	}
	worker.observation.URL = "http://private.internal/status"
	worker.observation.Origin = "http://private.internal"
	worker.resolveOrigin = worker.observation.Origin

	observation, err := broker.Observe(
		context.Background(), testOwner(), session.ID, session.TabID,
	)
	if err != nil {
		t.Fatalf("Observe() private origin error = %v", err)
	}
	if observation.URL != "http://private.internal/status" ||
		observation.Origin != "http://private.internal" {
		t.Fatalf("private observation = %+v", observation)
	}
	stored, err := store.GetSession(context.Background(), session.ID)
	if err != nil || stored.State != SessionReady || stored.SnapshotOrigin != "http://private.internal" {
		t.Fatalf("stored private session = %+v, %v", stored, err)
	}
}

func TestBrokerAnyHTTPRejectsNavigationWithEmptyPort(t *testing.T) {
	root := admittedBrowserConfig()
	target := root.Tools.Browser.Targets[config.BrowserDefaultTarget]
	profile := target.Profiles[config.BrowserDefaultProfile]
	profile.NetworkMode = config.BrowserNetworkAnyHTTP
	profile.AllowedOrigins = nil
	target.Profiles[config.BrowserDefaultProfile] = profile
	root.Tools.Browser.Targets[config.BrowserDefaultTarget] = target

	broker, worker, session := openActionTestBrokerWithConfig(t, root, NewMemoryStore())
	observation, err := broker.Observe(
		context.Background(), testOwner(), session.ID, session.TabID,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = broker.PrepareAction(context.Background(), PrepareActionRequest{
		Owner: testOwner(), RequestID: "request_empty_port", SessionID: session.ID, TabID: session.TabID,
		SnapshotID: observation.SnapshotID, SnapshotGeneration: observation.SnapshotGeneration,
		Action: Action{Kind: ActionNavigate, URL: "http://127.0.0.1:/health"},
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("PrepareAction() empty-port error = %v, want ErrInvalid", err)
	}
	if len(worker.actions) != 0 {
		t.Fatalf("empty-port navigation dispatched actions: %+v", worker.actions)
	}
}

func TestNavigationOriginPreservesMappedIPv6AddressFamily(t *testing.T) {
	origin, err := originFromURL("http://[::ffff:7f00:1]/health")
	if err != nil || origin != "http://[::ffff:127.0.0.1]" {
		t.Fatalf("originFromURL() = %q, %v", origin, err)
	}
}

func TestNavigationOriginPreservesScopedIPv6Zone(t *testing.T) {
	normalized, err := normalizeDriverNavigationURL("http://[FE80::1%25EtherNet]:8080/health")
	if err != nil || normalized != "http://[fe80::1%25EtherNet]:8080/health" {
		t.Fatalf("normalizeDriverNavigationURL() = %q, %v", normalized, err)
	}
	origin, err := originFromURL(normalized)
	if err != nil || origin != "http://[fe80::1%25EtherNet]:8080" {
		t.Fatalf("originFromURL() = %q, %v", origin, err)
	}
}

func TestBrokerAnyHTTPAdmitsScopedIPv6NavigationAndObservation(t *testing.T) {
	root := admittedBrowserConfig()
	target := root.Tools.Browser.Targets[config.BrowserDefaultTarget]
	profile := target.Profiles[config.BrowserDefaultProfile]
	profile.NetworkMode = config.BrowserNetworkAnyHTTP
	profile.AllowedOrigins = nil
	target.Profiles[config.BrowserDefaultProfile] = profile
	root.Tools.Browser.Targets[config.BrowserDefaultTarget] = target

	broker, worker, session := openActionTestBrokerWithConfig(t, root, NewMemoryStore())
	broker.lookupIP = func(_ context.Context, _, host string) ([]net.IP, error) {
		if host == "example.com" {
			return []net.IP{net.ParseIP("93.184.216.34")}, nil
		}
		t.Fatalf("scoped IPv6 literal used DNS for %q", host)
		return nil, nil
	}
	observation, err := broker.Observe(context.Background(), testOwner(), session.ID, session.TabID)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := broker.PrepareAction(context.Background(), PrepareActionRequest{
		Owner: testOwner(), RequestID: "request_scoped_ipv6", SessionID: session.ID, TabID: session.TabID,
		SnapshotID: observation.SnapshotID, SnapshotGeneration: observation.SnapshotGeneration,
		Action: Action{Kind: ActionNavigate, URL: "http://[FE80::1%25EtherNet]:8080/health"},
	})
	if err != nil || prepared.Action.DestinationOrigin != "http://[fe80::1%25EtherNet]:8080" {
		t.Fatalf("PrepareAction() scoped destination = %q, %v", prepared.Action.DestinationOrigin, err)
	}

	worker.observation.URL = "http://[fe80::1%25EtherNet]:8080/health"
	worker.observation.Origin = "http://[fe80::1%25EtherNet]:8080"
	if _, err = broker.Observe(context.Background(), testOwner(), session.ID, session.TabID); err != nil {
		t.Fatalf("Observe() scoped IPv6 error = %v", err)
	}
}

func TestBrokerPreparationQuarantinesDeniedDNSResolution(t *testing.T) {
	t.Run("current origin", func(t *testing.T) {
		store := NewMemoryStore()
		broker, worker, session := openActionTestBroker(t, store)
		owner := testOwner()
		observation, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
		if err != nil {
			t.Fatal(err)
		}
		broker.lookupIP = func(context.Context, string, string) ([]net.IP, error) {
			return []net.IP{net.ParseIP("100.64.0.1")}, nil
		}
		_, err = broker.PrepareAction(context.Background(), PrepareActionRequest{
			Owner: owner, RequestID: "request_prepare_rebind", SessionID: session.ID, TabID: session.TabID,
			SnapshotID: observation.SnapshotID, SnapshotGeneration: observation.SnapshotGeneration,
			Action: Action{Kind: ActionFill, Ref: onlyVisibleRef(t, observation.Snapshot), Value: "Ada"},
		})
		assertNetworkQuarantine(t, store, worker, session, err)
	})

	t.Run("navigation destination", func(t *testing.T) {
		root := admittedBrowserConfig()
		target := root.Tools.Browser.Targets["gateway"]
		profile := target.Profiles["managed"]
		profile.AllowedOrigins = append(profile.AllowedOrigins, "https://private.example")
		target.Profiles["managed"] = profile
		root.Tools.Browser.Targets["gateway"] = target
		store := NewMemoryStore()
		broker, worker, session := openActionTestBrokerWithConfig(t, root, store)
		broker.lookupIP = func(_ context.Context, _ string, host string) ([]net.IP, error) {
			if host == "private.example" {
				return []net.IP{net.ParseIP("198.18.0.1")}, nil
			}
			return []net.IP{net.ParseIP("93.184.216.34")}, nil
		}
		owner := testOwner()
		observation, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
		if err != nil {
			t.Fatal(err)
		}
		_, err = broker.PrepareAction(context.Background(), PrepareActionRequest{
			Owner: owner, RequestID: "request_private_destination", SessionID: session.ID,
			TabID: session.TabID, SnapshotID: observation.SnapshotID,
			SnapshotGeneration: observation.SnapshotGeneration,
			Action:             Action{Kind: ActionNavigate, URL: "https://private.example/listing"},
		})
		assertNetworkQuarantine(t, store, worker, session, err)
	})
}

func TestBrokerPreparesRuntimeEffectAndExecutesLocalEditOnce(t *testing.T) {
	store := NewMemoryStore()
	broker, worker, session := openActionTestBroker(t, store)
	owner := testOwner()
	observation, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
	if err != nil {
		t.Fatal(err)
	}
	ref := onlyVisibleRef(t, observation.Snapshot)
	worker.resolveElement = DriverElement{Target: "e1", Role: "textbox", Name: "Name"}
	worker.resolveOrigin = "https://example.com"
	prepared, err := broker.PrepareAction(context.Background(), PrepareActionRequest{
		Owner: owner, RequestID: "request_fill", SessionID: session.ID, TabID: session.TabID,
		SnapshotID: observation.SnapshotID, SnapshotGeneration: observation.SnapshotGeneration,
		Action: Action{Kind: ActionFill, Ref: ref, Value: "Ada"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Action.Effect != EffectLocalEdit || prepared.RequiresApproval {
		t.Fatalf("fill preparation = %+v", prepared)
	}
	first, err := broker.ExecuteAction(context.Background(), owner, prepared.Action.ID, nil)
	if err != nil || first.State != InvocationSucceeded || len(worker.actions) != 1 ||
		worker.actions[0].Target != "e1" || worker.actions[0].Value != "Ada" {
		t.Fatalf("ExecuteAction() = %+v, %v; actions = %+v", first, err, worker.actions)
	}
	second, err := broker.ExecuteAction(context.Background(), owner, prepared.Action.ID, nil)
	if err != nil || !reflect.DeepEqual(second, first) || len(worker.actions) != 1 {
		t.Fatalf("idempotent ExecuteAction() = %+v, %v; actions = %+v", second, err, worker.actions)
	}
	storedSession, err := store.GetSession(context.Background(), session.ID)
	if err != nil || storedSession.SnapshotID != "" || storedSession.SnapshotGeneration == 0 {
		t.Fatalf("session after action = %+v, %v", storedSession, err)
	}
}

func TestBrokerQuarantinesSessionWhenPostActionSnapshotInvalidationFails(t *testing.T) {
	store := &failNextSessionUpdateStore{MemoryStore: NewMemoryStore()}
	broker, worker, session := openActionTestBroker(t, store)
	owner := testOwner()
	observation, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
	if err != nil {
		t.Fatal(err)
	}
	ref := onlyVisibleRef(t, observation.Snapshot)
	worker.resolveElement = DriverElement{Target: "e1", Role: "textbox", Name: "Name"}
	worker.resolveOrigin = "https://example.com"
	prepared, err := broker.PrepareAction(context.Background(), PrepareActionRequest{
		Owner: owner, RequestID: "request_invalidation_failure", SessionID: session.ID,
		TabID: session.TabID, SnapshotID: observation.SnapshotID,
		SnapshotGeneration: observation.SnapshotGeneration,
		Action:             Action{Kind: ActionFill, Ref: ref, Value: "Ada"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// The first update records action activity; fail the following snapshot
	// invalidation write.
	store.failAfter = 2
	invocation, err := broker.ExecuteAction(context.Background(), owner, prepared.Action.ID, nil)
	if !errors.Is(err, ErrSnapshotInvalidation) || invocation.State != InvocationSucceeded ||
		len(worker.actions) != 1 || worker.closed != 1 {
		stored, storedErr := store.GetSession(context.Background(), session.ID)
		t.Fatalf(
			"ExecuteAction() = %+v, %v (snapshot failure = %t); actions = %+v; closes = %d; session = %+v, %v",
			invocation,
			err,
			errors.Is(err, ErrSnapshotInvalidation),
			worker.actions,
			worker.closed,
			stored,
			storedErr,
		)
	}
	stored, getErr := store.GetSession(context.Background(), session.ID)
	if getErr != nil || stored.State != SessionLost || stored.SnapshotID != "" ||
		stored.SafeFailure != "snapshot_invalidation_failed" {
		t.Fatalf("quarantined session = %+v, %v", stored, getErr)
	}
	_, err = broker.PrepareAction(context.Background(), PrepareActionRequest{
		Owner: owner, RequestID: "request_reuse_old_snapshot", SessionID: session.ID,
		TabID: session.TabID, SnapshotID: observation.SnapshotID,
		SnapshotGeneration: observation.SnapshotGeneration,
		Action:             Action{Kind: ActionFill, Ref: ref, Value: "Grace"},
	})
	if !errors.Is(err, ErrWorkerUnavailable) || len(worker.actions) != 1 {
		t.Fatalf("old snapshot reuse error = %v; actions = %+v", err, worker.actions)
	}
}

func TestFileStoreQuarantinesCommittedSnapshotInvalidationWarning(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "browser.json")
	store, err := NewFileStore(path, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	broker, worker, session := openActionTestBroker(t, store)
	owner := testOwner()
	observation, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
	if err != nil {
		t.Fatal(err)
	}
	ref := onlyVisibleRef(t, observation.Snapshot)
	worker.resolveElement = DriverElement{Target: "e1", Role: "textbox", Name: "Name"}
	worker.resolveOrigin = "https://example.com"
	prepared, err := broker.PrepareAction(context.Background(), PrepareActionRequest{
		Owner: owner, RequestID: "request_committed_invalidation", SessionID: session.ID,
		TabID: session.TabID, SnapshotID: observation.SnapshotID,
		SnapshotGeneration: observation.SnapshotGeneration,
		Action:             Action{Kind: ActionFill, Ref: ref, Value: "Ada"},
	})
	if err != nil {
		t.Fatal(err)
	}
	originalWrite := store.writeFile
	committedWarning := false
	store.writeFile = func(path string, data []byte, mode os.FileMode) error {
		if writeErr := originalWrite(path, data, mode); writeErr != nil {
			return writeErr
		}
		var document fileStoreDocument
		if jsonErr := json.Unmarshal(data, &document); jsonErr != nil {
			return jsonErr
		}
		persisted := document.Sessions[session.ID]
		if !committedWarning && persisted.State == SessionReady && persisted.SnapshotID == "" {
			committedWarning = true
			return &fileutil.CommittedWriteError{Err: errors.New("directory sync warning")}
		}
		return nil
	}
	invocation, err := broker.ExecuteAction(context.Background(), owner, prepared.Action.ID, nil)
	if !committedWarning || !errors.Is(err, ErrSnapshotInvalidation) ||
		invocation.State != InvocationSucceeded || len(worker.actions) != 1 || worker.closed != 1 {
		t.Fatalf(
			"ExecuteAction() = %+v, %v; warning = %t; actions = %+v; closes = %d",
			invocation, err, committedWarning, worker.actions, worker.closed,
		)
	}
	persisted, getErr := store.GetSession(context.Background(), session.ID)
	if getErr != nil || persisted.State != SessionLost ||
		persisted.SafeFailure != "snapshot_invalidation_failed" || persisted.SnapshotID != "" {
		t.Fatalf("quarantined session = %+v, %v", persisted, getErr)
	}
}

func TestBrokerRequiresExactApprovalAndDryRunStillDeniesCommit(t *testing.T) {
	store := NewMemoryStore()
	broker, worker, session := openActionTestBroker(t, store)
	owner := testOwner()
	worker.observation = driverObservationFixture(DriverElement{Target: "e2", Role: "button", Name: "Save"})
	observation, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
	if err != nil {
		t.Fatal(err)
	}
	worker.resolveElement = DriverElement{Target: "e2", Role: "button", Name: "Save"}
	worker.resolveOrigin = "https://example.com"
	prepared, err := broker.PrepareAction(context.Background(), PrepareActionRequest{
		Owner: owner, RequestID: "request_submit", SessionID: session.ID, TabID: session.TabID,
		SnapshotID: observation.SnapshotID, SnapshotGeneration: observation.SnapshotGeneration,
		Action: Action{Kind: ActionClick, Ref: onlyVisibleRef(t, observation.Snapshot)},
	})
	if err != nil || prepared.Action.Effect != EffectExternalCommit || !prepared.RequiresApproval {
		t.Fatalf("PrepareAction() = %+v, %v", prepared, err)
	}
	if _, err = broker.ExecuteAction(
		context.Background(), owner, prepared.Action.ID, nil,
	); !errors.Is(err, ErrApprovalRequired) {
		t.Fatalf("ExecuteAction() without approval error = %v", err)
	}
	forged := prepared.Approval
	forged.PolicyRevision = strings.Repeat("a", 64)
	if _, err = broker.ExecuteAction(
		context.Background(), owner, prepared.Action.ID, &forged,
	); !errors.Is(err, ErrApprovalRequired) {
		t.Fatalf("ExecuteAction() with forged approval error = %v", err)
	}
	denied, err := broker.ExecuteAction(context.Background(), owner, prepared.Action.ID, &prepared.Approval)
	if !errors.Is(err, ErrDenied) || denied.State != InvocationCanceled ||
		denied.SafeFailure != "dry_run_denied" || len(worker.actions) != 0 {
		t.Fatalf("approved dry-run ExecuteAction() = %+v, %v; actions = %+v", denied, err, worker.actions)
	}
	stored, getErr := store.GetInvocation(context.Background(), denied.ID)
	if getErr != nil || !reflect.DeepEqual(stored, denied) {
		t.Fatalf("stored dry-run denial = %+v, %v", stored, getErr)
	}
}

func TestBrokerRevalidatesResolvedSemanticsBeforeAcceptance(t *testing.T) {
	store := NewMemoryStore()
	broker, worker, session := openActionTestBroker(t, store)
	owner := testOwner()
	observation, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := broker.PrepareAction(context.Background(), PrepareActionRequest{
		Owner: owner, RequestID: "request_changed", SessionID: session.ID, TabID: session.TabID,
		SnapshotID: observation.SnapshotID, SnapshotGeneration: observation.SnapshotGeneration,
		Action: Action{Kind: ActionFill, Ref: onlyVisibleRef(t, observation.Snapshot), Value: "Ada"},
	})
	if err != nil {
		t.Fatal(err)
	}
	worker.resolveElement = DriverElement{Target: "e1", Role: "button", Name: "Name"}
	worker.resolveOrigin = "https://example.com"
	if _, err = broker.ExecuteAction(
		context.Background(), owner, prepared.Action.ID, nil,
	); !errors.Is(err, ErrStale) {
		t.Fatalf("ExecuteAction() changed semantics error = %v, want ErrStale", err)
	}
	invocation, err := store.GetInvocation(
		context.Background(),
		derivedIdentifier("invocation", owner, session.ID, "request_changed"),
	)
	if err != nil || invocation.State != InvocationPrepared || len(worker.actions) != 0 {
		t.Fatalf("invocation after stale semantics = %+v, %v; actions = %+v", invocation, err, worker.actions)
	}
}

func TestBrokerRevalidatesDNSBeforeAcceptance(t *testing.T) {
	store := NewMemoryStore()
	broker, worker, session := openActionTestBroker(t, store)
	owner := testOwner()
	observation, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := broker.PrepareAction(context.Background(), PrepareActionRequest{
		Owner: owner, RequestID: "request_dns_rebind", SessionID: session.ID, TabID: session.TabID,
		SnapshotID: observation.SnapshotID, SnapshotGeneration: observation.SnapshotGeneration,
		Action: Action{Kind: ActionFill, Ref: onlyVisibleRef(t, observation.Snapshot), Value: "Ada"},
	})
	if err != nil {
		t.Fatal(err)
	}
	broker.lookupIP = func(context.Context, string, string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("10.0.0.8")}, nil
	}
	if _, err = broker.ExecuteAction(
		context.Background(), owner, prepared.Action.ID, nil,
	); !errors.Is(err, ErrDenied) {
		t.Fatalf("ExecuteAction() rebound DNS error = %v, want ErrDenied", err)
	}
	invocation, err := store.GetInvocation(
		context.Background(),
		derivedIdentifier("invocation", owner, session.ID, "request_dns_rebind"),
	)
	if err != nil || invocation.State != InvocationCanceled || invocation.SafeFailure != "network_denied" ||
		len(worker.actions) != 0 || worker.closed != 1 {
		t.Fatalf("invocation after rebound DNS = %+v, %v; actions = %+v", invocation, err, worker.actions)
	}
}

func TestFileStorePersistsPreparedApprovalBinding(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "browser.json")
	store, err := NewFileStore(path, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	broker, _, session := openActionTestBroker(t, store)
	owner := testOwner()
	observation, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
	if err != nil {
		t.Fatal(err)
	}
	const secret = "credential-that-must-never-reach-browser-state"
	prepared, err := broker.PrepareAction(context.Background(), PrepareActionRequest{
		Owner: owner, RequestID: "request_durable", SessionID: session.ID, TabID: session.TabID,
		SnapshotID: observation.SnapshotID, SnapshotGeneration: observation.SnapshotGeneration,
		Action: Action{Kind: ActionFill, Ref: onlyVisibleRef(t, observation.Snapshot), Value: secret},
	})
	if err != nil {
		t.Fatal(err)
	}
	state, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(state, []byte(secret)) || prepared.Action.Action.Value != "" ||
		!validDigest(prepared.Action.InputDigest) || prepared.Action.InputBytes != len(secret) {
		t.Fatalf("durable prepared action exposed raw fill input: %+v", prepared.Action)
	}
	store.Close()
	reopened, err := NewFileStore(path, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.GetPreparedAction(context.Background(), prepared.Action.ID)
	if err != nil || got != prepared.Action {
		t.Fatalf("reopened prepared action = %+v, %v; want %+v", got, err, prepared.Action)
	}
}

func TestBrokerExecutesAdmittedSelectPressAndScrollActions(t *testing.T) {
	tests := []struct {
		name       string
		element    DriverElement
		action     Action
		wantEffect Effect
		wantDriver DriverAction
	}{
		{
			name: "select", element: DriverElement{Target: "e1", Role: "combobox", Name: "State"},
			action: Action{Kind: ActionSelect, Value: "CA"}, wantEffect: EffectLocalEdit,
			wantDriver: DriverAction{Kind: DriverSelect, Target: "e1", Element: "State", Value: "CA"},
		},
		{
			name: "scroll", action: Action{Kind: ActionScroll, Direction: "down", Amount: 3},
			wantEffect: EffectRead,
			wantDriver: DriverAction{Kind: DriverScroll, Direction: "down", Amount: 3},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := NewMemoryStore()
			broker, worker, session := openActionTestBroker(t, store)
			if test.element.Target != "" {
				worker.observation = driverObservationFixture(test.element)
				worker.resolveElement = test.element
			}
			owner := testOwner()
			observation, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
			if err != nil {
				t.Fatal(err)
			}
			action := test.action
			if test.element.Target != "" {
				action.Ref = onlyVisibleRef(t, observation.Snapshot)
			}
			prepared, err := broker.PrepareAction(context.Background(), PrepareActionRequest{
				Owner: owner, RequestID: "request_" + test.name, SessionID: session.ID, TabID: session.TabID,
				SnapshotID: observation.SnapshotID, SnapshotGeneration: observation.SnapshotGeneration,
				Action: action,
			})
			if err != nil {
				t.Fatal(err)
			}
			if prepared.RequiresApproval || prepared.Action.Effect != test.wantEffect {
				t.Fatalf("preparation = %+v", prepared)
			}
			if test.name == "select" && (prepared.Action.Action.Value != "" ||
				!validDigest(prepared.Action.InputDigest)) {
				t.Fatalf("durable selection exposed input: %+v", prepared.Action)
			}
			invocation, err := broker.ExecuteAction(context.Background(), owner, prepared.Action.ID, nil)
			if err != nil || invocation.State != InvocationSucceeded ||
				!reflect.DeepEqual(worker.actions, []DriverAction{test.wantDriver}) {
				t.Fatalf("execution = %+v, %v; driver actions = %+v", invocation, err, worker.actions)
			}
		})
	}
}

func TestBrokerTreatsGlobalPressAsUnknownAndDryRunDenied(t *testing.T) {
	broker, worker, session := openActionTestBroker(t, NewMemoryStore())
	pageHandlerCommitted := false
	worker.onExecute = func(DriverAction) { pageHandlerCommitted = true }
	owner := testOwner()
	observation, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := broker.PrepareAction(context.Background(), PrepareActionRequest{
		Owner: owner, RequestID: "request_press", SessionID: session.ID, TabID: session.TabID,
		SnapshotID: observation.SnapshotID, SnapshotGeneration: observation.SnapshotGeneration,
		Action: Action{Kind: ActionPress, Key: "Tab"},
	})
	if err != nil || !prepared.RequiresApproval || prepared.Action.Effect != EffectUnknown {
		t.Fatalf("PrepareAction(Tab) = %+v, %v", prepared, err)
	}
	if _, err = broker.ExecuteAction(
		context.Background(), owner, prepared.Action.ID, nil,
	); !errors.Is(err, ErrApprovalRequired) {
		t.Fatalf("ExecuteAction(Tab) error = %v, want ErrApprovalRequired", err)
	}
	if len(worker.actions) != 0 || pageHandlerCommitted {
		t.Fatalf("unapproved global press reached driver: %+v", worker.actions)
	}
	if _, err = broker.ExecuteAction(
		context.Background(), owner, prepared.Action.ID, &prepared.Approval,
	); !errors.Is(err, ErrDenied) {
		t.Fatalf("ExecuteAction(Tab, approved dry-run) error = %v, want ErrDenied", err)
	}
	if len(worker.actions) != 0 || pageHandlerCommitted {
		t.Fatalf("dry-run global press reached driver: %+v", worker.actions)
	}
}

func TestBrokerObservesAndDismissesBoundDialog(t *testing.T) {
	broker, worker, session := openActionTestBroker(t, NewMemoryStore())
	worker.observation = driverObservationFixture()
	worker.observation.PendingDialog = &DialogObservation{Type: "confirm", Message: "Discard draft?"}
	owner := testOwner()
	observation, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
	if err != nil || observation.PendingDialog == nil ||
		*observation.PendingDialog != (DialogObservation{Type: "confirm", Message: "Discard draft?"}) {
		t.Fatalf("Observe() dialog = %+v, %v", observation, err)
	}
	prepared, err := broker.PrepareAction(context.Background(), PrepareActionRequest{
		Owner: owner, RequestID: "request_dismiss", SessionID: session.ID, TabID: session.TabID,
		SnapshotID: observation.SnapshotID, SnapshotGeneration: observation.SnapshotGeneration,
		Action: Action{Kind: ActionDialog, Decision: "dismiss"},
	})
	if err != nil || prepared.RequiresApproval || prepared.Action.Effect != EffectLocalEdit ||
		prepared.Action.DialogType != "confirm" || prepared.Action.DialogMessage != "Discard draft?" {
		t.Fatalf("PrepareAction(dismiss) = %+v, %v", prepared, err)
	}
	invocation, err := broker.ExecuteAction(context.Background(), owner, prepared.Action.ID, nil)
	if err != nil || invocation.State != InvocationSucceeded || !reflect.DeepEqual(
		worker.actions, []DriverAction{{Kind: DriverDialog}},
	) {
		t.Fatalf("ExecuteAction(dismiss) = %+v, %v; actions = %+v", invocation, err, worker.actions)
	}
}

func TestBrokerBindsDialogPromptAndProtectsAcceptance(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "browser.json")
	store, err := NewFileStore(path, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	broker, worker, session := openActionTestBroker(t, store)
	worker.observation = driverObservationFixture()
	worker.observation.PendingDialog = &DialogObservation{Type: "prompt", Message: "Type confirmation"}
	owner := testOwner()
	observation, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := broker.PrepareAction(context.Background(), PrepareActionRequest{
		Owner: owner, RequestID: "request_accept", SessionID: session.ID, TabID: session.TabID,
		SnapshotID: observation.SnapshotID, SnapshotGeneration: observation.SnapshotGeneration,
		Action: Action{
			Kind: ActionDialog, Decision: "accept", Value: "prompt-secret", PromptProvided: true,
		},
	})
	if err != nil || !prepared.RequiresApproval || prepared.Action.Effect != EffectExternalCommit ||
		prepared.Action.Action.Value != "" || !validDigest(prepared.Action.InputDigest) {
		t.Fatalf("PrepareAction(accept) = %+v, %v", prepared, err)
	}
	state, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(state, []byte("prompt-secret")) {
		t.Fatalf("durable browser state exposed dialog prompt: %s", state)
	}
	if _, err = broker.ExecuteAction(
		context.Background(), owner, prepared.Action.ID, nil,
	); !errors.Is(err, ErrApprovalRequired) {
		t.Fatalf("ExecuteAction(accept) error = %v, want ErrApprovalRequired", err)
	}
	if _, err = broker.ExecuteAction(
		context.Background(), owner, prepared.Action.ID, &prepared.Approval,
	); !errors.Is(err, ErrDenied) {
		t.Fatalf("ExecuteAction(approved dry-run accept) error = %v, want ErrDenied", err)
	}
	if len(worker.actions) != 0 {
		t.Fatalf("protected dialog acceptance reached driver: %+v", worker.actions)
	}
}

func TestBrokerRejectsChangedDialogBeforeDispatch(t *testing.T) {
	broker, worker, session := openActionTestBroker(t, NewMemoryStore())
	worker.observation = driverObservationFixture()
	worker.observation.PendingDialog = &DialogObservation{Type: "confirm", Message: "First"}
	owner := testOwner()
	observation, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := broker.PrepareAction(context.Background(), PrepareActionRequest{
		Owner: owner, RequestID: "request_changed_dialog", SessionID: session.ID, TabID: session.TabID,
		SnapshotID: observation.SnapshotID, SnapshotGeneration: observation.SnapshotGeneration,
		Action: Action{Kind: ActionDialog, Decision: "dismiss"},
	})
	if err != nil {
		t.Fatal(err)
	}
	worker.observation.PendingDialog.Message = "Replacement"
	if _, err = broker.ExecuteAction(
		context.Background(), owner, prepared.Action.ID, nil,
	); !errors.Is(err, ErrStale) {
		t.Fatalf("ExecuteAction(changed dialog) error = %v, want ErrStale", err)
	}
	if len(worker.actions) != 0 {
		t.Fatalf("changed dialog reached driver: %+v", worker.actions)
	}
}

func TestActionValidationRejectsUnadmittedKeyAndUnboundedScroll(t *testing.T) {
	for _, action := range []Action{
		{Kind: ActionPress, Key: "a"},
		{Kind: ActionPress, Key: "Control+L"},
		{Kind: ActionPress, Key: "Enter"},
		{Kind: ActionScroll, Direction: "left", Amount: 1},
		{Kind: ActionScroll, Direction: "down", Amount: MaxScrollAmount + 1},
		{Kind: ActionDialog, Decision: "dismiss", Value: "unexpected"},
		{Kind: ActionDialog, Decision: "accept", Value: "unmarked-prompt"},
		{Kind: ActionDialog, Decision: "later"},
	} {
		if err := action.Validate(config.BrowserMaxTextInputBytes); !errors.Is(err, ErrInvalid) {
			t.Fatalf("Action.Validate(%+v) error = %v, want ErrInvalid", action, err)
		}
	}
}

func TestExecuteFillFailsClosedBeforeDriverCallWithoutExactLiveInput(t *testing.T) {
	tests := []struct {
		name  string
		input map[string]string
	}{
		{name: "missing"},
		{name: "mismatched", input: map[string]string{"prepared": "different-secret"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := NewMemoryStore()
			broker, worker, session := openActionTestBroker(t, store)
			owner := testOwner()
			observation, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := broker.PrepareAction(context.Background(), PrepareActionRequest{
				Owner: owner, RequestID: "request_live_input", SessionID: session.ID, TabID: session.TabID,
				SnapshotID: observation.SnapshotID, SnapshotGeneration: observation.SnapshotGeneration,
				Action: Action{Kind: ActionFill, Ref: onlyVisibleRef(t, observation.Snapshot), Value: "secret"},
			})
			if err != nil {
				t.Fatal(err)
			}
			if test.input == nil {
				broker.slots[session.ID].inputs = nil
			} else {
				broker.slots[session.ID].inputs = map[string]string{
					prepared.Action.ID: test.input["prepared"],
				}
			}
			resolveCalls := worker.resolveCalls
			if _, err = broker.ExecuteAction(
				context.Background(), owner, prepared.Action.ID, nil,
			); !errors.Is(err, ErrStale) {
				t.Fatalf("ExecuteAction() live input error = %v, want ErrStale", err)
			}
			if worker.resolveCalls != resolveCalls || len(worker.actions) != 0 {
				t.Fatalf(
					"driver reached with invalid live input: resolves %d -> %d, actions %+v",
					resolveCalls,
					worker.resolveCalls,
					worker.actions,
				)
			}
		})
	}
}

func TestFileStorePreparationIsAtomicAtRecordBound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "browser.json")
	store, err := NewFileStore(path, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	broker, _, session := openActionTestBroker(t, store)
	owner := testOwner()
	observation, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
	if err != nil {
		t.Fatal(err)
	}
	request := PrepareActionRequest{
		Owner: owner, RequestID: "request_bounded", SessionID: session.ID, TabID: session.TabID,
		SnapshotID: observation.SnapshotID, SnapshotGeneration: observation.SnapshotGeneration,
		Action: Action{Kind: ActionFill, Ref: onlyVisibleRef(t, observation.Snapshot), Value: "Ada"},
	}
	if _, err = broker.PrepareAction(context.Background(), request); !errors.Is(err, ErrStoreFull) {
		t.Fatalf("PrepareAction() bound error = %v, want ErrStoreFull", err)
	}
	preparedID := derivedIdentifier("prepared", owner, session.ID, request.RequestID)
	invocationID := derivedIdentifier("invocation", owner, session.ID, request.RequestID)
	if _, err = store.GetPreparedAction(context.Background(), preparedID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("partial prepared record error = %v, want ErrNotFound", err)
	}
	if _, err = store.GetInvocation(context.Background(), invocationID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("partial invocation record error = %v, want ErrNotFound", err)
	}
}

func TestFileStorePreparationCommittedWarningRetainsCompletePair(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "browser.json")
	store, err := NewFileStore(path, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	broker, _, session := openActionTestBroker(t, store)
	owner := testOwner()
	observation, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
	if err != nil {
		t.Fatal(err)
	}
	request := PrepareActionRequest{
		Owner: owner, RequestID: "request_committed", SessionID: session.ID, TabID: session.TabID,
		SnapshotID: observation.SnapshotID, SnapshotGeneration: observation.SnapshotGeneration,
		Action: Action{Kind: ActionFill, Ref: onlyVisibleRef(t, observation.Snapshot), Value: "Ada"},
	}
	originalWrite := store.writeFile
	store.writeFile = func(path string, data []byte, mode os.FileMode) error {
		if err := originalWrite(path, data, mode); err != nil {
			return err
		}
		return &fileutil.CommittedWriteError{Err: errors.New("directory sync warning")}
	}
	if _, err = broker.PrepareAction(context.Background(), request); !fileutil.IsCommittedWriteError(err) {
		t.Fatalf("PrepareAction() committed warning = %v", err)
	}
	preparedID := derivedIdentifier("prepared", owner, session.ID, request.RequestID)
	invocationID := derivedIdentifier("invocation", owner, session.ID, request.RequestID)
	prepared, preparedErr := store.GetPreparedAction(context.Background(), preparedID)
	invocation, invocationErr := store.GetInvocation(context.Background(), invocationID)
	if preparedErr != nil || invocationErr != nil || invocation.PreparedActionID != prepared.ID ||
		invocation.ActionHash != prepared.ActionHash {
		t.Fatalf(
			"committed pair = prepared %+v (%v), invocation %+v (%v)",
			prepared,
			preparedErr,
			invocation,
			invocationErr,
		)
	}
	retried, err := broker.PrepareAction(context.Background(), request)
	if err != nil || retried.Action != prepared {
		t.Fatalf("idempotent PrepareAction() = %+v, %v", retried, err)
	}
}

func TestPreparedRetentionWaitsForInvocationRetention(t *testing.T) {
	store := NewMemoryStore()
	broker, _, session := openActionTestBroker(t, store)
	owner := testOwner()
	observation, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := broker.PrepareAction(context.Background(), PrepareActionRequest{
		Owner: owner, RequestID: "request_retention", SessionID: session.ID, TabID: session.TabID,
		SnapshotID: observation.SnapshotID, SnapshotGeneration: observation.SnapshotGeneration,
		Action: Action{Kind: ActionFill, Ref: onlyVisibleRef(t, observation.Snapshot), Value: "Ada"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = broker.ExecuteAction(context.Background(), owner, prepared.Action.ID, nil); err != nil {
		t.Fatal(err)
	}
	if err = store.PrunePreparedActions(context.Background(), int64(^uint64(0)>>1)); err != nil {
		t.Fatal(err)
	}
	if _, err = store.GetPreparedAction(context.Background(), prepared.Action.ID); err != nil {
		t.Fatalf("prepared action pruned while invocation retained: %v", err)
	}
	if err = store.PruneInvocations(context.Background(), int64(^uint64(0)>>1)); err != nil {
		t.Fatal(err)
	}
	if err = store.PrunePreparedActions(context.Background(), int64(^uint64(0)>>1)); err != nil {
		t.Fatal(err)
	}
	if _, err = store.GetPreparedAction(
		context.Background(), prepared.Action.ID,
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unreferenced prepared action prune error = %v, want ErrNotFound", err)
	}
}

func openActionTestBroker(t *testing.T, store Store) (*Broker, *actionTestWorker, Session) {
	t.Helper()
	return openActionTestBrokerWithConfig(t, admittedBrowserConfig(), store)
}

func openActionTestBrokerWithConfig(
	t *testing.T,
	root *config.Config,
	store Store,
) (*Broker, *actionTestWorker, Session) {
	t.Helper()
	worker := &actionTestWorker{observation: driverObservationFixture(
		DriverElement{Target: "e1", Role: "textbox", Name: "Name"},
	)}
	worker.resolveElement = worker.observation.Elements[0]
	worker.resolveOrigin = worker.observation.Origin
	broker := newTestBroker(t, root, store, &actionTestFactory{worker: worker})
	broker.lookupIP = func(context.Context, string, string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("93.184.216.34")}, nil
	}
	now := time.Now().UTC()
	broker.now = func() time.Time {
		now = now.Add(time.Nanosecond)
		return now
	}
	session, err := broker.Open(context.Background(), OpenRequest{
		Owner: testOwner(), Target: "gateway", Profile: "managed",
	})
	if err != nil {
		t.Fatal(err)
	}
	return broker, worker, session
}

func assertNetworkQuarantine(
	t *testing.T,
	store *MemoryStore,
	worker *actionTestWorker,
	session Session,
	err error,
) {
	t.Helper()
	if !errors.Is(err, ErrDenied) {
		t.Fatalf("network policy error = %v, want ErrDenied", err)
	}
	stored, getErr := store.GetSession(context.Background(), session.ID)
	if getErr != nil || stored.State != SessionLost || stored.SafeFailure != "network_denied" ||
		worker.closed != 1 {
		t.Fatalf("network quarantine = %+v, %v; worker = %+v", stored, getErr, worker)
	}
}

func driverObservationFixture(elements ...DriverElement) DriverObservation {
	lines := make([]string, 0, len(elements))
	for _, element := range elements {
		lines = append(lines, "- "+element.Role+" \""+element.Name+"\" [ref="+element.Target+"]")
	}
	return DriverObservation{
		URL: "https://example.com/form", Origin: "https://example.com", Title: "Fixture",
		Snapshot: strings.Join(lines, "\n"), Elements: elements,
	}
}

func onlyVisibleRef(t *testing.T, snapshot string) string {
	t.Helper()
	start := strings.Index(snapshot, "[ref=")
	if start < 0 {
		t.Fatalf("snapshot has no ref: %q", snapshot)
	}
	start += len("[ref=")
	end := strings.Index(snapshot[start:], "]")
	if end < 0 {
		t.Fatalf("snapshot has malformed ref: %q", snapshot)
	}
	return snapshot[start : start+end]
}
