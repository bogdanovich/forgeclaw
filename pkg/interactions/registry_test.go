package interactions

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

func newTestRegistry(t *testing.T) (*Registry, *testClock, string) {
	t.Helper()
	clock := &testClock{now: time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)}
	path := WorkspaceStorePath(t.TempDir())
	registry := NewRegistryWithOptions(path, Options{Now: clock.Now})
	if err := registry.LastLoadError(); err != nil {
		t.Fatalf("load registry: %v", err)
	}
	return registry, clock, path
}

func validCreate(clock *testClock, id, session string) CreateRequest {
	return CreateRequest{
		ID:   id,
		Kind: KindQuestion,
		Route: Route{
			AgentID:         "main",
			SessionKey:      session,
			RouteSessionKey: "telegram:default:chat-1",
			Channel:         "telegram",
			AccountID:       "default",
			ChatID:          "chat-1",
			TopicID:         "topic-1",
			SenderID:        "user-1",
		},
		Origin: Origin{
			TurnID:     "turn-1",
			ToolCallID: "call-1",
			ToolName:   "request_user_input",
		},
		Questions: []Question{{
			ID:       "deploy_target",
			Header:   "Target",
			Question: "Where should this be deployed?",
			Options: []Option{
				{Label: "Staging", Description: "Deploy to staging first."},
				{Label: "Production", Description: "Deploy directly to production."},
			},
		}},
		PromptSummary: "Choose a deployment target.",
		ExpiresAt:     clock.Now().Add(time.Hour),
	}
}

func makeWaiting(t *testing.T, registry *Registry, clock *testClock, id, session string) Record {
	t.Helper()
	rec, err := registry.Create(validCreate(clock, id, session))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	rec, err = registry.RecordDeliveryAttempt(rec.ID, rec.Revision, true, "")
	if err != nil {
		t.Fatalf("record delivery: %v", err)
	}
	rec, err = registry.MarkWaiting(rec.ID, rec.Revision)
	if err != nil {
		t.Fatalf("mark waiting: %v", err)
	}
	return rec
}

func TestRegistryLifecyclePersistsAndReloads(t *testing.T) {
	registry, clock, path := newTestRegistry(t)
	rec, err := registry.Create(validCreate(clock, "interaction_aaaaaaaa11111111", "session-1"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if rec.Status != StatusCreated || rec.Revision != 1 || rec.ShortID != "aaaaaaaa" {
		t.Fatalf("created record = %+v", rec)
	}

	rec, err = registry.RecordDeliveryAttempt(
		rec.ID,
		rec.Revision,
		false,
		"temporary channel error",
	)
	if err != nil {
		t.Fatalf("record failed delivery: %v", err)
	}
	if rec.Status != StatusCreated || rec.DeliveryTries != 1 || rec.DeliveryError == "" {
		t.Fatalf("failed delivery record = %+v", rec)
	}
	rec, err = registry.RecordDeliveryAttempt(rec.ID, rec.Revision, true, "")
	if err != nil {
		t.Fatalf("record successful delivery: %v", err)
	}
	if !rec.PromptDelivered {
		t.Fatal("successful prompt delivery was not persisted")
	}
	rec, err = registry.MarkWaiting(rec.ID, rec.Revision)
	if err != nil {
		t.Fatalf("mark waiting: %v", err)
	}
	rec, err = registry.ClaimAnswer(rec.ID, rec.Revision, Answer{
		Text:      "Staging",
		Values:    map[string]string{"deploy_target": "Staging"},
		MessageID: "inbound-1",
	}, OutcomeAnswered)
	if err != nil {
		t.Fatalf("claim answer: %v", err)
	}
	if rec.Status != StatusClaimed || rec.Outcome != OutcomeAnswered || rec.Answer == nil {
		t.Fatalf("claimed record = %+v", rec)
	}
	rec, err = registry.MarkResuming(rec.ID, rec.Revision)
	if err != nil {
		t.Fatalf("mark resuming: %v", err)
	}
	rec, err = registry.Resolve(rec.ID, rec.Revision)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if rec.Status != StatusResolved || rec.CleanupAfter == 0 || rec.Revision != 7 {
		t.Fatalf("resolved record = %+v", rec)
	}

	reloaded := NewRegistryWithOptions(path, Options{Now: clock.Now})
	if err := reloaded.LastLoadError(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	got, ok := reloaded.Get(rec.ID)
	if !ok || got.Status != StatusResolved || got.Answer == nil || got.Answer.Text != "Staging" {
		t.Fatalf("reloaded record = %+v, found=%v", got, ok)
	}
	events := reloaded.ListEvents(rec.ID)
	if len(events) != 7 {
		t.Fatalf("events = %d, want 7", len(events))
	}
	for i, event := range events {
		if event.Sequence != int64(i+1) || event.Revision != int64(i+1) {
			t.Fatalf("event %d = %+v", i, event)
		}
	}
}

func TestRegistryCreateRejectsSecondActiveSessionAndShortID(t *testing.T) {
	registry, clock, _ := newTestRegistry(t)
	first := makeWaiting(t, registry, clock, "interaction_aaaaaaaa11111111", "session-1")

	request := validCreate(clock, "interaction_bbbbbbbb11111111", "session-1")
	if _, err := registry.Create(request); !errors.Is(err, ErrSessionHasActive) {
		t.Fatalf("same session error = %v", err)
	}

	request = validCreate(clock, "interaction_aaaaaaaa22222222", "session-2")
	if _, err := registry.Create(request); !errors.Is(err, ErrConflict) {
		t.Fatalf("same short id error = %v", err)
	}

	claimed, err := registry.ClaimAnswer(
		first.ID,
		first.Revision,
		Answer{Text: "Staging"},
		OutcomeAnswered,
	)
	if err != nil {
		t.Fatalf("claim first: %v", err)
	}
	resuming, err := registry.MarkResuming(claimed.ID, claimed.Revision)
	if err != nil {
		t.Fatalf("resume first: %v", err)
	}
	if _, err := registry.Resolve(resuming.ID, resuming.Revision); err != nil {
		t.Fatalf("resolve first: %v", err)
	}
	request = validCreate(clock, "interaction_bbbbbbbb11111111", "session-1")
	if _, err := registry.Create(request); err != nil {
		t.Fatalf("create after terminal: %v", err)
	}
}

func TestRegistryChainsSequentialForegroundInteractions(t *testing.T) {
	registry, clock, _ := newTestRegistry(t)
	request := validCreate(clock, "interaction_chain11111111", "session-chain")
	request.Origin.ContinuationSessionKey = "session-chain"
	first, err := registry.Create(request)
	if err != nil {
		t.Fatal(err)
	}
	first, _ = registry.RecordDeliveryAttempt(first.ID, first.Revision, true, "")
	first, _ = registry.MarkWaiting(first.ID, first.Revision)
	first, _ = registry.ClaimAnswer(
		first.ID, first.Revision, Answer{Text: "continue"}, OutcomeAnswered,
	)
	first, _ = registry.MarkResuming(first.ID, first.Revision)
	nextRequest := validCreate(clock, "interaction_chain22222222", "session-chain")
	nextRequest.Origin.ContinuationSessionKey = "session-chain"
	next, err := registry.Create(nextRequest)
	if err != nil {
		t.Fatalf("Create(chained foreground) error = %v", err)
	}
	resolved, _ := registry.Get(first.ID)
	if resolved.Status != StatusResolved || next.Status != StatusCreated {
		t.Fatalf("chained records = first:%#v next:%#v", resolved, next)
	}
}

func TestRegistryConcurrentAnswerClaimIsExactlyOnce(t *testing.T) {
	registry, clock, _ := newTestRegistry(t)
	rec := makeWaiting(t, registry, clock, "interaction_cccccccc11111111", "session-1")

	const contenders = 32
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := registry.ClaimAnswer(rec.ID, rec.Revision, Answer{
				Text:      "Staging",
				MessageID: "same-delivery",
			}, OutcomeAnswered)
			if err == nil {
				successes.Add(1)
				return
			}
			if !errors.Is(err, ErrConflict) && !errors.Is(err, ErrAnswerTooLate) {
				t.Errorf("unexpected claim error: %v", err)
			}
		}()
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatalf("successful claims = %d, want 1", successes.Load())
	}
	got, _ := registry.Get(rec.ID)
	if got.Status != StatusClaimed || got.Answer == nil || got.Answer.MessageID != "same-delivery" {
		t.Fatalf("claimed record = %+v", got)
	}
}

func TestConcurrentRegistryInstancesDoNotLoseWrites(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)}
	path := WorkspaceStorePath(t.TempDir())
	first := NewRegistryWithOptions(path, Options{Now: clock.Now})
	second := NewRegistryWithOptions(path, Options{Now: clock.Now})

	requests := []struct {
		registry *Registry
		request  CreateRequest
	}{
		{first, validCreate(clock, "interaction_17171717aaaaaaaa", "session-1")},
		{second, validCreate(clock, "interaction_18181818aaaaaaaa", "session-2")},
	}
	start := make(chan struct{})
	errorsByCall := make(chan error, len(requests))
	for _, call := range requests {
		go func() {
			<-start
			_, err := call.registry.Create(call.request)
			errorsByCall <- err
		}()
	}
	close(start)
	for range requests {
		select {
		case err := <-errorsByCall:
			if err != nil {
				t.Fatalf("concurrent create: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent registry create deadlocked")
		}
	}

	reloaded := NewRegistryWithOptions(path, Options{Now: clock.Now})
	if err := reloaded.LastLoadError(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if records := reloaded.List(); len(records) != 2 {
		t.Fatalf("records after concurrent writes = %+v", records)
	}
	if events := reloaded.ListEvents(""); len(events) != 2 {
		t.Fatalf("events after concurrent writes = %+v", events)
	}
}

func TestConcurrentRegistryInstancesEnforceActiveSessionUniqueness(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)}
	path := WorkspaceStorePath(t.TempDir())
	first := NewRegistryWithOptions(path, Options{Now: clock.Now})
	second := NewRegistryWithOptions(path, Options{Now: clock.Now})

	start := make(chan struct{})
	errorsByCall := make(chan error, 2)
	for _, call := range []struct {
		registry *Registry
		id       string
	}{
		{first, "interaction_19191919aaaaaaaa"},
		{second, "interaction_20202020aaaaaaaa"},
	} {
		go func() {
			<-start
			_, err := call.registry.Create(validCreate(clock, call.id, "shared-session"))
			errorsByCall <- err
		}()
	}
	close(start)
	var successes, conflicts int
	for range 2 {
		err := <-errorsByCall
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrSessionHasActive):
			conflicts++
		default:
			t.Fatalf("unexpected concurrent create error: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}
}

func TestStaleRegistryRevisionConflictsAfterLockedReload(t *testing.T) {
	registry, clock, path := newTestRegistry(t)
	rec := makeWaiting(t, registry, clock, "interaction_21212121aaaaaaaa", "session-1")
	stale := NewRegistryWithOptions(path, Options{Now: clock.Now})

	if _, err := registry.ClaimAnswer(
		rec.ID,
		rec.Revision,
		Answer{Text: "Staging", MessageID: "inbound-1"},
		OutcomeAnswered,
	); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	_, err := stale.ClaimAnswer(
		rec.ID,
		rec.Revision,
		Answer{Text: "Production", MessageID: "inbound-2"},
		OutcomeAnswered,
	)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("stale claim error = %v", err)
	}
}

func TestRegistryRejectsAnswerMessageClaimedByAnotherInteraction(t *testing.T) {
	registry, clock, _ := newTestRegistry(t)
	first := makeWaiting(t, registry, clock, "interaction_dddddddd11111111", "session-1")
	first, err := registry.ClaimAnswer(first.ID, first.Revision, Answer{
		Text:      "Staging",
		MessageID: "inbound-duplicate",
	}, OutcomeAnswered)
	if err != nil {
		t.Fatalf("claim first: %v", err)
	}
	first, _ = registry.MarkResuming(first.ID, first.Revision)
	if _, resolveErr := registry.Resolve(first.ID, first.Revision); resolveErr != nil {
		t.Fatalf("resolve first: %v", resolveErr)
	}

	second := makeWaiting(t, registry, clock, "interaction_eeeeeeee11111111", "session-2")
	_, err = registry.ClaimAnswer(second.ID, second.Revision, Answer{
		Text:      "Production",
		MessageID: "inbound-duplicate",
	}, OutcomeAnswered)
	if !errors.Is(err, ErrDuplicateAnswer) {
		t.Fatalf("duplicate answer error = %v", err)
	}
}

func TestRegistryAllowsSameAnswerMessageIDOnDifferentRoutes(t *testing.T) {
	registry, clock, _ := newTestRegistry(t)
	first := makeWaiting(t, registry, clock, "interaction_dddddddd22222222", "session-1")
	first, err := registry.ClaimAnswer(first.ID, first.Revision, Answer{
		Text: "Staging", MessageID: "message-1",
	}, OutcomeAnswered)
	if err != nil {
		t.Fatalf("claim first: %v", err)
	}
	first, _ = registry.MarkResuming(first.ID, first.Revision)
	if _, resolveErr := registry.Resolve(first.ID, first.Revision); resolveErr != nil {
		t.Fatalf("resolve first: %v", resolveErr)
	}

	request := validCreate(clock, "interaction_eeeeeeee22222222", "session-2")
	request.Route.ChatID = "chat-2"
	request.Route.RouteSessionKey = "telegram:default:chat-2"
	second, err := registry.Create(request)
	if err != nil {
		t.Fatalf("create second: %v", err)
	}
	second, _ = registry.RecordDeliveryAttempt(second.ID, second.Revision, true, "")
	second, _ = registry.MarkWaiting(second.ID, second.Revision)
	if _, err := registry.ClaimAnswer(second.ID, second.Revision, Answer{
		Text: "Production", MessageID: "message-1",
	}, OutcomeAnswered); err != nil {
		t.Fatalf("same message ID on another route was rejected: %v", err)
	}
}

func TestRegistryClaimOverdueUsesResumableTimeoutOutcome(t *testing.T) {
	registry, clock, path := newTestRegistry(t)
	rec := makeWaiting(t, registry, clock, "interaction_ffffffff11111111", "session-1")
	created, err := registry.Create(validCreate(clock, "interaction_aaaaaaaa22222222", "session-2"))
	if err != nil {
		t.Fatalf("create undelivered interaction: %v", err)
	}
	clock.Advance(2 * time.Hour)

	claimed, err := registry.ClaimOverdue(time.Time{})
	if err != nil {
		t.Fatalf("claim overdue: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed overdue = %+v", claimed)
	}
	for _, item := range claimed {
		if item.Status != StatusClaimed || item.Outcome != OutcomeTimedOut || item.Answer == nil {
			t.Fatalf("claimed overdue item = %+v", item)
		}
	}
	if claimed[0].ID != created.ID || claimed[1].ID != rec.ID {
		t.Fatalf("claimed ids = %q, %q", claimed[0].ID, claimed[1].ID)
	}

	reloaded := NewRegistryWithOptions(path, Options{Now: clock.Now})
	got, ok := reloaded.Get(rec.ID)
	if !ok || got.Status != StatusClaimed || got.Outcome != OutcomeTimedOut {
		t.Fatalf("reloaded timeout = %+v, found=%v", got, ok)
	}
}

func TestRegistryRevisionAndTransitionConflictsFailClosed(t *testing.T) {
	registry, clock, _ := newTestRegistry(t)
	rec, err := registry.Create(validCreate(clock, "interaction_11111111aaaaaaaa", "session-1"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := registry.MarkWaiting(rec.ID, rec.Revision+1); !errors.Is(err, ErrConflict) {
		t.Fatalf("revision error = %v", err)
	}
	if _, err := registry.MarkResuming(rec.ID, rec.Revision); !errors.Is(
		err,
		ErrInvalidTransition,
	) {
		t.Fatalf("transition error = %v", err)
	}
	got, _ := registry.Get(rec.ID)
	if got.Status != StatusCreated || got.Revision != rec.Revision {
		t.Fatalf("record changed after rejected operations: %+v", got)
	}
}

func TestRegistryWaitingRequiresSuccessfulDelivery(t *testing.T) {
	registry, clock, _ := newTestRegistry(t)
	rec, err := registry.Create(validCreate(clock, "interaction_12121212aaaaaaaa", "session-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, waitErr := registry.MarkWaiting(rec.ID, rec.Revision); !errors.Is(
		waitErr,
		ErrInvalidTransition,
	) {
		t.Fatalf("waiting without delivery error = %v", waitErr)
	}
	rec, err = registry.RecordDeliveryAttempt(rec.ID, rec.Revision, false, "channel unavailable")
	if err != nil {
		t.Fatal(err)
	}
	if _, waitErr := registry.MarkWaiting(rec.ID, rec.Revision); !errors.Is(
		waitErr,
		ErrInvalidTransition,
	) {
		t.Fatalf("waiting after failed delivery error = %v", waitErr)
	}
}

func TestRegistryPersistsAmbiguousPromptDeliveryWindow(t *testing.T) {
	registry, clock, path := newTestRegistry(t)
	record, err := registry.Create(validCreate(
		clock,
		"interaction_14141414aaaaaaaa",
		"session-ambiguous",
	))
	if err != nil {
		t.Fatal(err)
	}
	record, err = registry.BeginPromptDelivery(record.ID, record.Revision)
	if err != nil || record.PromptDeliveryState != DeliveryStateSending || record.DeliveryTries != 1 {
		t.Fatalf("begin prompt delivery = (%#v, %v)", record, err)
	}
	if _, waitErr := registry.MarkWaiting(record.ID, record.Revision); !errors.Is(
		waitErr,
		ErrInvalidTransition,
	) {
		t.Fatalf("waiting during ambiguous send error = %v", waitErr)
	}

	reloaded := NewRegistryWithOptions(path, Options{Now: clock.Now})
	record, _ = reloaded.Get(record.ID)
	if record.PromptDeliveryState != DeliveryStateSending || record.PromptDelivered {
		t.Fatalf("reloaded ambiguous delivery = %#v", record)
	}
	record, err = reloaded.ClaimDeliveryUnknown(record.ID, record.Revision)
	if err != nil || record.Status != StatusClaimed || record.Outcome != OutcomeDeliveryUnknown {
		t.Fatalf("claim unknown delivery = (%#v, %v)", record, err)
	}
}

func TestRegistryFindWaitingByRouteRequiresExactSenderAndTopic(t *testing.T) {
	registry, clock, _ := newTestRegistry(t)
	rec := makeWaiting(t, registry, clock, "interaction_13131313aaaaaaaa", "session-1")
	if got := registry.FindWaitingByRoute(rec.Route); len(got) != 1 || got[0].ID != rec.ID {
		t.Fatalf("exact route matches = %+v", got)
	}
	wrongSender := rec.Route
	wrongSender.SenderID = "user-2"
	if got := registry.FindWaitingByRoute(wrongSender); len(got) != 0 {
		t.Fatalf("wrong sender matched = %+v", got)
	}
	wrongTopic := rec.Route
	wrongTopic.TopicID = "topic-2"
	if got := registry.FindWaitingByRoute(wrongTopic); len(got) != 0 {
		t.Fatalf("wrong topic matched = %+v", got)
	}
}

func TestRegistryCancellationWorksAcrossNonterminalPhases(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, *Registry, *testClock) Record
	}{
		{
			name: "created",
			setup: func(t *testing.T, registry *Registry, clock *testClock) Record {
				rec, err := registry.Create(
					validCreate(clock, "interaction_22222222aaaaaaaa", "created"),
				)
				if err != nil {
					t.Fatal(err)
				}
				return rec
			},
		},
		{name: "waiting", setup: func(t *testing.T, registry *Registry, clock *testClock) Record {
			return makeWaiting(t, registry, clock, "interaction_33333333aaaaaaaa", "waiting")
		}},
		{name: "claimed", setup: func(t *testing.T, registry *Registry, clock *testClock) Record {
			rec := makeWaiting(t, registry, clock, "interaction_44444444aaaaaaaa", "claimed")
			rec, err := registry.ClaimAnswer(
				rec.ID,
				rec.Revision,
				Answer{Text: "answer"},
				OutcomeAnswered,
			)
			if err != nil {
				t.Fatal(err)
			}
			return rec
		}},
		{name: "resuming", setup: func(t *testing.T, registry *Registry, clock *testClock) Record {
			rec := makeWaiting(t, registry, clock, "interaction_55555555aaaaaaaa", "resuming")
			rec, err := registry.ClaimAnswer(
				rec.ID,
				rec.Revision,
				Answer{Text: "answer"},
				OutcomeAnswered,
			)
			if err != nil {
				t.Fatal(err)
			}
			rec, err = registry.MarkResuming(rec.ID, rec.Revision)
			if err != nil {
				t.Fatal(err)
			}
			return rec
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry, clock, _ := newTestRegistry(t)
			rec := test.setup(t, registry, clock)
			canceled, err := registry.Cancel(rec.ID, rec.Revision, "user_canceled")
			if err != nil {
				t.Fatalf("cancel: %v", err)
			}
			if canceled.Status != StatusCancelled || canceled.CleanupAfter == 0 {
				t.Fatalf("canceled record = %+v", canceled)
			}
		})
	}
}

func TestRegistryPrunesOnlyExpiredTerminalRecords(t *testing.T) {
	registry, clock, _ := newTestRegistry(t)
	terminal := makeWaiting(t, registry, clock, "interaction_66666666aaaaaaaa", "terminal")
	terminal, _ = registry.ClaimAnswer(
		terminal.ID,
		terminal.Revision,
		Answer{Text: "answer"},
		OutcomeAnswered,
	)
	terminal, _ = registry.MarkResuming(terminal.ID, terminal.Revision)
	terminal, _ = registry.Resolve(terminal.ID, terminal.Revision)
	active := makeWaiting(t, registry, clock, "interaction_77777777aaaaaaaa", "active")

	clock.Advance(DefaultRetention + time.Hour)
	if err := registry.Prune(time.Time{}); err != nil {
		t.Fatalf("prune: %v", err)
	}
	if _, ok := registry.Get(terminal.ID); ok {
		t.Fatal("terminal record was not pruned")
	}
	if got, ok := registry.Get(active.ID); !ok || got.Status != StatusWaiting {
		t.Fatalf("active record was pruned: %+v, found=%v", got, ok)
	}
	for _, event := range registry.ListEvents("") {
		if event.InteractionID == terminal.ID {
			t.Fatalf("terminal event was not pruned: %+v", event)
		}
	}
}

func TestRegistryCorruptSnapshotFailsClosed(t *testing.T) {
	path := WorkspaceStorePath(t.TempDir())
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	corrupt := []byte(
		`{"schema_version":"interaction_snapshot.v1","records":[{"id":"bad"}]}`,
	)
	if err := os.WriteFile(path, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}
	clock := &testClock{now: time.Now()}
	registry := NewRegistryWithOptions(path, Options{Now: clock.Now})
	if registry.LastLoadError() == nil {
		t.Fatal("expected load error")
	}
	_, err := registry.Create(validCreate(clock, "interaction_88888888aaaaaaaa", "session-1"))
	if !errors.Is(err, ErrStoreUnavailable) {
		t.Fatalf("create error = %v", err)
	}
}

func TestRegistrySnapshotRejectsDuplicateActiveSession(t *testing.T) {
	registry, clock, path := newTestRegistry(t)
	first := makeWaiting(t, registry, clock, "interaction_14141414aaaaaaaa", "session-1")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot Snapshot
	if unmarshalErr := json.Unmarshal(data, &snapshot); unmarshalErr != nil {
		t.Fatal(unmarshalErr)
	}
	duplicate := first
	duplicate.ID = "interaction_15151515aaaaaaaa"
	duplicate.ShortID = "15151515"
	snapshot.Records = append(snapshot.Records, duplicate)
	data, err = json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if writeErr := os.WriteFile(path, data, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	reloaded := NewRegistryWithOptions(path, Options{Now: clock.Now})
	if reloaded.LastLoadError() == nil {
		t.Fatal("expected duplicate active session load error")
	}
}

func TestRegistryReloadsTrimmedContiguousEventTail(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)}
	path := WorkspaceStorePath(t.TempDir())
	options := Options{Now: clock.Now, MaxEvents: 2}
	registry := NewRegistryWithOptions(path, options)
	rec := makeWaiting(t, registry, clock, "interaction_16161616aaaaaaaa", "session-1")
	rec, err := registry.ClaimAnswer(rec.ID, rec.Revision, Answer{Text: "Staging"}, OutcomeAnswered)
	if err != nil {
		t.Fatal(err)
	}
	rec, err = registry.MarkResuming(rec.ID, rec.Revision)
	if err != nil {
		t.Fatal(err)
	}
	events := registry.ListEvents(rec.ID)
	if len(events) != 2 || events[0].Sequence <= 1 || events[1].Sequence != events[0].Sequence+1 {
		t.Fatalf("trimmed events = %+v", events)
	}
	reloaded := NewRegistryWithOptions(path, options)
	if err := reloaded.LastLoadError(); err != nil {
		t.Fatalf("reload trimmed events: %v", err)
	}
	rec, ok := reloaded.Get(rec.ID)
	if !ok {
		t.Fatal("reloaded interaction not found")
	}
	if _, err := reloaded.Resolve(rec.ID, rec.Revision); err != nil {
		t.Fatalf("resolve after trimmed reload: %v", err)
	}
	events = reloaded.ListEvents(rec.ID)
	if len(events) != 2 || events[1].Sequence != rec.LastEventSeq+1 {
		t.Fatalf("post-reload event sequence = %+v, previous=%d", events, rec.LastEventSeq)
	}
}

func TestRegistrySnapshotBudgetFailureRollsBackMemoryAndEvents(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)}
	registry := NewRegistryWithOptions(WorkspaceStorePath(t.TempDir()), Options{
		Now:              clock.Now,
		MaxSnapshotBytes: 200,
	})
	_, err := registry.Create(validCreate(clock, "interaction_99999999aaaaaaaa", "session-1"))
	if !errors.Is(err, ErrSnapshotOverBudget) {
		t.Fatalf("create error = %v", err)
	}
	if len(registry.List()) != 0 || len(registry.ListEvents("")) != 0 {
		t.Fatalf(
			"failed create leaked state: records=%d events=%d",
			len(registry.List()),
			len(registry.ListEvents("")),
		)
	}
}

func TestRegistryObserverRunsOutsideLockAndReceivesBoundedEvents(t *testing.T) {
	registry, clock, _ := newTestRegistry(t)
	var observed []Event
	unsubscribe := registry.Subscribe(func(observation EventObservation) {
		observed = append(observed, observation.Event)
		registry.Get(observation.Event.InteractionID)
		if observation.Record.ID != observation.Event.InteractionID {
			t.Errorf("observation record = %+v", observation.Record)
		}
	})
	rec := makeWaiting(t, registry, clock, "interaction_abababab11111111", "session-1")
	if len(observed) != 3 {
		t.Fatalf("observed events = %d, want 3", len(observed))
	}
	if observed[0].Type != EventCreated || observed[2].Type != EventWaiting {
		t.Fatalf("observed events = %+v", observed)
	}
	if observed[2].InteractionID != rec.ID {
		t.Fatalf("observed interaction = %q, want %q", observed[2].InteractionID, rec.ID)
	}
	unsubscribe()
	if _, err := registry.ClaimAnswer(rec.ID, rec.Revision, Answer{Text: "Staging"}, OutcomeAnswered); err != nil {
		t.Fatal(err)
	}
	if len(observed) != 3 {
		t.Fatalf("observer received event after unsubscribe: %d", len(observed))
	}
}

func TestRegistryObserverPanicDoesNotFailDurableWrite(t *testing.T) {
	registry, clock, path := newTestRegistry(t)
	registry.Subscribe(func(EventObservation) { panic("observer failed") })
	rec, err := registry.Create(validCreate(clock, "interaction_bcbcbcbc11111111", "session-1"))
	if err != nil {
		t.Fatalf("create after observer panic: %v", err)
	}
	reloaded := NewRegistryWithOptions(path, Options{Now: clock.Now})
	if _, ok := reloaded.Get(rec.ID); !ok {
		t.Fatal("durable record missing after observer panic")
	}
}

func TestRegistryReturnsDefensiveCopies(t *testing.T) {
	registry, clock, _ := newTestRegistry(t)
	rec := makeWaiting(t, registry, clock, "interaction_cdcdcdcd11111111", "session-1")
	rec.Questions[0].Question = "mutated"
	rec.Questions[0].Options[0].Label = "mutated"
	got, _ := registry.Get(rec.ID)
	if got.Questions[0].Question == "mutated" || got.Questions[0].Options[0].Label == "mutated" {
		t.Fatalf("stored questions were mutated: %+v", got.Questions)
	}

	claimed, err := registry.ClaimAnswer(got.ID, got.Revision, Answer{
		Values: map[string]string{"deploy_target": "Staging"},
	}, OutcomeAnswered)
	if err != nil {
		t.Fatal(err)
	}
	claimed.Answer.Values["deploy_target"] = "mutated"
	got, _ = registry.Get(rec.ID)
	if got.Answer.Values["deploy_target"] != "Staging" {
		t.Fatalf("stored answer was mutated: %+v", got.Answer)
	}
}

func TestValidateQuestionsAndApprovalAuthorityBounds(t *testing.T) {
	registry, clock, _ := newTestRegistry(t)
	request := validCreate(clock, "interaction_efefefef11111111", "session-1")
	request.Questions[0].ID = "Not Snake Case"
	if _, err := registry.Create(request); !errors.Is(err, ErrInvalidInteraction) {
		t.Fatalf("invalid question error = %v", err)
	}

	request = validCreate(clock, "interaction_fafafafa11111111", "session-2")
	request.Kind = KindApproval
	if _, err := registry.Create(request); !errors.Is(err, ErrInvalidInteraction) {
		t.Fatalf("model-authored approval question error = %v", err)
	}

	request.Questions = nil
	request.PromptSummary = "Policy requests one-time approval."
	request.Origin.ArgumentHash = strings.Repeat("a", 64)
	approval, err := registry.Create(request)
	if err != nil {
		t.Fatalf("policy approval create: %v", err)
	}
	approval, err = registry.RecordDeliveryAttempt(approval.ID, approval.Revision, true, "")
	if err != nil {
		t.Fatal(err)
	}
	approval, err = registry.MarkWaiting(approval.ID, approval.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.ClaimAnswer(
		approval.ID,
		approval.Revision,
		Answer{Text: "yes"},
		OutcomeAnswered,
	); !errors.Is(err, ErrInvalidInteraction) {
		t.Fatalf("approval accepted question outcome: %v", err)
	}
	if _, err := registry.ClaimAnswer(
		approval.ID,
		approval.Revision,
		Answer{Text: "allow once"},
		OutcomeAllowed,
	); err != nil {
		t.Fatalf("approval allow outcome: %v", err)
	}
}

func TestRegistryConsumesApprovalExactlyOnceAndMatchesCall(t *testing.T) {
	registry, clock, path := newTestRegistry(t)
	request := validCreate(clock, "interaction_approval111111", "session-approval")
	request.Kind = KindApproval
	request.Questions = nil
	request.PromptSummary = "Policy requires approval."
	request.Origin.ArgumentHash = strings.Repeat("a", 64)
	record, err := registry.Create(request)
	if err != nil {
		t.Fatal(err)
	}
	record, _ = registry.RecordDeliveryAttempt(record.ID, record.Revision, true, "")
	record, _ = registry.MarkWaiting(record.ID, record.Revision)
	record, err = registry.ClaimAnswer(
		record.ID, record.Revision,
		Answer{Text: "allow_once", ReceivedAt: clock.Now().UnixMilli()},
		OutcomeAllowed,
	)
	if err != nil {
		t.Fatal(err)
	}
	record, err = registry.MarkResuming(record.ID, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = registry.ConsumeApproval(
		record.ID, record.Revision, record.Origin.ToolCallID,
		record.Origin.ToolName, strings.Repeat("b", 64),
	); err == nil {
		t.Fatal("mismatched argument hash consumed approval")
	}
	record, err = registry.ConsumeApproval(
		record.ID, record.Revision, record.Origin.ToolCallID,
		record.Origin.ToolName, record.Origin.ArgumentHash,
	)
	if err != nil || record.ApprovalConsumedAt == 0 {
		t.Fatalf("ConsumeApproval() = (%#v, %v)", record, err)
	}
	if _, err = registry.ConsumeApproval(
		record.ID, record.Revision, record.Origin.ToolCallID,
		record.Origin.ToolName, record.Origin.ArgumentHash,
	); err == nil {
		t.Fatal("approval was consumed twice")
	}
	record, err = registry.BeginCancellation(record.ID, record.Revision, "operator_stop")
	if err != nil {
		t.Fatal(err)
	}
	record, err = registry.CompleteCancellation(record.ID, record.Revision)
	if err != nil || record.ApprovalConsumedAt == 0 {
		t.Fatalf("cancel consumed approval = (%#v, %v)", record, err)
	}
	if reloaded := NewRegistryWithOptions(path, Options{Now: clock.Now}); reloaded.LastLoadError() != nil {
		t.Fatalf("reload consumed canceled approval: %v", reloaded.LastLoadError())
	}
}

func TestStoredV1QuestionsRemainLoadableUnderTighterCreatePolicy(t *testing.T) {
	legacySingle := []Question{{
		ID: "legacy_single", Question: "Legacy choice?",
		Options: []Option{{Label: "Only", Description: "Previously valid."}},
	}}
	if err := validateStoredQuestions(KindQuestion, legacySingle); err != nil {
		t.Fatalf("stored single-option question rejected: %v", err)
	}
	if err := validateQuestions(KindQuestion, legacySingle); err == nil {
		t.Fatal("new single-option question accepted")
	}

	legacyFour := []Question{{
		ID: "legacy_four", Question: "Legacy choices?",
		Options: []Option{
			{Label: "Same", Description: "First."},
			{Label: "same", Description: "Duplicate labels were previously valid."},
			{Label: "Third", Description: "Third."},
			{Label: "Fourth", Description: "Fourth."},
		},
	}}
	if err := validateStoredQuestions(KindQuestion, legacyFour); err != nil {
		t.Fatalf("stored four-option question rejected: %v", err)
	}
	if err := validateQuestions(KindQuestion, legacyFour); err == nil {
		t.Fatal("new four-option question accepted")
	}
}
