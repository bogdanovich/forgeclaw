package outbox

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/bus"
)

func TestDeliveryIDDoesNotDependOnRoute(t *testing.T) {
	first := testIdentity()
	second := first
	second.Channel = "slack"
	second.ChatID = "rerouted-chat"
	second.SessionKey = "agent:other:slack:rerouted-chat"

	firstID, err := DeliveryID(first)
	if err != nil {
		t.Fatalf("DeliveryID(first) error = %v", err)
	}
	secondID, err := DeliveryID(second)
	if err != nil {
		t.Fatalf("DeliveryID(second) error = %v", err)
	}
	if firstID != secondID {
		t.Fatalf("route changed delivery ID from %q to %q", firstID, secondID)
	}
}

func TestCoordinatorKeepsFirstOwnerRouteAndPayload(t *testing.T) {
	coordinator, err := OpenCoordinator(t.TempDir())
	if err != nil {
		t.Fatalf("OpenCoordinator() error = %v", err)
	}
	t.Cleanup(func() { _ = coordinator.Close() })
	identity := testIdentity()
	first, err := coordinator.AdmitMessage("/agents/main", identity, bus.OutboundMessage{Content: "first"})
	if err != nil {
		t.Fatalf("AdmitMessage(first) error = %v", err)
	}
	if !first.Dispatch {
		t.Fatal("first admission did not own dispatch")
	}

	replayedIdentity := identity
	replayedIdentity.Channel = "slack"
	replayedIdentity.ChatID = "rerouted-chat"
	replayedIdentity.SessionKey = "agent:other:slack:rerouted-chat"
	replayed, err := coordinator.AdmitMessage(
		"/agents/rerouted",
		replayedIdentity,
		bus.OutboundMessage{Content: "regenerated"},
	)
	if err != nil {
		t.Fatalf("AdmitMessage(replay) error = %v", err)
	}
	if replayed.Dispatch {
		t.Fatal("duplicate admission owned a second dispatch")
	}
	if replayed.Intent.OwnerWorkspace != "/agents/main" || replayed.Intent.Identity != identity ||
		replayed.Intent.Message.Content != "first" {
		t.Fatalf("replayed intent = %#v, want first canonical intent", replayed.Intent)
	}
}

func TestCoordinatorReopenUsesOneCanonicalStore(t *testing.T) {
	instanceRoot := t.TempDir()
	first, err := OpenCoordinator(instanceRoot)
	if err != nil {
		t.Fatalf("OpenCoordinator(first) error = %v", err)
	}
	identity := testIdentity()
	created, err := first.AdmitMessage("/agents/main", identity, bus.OutboundMessage{Content: "first"})
	if err != nil {
		t.Fatalf("AdmitMessage(first) error = %v", err)
	}
	if closeErr := first.Close(); closeErr != nil {
		t.Fatalf("Close(first) error = %v", closeErr)
	}

	second, err := OpenCoordinator(instanceRoot)
	if err != nil {
		t.Fatalf("OpenCoordinator(second) error = %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })
	identity.Channel = "slack"
	identity.ChatID = "rerouted-chat"
	identity.SessionKey = "agent:other:slack:rerouted-chat"
	replayed, err := second.AdmitMessage("/agents/other", identity, bus.OutboundMessage{Content: "second"})
	if err != nil {
		t.Fatalf("AdmitMessage(replay) error = %v", err)
	}
	if !replayed.Dispatch {
		t.Fatal("pending admission after restart did not resume dispatch")
	}
	assertSamePersistedIntent(t, replayed.Intent, created.Intent)
	if got, want := filepath.Dir(first.store.dir), filepath.Join(first.root, "state"); got != want {
		t.Fatalf("outbox parent = %q, want %q", got, want)
	}
}

func TestCoordinatorReleaseAllowsCanonicalRedispatch(t *testing.T) {
	coordinator, err := OpenCoordinator(t.TempDir())
	if err != nil {
		t.Fatalf("OpenCoordinator() error = %v", err)
	}
	t.Cleanup(func() { _ = coordinator.Close() })
	identity := testIdentity()
	first, err := coordinator.AdmitMessage("/agents/main", identity, bus.OutboundMessage{Content: "first"})
	if err != nil {
		t.Fatalf("AdmitMessage(first) error = %v", err)
	}
	if releaseErr := coordinator.ReleaseAdmission(first.Lease); releaseErr != nil {
		t.Fatalf("ReleaseAdmission() error = %v", releaseErr)
	}

	identity.Channel = "slack"
	identity.ChatID = "rerouted-chat"
	replayed, err := coordinator.AdmitMessage("/agents/other", identity, bus.OutboundMessage{Content: "second"})
	if err != nil {
		t.Fatalf("AdmitMessage(replay) error = %v", err)
	}
	if !replayed.Dispatch || replayed.Intent.OwnerWorkspace != "/agents/main" ||
		replayed.Intent.Message.Content != "first" {
		t.Fatalf("released admission = %#v, want canonical redispatch", replayed)
	}
}

func TestCoordinatorRejectsSecondLiveOwnerForInstanceRoot(t *testing.T) {
	instanceRoot := t.TempDir()
	type result struct {
		coordinator *Coordinator
		err         error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	for range 2 {
		go func() {
			<-start
			coordinator, err := OpenCoordinator(instanceRoot)
			results <- result{coordinator: coordinator, err: err}
		}()
	}
	close(start)

	opened := 0
	rejected := 0
	var owner *Coordinator
	for range 2 {
		result := <-results
		if result.err != nil {
			rejected++
			continue
		}
		opened++
		owner = result.coordinator
	}
	if owner != nil {
		t.Cleanup(func() { _ = owner.Close() })
	}
	if opened != 1 || rejected != 1 {
		t.Fatalf("concurrent opens = %d accepted, %d rejected; want 1 and 1", opened, rejected)
	}
}

func TestCoordinatorStaleReleaseCannotClearNewLease(t *testing.T) {
	coordinator, err := OpenCoordinator(t.TempDir())
	if err != nil {
		t.Fatalf("OpenCoordinator() error = %v", err)
	}
	t.Cleanup(func() { _ = coordinator.Close() })
	identity := testIdentity()
	first, err := coordinator.AdmitMessage("/agents/main", identity, bus.OutboundMessage{Content: "first"})
	if err != nil {
		t.Fatalf("AdmitMessage(first) error = %v", err)
	}
	if releaseErr := coordinator.ReleaseAdmission(first.Lease); releaseErr != nil {
		t.Fatalf("ReleaseAdmission(first) error = %v", releaseErr)
	}
	second, err := coordinator.AdmitMessage("/agents/main", identity, bus.OutboundMessage{Content: "second"})
	if err != nil {
		t.Fatalf("AdmitMessage(second) error = %v", err)
	}
	if !second.Dispatch {
		t.Fatal("second admission did not reacquire dispatch")
	}

	if releaseErr := coordinator.ReleaseAdmission(first.Lease); releaseErr == nil {
		t.Fatal("stale release cleared the current dispatch lease")
	}
	third, err := coordinator.AdmitMessage("/agents/main", identity, bus.OutboundMessage{Content: "third"})
	if err != nil {
		t.Fatalf("AdmitMessage(third) error = %v", err)
	}
	if third.Dispatch {
		t.Fatal("third admission acquired dispatch while second lease remained active")
	}
}

func TestCoordinatorStaleLeaseCannotCrossReopen(t *testing.T) {
	instanceRoot := t.TempDir()
	first, err := OpenCoordinator(instanceRoot)
	if err != nil {
		t.Fatalf("OpenCoordinator(first) error = %v", err)
	}
	identity := testIdentity()
	oldAdmission, err := first.AdmitMessage("/agents/main", identity, bus.OutboundMessage{Content: "first"})
	if err != nil {
		t.Fatalf("AdmitMessage(first) error = %v", err)
	}
	if closeErr := first.Close(); closeErr != nil {
		t.Fatalf("Close(first) error = %v", closeErr)
	}

	second, err := OpenCoordinator(instanceRoot)
	if err != nil {
		t.Fatalf("OpenCoordinator(second) error = %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })
	current, err := second.AdmitMessage("/agents/main", identity, bus.OutboundMessage{Content: "second"})
	if err != nil {
		t.Fatalf("AdmitMessage(second) error = %v", err)
	}
	if !current.Dispatch {
		t.Fatal("reopened coordinator did not acquire pending dispatch")
	}
	if releaseErr := second.ReleaseAdmission(oldAdmission.Lease); releaseErr == nil {
		t.Fatal("lease from closed coordinator released current owner")
	}
	third, err := second.AdmitMessage("/agents/main", identity, bus.OutboundMessage{Content: "third"})
	if err != nil {
		t.Fatalf("AdmitMessage(third) error = %v", err)
	}
	if third.Dispatch {
		t.Fatal("third admission acquired dispatch while reopened lease remained active")
	}
}

func TestCoordinatorConcurrentReroutesHaveOneOwner(t *testing.T) {
	coordinator, err := OpenCoordinator(t.TempDir())
	if err != nil {
		t.Fatalf("OpenCoordinator() error = %v", err)
	}
	t.Cleanup(func() { _ = coordinator.Close() })

	const attempts = 16
	results := make(chan Admission, attempts)
	errs := make(chan error, attempts)
	var wg sync.WaitGroup
	for index := 0; index < attempts; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			identity := testIdentity()
			identity.Channel = "channel-" + string(rune('a'+index))
			identity.ChatID = "chat-" + string(rune('a'+index))
			admission, admitErr := coordinator.AdmitMessage(
				"/agents/"+string(rune('a'+index)),
				identity,
				bus.OutboundMessage{Content: identity.Channel},
			)
			if admitErr != nil {
				errs <- admitErr
				return
			}
			results <- admission
		}(index)
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatalf("AdmitMessage() error = %v", err)
	}

	dispatches := 0
	var canonical Intent
	for result := range results {
		if result.Dispatch {
			dispatches++
			canonical = result.Intent
		}
	}
	if dispatches != 1 {
		t.Fatalf("dispatches = %d, want 1", dispatches)
	}
	loaded, err := coordinator.store.Get(canonical.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	assertSamePersistedIntent(t, loaded, canonical)
}

func assertSamePersistedIntent(t *testing.T, got, want Intent) {
	t.Helper()
	if got.ID != want.ID || got.OwnerWorkspace != want.OwnerWorkspace || got.Identity != want.Identity ||
		got.Status != want.Status || got.Attempts != want.Attempts || got.Message == nil || want.Message == nil ||
		got.Message.Content != want.Message.Content || !got.CreatedAt.Equal(want.CreatedAt) ||
		!got.UpdatedAt.Equal(want.UpdatedAt) {
		t.Fatalf("intent = %#v, want persisted contract %#v", got, want)
	}
}
