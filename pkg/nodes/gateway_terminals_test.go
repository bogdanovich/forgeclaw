package nodes

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/fileutil"
	"github.com/bogdanovich/mintclaw/pkg/nodes/internal/jsonstrict"
)

func TestGatewayTerminalStorePersistsImmutableOwnerBoundPlan(t *testing.T) {
	now := time.Now()
	path := filepath.Join(t.TempDir(), "state", "node_terminals.json")
	store, err := NewGatewayTerminalStore(path, 8, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	plan := gatewayTerminalTestPlan(t, "open_persist", "idem_persist", now)
	record, created, err := store.Prepare(plan)
	if err != nil || !created || record.ExpectedPlanHash != plan.PlanHash {
		t.Fatalf("Prepare() = (%#v, %v, %v)", record, created, err)
	}
	repeated, created, err := store.Prepare(plan)
	if err != nil || created || repeated.CreatedAt != record.CreatedAt {
		t.Fatalf("repeated Prepare() = (%#v, %v, %v)", repeated, created, err)
	}
	reloaded, err := NewGatewayTerminalStore(path, 8, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := reloaded.Lookup(plan.Owner, plan.OpenID)
	if err != nil || !found || got.Plan != plan {
		t.Fatalf("reloaded record = (%#v, %v, %v)", got, found, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("store mode = %o", info.Mode().Perm())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"input_base64", "data_base64", "transcript"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("terminal store retained forbidden field %q", forbidden)
		}
	}
}

func TestOpenExistingGatewayTerminalStoreRejectsHardLinkedLeaf(t *testing.T) {
	targetPath := filepath.Join(t.TempDir(), "target", "node_terminals.json")
	target, err := NewGatewayTerminalStore(targetPath, 8, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	plan := gatewayTerminalTestPlan(t, "open_hard_link", "idem_hard_link", time.Now())
	if _, _, err := target.Prepare(plan); err != nil {
		t.Fatal(err)
	}
	if _, _, err := target.MarkDispatched(plan.Owner, plan.OpenID, plan.PlanHash); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	linkedPath := filepath.Join(t.TempDir(), "state", "node_terminals.json")
	if err := os.MkdirAll(filepath.Dir(linkedPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(targetPath, linkedPath); err != nil {
		t.Skipf("create terminal store hard link: %v", err)
	}
	if store, found, err := OpenExistingGatewayTerminalStore(linkedPath, 8, 1024*1024); err == nil ||
		found ||
		store != nil {
		t.Fatalf("hard-linked terminal store = (%#v, %v, %v)", store, found, err)
	}
	after, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatal("target terminal store changed through hard link")
	}
}

func TestGatewayTerminalStoreRejectsRebindingAndBearerOnlyLookup(t *testing.T) {
	store := newGatewayTerminalStore("", 8, 1024*1024, time.Now)
	plan := gatewayTerminalTestPlan(t, "open_owner", "idem_owner", time.Now())
	if _, _, err := store.Prepare(plan); err != nil {
		t.Fatal(err)
	}
	rebound := gatewayTerminalTestPlan(t, "open_other", plan.IdempotencyKey, time.Now())
	if _, _, err := store.Prepare(rebound); !errors.Is(err, ErrGatewayTerminalConflict) {
		t.Fatalf("idempotency rebinding error = %v", err)
	}
	wrongOwner := plan.Owner
	wrongOwner.RouteID = "route_other"
	if _, found, err := store.Lookup(wrongOwner, plan.OpenID); err != nil || found {
		t.Fatalf("wrong-route lookup = (%v, %v)", found, err)
	}
	if _, found, err := store.Lookup(TerminalOwner{}, plan.OpenID); err == nil || found {
		t.Fatalf("bearer-only lookup = (%v, %v)", found, err)
	}
}

func TestGatewayTerminalStoreRejectsCrossNamespaceIdentityCollisions(t *testing.T) {
	now := time.Now()
	store := newGatewayTerminalStore("", 8, 1024*1024, func() time.Time { return now })
	first := gatewayTerminalTestPlan(t, "open_first", "idem_first", now)
	second := gatewayTerminalTestPlan(t, "open_second", "idem_second", now)
	for _, plan := range []TerminalOpenPlan{first, second} {
		if _, _, err := store.Prepare(plan); err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.MarkDispatched(
			plan.Owner,
			plan.OpenID,
			plan.PlanHash,
		); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := store.RecordOpened(first.Owner, first.OpenID, TerminalMetadata{
		TerminalID: second.OpenID,
		Owner:      first.Owner,
		State:      string(GatewayTerminalPendingAttach),
		StartedAt:  now.Unix(),
	}); !errors.Is(err, ErrGatewayTerminalConflict) {
		t.Fatalf("terminal/open collision error = %v", err)
	}
	firstMetadata := TerminalMetadata{
		TerminalID: "terminal_shared",
		Owner:      first.Owner,
		State:      string(GatewayTerminalPendingAttach),
		StartedAt:  now.Unix(),
	}
	if _, _, err := store.RecordOpened(first.Owner, first.OpenID, firstMetadata); err != nil {
		t.Fatal(err)
	}
	secondMetadata := firstMetadata
	secondMetadata.Owner = second.Owner
	if _, _, err := store.RecordOpened(
		second.Owner,
		second.OpenID,
		secondMetadata,
	); !errors.Is(err, ErrGatewayTerminalConflict) {
		t.Fatalf("duplicate terminal identity error = %v", err)
	}
	collidingPlan := gatewayTerminalTestPlan(
		t,
		firstMetadata.TerminalID,
		"idem_collision",
		now,
	)
	if _, _, err := store.Prepare(collidingPlan); !errors.Is(err, ErrGatewayTerminalConflict) {
		t.Fatalf("open/terminal collision error = %v", err)
	}
}

func TestGatewayTerminalStoreTracksRedactedLifecycle(t *testing.T) {
	now := time.Now()
	store := newGatewayTerminalStore("", 8, 1024*1024, func() time.Time { return now })
	plan := gatewayTerminalTestPlan(t, "open_lifecycle", "idem_lifecycle", now)
	if _, _, err := store.Prepare(plan); err != nil {
		t.Fatal(err)
	}
	if _, transitioned, err := store.MarkDispatched(
		plan.Owner,
		plan.OpenID,
		plan.PlanHash,
	); err != nil || !transitioned {
		t.Fatalf("MarkDispatched() = (%v, %v)", transitioned, err)
	}
	opened := TerminalMetadata{
		TerminalID: "terminal_lifecycle",
		Owner:      plan.Owner,
		State:      string(GatewayTerminalPendingAttach),
		StartedAt:  now.Unix(),
	}
	if _, transitioned, err := store.RecordOpened(
		plan.Owner,
		plan.OpenID,
		opened,
	); err != nil || !transitioned {
		t.Fatalf("RecordOpened() = (%v, %v)", transitioned, err)
	}
	live := opened
	live.State = string(GatewayTerminalLive)
	if _, transitioned, err := store.RecordLifecycle(
		plan.Owner,
		opened.TerminalID,
		live,
	); err != nil || !transitioned {
		t.Fatalf("live RecordLifecycle() = (%v, %v)", transitioned, err)
	}
	closing := live
	closing.State = string(GatewayTerminalClosing)
	closing.Reason = "close"
	if _, transitioned, err := store.RecordLifecycle(
		plan.Owner,
		opened.TerminalID,
		closing,
	); err != nil || !transitioned {
		t.Fatalf("closing RecordLifecycle() = (%v, %v)", transitioned, err)
	}
	closed := closing
	closed.State = string(GatewayTerminalClosed)
	closed.Reason = "exit"
	closed.CompletedAt = now.Add(time.Second).Unix()
	closed.ExitCode = 7
	closed.TerminationConfirmed = true
	record, transitioned, err := store.RecordLifecycle(plan.Owner, opened.TerminalID, closed)
	if err != nil || !transitioned || record.ExitCode != 7 ||
		record.State != GatewayTerminalClosed {
		t.Fatalf("closed RecordLifecycle() = (%#v, %v, %v)", record, transitioned, err)
	}
	repeated, transitioned, err := store.RecordLifecycle(plan.Owner, opened.TerminalID, closed)
	if err != nil || transitioned || repeated.UpdatedAt != record.UpdatedAt {
		t.Fatalf("repeated lifecycle = (%#v, %v, %v)", repeated, transitioned, err)
	}
	now = now.Add(DefaultGatewayTerminalRetention + time.Second)
	if _, found, err := store.Lookup(plan.Owner, opened.TerminalID); err != nil || found {
		t.Fatalf("expired lifecycle lookup = (%v, %v)", found, err)
	}
}

func TestGatewayTerminalStoreAcceptsUnknownWithoutCompletionTime(t *testing.T) {
	now := time.Now()
	path := filepath.Join(t.TempDir(), "node_terminals.json")
	store := newGatewayTerminalStore(path, 8, 1024*1024, func() time.Time { return now })
	plan := gatewayTerminalTestPlan(t, "open_unknown", "idem_unknown", now)
	if _, _, err := store.Prepare(plan); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.MarkDispatched(plan.Owner, plan.OpenID, plan.PlanHash); err != nil {
		t.Fatal(err)
	}
	opened := TerminalMetadata{
		TerminalID: "terminal_unknown",
		Owner:      plan.Owner,
		State:      string(GatewayTerminalPendingAttach),
		StartedAt:  now.Unix(),
	}
	if _, _, err := store.RecordOpened(plan.Owner, plan.OpenID, opened); err != nil {
		t.Fatal(err)
	}
	unknown := opened
	unknown.State = string(GatewayTerminalUnknown)
	unknown.Reason = "transport_unknown"
	record, transitioned, err := store.RecordLifecycle(plan.Owner, opened.TerminalID, unknown)
	if err != nil || !transitioned || record.CompletedAt != 0 {
		t.Fatalf("unknown RecordLifecycle() = (%#v, %v, %v)", record, transitioned, err)
	}
	reloaded, err := NewGatewayTerminalStore(path, 8, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	retained, found, err := reloaded.Lookup(plan.Owner, opened.TerminalID)
	if err != nil || !found ||
		retained.State != GatewayTerminalUnknown ||
		retained.CompletedAt != 0 {
		t.Fatalf("reloaded unknown = (%#v, %v, %v)", retained, found, err)
	}
}

func TestGatewayTerminalStoreRestartFailsActiveSessionsClosed(t *testing.T) {
	now := time.Now()
	path := filepath.Join(t.TempDir(), "node_terminals.json")
	store := newGatewayTerminalStore(path, 8, 1024*1024, func() time.Time { return now })
	plan := gatewayTerminalTestPlan(t, "open_restart", "idem_restart", now)
	if _, _, err := store.Prepare(plan); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.MarkDispatched(plan.Owner, plan.OpenID, plan.PlanHash); err != nil {
		t.Fatal(err)
	}
	opened := TerminalMetadata{
		TerminalID: "terminal_restart",
		Owner:      plan.Owner,
		State:      string(GatewayTerminalPendingAttach),
		StartedAt:  now.Unix(),
	}
	if _, _, err := store.RecordOpened(plan.Owner, plan.OpenID, opened); err != nil {
		t.Fatal(err)
	}
	reloaded, err := NewGatewayTerminalStore(path, 8, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	record, found, err := reloaded.Lookup(plan.Owner, opened.TerminalID)
	if err != nil || !found ||
		record.State != GatewayTerminalUnknown ||
		record.Reason != "gateway_restarted" ||
		record.TerminationConfirmed {
		t.Fatalf("recovered terminal = (%#v, %v, %v)", record, found, err)
	}
}

func TestGatewayTerminalStoreReconcilesShutdownAndPersists(t *testing.T) {
	now := time.Now()
	path := filepath.Join(t.TempDir(), "node_terminals.json")
	store := newGatewayTerminalStore(path, 8, 1024*1024, func() time.Time { return now })
	plan := gatewayTerminalTestPlan(t, "open_shutdown", "idem_shutdown", now)
	if _, _, err := store.Prepare(plan); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.MarkDispatched(plan.Owner, plan.OpenID, plan.PlanHash); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileShutdown(); err != nil {
		t.Fatal(err)
	}
	record, found, err := store.Lookup(plan.Owner, plan.OpenID)
	if err != nil || !found ||
		record.State != GatewayTerminalUnknown ||
		record.Reason != gatewayTerminalShutdownReason ||
		record.TerminationConfirmed {
		t.Fatalf("shutdown terminal = (%#v, %v, %v)", record, found, err)
	}
	reloaded, err := NewGatewayTerminalStore(path, 8, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	retained, found, err := reloaded.Lookup(plan.Owner, plan.OpenID)
	if err != nil || !found ||
		retained.State != GatewayTerminalUnknown ||
		retained.Reason != gatewayTerminalShutdownReason {
		t.Fatalf("reloaded shutdown terminal = (%#v, %v, %v)", retained, found, err)
	}
}

func TestGatewayTerminalStoreRetriesFailedShutdownPersistence(t *testing.T) {
	now := time.Now()
	path := filepath.Join(t.TempDir(), "node_terminals.json")
	store := newGatewayTerminalStore(path, 8, 1024*1024, func() time.Time { return now })
	plan := gatewayTerminalTestPlan(t, "open_shutdown_retry", "idem_shutdown_retry", now)
	if _, _, err := store.Prepare(plan); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.MarkDispatched(plan.Owner, plan.OpenID, plan.PlanHash); err != nil {
		t.Fatal(err)
	}
	writeFile := store.writeFile
	attempts := 0
	store.writeFile = func(
		directory *anchoredDirectory,
		name string,
		data []byte,
		mode os.FileMode,
	) error {
		attempts++
		if attempts == 1 {
			return &fileutil.CommittedWriteError{Err: errors.New("directory sync failed")}
		}
		return writeFile(directory, name, data, mode)
	}
	if err := store.ReconcileShutdown(); !fileutil.IsCommittedWriteError(err) {
		t.Fatalf("first ReconcileShutdown() error = %v", err)
	}
	if err := store.ReconcileShutdown(); err != nil {
		t.Fatalf("retry ReconcileShutdown() error = %v", err)
	}
	if attempts != 2 {
		t.Fatalf("shutdown persistence attempts = %d, want 2", attempts)
	}
	record, found, err := store.Lookup(plan.Owner, plan.OpenID)
	if err != nil || !found ||
		record.State != GatewayTerminalUnknown ||
		record.Reason != gatewayTerminalShutdownReason {
		t.Fatalf("retried shutdown terminal = (%#v, %v, %v)", record, found, err)
	}
}

func TestGatewayTerminalStoreRestartClampsSkewedCompletionTime(t *testing.T) {
	now := time.Now()
	path := filepath.Join(t.TempDir(), "node_terminals.json")
	store := newGatewayTerminalStore(path, 8, 1024*1024, func() time.Time { return now })
	plan := gatewayTerminalTestPlan(t, "open_skew", "idem_skew", now)
	if _, _, err := store.Prepare(plan); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.MarkDispatched(plan.Owner, plan.OpenID, plan.PlanHash); err != nil {
		t.Fatal(err)
	}
	startedAt := now.Add(time.Hour).Unix()
	if _, _, err := store.RecordOpened(plan.Owner, plan.OpenID, TerminalMetadata{
		TerminalID: "terminal_skew",
		Owner:      plan.Owner,
		State:      string(GatewayTerminalPendingAttach),
		StartedAt:  startedAt,
	}); err != nil {
		t.Fatal(err)
	}
	reloaded, err := NewGatewayTerminalStore(path, 8, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	record, found, err := reloaded.Lookup(plan.Owner, "terminal_skew")
	if err != nil || !found ||
		record.State != GatewayTerminalUnknown ||
		record.CompletedAt != startedAt {
		t.Fatalf("skew-recovered terminal = (%#v, %v, %v)", record, found, err)
	}
}

func TestGatewayTerminalStoreEnforcesBoundsAndRejectsCorruption(t *testing.T) {
	now := time.Now()
	store := newGatewayTerminalStore("", 1, 1024*1024, func() time.Time { return now })
	first := gatewayTerminalTestPlan(t, "open_first", "idem_first", now)
	if _, _, err := store.Prepare(first); err != nil {
		t.Fatal(err)
	}
	second := gatewayTerminalTestPlan(t, "open_second", "idem_second", now)
	if _, _, err := store.Prepare(second); !errors.Is(err, ErrGatewayTerminalStoreFull) {
		t.Fatalf("record bound error = %v", err)
	}
	byteBounded := newGatewayTerminalStore("", 8, 128, func() time.Time { return now })
	if _, created, err := byteBounded.Prepare(first); !errors.Is(
		err,
		ErrGatewayTerminalStoreFull,
	) || created {
		t.Fatalf("byte bound prepare = (%v, %v)", created, err)
	}

	path := filepath.Join(t.TempDir(), "node_terminals.json")
	validRecord := GatewayTerminalRecord{
		Plan:             first,
		ExpectedPlanHash: first.PlanHash,
		State:            GatewayTerminalPrepared,
		CreatedAt:        now.UnixNano(),
		UpdatedAt:        now.UnixNano(),
	}
	record := validRecord
	record.Plan.Owner.RouteID = "mutated"
	data, err := json.Marshal(gatewayTerminalDocument{
		Version: gatewayTerminalStoreVersion,
		Records: map[string]GatewayTerminalRecord{first.OpenID: record},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewGatewayTerminalStore(path, 8, 1024*1024); err == nil {
		t.Fatal("mutated persisted terminal plan was accepted")
	}
	validRecord.Reason = "retained_output"
	data, err = json.Marshal(gatewayTerminalDocument{
		Version: gatewayTerminalStoreVersion,
		Records: map[string]GatewayTerminalRecord{first.OpenID: validRecord},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewGatewayTerminalStore(path, 8, 1024*1024); err == nil {
		t.Fatal("prepared terminal with lifecycle metadata was accepted")
	}
	validRecord.Reason = ""
	data, err = json.Marshal(gatewayTerminalDocument{
		Version: gatewayTerminalStoreVersion,
		Records: map[string]GatewayTerminalRecord{first.OpenID: validRecord},
	})
	if err != nil {
		t.Fatal(err)
	}
	duplicatedOwner := strings.Replace(
		string(data),
		`"actor_id":"actor_test"`,
		`"actor_id":"shadowed","actor_id":"actor_test"`,
		1,
	)
	if err := os.WriteFile(path, []byte(duplicatedOwner), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewGatewayTerminalStore(path, 8, 1024*1024); !errors.Is(
		err,
		jsonstrict.ErrDuplicateMember,
	) {
		t.Fatalf("duplicate nested owner error = %v", err)
	}
}

func gatewayTerminalTestPlan(
	t *testing.T,
	openID string,
	idempotencyKey string,
	preparedAt time.Time,
) TerminalOpenPlan {
	t.Helper()
	plan, err := PrepareTerminalOpenPlan(TerminalOpenPlan{
		OpenID:          openID,
		IdempotencyKey:  idempotencyKey,
		NodeID:          ID("node_test"),
		Owner:           testTerminalOwner(),
		CatalogHash:     strings.Repeat("a", 64),
		AuthorityDigest: strings.Repeat("b", 64),
		WorkingScope:    "workspace",
		Columns:         80,
		Rows:            24,
		ApprovalMode:    "session_start",
	}, preparedAt, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}
