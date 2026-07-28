package nodes

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
	closed := live
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
