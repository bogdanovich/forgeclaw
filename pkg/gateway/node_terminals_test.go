package gateway

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/fileutil"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
	nodews "github.com/bogdanovich/mintclaw/pkg/nodes/ws"
)

type fakeGatewayTerminalHandler struct {
	*fakeNodeAdmissionHandler
	registration      nodes.Registration
	approval          nodes.CommandApproval
	openMetadata      nodes.TerminalMetadata
	statusMetadata    nodes.TerminalMetadata
	terminateMetadata nodes.TerminalMetadata
	openCalls         atomic.Int32
	openWrites        atomic.Int32
	statusCalls       atomic.Int32
	terminateCalls    atomic.Int32
	afterCommit       func()
	openResultErr     error
}

func (handler *fakeGatewayTerminalHandler) WithPreparationAuthority(
	nodeID nodes.ID,
	_ string,
	command string,
	operation func(nodes.Registration, nodes.CommandApproval) error,
) (nodes.CommandApproval, error) {
	if nodeID != handler.registration.Snapshot.ID || command != "shell.exec.v1" {
		return nodes.CommandApproval{}, nodes.ErrCommandDenied
	}
	return handler.approval, operation(handler.registration, handler.approval)
}

func (handler *fakeGatewayTerminalHandler) OpenTerminal(
	_ context.Context,
	_ nodes.ID,
	_ nodes.TerminalOpenPlan,
	commit func() error,
) (nodes.TerminalMetadata, bool, error) {
	handler.openCalls.Add(1)
	if err := commit(); err != nil {
		return nodes.TerminalMetadata{}, false, err
	}
	if handler.afterCommit != nil {
		handler.afterCommit()
	}
	handler.openWrites.Add(1)
	return handler.openMetadata, true, handler.openResultErr
}

func (*fakeGatewayTerminalHandler) AttachTerminal(
	context.Context,
	nodes.ID,
	nodes.TerminalSessionRequest,
) (*nodews.TerminalStream, nodes.TerminalMetadata, error) {
	return nil, nodes.TerminalMetadata{}, errors.New("unexpected terminal attach")
}

func (handler *fakeGatewayTerminalHandler) TerminalStatus(
	context.Context,
	nodes.ID,
	nodes.TerminalSessionRequest,
) (nodes.TerminalMetadata, error) {
	handler.statusCalls.Add(1)
	return handler.statusMetadata, nil
}

func (handler *fakeGatewayTerminalHandler) TerminateTerminal(
	context.Context,
	nodes.ID,
	nodes.TerminalSessionRequest,
) (nodes.TerminalMetadata, error) {
	handler.terminateCalls.Add(1)
	return handler.terminateMetadata, nil
}

func TestNodeTerminalSourcePreparesCurrentProfileAuthority(t *testing.T) {
	source, handler, owner, nodeID := newTestNodeTerminalSource(t)
	record, created, err := source.PrepareTerminal(
		nodeID,
		"vpn-node",
		"open_test",
		"idem_test",
		owner,
		"workspace",
		80,
		24,
	)
	if err != nil || !created {
		t.Fatalf("PrepareTerminal() = (%#v, %v, %v)", record, created, err)
	}
	contract := handler.approval.Descriptor.ModelContract
	if record.Plan.NodeID != nodeID ||
		record.Plan.Owner != owner ||
		record.Plan.CatalogHash != handler.approval.CatalogHash ||
		record.Plan.AuthorityDigest != contract.AuthorityDigest ||
		record.State != nodes.GatewayTerminalPrepared {
		t.Fatalf("prepared terminal = %#v", record)
	}
	source.now = func() time.Time {
		return time.Unix(record.Plan.PreparedAt, 0).Add(2 * time.Second)
	}
	repeated, created, err := source.PrepareTerminal(
		nodeID,
		"vpn-node",
		"open_test",
		"idem_test",
		owner,
		"workspace",
		80,
		24,
	)
	if err != nil || created ||
		repeated.CreatedAt != record.CreatedAt ||
		repeated.Plan.PlanHash != record.Plan.PlanHash ||
		repeated.Plan.PreparedAt != record.Plan.PreparedAt {
		t.Fatalf("repeated preparation = (%#v, %v, %v)", repeated, created, err)
	}
	if _, _, err := source.PrepareTerminal(
		nodeID,
		"vpn-node",
		"open_test",
		"idem_test",
		owner,
		"workspace",
		81,
		24,
	); !errors.Is(err, nodes.ErrGatewayTerminalConflict) {
		t.Fatalf("changed repeated preparation error = %v", err)
	}
}

func TestNodeTerminalSourceTerminatesCommittedOpenWarning(t *testing.T) {
	source, handler, owner, nodeID := newTestNodeTerminalSource(t)
	record, _, err := source.PrepareTerminal(
		nodeID,
		"vpn-node",
		"open_warning",
		"idem_warning",
		owner,
		"workspace",
		80,
		24,
	)
	if err != nil {
		t.Fatal(err)
	}
	handler.openMetadata = nodes.TerminalMetadata{
		TerminalID: "terminal_warning",
		Owner:      owner,
		State:      string(nodes.GatewayTerminalPendingAttach),
		StartedAt:  source.now().Unix(),
	}
	handler.openResultErr = &fileutil.CommittedWriteError{Err: errors.New("directory sync failed")}
	handler.terminateMetadata = nodes.TerminalMetadata{
		TerminalID:           handler.openMetadata.TerminalID,
		Owner:                owner,
		State:                string(nodes.GatewayTerminalClosed),
		Reason:               "close",
		StartedAt:            handler.openMetadata.StartedAt,
		CompletedAt:          handler.openMetadata.StartedAt + 1,
		TerminationConfirmed: true,
	}
	if _, dispatched, openErr := source.OpenTerminal(
		t.Context(),
		owner,
		record.Plan.OpenID,
		record.ExpectedPlanHash,
	); !dispatched || !fileutil.IsCommittedWriteError(openErr) {
		t.Fatalf("committed warning open = (dispatched %v, error %v)", dispatched, openErr)
	}
	if handler.terminateCalls.Load() != 1 {
		t.Fatalf("terminate calls = %d, want 1", handler.terminateCalls.Load())
	}
	retained, found, lookupErr := source.store.Lookup(owner, handler.openMetadata.TerminalID)
	if lookupErr != nil || !found || retained.State != nodes.GatewayTerminalClosed {
		t.Fatalf("retained cleanup = (%#v, %v, %v)", retained, found, lookupErr)
	}
}

func TestNodeTerminalSourceDeniesUnapprovedProfileBeforePersistence(t *testing.T) {
	source, _, owner, nodeID := newTestNodeTerminalSource(t)
	owner.Profile = "unapproved"
	if _, created, err := source.PrepareTerminal(
		nodeID,
		"vpn-node",
		"open_denied",
		"idem_denied",
		owner,
		"workspace",
		80,
		24,
	); err == nil || created {
		t.Fatalf("unapproved profile preparation = (%v, %v)", created, err)
	}
	if _, found, err := source.store.Lookup(owner, "open_denied"); err != nil || found {
		t.Fatalf("denied authority persisted = (%v, %v)", found, err)
	}
}

func TestNodeTerminalSourceDeniesRelaxedApprovalBeforePersistence(t *testing.T) {
	source, handler, owner, nodeID := newTestNodeTerminalSource(t)
	handler.approval.Descriptor.ModelContract.ApprovalMode = "session_start"
	if _, created, err := source.PrepareTerminal(
		nodeID,
		"vpn-node",
		"open_relaxed",
		"idem_relaxed",
		owner,
		"workspace",
		80,
		24,
	); !errors.Is(err, nodes.ErrCommandDenied) || created {
		t.Fatalf("relaxed approval preparation = (%v, %v)", created, err)
	}
	if _, found, err := source.store.Lookup(owner, "open_relaxed"); err != nil || found {
		t.Fatalf("relaxed approval persisted = (%v, %v)", found, err)
	}
}

func TestNodeTerminalSourceCommitsBeforeOpenAndPersistsLifecycle(t *testing.T) {
	source, handler, owner, nodeID := newTestNodeTerminalSource(t)
	record, _, err := source.PrepareTerminal(
		nodeID,
		"vpn-node",
		"open_dispatch",
		"idem_dispatch",
		owner,
		"workspace",
		80,
		24,
	)
	if err != nil {
		t.Fatal(err)
	}
	handler.openMetadata = nodes.TerminalMetadata{
		TerminalID: "terminal_dispatch",
		Owner:      owner,
		State:      string(nodes.GatewayTerminalPendingAttach),
		StartedAt:  source.now().Unix(),
	}
	handler.afterCommit = func() {
		retained, found, lookupErr := source.store.Lookup(owner, record.Plan.OpenID)
		if lookupErr != nil || !found || retained.State != nodes.GatewayTerminalDispatched {
			t.Errorf("pre-write durable state = (%#v, %v, %v)", retained, found, lookupErr)
		}
	}
	metadata, dispatched, err := source.OpenTerminal(
		t.Context(),
		owner,
		record.Plan.OpenID,
		record.ExpectedPlanHash,
	)
	if err != nil || !dispatched || metadata.TerminalID != "terminal_dispatch" {
		t.Fatalf("OpenTerminal() = (%#v, %v, %v)", metadata, dispatched, err)
	}
	retained, found, err := source.store.Lookup(owner, metadata.TerminalID)
	if err != nil || !found ||
		retained.State != nodes.GatewayTerminalPendingAttach ||
		handler.openWrites.Load() != 1 {
		t.Fatalf("retained opened terminal = (%#v, %v, %v)", retained, found, err)
	}
	wrongOwner := owner
	wrongOwner.RouteID = "route_other"
	if _, _, err := source.OpenTerminal(
		t.Context(),
		wrongOwner,
		record.Plan.OpenID,
		record.ExpectedPlanHash,
	); !errors.Is(err, nodes.ErrGatewayTerminalConflict) {
		t.Fatalf("wrong-owner open error = %v", err)
	}
	if handler.openWrites.Load() != 1 {
		t.Fatalf("wrong owner dispatched %d writes", handler.openWrites.Load())
	}

	handler.statusMetadata = nodes.TerminalMetadata{
		TerminalID:           metadata.TerminalID,
		Owner:                owner,
		State:                string(nodes.GatewayTerminalClosed),
		Reason:               "exit",
		StartedAt:            metadata.StartedAt,
		CompletedAt:          metadata.StartedAt + 1,
		TerminationConfirmed: true,
	}
	status, err := source.TerminalStatus(t.Context(), owner, metadata.TerminalID)
	if err != nil || status.State != string(nodes.GatewayTerminalClosed) {
		t.Fatalf("TerminalStatus() = (%#v, %v)", status, err)
	}
	retained, found, err = source.store.Lookup(owner, metadata.TerminalID)
	if err != nil || !found || retained.State != nodes.GatewayTerminalClosed {
		t.Fatalf("retained terminal status = (%#v, %v, %v)", retained, found, err)
	}
}

func TestNodeTerminalSourceTerminatesWhenOpenedMetadataCannotPersist(t *testing.T) {
	source, handler, owner, nodeID := newTestNodeTerminalSource(t)
	record, _, err := source.PrepareTerminal(
		nodeID,
		"vpn-node",
		"open_cleanup",
		"idem_cleanup",
		owner,
		"workspace",
		80,
		24,
	)
	if err != nil {
		t.Fatal(err)
	}
	measurePath := filepath.Join(t.TempDir(), "measure.json")
	measure, err := nodes.NewGatewayTerminalStore(measurePath, 8, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := measure.Prepare(record.Plan); err != nil {
		t.Fatal(err)
	}
	if _, _, err := measure.MarkDispatched(
		owner,
		record.Plan.OpenID,
		record.ExpectedPlanHash,
	); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(measurePath)
	if err != nil {
		t.Fatal(err)
	}
	boundedPath := filepath.Join(t.TempDir(), "bounded.json")
	initial, err := nodes.NewGatewayTerminalStore(boundedPath, 8, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := initial.Prepare(record.Plan); err != nil {
		t.Fatal(err)
	}
	bounded, err := nodes.NewGatewayTerminalStore(
		boundedPath,
		8,
		int(info.Size()),
	)
	if err != nil {
		t.Fatal(err)
	}
	source.store = bounded
	handler.openMetadata = nodes.TerminalMetadata{
		TerminalID: "terminal_cleanup",
		Owner:      owner,
		State:      string(nodes.GatewayTerminalPendingAttach),
		StartedAt:  source.now().Unix(),
	}
	handler.terminateMetadata = nodes.TerminalMetadata{
		TerminalID:           handler.openMetadata.TerminalID,
		Owner:                owner,
		State:                string(nodes.GatewayTerminalClosed),
		Reason:               "close",
		StartedAt:            handler.openMetadata.StartedAt,
		CompletedAt:          handler.openMetadata.StartedAt + 1,
		TerminationConfirmed: true,
	}
	if _, dispatched, err := source.OpenTerminal(
		t.Context(),
		owner,
		record.Plan.OpenID,
		record.ExpectedPlanHash,
	); err == nil || !dispatched {
		t.Fatalf("unretained open = (dispatched %v, error %v)", dispatched, err)
	}
	if handler.openWrites.Load() != 1 || handler.terminateCalls.Load() != 1 {
		t.Fatalf(
			"fail-closed calls = writes %d, terminates %d",
			handler.openWrites.Load(),
			handler.terminateCalls.Load(),
		)
	}
	retained, found, err := source.store.Lookup(owner, record.Plan.OpenID)
	if err != nil || !found || retained.State != nodes.GatewayTerminalDispatched {
		t.Fatalf("failed-open durable state = (%#v, %v, %v)", retained, found, err)
	}
}

func TestNewNodeTerminalSourceIsDisabledByDefault(t *testing.T) {
	cfg := &config.Config{}
	cfg.Nodes.Enabled = true
	if source, err := newNodeTerminalSource(cfg, nil); err != nil || source != nil {
		t.Fatalf("default terminal source = (%#v, %v)", source, err)
	}
}

func newTestNodeTerminalSource(
	t *testing.T,
) (*nodeTerminalSource, *fakeGatewayTerminalHandler, nodes.TerminalOwner, nodes.ID) {
	t.Helper()
	nodeID := nodes.ID("node_test")
	owner := nodes.TerminalOwner{
		ActorID: "actor_test", AgentID: "agent_test", RouteID: "route_test",
		SessionID: "session_test", WorkspaceID: "workspace_test",
		Target: "vpn", Profile: "owner",
	}
	contract := &nodes.CommandModelContract{
		Availability:    nodes.ModelAvailable,
		AuthorityDigest: testDigest("b"),
		ApprovalMode:    "each_command",
		Constraints: nodes.CommandModelConstraints{
			ProfileAliases: []string{"owner"},
			WorkingScopes:  []string{"workspace"},
		},
	}
	handler := &fakeGatewayTerminalHandler{
		fakeNodeAdmissionHandler: &fakeNodeAdmissionHandler{},
		registration: nodes.Registration{
			Snapshot: nodes.Snapshot{ID: nodeID, State: nodes.StateConnected},
		},
		approval: nodes.CommandApproval{
			Descriptor: nodes.CommandDescriptor{
				Name:          "shell.exec.v1",
				Risk:          nodes.RiskPrivileged,
				ModelContract: contract,
			},
			CatalogHash: testDigest("a"),
		},
	}
	registryPath := t.TempDir() + "/registry.json"
	runtime := &nodeAdmissionRuntime{
		registryPath: registryPath,
		handler:      handler,
		generation:   1,
		mounted:      true,
	}
	store, err := nodes.NewGatewayTerminalStore(
		nodes.GatewayTerminalStorePath(t.TempDir()),
		8,
		1024*1024,
	)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	source := &nodeTerminalSource{
		nodeDiscoverySource: nodeDiscoverySource{
			runtime: runtime, registryPath: registryPath,
		},
		store: store, generation: runtime.generation,
		now: func() time.Time {
			return now
		},
	}
	return source, handler, owner, nodeID
}

func testDigest(character string) string {
	return strings.Repeat(character, 64)
}
