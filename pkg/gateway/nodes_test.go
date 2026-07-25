package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/nodes"
)

type fakeNodeAdmissionRoutes struct {
	handler         http.Handler
	registerCount   int
	replaceCount    int
	unregisterCount int
}

func TestNodeAdmissionWorkspaceChangeFailsClosed(t *testing.T) {
	routes := &fakeNodeAdmissionRoutes{}
	runtime := &nodeAdmissionRuntime{routes: routes}
	cfg := config.DefaultConfig()
	cfg.Nodes.Enabled = true
	cfg.Agents.Defaults.Workspace = t.TempDir()
	if err := runtime.Reconcile(cfg); err != nil {
		t.Fatal(err)
	}

	badWorkspace := t.TempDir()
	if err := os.Mkdir(filepath.Join(badWorkspace, "state"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(badWorkspace, "state", "nodes"),
		[]byte("not a directory"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	cfg.Agents.Defaults.Workspace = badWorkspace
	if err := runtime.Reconcile(cfg); err == nil {
		t.Fatal("workspace change accepted an unreadable replacement registry")
	}
	if runtime.mounted || runtime.registry != nil || runtime.sessions != nil || routes.handler != nil {
		t.Fatal("failed workspace change retained the previous node authority domain")
	}
}

func TestNodeAdmissionWorkspaceChangeWaitsForSuccessfulDrain(t *testing.T) {
	routes := &fakeNodeAdmissionRoutes{}
	runtime := &nodeAdmissionRuntime{routes: routes}
	cfg := config.DefaultConfig()
	cfg.Nodes.Enabled = true
	cfg.Agents.Defaults.Workspace = t.TempDir()
	if err := runtime.Reconcile(cfg); err != nil {
		t.Fatal(err)
	}
	oldRegistryPath := runtime.registryPath
	oldSource := &nodeDiscoverySource{runtime: runtime, registryPath: oldRegistryPath}

	disconnectCalls := 0
	release, err := runtime.sessions.Claim(
		nodes.ID("node_test"),
		&testNodeConnection{},
		nil,
		func() error {
			disconnectCalls++
			if disconnectCalls < 3 {
				return errors.New("registry unavailable")
			}
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := release(); err == nil {
		t.Fatal("initial disconnect unexpectedly succeeded")
	}

	cfg.Agents.Defaults.Workspace = t.TempDir()
	if err := runtime.Reconcile(cfg); err == nil {
		t.Fatal("workspace change ignored failed node drain")
	}
	if runtime.handler == nil || runtime.registryPath != oldRegistryPath || runtime.mounted || routes.handler != nil {
		t.Fatal("failed drain discarded the closing authority runtime")
	}
	if _, _, lookupErr := oldSource.Lookup("node_test"); !errors.Is(
		lookupErr,
		errNodeDiscoveryAuthorityUnavailable,
	) {
		t.Fatalf("failed drain left old discovery authority readable: %v", lookupErr)
	}
	if err := runtime.Reconcile(cfg); err != nil {
		t.Fatalf("workspace change did not recover after drain retry: %v", err)
	}
	if !runtime.mounted || runtime.registryPath == oldRegistryPath || routes.handler == nil {
		t.Fatal("successful retry did not mount the replacement authority runtime")
	}
}

type testNodeConnection struct{}

func (*testNodeConnection) Close() error { return nil }

func TestNodeDiscoverySourceBindsWorkspaceAuthority(t *testing.T) {
	oldPath := filepath.Join(t.TempDir(), "old", "registry.json")
	registry, err := nodes.NewFileRegistry(oldPath, 4)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &nodeAdmissionRuntime{
		registry:     registry,
		registryPath: oldPath,
		mounted:      true,
	}
	oldSource := &nodeDiscoverySource{runtime: runtime, registryPath: oldPath}
	if _, found, lookupErr := oldSource.Lookup("missing"); lookupErr != nil || found {
		t.Fatalf("active authority lookup = found %v, error %v", found, lookupErr)
	}

	newSource := &nodeDiscoverySource{
		runtime:      runtime,
		registryPath: filepath.Join(t.TempDir(), "new", "registry.json"),
	}
	if _, _, lookupErr := newSource.Lookup("missing"); !errors.Is(
		lookupErr,
		errNodeDiscoveryAuthorityUnavailable,
	) {
		t.Fatalf("cross-workspace lookup error = %v", lookupErr)
	}

	runtime.registryMu.Lock()
	runtime.mounted = false
	runtime.registryMu.Unlock()
	if _, _, lookupErr := oldSource.Lookup("missing"); !errors.Is(
		lookupErr,
		errNodeDiscoveryAuthorityUnavailable,
	) {
		t.Fatalf("inactive authority lookup error = %v", lookupErr)
	}
}

func TestNodeInvocationSourceRejectsStaleRuntimeGeneration(t *testing.T) {
	routes := &fakeNodeAdmissionRoutes{}
	runtime := &nodeAdmissionRuntime{routes: routes}
	cfg := config.DefaultConfig()
	cfg.Nodes.Enabled = true
	cfg.Agents.Defaults.Workspace = t.TempDir()
	if err := runtime.Reconcile(cfg); err != nil {
		t.Fatal(err)
	}
	store, err := nodes.NewGatewayInvocationStore(
		nodes.GatewayInvocationStorePath(cfg.WorkspacePath()),
		8,
		1024*1024,
	)
	if err != nil {
		t.Fatal(err)
	}
	source := &nodeInvocationSource{
		nodeDiscoverySource: nodeDiscoverySource{
			runtime: runtime, registryPath: runtime.registryPath,
		},
		store: store, generation: runtime.invocationGeneration(),
	}
	if err := runtime.Reconcile(cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := source.PrepareInvocation(
		"build",
		"call_1",
		nodes.ExecutionPlan{},
		nodes.CommandDescriptor{},
	); !errors.Is(err, errNodeDiscoveryAuthorityUnavailable) {
		t.Fatalf("stale prepare error = %v", err)
	}
	if _, dispatched, err := source.DispatchInvocation(
		context.Background(),
		nodes.GatewayInvocationOwner{},
		"inv_1",
		"plan_1",
	); !errors.Is(err, errNodeDiscoveryAuthorityUnavailable) || dispatched {
		t.Fatalf("stale dispatch = (dispatched %v, error %v)", dispatched, err)
	}
}

func TestNodeInvocationSourceRejectsExplicitlyInvalidatedGeneration(t *testing.T) {
	routes := &fakeNodeAdmissionRoutes{}
	runtime := &nodeAdmissionRuntime{routes: routes}
	cfg := config.DefaultConfig()
	cfg.Nodes.Enabled = true
	cfg.Agents.Defaults.Workspace = t.TempDir()
	if err := runtime.Reconcile(cfg); err != nil {
		t.Fatal(err)
	}
	source := &nodeInvocationSource{
		nodeDiscoverySource: nodeDiscoverySource{
			runtime: runtime, registryPath: runtime.registryPath,
		},
		generation: runtime.invocationGeneration(),
	}

	runtime.invalidateInvocationAuthority()

	if _, err := source.PrepareInvocation(
		"build",
		"call_1",
		nodes.ExecutionPlan{},
		nodes.CommandDescriptor{},
	); !errors.Is(err, errNodeDiscoveryAuthorityUnavailable) {
		t.Fatalf("invalidated prepare error = %v", err)
	}
}

func TestVerifyRemoteInvocationRejectsAuthorityMismatch(t *testing.T) {
	gateway := nodes.GatewayInvocationRecord{
		ExpectedPlanHash: "plan-1",
		Descriptor: nodes.CommandDescriptor{
			Name:         "system.exec.v1",
			InputSchema:  json.RawMessage(`{"type":"object"}`),
			OutputSchema: json.RawMessage(`{"type":"object"}`),
			Risk:         nodes.RiskWrite,
		},
		Plan: nodes.ExecutionPlan{
			InvocationRequest: nodes.InvocationRequest{
				InvocationID: "inv-1", IdempotencyKey: "idem-1",
				NodeID: "node-1", CatalogHash: "catalog-1",
				Command: "system.exec.v1",
			},
			Risk: nodes.RiskWrite,
		},
	}
	remote := nodes.InvocationRecord{
		InvocationID: "inv-1", IdempotencyKey: "idem-1",
		PlanHash: "plan-1", NodeID: "node-1", CatalogHash: "catalog-1",
		Command: "system.exec.v1", Risk: nodes.RiskWrite,
	}
	if err := verifyRemoteInvocation(gateway, &remote); err != nil {
		t.Fatalf("matching remote invocation = %v", err)
	}
	remote.PlanHash = "different-plan"
	if err := verifyRemoteInvocation(gateway, &remote); !errors.Is(
		err,
		nodes.ErrGatewayInvocationConflict,
	) {
		t.Fatalf("mismatched remote invocation error = %v", err)
	}
}

func TestVerifyRemoteInvocationValidatesRecoveredOutput(t *testing.T) {
	gateway := nodes.GatewayInvocationRecord{
		ExpectedPlanHash: "plan-1",
		Descriptor: nodes.CommandDescriptor{
			Name:        "system.exec.v1",
			InputSchema: json.RawMessage(`{"type":"object"}`),
			OutputSchema: json.RawMessage(
				`{"type":"object","properties":{"stdout":{"type":"string"}},"required":["stdout"],"additionalProperties":false}`,
			),
			Risk: nodes.RiskWrite,
		},
		Plan: nodes.ExecutionPlan{
			InvocationRequest: nodes.InvocationRequest{
				InvocationID: "inv-1", IdempotencyKey: "idem-1",
				NodeID: "node-1", CatalogHash: "catalog-1",
				Command: "system.exec.v1", OutputLimitBytes: 64,
			},
			Risk: nodes.RiskWrite,
		},
	}
	remote := nodes.InvocationRecord{
		InvocationID: "inv-1", IdempotencyKey: "idem-1",
		PlanHash: "plan-1", NodeID: "node-1", CatalogHash: "catalog-1",
		Command: "system.exec.v1", Risk: nodes.RiskWrite,
		State: nodes.InvocationSucceeded, Result: json.RawMessage(`{"stdout":"ok"}`),
	}
	if err := verifyRemoteInvocation(gateway, &remote); err != nil {
		t.Fatalf("valid recovered output = %v", err)
	}

	remote.Result = json.RawMessage(`{"unexpected":true}`)
	if err := verifyRemoteInvocation(gateway, &remote); !errors.Is(
		err,
		nodes.ErrGatewayInvocationConflict,
	) {
		t.Fatalf("invalid recovered schema error = %v", err)
	}
	remote.Result = json.RawMessage(`{"stdout":"` + strings.Repeat("x", 80) + `"}`)
	if err := verifyRemoteInvocation(gateway, &remote); !errors.Is(
		err,
		nodes.ErrGatewayInvocationConflict,
	) {
		t.Fatalf("oversized recovered output error = %v", err)
	}
}

func TestServiceShutdownClosesNodeAdmissionOutsideReload(t *testing.T) {
	routes := &fakeNodeAdmissionRoutes{}
	runtime := &nodeAdmissionRuntime{routes: routes}
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = t.TempDir()
	cfg.Nodes.Enabled = true
	if err := runtime.Reconcile(cfg); err != nil {
		t.Fatal(err)
	}

	stopAndCleanupServices(&services{NodeAdmission: runtime}, time.Second, true)
	if !runtime.mounted {
		t.Fatal("service reload closed node admission")
	}
	stopAndCleanupServices(&services{NodeAdmission: runtime}, time.Second, false)
	if runtime.mounted || runtime.sessions != nil || routes.handler != nil {
		t.Fatal("gateway shutdown left node admission active")
	}
}

func (routes *fakeNodeAdmissionRoutes) RegisterHTTPHandler(_ string, handler http.Handler) error {
	if routes.handler != nil {
		return errors.New("route already registered")
	}
	routes.handler = handler
	routes.registerCount++
	return nil
}

func (routes *fakeNodeAdmissionRoutes) ReplaceHTTPHandler(_ string, handler http.Handler) error {
	if routes.handler == nil {
		return errors.New("route not registered")
	}
	routes.handler = handler
	routes.replaceCount++
	return nil
}

func (routes *fakeNodeAdmissionRoutes) UnregisterHTTPHandler(string) {
	routes.handler = nil
	routes.unregisterCount++
}

func TestNodeAdmissionRuntimeReconcilesConfigLifecycle(t *testing.T) {
	routes := &fakeNodeAdmissionRoutes{}
	runtime := &nodeAdmissionRuntime{routes: routes}
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = filepath.Join(t.TempDir(), "first")

	if err := runtime.Reconcile(cfg); err != nil {
		t.Fatal(err)
	}
	if runtime.mounted || routes.handler != nil {
		t.Fatal("disabled node admission mounted a route")
	}

	cfg.Nodes.Enabled = true
	if err := runtime.Reconcile(cfg); err != nil {
		t.Fatal(err)
	}
	firstRegistry := runtime.registry
	firstSessions := runtime.sessions
	if !runtime.mounted || firstRegistry == nil || routes.registerCount != 1 {
		t.Fatalf("enabled runtime = %#v, routes = %#v", runtime, routes)
	}

	cfg.Nodes.AllowLoopbackPlaintext = true
	if err := runtime.Reconcile(cfg); err != nil {
		t.Fatal(err)
	}
	if runtime.registry == firstRegistry || routes.replaceCount != 1 {
		t.Fatalf("reloaded runtime = %#v, routes = %#v", runtime, routes)
	}
	if runtime.sessions != firstSessions {
		t.Fatal("config reload replaced shared node session ownership")
	}

	cfg.Agents.Defaults.Workspace = filepath.Join(t.TempDir(), "second")
	if err := runtime.Reconcile(cfg); err != nil {
		t.Fatal(err)
	}
	if runtime.sessions == firstSessions {
		t.Fatal("workspace change retained node session ownership across registries")
	}
	if routes.replaceCount != 1 {
		t.Fatalf("route replacement count = %d", routes.replaceCount)
	}
	if routes.registerCount != 2 || routes.unregisterCount != 1 {
		t.Fatalf("workspace rotation route counts = %#v", routes)
	}

	cfg.Nodes.Enabled = false
	if err := runtime.Reconcile(cfg); err != nil {
		t.Fatal(err)
	}
	if runtime.mounted || runtime.registry != nil || runtime.registryPath != "" || runtime.sessions != nil ||
		routes.handler != nil || routes.unregisterCount != 2 {
		t.Fatalf("disabled runtime = %#v, routes = %#v", runtime, routes)
	}
}
