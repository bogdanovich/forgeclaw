package gateway

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
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
	if err := runtime.Reconcile(cfg); err != nil {
		t.Fatalf("workspace change did not recover after drain retry: %v", err)
	}
	if !runtime.mounted || runtime.registryPath == oldRegistryPath || routes.handler == nil {
		t.Fatal("successful retry did not mount the replacement authority runtime")
	}
}

type testNodeConnection struct{}

func (*testNodeConnection) Close() error { return nil }

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
