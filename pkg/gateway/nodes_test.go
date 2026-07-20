package gateway

import (
	"errors"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/config"
)

type fakeNodeAdmissionRoutes struct {
	handler         http.Handler
	registerCount   int
	replaceCount    int
	unregisterCount int
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
	if routes.replaceCount != 2 {
		t.Fatalf("route replacement count = %d", routes.replaceCount)
	}

	cfg.Nodes.Enabled = false
	if err := runtime.Reconcile(cfg); err != nil {
		t.Fatal(err)
	}
	if runtime.mounted || runtime.registry != nil || runtime.registryPath != "" || runtime.sessions != nil ||
		routes.handler != nil || routes.unregisterCount != 1 {
		t.Fatalf("disabled runtime = %#v, routes = %#v", runtime, routes)
	}
}
