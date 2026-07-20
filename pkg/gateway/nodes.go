package gateway

import (
	"fmt"
	"net/http"

	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/nodes"
	nodews "github.com/sipeed/picoclaw/pkg/nodes/ws"
)

type nodeAdmissionRoutes interface {
	RegisterHTTPHandler(string, http.Handler) error
	ReplaceHTTPHandler(string, http.Handler) error
	UnregisterHTTPHandler(string)
}

type nodeAdmissionRuntime struct {
	routes   nodeAdmissionRoutes
	registry *nodes.FileRegistry
	handler  *nodews.AdmissionHandler
	mounted  bool
}

func setupNodeAdmission(
	cfg *config.Config,
	manager *channels.Manager,
) (*nodeAdmissionRuntime, error) {
	runtime := &nodeAdmissionRuntime{routes: manager}
	if err := runtime.Reconcile(cfg); err != nil {
		return nil, err
	}
	return runtime, nil
}

func (runtime *nodeAdmissionRuntime) Reconcile(cfg *config.Config) error {
	if cfg == nil || !cfg.Nodes.Enabled {
		if runtime.mounted {
			runtime.routes.UnregisterHTTPHandler(nodews.Path)
		}
		if runtime.handler != nil {
			runtime.handler.Close()
		}
		runtime.registry = nil
		runtime.handler = nil
		runtime.mounted = false
		return nil
	}

	registry, err := nodes.NewFileRegistry(
		nodes.RegistryPath(cfg.WorkspacePath()),
		cfg.Nodes.MaxPendingPairings,
	)
	if err != nil {
		return fmt.Errorf("open node registry: %w", err)
	}
	authenticator, err := nodes.NewAuthenticator(registry, nodes.AdmissionConfig{})
	if err != nil {
		return fmt.Errorf("create node authenticator: %w", err)
	}
	handler, err := nodews.NewAdmissionHandler(authenticator, nodews.AdmissionConfig{
		AllowLoopbackPlaintext: cfg.Nodes.AllowLoopbackPlaintext,
	})
	if err != nil {
		return fmt.Errorf("create node admission handler: %w", err)
	}
	if runtime.mounted {
		err = runtime.routes.ReplaceHTTPHandler(nodews.Path, handler)
	} else {
		err = runtime.routes.RegisterHTTPHandler(nodews.Path, handler)
	}
	if err != nil {
		return fmt.Errorf("mount node admission route: %w", err)
	}
	previousHandler := runtime.handler
	runtime.registry = registry
	runtime.handler = handler
	runtime.mounted = true
	if previousHandler != nil {
		previousHandler.Close()
	}
	logger.InfoCF("nodes", "Node admission enabled", map[string]any{
		"path":                     nodews.Path,
		"allow_loopback_plaintext": cfg.Nodes.AllowLoopbackPlaintext,
	})
	return nil
}
