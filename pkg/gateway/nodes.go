package gateway

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/nodes"
	nodews "github.com/sipeed/picoclaw/pkg/nodes/ws"
)

const nodeAdmissionDrainTimeout = 5 * time.Second

type nodeAdmissionRoutes interface {
	RegisterHTTPHandler(string, http.Handler) error
	ReplaceHTTPHandler(string, http.Handler) error
	UnregisterHTTPHandler(string)
}

type nodeAdmissionRuntime struct {
	routes       nodeAdmissionRoutes
	registry     *nodes.FileRegistry
	registryPath string
	handler      *nodews.AdmissionHandler
	sessions     *nodews.SessionHub
	mounted      bool
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
		ctx, cancel := context.WithTimeout(context.Background(), nodeAdmissionDrainTimeout)
		defer cancel()
		return runtime.Close(ctx)
	}

	registryPath := nodes.RegistryPath(cfg.WorkspacePath())
	registry, err := nodes.NewFileRegistry(
		registryPath,
		cfg.Nodes.MaxPendingPairings,
	)
	if err != nil {
		return fmt.Errorf("open node registry: %w", err)
	}
	authenticator, err := nodes.NewAuthenticator(registry, nodes.AdmissionConfig{})
	if err != nil {
		return fmt.Errorf("create node authenticator: %w", err)
	}
	sameRegistry := runtime.mounted && registryPath == runtime.registryPath
	sessions := runtime.sessions
	if sessions == nil || !sameRegistry {
		sessions = nodews.NewSessionHub()
	}
	handler, err := nodews.NewAdmissionHandler(authenticator, nodews.AdmissionConfig{
		AllowLoopbackPlaintext: cfg.Nodes.AllowLoopbackPlaintext,
		Sessions:               sessions,
	})
	if err != nil {
		return fmt.Errorf("create node admission handler: %w", err)
	}
	wasMounted := runtime.mounted
	previousHandler := runtime.handler
	if wasMounted {
		err = runtime.routes.ReplaceHTTPHandler(nodews.Path, handler)
	} else {
		err = runtime.routes.RegisterHTTPHandler(nodews.Path, handler)
	}
	if err != nil {
		return fmt.Errorf("mount node admission route: %w", err)
	}
	runtime.registry = registry
	runtime.registryPath = registryPath
	runtime.handler = handler
	runtime.sessions = sessions
	runtime.mounted = true
	if wasMounted && !sameRegistry && previousHandler != nil {
		ctx, cancel := context.WithTimeout(context.Background(), nodeAdmissionDrainTimeout)
		if closeErr := previousHandler.Close(ctx); closeErr != nil {
			logger.WarnCF("nodes", "Node sessions did not drain after workspace change", map[string]any{
				"error": closeErr.Error(),
			})
		}
		cancel()
	}
	logger.InfoCF("nodes", "Node admission enabled", map[string]any{
		"path":                     nodews.Path,
		"allow_loopback_plaintext": cfg.Nodes.AllowLoopbackPlaintext,
	})
	return nil
}

func (runtime *nodeAdmissionRuntime) Close(ctx context.Context) error {
	if runtime.mounted {
		runtime.routes.UnregisterHTTPHandler(nodews.Path)
	}
	var err error
	if runtime.handler != nil {
		err = runtime.handler.Close(ctx)
	}
	runtime.registry = nil
	runtime.registryPath = ""
	runtime.handler = nil
	runtime.sessions = nil
	runtime.mounted = false
	return err
}
