package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/nodes"
	nodews "github.com/sipeed/picoclaw/pkg/nodes/ws"
	"github.com/sipeed/picoclaw/pkg/tools"
)

const nodeAdmissionDrainTimeout = 5 * time.Second

var errNodeDiscoveryAuthorityUnavailable = errors.New("node discovery authority unavailable")

type nodeAdmissionRoutes interface {
	RegisterHTTPHandler(string, http.Handler) error
	ReplaceHTTPHandler(string, http.Handler) error
	UnregisterHTTPHandler(string)
}

type nodeAdmissionRuntime struct {
	registryMu   sync.RWMutex
	routes       nodeAdmissionRoutes
	registry     *nodes.FileRegistry
	registryPath string
	handler      *nodews.AdmissionHandler
	sessions     *nodews.SessionHub
	mounted      bool
}

type nodeDiscoverySource struct {
	runtime      *nodeAdmissionRuntime
	registryPath string
}

func (source *nodeDiscoverySource) Lookup(
	ref string,
) (tools.NodeDiscoveryRecord, bool, error) {
	if source == nil || source.runtime == nil {
		return tools.NodeDiscoveryRecord{}, false, errNodeDiscoveryAuthorityUnavailable
	}
	return source.runtime.lookup(source.registryPath, ref)
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
	if runtime.handler != nil && (!runtime.mounted || registryPath != runtime.registryPath) {
		ctx, cancel := context.WithTimeout(context.Background(), nodeAdmissionDrainTimeout)
		closeErr := runtime.Close(ctx)
		cancel()
		if closeErr != nil {
			return fmt.Errorf("drain previous node admission runtime: %w", closeErr)
		}
	}
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
	sessions := runtime.currentSessions()
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
	if runtime.mounted {
		err = runtime.routes.ReplaceHTTPHandler(nodews.Path, handler)
	} else {
		err = runtime.routes.RegisterHTTPHandler(nodews.Path, handler)
	}
	if err != nil {
		return fmt.Errorf("mount node admission route: %w", err)
	}
	runtime.registryMu.Lock()
	runtime.registry = registry
	runtime.sessions = sessions
	runtime.registryPath = registryPath
	runtime.mounted = true
	runtime.registryMu.Unlock()
	runtime.handler = handler
	logger.InfoCF("nodes", "Node admission enabled", map[string]any{
		"path":                     nodews.Path,
		"allow_loopback_plaintext": cfg.Nodes.AllowLoopbackPlaintext,
	})
	return nil
}

func (runtime *nodeAdmissionRuntime) lookup(
	expectedRegistryPath string,
	ref string,
) (tools.NodeDiscoveryRecord, bool, error) {
	runtime.registryMu.RLock()
	defer runtime.registryMu.RUnlock()
	if !runtime.mounted ||
		runtime.registry == nil ||
		runtime.registryPath != expectedRegistryPath {
		return tools.NodeDiscoveryRecord{}, false, errNodeDiscoveryAuthorityUnavailable
	}
	snapshot, found, err := runtime.registry.Resolve(ref)
	if err != nil || !found {
		return tools.NodeDiscoveryRecord{}, found, err
	}
	record := tools.NodeDiscoveryRecord{
		Snapshot:  snapshot,
		Connected: runtime.sessions != nil && runtime.sessions.Connected(snapshot.ID),
	}
	registration, registered, err := runtime.registry.Registration(snapshot.ID)
	if err != nil {
		return tools.NodeDiscoveryRecord{}, false, err
	}
	if registered {
		record.Snapshot = registration.Snapshot
		record.Registration = &registration
	}
	return record, true, nil
}

func (runtime *nodeAdmissionRuntime) currentSessions() *nodews.SessionHub {
	runtime.registryMu.RLock()
	defer runtime.registryMu.RUnlock()
	return runtime.sessions
}

func (runtime *nodeAdmissionRuntime) Close(ctx context.Context) error {
	runtime.registryMu.Lock()
	wasMounted := runtime.mounted
	runtime.mounted = false
	runtime.registryMu.Unlock()
	if wasMounted {
		runtime.routes.UnregisterHTTPHandler(nodews.Path)
	}
	if runtime.handler != nil {
		if err := runtime.handler.Close(ctx); err != nil {
			return err
		}
	}
	runtime.registryMu.Lock()
	runtime.registry = nil
	runtime.sessions = nil
	runtime.registryPath = ""
	runtime.registryMu.Unlock()
	runtime.handler = nil
	return nil
}
