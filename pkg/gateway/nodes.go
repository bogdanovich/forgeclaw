package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/channels"
	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/logger"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
	nodews "github.com/bogdanovich/mintclaw/pkg/nodes/ws"
	"github.com/bogdanovich/mintclaw/pkg/tools"
)

const nodeAdmissionDrainTimeout = 5 * time.Second

var errNodeDiscoveryAuthorityUnavailable = errors.New("node discovery authority unavailable")

type nodeAdmissionRoutes interface {
	RegisterHTTPHandler(string, http.Handler) error
	ReplaceHTTPHandler(string, http.Handler) error
	UnregisterHTTPHandler(string)
}

type nodeAdmissionHandler interface {
	http.Handler
	Close(context.Context) error
	WithResolvedApprovedCommand(
		string,
		string,
		func(nodes.Registration, nodes.CommandApproval) error,
	) (nodes.CommandApproval, error)
	Invoke(
		context.Context,
		nodes.ID,
		nodes.ExecutionPlan,
		func() error,
	) (json.RawMessage, bool, error)
	Invocation(context.Context, nodes.ID, string) (nodes.InvocationRecord, error)
}

type nodeAdmissionRuntime struct {
	registryMu   sync.RWMutex
	routes       nodeAdmissionRoutes
	registry     *nodes.FileRegistry
	registryPath string
	handler      nodeAdmissionHandler
	sessions     *nodews.SessionHub
	generation   uint64
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
	runtime.handler = handler
	runtime.generation++
	runtime.mounted = true
	runtime.registryMu.Unlock()
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

func (runtime *nodeAdmissionRuntime) invocationGeneration() uint64 {
	runtime.registryMu.RLock()
	defer runtime.registryMu.RUnlock()
	return runtime.generation
}

func (runtime *nodeAdmissionRuntime) withInvocationHandler(
	expectedRegistryPath string,
	expectedGeneration uint64,
	fn func(nodeAdmissionHandler) error,
) error {
	runtime.registryMu.RLock()
	defer runtime.registryMu.RUnlock()
	if !runtime.mounted ||
		runtime.handler == nil ||
		runtime.registryPath != expectedRegistryPath ||
		runtime.generation != expectedGeneration {
		return errNodeDiscoveryAuthorityUnavailable
	}
	return fn(runtime.handler)
}

func (runtime *nodeAdmissionRuntime) Close(ctx context.Context) error {
	runtime.registryMu.Lock()
	wasMounted := runtime.mounted
	handler := runtime.handler
	runtime.mounted = false
	runtime.generation++
	runtime.registryMu.Unlock()
	if wasMounted {
		runtime.routes.UnregisterHTTPHandler(nodews.Path)
	}
	if handler != nil {
		if err := handler.Close(ctx); err != nil {
			return err
		}
	}
	runtime.registryMu.Lock()
	runtime.registry = nil
	runtime.sessions = nil
	runtime.registryPath = ""
	runtime.handler = nil
	runtime.registryMu.Unlock()
	return nil
}
