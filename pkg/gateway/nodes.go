package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
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
	WithPreparationAuthority(
		nodes.ID,
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
	CancelInvocation(context.Context, nodes.ID, string) (nodes.InvocationRecord, error)
}

type nodeAdmissionRuntime struct {
	registryMu        sync.RWMutex
	routes            nodeAdmissionRoutes
	registry          *nodes.FileRegistry
	registryPath      string
	handler           nodeAdmissionHandler
	sessions          *nodews.SessionHub
	terminalStore     *nodes.GatewayTerminalStore
	terminalStorePath string
	terminalHub       *nodeTerminalOperatorHub
	terminalMounted   bool
	generation        uint64
	mounted           bool
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
	registry, sessions, generation, err := runtime.discoveryAuthoritySnapshot(
		expectedRegistryPath,
	)
	if err != nil {
		return tools.NodeDiscoveryRecord{}, false, err
	}
	snapshot, found, err := registry.Resolve(ref)
	if err != nil || !found {
		if revalidateErr := runtime.revalidateDiscoveryAuthority(
			expectedRegistryPath,
			generation,
			registry,
			sessions,
		); revalidateErr != nil {
			return tools.NodeDiscoveryRecord{}, false, revalidateErr
		}
		return tools.NodeDiscoveryRecord{}, found, err
	}
	record := tools.NodeDiscoveryRecord{
		Snapshot:  snapshot,
		Connected: sessions != nil && sessions.Connected(snapshot.ID),
	}
	registration, registered, err := registry.Registration(snapshot.ID)
	if err != nil {
		if revalidateErr := runtime.revalidateDiscoveryAuthority(
			expectedRegistryPath,
			generation,
			registry,
			sessions,
		); revalidateErr != nil {
			return tools.NodeDiscoveryRecord{}, false, revalidateErr
		}
		return tools.NodeDiscoveryRecord{}, false, err
	}
	if registered {
		record.Snapshot = registration.Snapshot
		record.Registration = &registration
	}
	if err := runtime.revalidateDiscoveryAuthority(
		expectedRegistryPath,
		generation,
		registry,
		sessions,
	); err != nil {
		return tools.NodeDiscoveryRecord{}, false, err
	}
	return record, true, nil
}

func (runtime *nodeAdmissionRuntime) discoveryAuthoritySnapshot(
	expectedRegistryPath string,
) (*nodes.FileRegistry, *nodews.SessionHub, uint64, error) {
	runtime.registryMu.RLock()
	defer runtime.registryMu.RUnlock()
	if !runtime.mounted ||
		runtime.registry == nil ||
		runtime.registryPath != expectedRegistryPath {
		return nil, nil, 0, errNodeDiscoveryAuthorityUnavailable
	}
	return runtime.registry, runtime.sessions, runtime.generation, nil
}

func (runtime *nodeAdmissionRuntime) revalidateDiscoveryAuthority(
	expectedRegistryPath string,
	expectedGeneration uint64,
	expectedRegistry *nodes.FileRegistry,
	expectedSessions *nodews.SessionHub,
) error {
	runtime.registryMu.RLock()
	defer runtime.registryMu.RUnlock()
	if !runtime.mounted ||
		runtime.registryPath != expectedRegistryPath ||
		runtime.generation != expectedGeneration ||
		runtime.registry != expectedRegistry ||
		runtime.sessions != expectedSessions {
		return errNodeDiscoveryAuthorityUnavailable
	}
	return nil
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

func (runtime *nodeAdmissionRuntime) gatewayTerminalStore(
	path string,
	maxRecords int,
	maxBytes int,
) (*nodes.GatewayTerminalStore, error) {
	runtime.registryMu.Lock()
	defer runtime.registryMu.Unlock()
	if runtime.terminalStore != nil && runtime.terminalStorePath == path {
		return runtime.terminalStore, nil
	}
	store, err := nodes.NewGatewayTerminalStore(path, maxRecords, maxBytes)
	if err != nil {
		return nil, err
	}
	runtime.terminalStore = store
	runtime.terminalStorePath = path
	return store, nil
}

func (runtime *nodeAdmissionRuntime) existingGatewayTerminalStore(
	path string,
	maxRecords int,
	maxBytes int,
) (*nodes.GatewayTerminalStore, bool, error) {
	runtime.registryMu.Lock()
	defer runtime.registryMu.Unlock()
	if runtime.terminalStore != nil && runtime.terminalStorePath == path {
		if err := runtime.terminalStore.ReconcileShutdown(); err != nil {
			return nil, true, err
		}
		return runtime.terminalStore, true, nil
	}
	store, found, err := nodes.OpenExistingGatewayTerminalStore(path, maxRecords, maxBytes)
	if err != nil || !found {
		return nil, found, err
	}
	runtime.terminalStore = store
	runtime.terminalStorePath = path
	return store, true, nil
}

func (runtime *nodeAdmissionRuntime) invocationHandlerSnapshot(
	expectedRegistryPath string,
	expectedGeneration uint64,
) (nodeAdmissionHandler, error) {
	runtime.registryMu.RLock()
	defer runtime.registryMu.RUnlock()
	if !runtime.mounted ||
		runtime.handler == nil ||
		runtime.registryPath != expectedRegistryPath ||
		runtime.generation != expectedGeneration {
		return nil, errNodeDiscoveryAuthorityUnavailable
	}
	return runtime.handler, nil
}

func (runtime *nodeAdmissionRuntime) terminalHandlerSnapshot(
	expectedRegistryPath string,
	expectedGeneration uint64,
) (nodeTerminalHandler, error) {
	handler, err := runtime.invocationHandlerSnapshot(
		expectedRegistryPath,
		expectedGeneration,
	)
	if err != nil {
		return nil, err
	}
	terminalHandler, ok := handler.(nodeTerminalHandler)
	if !ok {
		return nil, errNodeDiscoveryAuthorityUnavailable
	}
	return terminalHandler, nil
}

func (runtime *nodeAdmissionRuntime) terminalOperatorHub() *nodeTerminalOperatorHub {
	runtime.registryMu.RLock()
	defer runtime.registryMu.RUnlock()
	return runtime.terminalHub
}

func (runtime *nodeAdmissionRuntime) configureTerminalOperator(cfg *config.Config) error {
	token, allowOrigins, err := terminalOperatorAuthentication(cfg)
	if err != nil {
		return err
	}
	runtime.registryMu.RLock()
	wasMounted := runtime.terminalMounted
	oldHub := runtime.terminalHub
	runtime.registryMu.RUnlock()
	if token == "" {
		if wasMounted {
			runtime.routes.UnregisterHTTPHandler(nodeTerminalOperatorPath)
		}
		runtime.registryMu.Lock()
		runtime.terminalMounted = false
		runtime.terminalHub = nil
		runtime.registryMu.Unlock()
		if oldHub != nil {
			oldHub.shutdown()
		}
		return nil
	}
	hub := newNodeTerminalOperatorHub(token, allowOrigins)
	if wasMounted {
		err = runtime.routes.ReplaceHTTPHandler(nodeTerminalOperatorPath, hub)
	} else {
		err = runtime.routes.RegisterHTTPHandler(nodeTerminalOperatorPath, hub)
	}
	if err != nil {
		return fmt.Errorf("mount authenticated terminal operator route: %w", err)
	}
	runtime.registryMu.Lock()
	runtime.terminalMounted = true
	runtime.terminalHub = hub
	runtime.registryMu.Unlock()
	if oldHub != nil {
		oldHub.shutdown()
	}
	return nil
}

func terminalOperatorAuthentication(cfg *config.Config) (string, []string, error) {
	if cfg == nil {
		return "", nil, nil
	}
	channel := cfg.Channels.GetByType(config.ChannelMintClaw)
	if channel == nil || !channel.Enabled {
		return "", nil, nil
	}
	var settings config.MintClawSettings
	if err := channel.Decode(&settings); err != nil {
		return "", nil, fmt.Errorf("decode MintClaw operator authentication: %w", err)
	}
	return strings.TrimSpace(settings.Token.String()), append([]string(nil), settings.AllowOrigins...), nil
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
	terminalStore := runtime.terminalStore
	runtime.mounted = false
	runtime.generation++
	runtime.registryMu.Unlock()
	if wasMounted {
		runtime.routes.UnregisterHTTPHandler(nodews.Path)
	}
	runtime.registryMu.Lock()
	terminalMounted := runtime.terminalMounted
	terminalHub := runtime.terminalHub
	runtime.terminalMounted = false
	runtime.terminalHub = nil
	runtime.registryMu.Unlock()
	if terminalMounted {
		runtime.routes.UnregisterHTTPHandler(nodeTerminalOperatorPath)
	}
	if terminalHub != nil {
		terminalHub.shutdown()
	}
	var closeErr error
	if handler != nil {
		closeErr = handler.Close(ctx)
		if errors.Is(closeErr, nodews.ErrSessionDrainIncomplete) {
			return closeErr
		}
	}
	var terminalErr error
	if terminalStore != nil {
		if err := terminalStore.ReconcileShutdown(); err != nil {
			terminalErr = fmt.Errorf("reconcile gateway terminals after node drain: %w", err)
		}
	}
	if closeErr != nil || terminalErr != nil {
		return errors.Join(closeErr, terminalErr)
	}
	runtime.registryMu.Lock()
	runtime.registry = nil
	runtime.sessions = nil
	runtime.terminalStore = nil
	runtime.terminalStorePath = ""
	runtime.registryPath = ""
	runtime.handler = nil
	runtime.registryMu.Unlock()
	return nil
}
