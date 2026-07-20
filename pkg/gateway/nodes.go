package gateway

import (
	"fmt"
	"path/filepath"

	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/nodes"
	nodews "github.com/sipeed/picoclaw/pkg/nodes/ws"
)

func setupNodeAdmission(cfg *config.Config, manager *channels.Manager) (*nodes.FileRegistry, error) {
	if cfg == nil || !cfg.Nodes.Enabled {
		return nil, nil
	}
	registry, err := nodes.NewFileRegistry(
		filepath.Join(cfg.WorkspacePath(), "state", "nodes", "registry.json"),
		cfg.Nodes.MaxPendingPairings,
	)
	if err != nil {
		return nil, fmt.Errorf("open node registry: %w", err)
	}
	authenticator, err := nodes.NewAuthenticator(registry, nodes.AdmissionConfig{})
	if err != nil {
		return nil, fmt.Errorf("create node authenticator: %w", err)
	}
	handler, err := nodews.NewAdmissionHandler(authenticator, nodews.AdmissionConfig{
		AllowLoopbackPlaintext: cfg.Nodes.AllowLoopbackPlaintext,
	})
	if err != nil {
		return nil, fmt.Errorf("create node admission handler: %w", err)
	}
	if err := manager.RegisterHTTPHandler(nodews.Path, handler); err != nil {
		return nil, fmt.Errorf("register node admission route: %w", err)
	}
	logger.InfoCF("nodes", "Node admission enabled", map[string]any{
		"path":                     nodews.Path,
		"allow_loopback_plaintext": cfg.Nodes.AllowLoopbackPlaintext,
	})
	return registry, nil
}
