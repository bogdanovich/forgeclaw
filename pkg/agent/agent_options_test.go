package agent

import (
	"testing"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
)

func TestWithIsolatedToolBootstrapSkipsSharedProductionStateAndTools(t *testing.T) {
	cfg := &config.Config{Agents: config.AgentsConfig{Defaults: config.AgentDefaults{
		Workspace: t.TempDir(), ModelName: "test-model", MaxTokens: 100, MaxToolIterations: 2,
	}}}
	loop := NewAgentLoop(cfg, bus.NewMessageBus(), &mockProvider{}, WithIsolatedToolBootstrap())
	t.Cleanup(loop.Close)

	if loop.state != nil {
		t.Fatal("isolated bootstrap constructed production state manager")
	}
	instance := loop.registry.GetDefaultAgent()
	if instance == nil {
		t.Fatal("isolated bootstrap has no default agent")
	}
	if got := instance.Tools.List(); len(got) != 0 {
		t.Fatalf("isolated bootstrap registered production tools: %v", got)
	}
}
