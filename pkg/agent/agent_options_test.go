package agent

import (
	"path/filepath"
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

func TestWithIsolatedSkillBootstrapUsesOnlyWorkspaceSkillRoot(t *testing.T) {
	workspace := t.TempDir()
	t.Setenv(config.EnvHome, t.TempDir())
	t.Setenv(config.EnvBuiltinSkills, t.TempDir())
	cfg := &config.Config{Agents: config.AgentsConfig{Defaults: config.AgentDefaults{
		Workspace: workspace, ModelName: "test-model", MaxTokens: 100, MaxToolIterations: 2,
	}}}
	loop := NewAgentLoop(cfg, bus.NewMessageBus(), &mockProvider{}, WithIsolatedSkillBootstrap())
	t.Cleanup(loop.Close)

	instance := loop.registry.GetDefaultAgent()
	if instance == nil || instance.ContextBuilder == nil {
		t.Fatal("isolated skill bootstrap has no context builder")
	}
	roots := instance.ContextBuilder.skillsLoader.SkillRoots()
	want := filepath.Join(workspace, "skills")
	if len(roots) != 1 || roots[0] != want {
		t.Fatalf("isolated skill roots = %v, want [%s]", roots, want)
	}
}
