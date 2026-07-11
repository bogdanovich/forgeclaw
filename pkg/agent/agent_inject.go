// PicoClaw - Ultra-lightweight personal AI agent

package agent

import (
	"fmt"
	"sort"
	"strings"

	"github.com/sipeed/picoclaw/pkg/audio/asr"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/media"
	"github.com/sipeed/picoclaw/pkg/tools"
)

type RuntimeToolFactory func(cfg *config.Config) (tools.Tool, error)

func (al *AgentLoop) RegisterTool(tool tools.Tool) {
	registry := al.GetRegistry()
	registerToolOnRegistry(registry, tool)
}

func (al *AgentLoop) RegisterRuntimeTool(name string, factory RuntimeToolFactory) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("runtime tool name is required")
	}
	if factory == nil {
		return fmt.Errorf("runtime tool factory is required for %s", name)
	}
	cfg := al.GetConfig()
	tool, err := factory(cfg)
	if err != nil {
		return err
	}

	al.mu.Lock()
	if al.runtimeTools == nil {
		al.runtimeTools = make(map[string]RuntimeToolFactory)
	}
	al.runtimeTools[name] = factory
	registry := al.registry
	al.mu.Unlock()

	registerToolOnRegistry(registry, tool)
	return nil
}

func (al *AgentLoop) registerRuntimeToolsForRegistry(cfg *config.Config, registry *AgentRegistry) error {
	factories := al.runtimeToolFactories()
	for _, name := range sortedRuntimeToolNames(factories) {
		tool, err := factories[name](cfg)
		if err != nil {
			return fmt.Errorf("register runtime tool %s: %w", name, err)
		}
		registerToolOnRegistry(registry, tool)
	}
	return nil
}

func (al *AgentLoop) runtimeToolFactories() map[string]RuntimeToolFactory {
	al.mu.RLock()
	defer al.mu.RUnlock()
	if len(al.runtimeTools) == 0 {
		return nil
	}
	factories := make(map[string]RuntimeToolFactory, len(al.runtimeTools))
	for name, factory := range al.runtimeTools {
		factories[name] = factory
	}
	return factories
}

func sortedRuntimeToolNames(factories map[string]RuntimeToolFactory) []string {
	if len(factories) == 0 {
		return nil
	}
	names := make([]string, 0, len(factories))
	for name := range factories {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func registerToolOnRegistry(registry *AgentRegistry, tool tools.Tool) {
	if registry == nil || tool == nil {
		return
	}
	for _, agentID := range registry.ListAgentIDs() {
		if agent, ok := registry.GetAgent(agentID); ok {
			registerToolIfAllowed(agent, tool)
		}
	}
}

func (al *AgentLoop) SetChannelManager(cm *channels.Manager) {
	al.channelManager = cm
}

func (al *AgentLoop) GetRegistry() *AgentRegistry {
	al.mu.RLock()
	defer al.mu.RUnlock()
	return al.registry
}

func (al *AgentLoop) GetConfig() *config.Config {
	al.mu.RLock()
	defer al.mu.RUnlock()
	return al.cfg
}

func (al *AgentLoop) SetMediaStore(s media.MediaStore) {
	al.mediaStore = s

	// Propagate store to all registered tools that can emit media.
	registry := al.GetRegistry()
	for _, agentID := range registry.ListAgentIDs() {
		if agent, ok := registry.GetAgent(agentID); ok {
			agent.Tools.SetMediaStore(s)
		}
	}
	registry.ForEachTool("send_tts", func(t tools.Tool) {
		if st, ok := t.(*tools.SendTTSTool); ok {
			st.SetMediaStore(s)
		}
	})
}

func (al *AgentLoop) SetTranscriber(t asr.Transcriber) {
	al.transcriber = t
}

func (al *AgentLoop) SetReloadFunc(fn func() error) {
	al.reloadFunc = fn
}

func (al *AgentLoop) RecordLastChannel(channel string) error {
	if al.state == nil {
		return nil
	}
	return al.state.SetLastChannel(channel)
}

func (al *AgentLoop) RecordLastChatID(chatID string) error {
	if al.state == nil {
		return nil
	}
	return al.state.SetLastChatID(chatID)
}

func (al *AgentLoop) GetStartupInfo() map[string]any {
	info := make(map[string]any)

	registry := al.GetRegistry()
	agent := registry.GetDefaultAgent()
	if agent == nil {
		return info
	}

	// Tools info
	toolsList := agent.Tools.List()
	info["tools"] = map[string]any{
		"count": len(toolsList),
		"names": toolsList,
	}

	// Skills info
	info["skills"] = agent.ContextBuilder.GetSkillsInfo()

	// Agents info
	info["agents"] = map[string]any{
		"count": len(registry.ListAgentIDs()),
		"ids":   registry.ListAgentIDs(),
	}

	return info
}
