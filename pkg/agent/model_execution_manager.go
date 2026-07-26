package agent

import (
	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/providers"
	"github.com/bogdanovich/mintclaw/pkg/state"
)

type modelProviderFactory func(*config.ModelConfig) (providers.LLMProvider, string, error)

type modelExecutionManager struct {
	configProvider  func() *config.Config
	state           *state.Manager
	providerFactory func() modelProviderFactory
}

func (al *AgentLoop) modelExecutionManager() *modelExecutionManager {
	if al == nil {
		return nil
	}
	if al.modelExecution == nil {
		al.modelExecution = &modelExecutionManager{
			configProvider: al.GetConfig,
			state:          al.state,
			providerFactory: func() modelProviderFactory {
				return al.providerFactory
			},
		}
	}
	return al.modelExecution
}

func (m *modelExecutionManager) config() *config.Config {
	if m == nil {
		return nil
	}
	if m.configProvider == nil {
		return nil
	}
	return m.configProvider()
}

func (m *modelExecutionManager) currentProviderFactory() modelProviderFactory {
	if m != nil && m.providerFactory != nil {
		if factory := m.providerFactory(); factory != nil {
			return factory
		}
	}
	return providers.CreateProviderFromConfig
}
