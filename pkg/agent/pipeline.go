// PicoClaw - Ultra-lightweight personal AI agent

package agent

import (
	"github.com/sipeed/picoclaw/pkg/agent/interfaces"
	"github.com/sipeed/picoclaw/pkg/config"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/media"
	"github.com/sipeed/picoclaw/pkg/providers"
)

// Pipeline holds the runtime dependencies used by Pipeline methods.
// It is constructed by runTurn via NewPipeline and passed to sub-methods
// so that the coordinator can delegate phase execution.
type Pipeline struct {
	Bus            interfaces.MessageBus
	Cfg            *config.Config
	ContextManager ContextManager
	Events         runtimeEventEmitter
	Hooks          *HookManager
	Fallback       *providers.FallbackChain
	ChannelManager interfaces.ChannelManager
	MediaStore     media.MediaStore
	al             *AgentLoop
}

type runtimeEventEmitter interface {
	emitEvent(kind runtimeevents.Kind, meta HookMeta, payload any)
}

// NewPipeline creates a Pipeline from an AgentLoop instance.
func NewPipeline(al *AgentLoop) *Pipeline {
	return &Pipeline{
		Bus:            al.bus,
		Cfg:            al.GetConfig(),
		ContextManager: al.contextManager,
		Events:         al,
		Hooks:          al.hooks,
		Fallback:       al.fallback,
		ChannelManager: al.channelManager,
		MediaStore:     al.mediaStore,
		al:             al,
	}
}

func (p *Pipeline) emitEvent(kind runtimeevents.Kind, meta HookMeta, payload any) {
	if p == nil || p.Events == nil {
		return
	}
	p.Events.emitEvent(kind, meta, payload)
}
