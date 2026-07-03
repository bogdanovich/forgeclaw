// PicoClaw - Ultra-lightweight personal AI agent

package agent

import (
	"context"

	"github.com/sipeed/picoclaw/pkg/agent/interfaces"
	"github.com/sipeed/picoclaw/pkg/config"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/media"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/tools"
)

// Pipeline holds the runtime dependencies used by Pipeline methods.
// It is constructed by runTurn via NewPipeline and passed to sub-methods
// so that the coordinator can delegate phase execution.
type Pipeline struct {
	Bus                  interfaces.MessageBus
	Cfg                  *config.Config
	ContextManager       ContextManager
	BackgroundCompaction backgroundCompactionScheduler
	Events               runtimeEventEmitter
	ActiveRequests       activeRequestTracker
	ModelExecution       modelExecutionResolver
	FallbackState        fallbackSelectionUpdater
	Steering             steeringDequeuer
	Reasoning            reasoningPublisher
	ToolDelivery         toolDeliveryManager
	TurnControl          turnController
	Hooks                *HookManager
	Fallback             *providers.FallbackChain
	ChannelManager       interfaces.ChannelManager
	MediaStore           media.MediaStore
}

type runtimeEventEmitter interface {
	emitEvent(kind runtimeevents.Kind, meta HookMeta, payload any)
}

type backgroundCompactionScheduler interface {
	scheduleBackgroundCompaction(
		agent *AgentInstance,
		sessionKey string,
		reason ContextCompressReason,
		budget int,
		messageKind string,
	)
}

type activeRequestTracker interface {
	activeRequestsInc()
	activeRequestsDec()
}

type modelExecutionResolver interface {
	selectCandidates(
		execution effectiveExecutionState,
		userMsg string,
		history []providers.Message,
		routeSessionKey string,
	) modelSelectionDecision
	maybeBuildVisionExecutionState(
		baseAgent *AgentInstance,
		execution effectiveExecutionState,
		messages []providers.Message,
	) (effectiveExecutionState, func(), string, bool, error)
	maybeApplyVisionExecutionState(baseAgent *AgentInstance, exec *turnExecution) (bool, error)
	buildExecutionStateForModel(
		baseAgent *AgentInstance,
		modelName string,
		fallbacks []string,
	) (effectiveExecutionState, func(), error)
}

type fallbackSelectionUpdater interface {
	updateAutoFallbackSelection(
		routeSessionKey string,
		selectedCandidates []providers.FallbackCandidate,
		result *providers.FallbackResult,
		usedLight bool,
	)
}

type steeringDequeuer interface {
	dequeueSteeringMessagesForTurn(scope, senderID string) []providers.Message
}

type reasoningPublisher interface {
	targetReasoningChannelID(channelName string) string
	publishPicoReasoning(ctx context.Context, reasoningContent, chatID, sessionKey, modelName string)
	publishPicoToolCallInterim(
		ctx context.Context,
		ts *turnState,
		modelName string,
		reasoningContent string,
		content string,
		toolCalls []providers.ToolCall,
	)
	handleReasoning(ctx context.Context, reasoningContent, channelName, channelID string)
}

type toolDeliveryManager interface {
	publishToolFeedbackForCall(
		ctx context.Context,
		ts *turnState,
		response *providers.LLMResponse,
		toolCall providers.ToolCall,
		toolName string,
		toolArgs map[string]any,
		messages []providers.Message,
	)
	applySyncToolResultDelivery(
		ctx context.Context,
		ts *turnState,
		result *tools.ToolResult,
		toolName string,
	) ([]providers.Attachment, *tools.ToolResult)
	deliverAsyncToolCompletion(req AsyncDeliveryRequest)
	dismissToolFeedbackForTurn(ctx context.Context, ts *turnState)
}

type turnController interface {
	abortTurn(ts *turnState) (turnResult, error)
}

// NewPipeline creates a Pipeline from an AgentLoop instance.
func NewPipeline(al *AgentLoop) *Pipeline {
	return &Pipeline{
		Bus:                  al.bus,
		Cfg:                  al.GetConfig(),
		ContextManager:       al.contextManager,
		BackgroundCompaction: al.backgroundCompactionRunner(),
		Events:               al,
		ActiveRequests:       al.activeRequestCounter(),
		ModelExecution:       al.modelExecutionManager(),
		FallbackState:        al,
		Steering:             al,
		Reasoning:            al,
		ToolDelivery:         al,
		TurnControl:          al,
		Hooks:                al.hooks,
		Fallback:             al.fallback,
		ChannelManager:       al.channelManager,
		MediaStore:           al.mediaStore,
	}
}

func (p *Pipeline) emitEvent(kind runtimeevents.Kind, meta HookMeta, payload any) {
	if p == nil || p.Events == nil {
		return
	}
	p.Events.emitEvent(kind, meta, payload)
}

func (p *Pipeline) trackActiveRequest() func() {
	if p == nil || p.ActiveRequests == nil {
		return func() {}
	}
	p.ActiveRequests.activeRequestsInc()
	return p.ActiveRequests.activeRequestsDec
}
