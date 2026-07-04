// PicoClaw - Ultra-lightweight personal AI agent

package agent

import (
	"context"

	"github.com/sipeed/picoclaw/pkg/bus"
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
	Bus                  pipelineBus
	Cfg                  *config.Config
	ContextRuntime       pipelineContextRuntime
	BackgroundCompaction backgroundCompactionScheduler
	Events               runtimeEventEmitter
	ActiveRequests       activeRequestTracker
	ModelExecution       modelExecutionResolver
	Steering             steeringDequeuer
	Reasoning            reasoningPublisher
	ToolFeedback         toolFeedbackManager
	SyncToolDelivery     syncToolResultDeliveryManager
	ToolDelivery         toolDeliveryManager
	TurnControl          turnController
	Hooks                hookInterceptor
	Fallback             fallbackExecutor
	MediaResolver        mediaResolver
}

// PipelineDependencies is the explicit dependency set required by Pipeline.
type PipelineDependencies struct {
	Bus                  pipelineBus
	Cfg                  *config.Config
	ContextRuntime       pipelineContextRuntime
	BackgroundCompaction backgroundCompactionScheduler
	Events               runtimeEventEmitter
	ActiveRequests       activeRequestTracker
	ModelExecution       modelExecutionResolver
	Steering             steeringDequeuer
	Reasoning            reasoningPublisher
	ToolFeedback         toolFeedbackManager
	SyncToolDelivery     syncToolResultDeliveryManager
	ToolDelivery         toolDeliveryManager
	TurnControl          turnController
	Hooks                hookInterceptor
	Fallback             fallbackExecutor
	MediaResolver        mediaResolver
}

type runtimeEventEmitter interface {
	emitEvent(kind runtimeevents.Kind, meta HookMeta, payload any)
}

type pipelineBus interface {
	PublishOutbound(ctx context.Context, msg bus.OutboundMessage) error
	GetStreamer(ctx context.Context, channel, chatID, sessionKey string) (bus.Streamer, bool)
}

type pipelineContextRuntime interface {
	Assemble(ctx context.Context, req *AssembleRequest) (*AssembleResponse, error)
	Compact(ctx context.Context, req *CompactRequest) error
	Ingest(ctx context.Context, req *IngestRequest) error
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
	deliverAsyncToolCompletion(req AsyncDeliveryRequest)
}

type syncToolResultDeliveryManager interface {
	applySyncToolResultDelivery(
		ctx context.Context,
		ts *turnState,
		result *tools.ToolResult,
		toolName string,
	) ([]providers.Attachment, *tools.ToolResult)
}

type toolFeedbackManager interface {
	publishToolFeedbackForCall(
		ctx context.Context,
		ts *turnState,
		response *providers.LLMResponse,
		toolCall providers.ToolCall,
		toolName string,
		toolArgs map[string]any,
		messages []providers.Message,
	)
	dismissToolFeedbackForTurn(ctx context.Context, ts *turnState)
}

type turnController interface {
	abortTurn(ts *turnState) (turnResult, error)
}

type hookInterceptor interface {
	BeforeLLM(ctx context.Context, req *LLMHookRequest) (*LLMHookRequest, HookDecision)
	AfterLLM(ctx context.Context, resp *LLMHookResponse) (*LLMHookResponse, HookDecision)
	BeforeTool(ctx context.Context, req *ToolCallHookRequest) (*ToolCallHookRequest, HookDecision)
	AfterTool(ctx context.Context, resp *ToolResultHookResponse) (*ToolResultHookResponse, HookDecision)
	ApproveTool(ctx context.Context, req *ToolApprovalRequest) ApprovalDecision
}

type fallbackExecutor interface {
	ExecuteCandidate(
		ctx context.Context,
		candidates []providers.FallbackCandidate,
		run func(context.Context, providers.FallbackCandidate) (*providers.LLMResponse, error),
	) (*providers.FallbackResult, error)
}

type mediaResolver interface {
	ResolveWithMeta(ref string) (localPath string, meta media.MediaMeta, err error)
}

// NewPipelineFromDependencies creates a Pipeline from explicit dependencies.
func NewPipelineFromDependencies(deps PipelineDependencies) *Pipeline {
	return &Pipeline{
		Bus:                  deps.Bus,
		Cfg:                  deps.Cfg,
		ContextRuntime:       deps.ContextRuntime,
		BackgroundCompaction: deps.BackgroundCompaction,
		Events:               deps.Events,
		ActiveRequests:       deps.ActiveRequests,
		ModelExecution:       deps.ModelExecution,
		Steering:             deps.Steering,
		Reasoning:            deps.Reasoning,
		ToolFeedback:         deps.ToolFeedback,
		SyncToolDelivery:     deps.SyncToolDelivery,
		ToolDelivery:         deps.ToolDelivery,
		TurnControl:          deps.TurnControl,
		Hooks:                deps.Hooks,
		Fallback:             deps.Fallback,
		MediaResolver:        deps.MediaResolver,
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
