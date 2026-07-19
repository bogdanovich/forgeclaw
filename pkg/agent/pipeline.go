// PicoClaw - Ultra-lightweight personal AI agent

package agent

import (
	"context"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/interactions"
	"github.com/sipeed/picoclaw/pkg/media"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/tools"
)

// Pipeline holds the runtime dependencies used by Pipeline methods.
// It is constructed by runTurn via NewPipeline and passed to sub-methods
// so that the coordinator can delegate phase execution.
type Pipeline struct {
	Cfg         *config.Config
	Runtime     PipelineRuntimeServices
	Config      PipelineConfigServices
	Context     PipelineContextServices
	Interaction PipelineInteractionServices
}

// PipelineDependencies is the explicit dependency set required by Pipeline.
type PipelineDependencies struct {
	Cfg         *config.Config
	Runtime     PipelineRuntimeServices
	Config      PipelineConfigServices
	Context     PipelineContextServices
	Interaction PipelineInteractionServices
}

type PipelineRuntimeServices struct {
	Bus            pipelineBus
	Events         runtimeEventEmitter
	ActiveRequests activeRequestTracker
	TurnControl    turnController
}

type PipelineConfigServices struct {
	ChannelStreaming  channelStreamingConfigProvider
	NativeSearch      nativeSearchPolicy
	LLMRetry          llmRetryPolicy
	MediaLimits       mediaLimitsProvider
	FinalTurnRender   finalTurnRenderPolicy
	ModelResolution   pipelineModelResolution
	PromptBuilder     pipelinePromptBuilder
	ToolContentFilter toolContentFilter
}

type PipelineContextServices struct {
	Runtime              pipelineContextRuntime
	BackgroundCompaction backgroundCompactionScheduler
	ModelExecution       modelExecutionResolver
	Steering             steeringDequeuer
	MediaResolver        mediaResolver
}

type PipelineInteractionServices struct {
	Reasoning        reasoningPublisher
	ToolFeedback     toolFeedbackManager
	SyncToolDelivery syncToolResultDeliveryManager
	ToolDelivery     toolDeliveryManager
	Hooks            hookInterceptor
	Fallback         fallbackExecutor
	Suspension       toolSuspensionManager
}

type ToolSuspensionRequest struct {
	Workspace        string
	Prompt           interactions.SuspensionRequest
	Route            interactions.Route
	Origin           interactions.Origin
	ApprovalAction   string
	ExecutionContext *bus.InboundContext
}

// ToolSuspensionDisposition distinguishes a durable handoff from a failure
// that is still safe to return to the model as an ordinary tool error.
type ToolSuspensionDisposition struct {
	InteractionID string
	Durable       bool
}

type ToolApprovalGrant struct {
	InteractionID string
	Revision      int64
}

type ToolApprovalConsumptionRequest struct {
	Workspace     string
	InteractionID string
	Revision      int64
	Origin        interactions.Origin
}

type toolSuspensionManager interface {
	SuspendToolCall(
		ctx context.Context,
		request ToolSuspensionRequest,
	) (ToolSuspensionDisposition, error)
	ConsumeApproval(ctx context.Context, request ToolApprovalConsumptionRequest) error
}

type runtimeEventEmitter interface {
	emitEvent(kind runtimeevents.Kind, meta HookMeta, payload any)
}

type pipelineBus interface {
	PublishOutbound(ctx context.Context, msg bus.OutboundMessage) error
	GetStreamer(ctx context.Context, channel, chatID, sessionKey string) (bus.Streamer, bool)
}

type channelStreamingConfigProvider interface {
	channelStreamingConfig(channelName string) (config.StreamingConfig, bool)
}

type nativeSearchPolicy interface {
	useNativeSearch(profile config.EffectiveTurnProfile, provider providers.LLMProvider) bool
}

type llmRetryPolicy interface {
	llmRetrySettings() (maxRetries int, backoffSecs int)
}

type mediaLimitsProvider interface {
	maxMediaSize() int
}

type finalTurnRenderPolicy interface {
	shouldFinalizeAfterToolLoop(exec *turnExecution) bool
}

type pipelineModelResolution interface {
	modelCandidates(primary string, fallbacks []string) []providers.FallbackCandidate
	activeModelConfig(
		workspace string,
		candidates []providers.FallbackCandidate,
		activeModel string,
	) *config.ModelConfig
}

type pipelinePromptBuilder interface {
	buildTurnMessages(
		ts *turnState,
		history []providers.Message,
		summary string,
		currentMessage string,
		media []string,
		activeSkills []string,
	) []providers.Message
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
	publishPicoReasoning(
		ctx context.Context,
		reasoningContent, chatID, sessionKey, modelName string,
	)
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
	shouldPublishToolFeedback(ts *turnState) bool
}

type toolContentFilter interface {
	filterToolContentForLLM(content string) string
}

type turnController interface {
	abortTurn(ts *turnState) (turnResult, error)
}

type hookInterceptor interface {
	BeforeLLM(ctx context.Context, req *LLMHookRequest) (*LLMHookRequest, HookDecision)
	AfterLLM(ctx context.Context, resp *LLMHookResponse) (*LLMHookResponse, HookDecision)
	BeforeTool(ctx context.Context, req *ToolCallHookRequest) (*ToolCallHookRequest, HookDecision)
	AfterTool(
		ctx context.Context,
		resp *ToolResultHookResponse,
	) (*ToolResultHookResponse, HookDecision)
	ApproveTool(ctx context.Context, req *ToolApprovalRequest) ApprovalDecision
}

type fallbackExecutor interface {
	ExecuteCandidate(
		ctx context.Context,
		candidates []providers.FallbackCandidate,
		run func(context.Context, providers.FallbackCandidate) (*providers.LLMResponse, error),
	) (*providers.FallbackResult, error)
}

type observedFallbackExecutor interface {
	ExecuteCandidateObserved(
		ctx context.Context,
		candidates []providers.FallbackCandidate,
		run func(context.Context, providers.FallbackCandidate) (*providers.LLMResponse, error),
		observer providers.FallbackAttemptObserver,
	) (*providers.FallbackResult, error)
}

func executeFallbackWithObserver(
	executor fallbackExecutor,
	ctx context.Context,
	candidates []providers.FallbackCandidate,
	run func(context.Context, providers.FallbackCandidate) (*providers.LLMResponse, error),
	observer providers.FallbackAttemptObserver,
) (*providers.FallbackResult, error) {
	if observed, ok := executor.(observedFallbackExecutor); ok {
		return observed.ExecuteCandidateObserved(ctx, candidates, run, observer)
	}
	return executor.ExecuteCandidate(ctx, candidates, run)
}

type mediaResolver interface {
	ResolveWithMeta(ref string) (localPath string, meta media.MediaMeta, err error)
}

// NewPipelineFromDependencies creates a Pipeline from explicit dependencies.
func NewPipelineFromDependencies(deps PipelineDependencies) *Pipeline {
	return &Pipeline{
		Cfg:         deps.Cfg,
		Runtime:     deps.Runtime,
		Config:      deps.Config,
		Context:     deps.Context,
		Interaction: deps.Interaction,
	}
}

func (p *Pipeline) emitEvent(kind runtimeevents.Kind, meta HookMeta, payload any) {
	if p == nil || p.Runtime.Events == nil {
		return
	}
	p.Runtime.Events.emitEvent(kind, meta, payload)
}

func (p *Pipeline) trackActiveRequest() func() {
	if p == nil || p.Runtime.ActiveRequests == nil {
		return func() {}
	}
	p.Runtime.ActiveRequests.activeRequestsInc()
	return p.Runtime.ActiveRequests.activeRequestsDec
}
