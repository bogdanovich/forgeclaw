package agent

// NewPipeline creates a Pipeline from an AgentLoop instance.
func NewPipeline(al *AgentLoop) *Pipeline {
	cfg := al.GetConfig()
	return NewPipelineFromDependencies(PipelineDependencies{
		Cfg: cfg,
		Runtime: PipelineRuntimeServices{
			Bus:            al.bus,
			Events:         al.runtimeEventEmitter(),
			ActiveRequests: al.activeRequestCounter(),
			TurnControl:    al.turnAbortController(),
		},
		Config: PipelineConfigServices{
			ChannelStreaming:  newConfigChannelStreamingProvider(cfg),
			NativeSearch:      newConfigNativeSearchPolicy(cfg),
			LLMRetry:          newConfigLLMRetryPolicy(cfg),
			MediaLimits:       newConfigMediaLimitsProvider(cfg),
			FinalTurnRender:   newConfigFinalTurnRenderPolicy(cfg),
			ModelResolution:   newConfigPipelineModelResolution(cfg),
			PromptBuilder:     newConfigPipelinePromptBuilder(cfg),
			ToolContentFilter: newConfigToolContentFilter(cfg),
		},
		Context: PipelineContextServices{
			Runtime:              al.contextManager,
			BackgroundCompaction: al.backgroundCompactionRunner(),
			ModelExecution:       al.modelExecutionManager(),
			Steering:             al.steering,
			MediaResolver:        al.mediaStore,
		},
		Interaction: PipelineInteractionServices{
			Reasoning:        al.reasoningPublisher(),
			ToolFeedback:     al.toolFeedbackPublisher(),
			SyncToolDelivery: al.syncToolResultDelivery(),
			ToolDelivery:     al.asyncToolCompletionDelivery(),
			Hooks:            al.hooks,
			Fallback:         al.fallback,
			Suspension:       al.humanInteractionRuntime(),
		},
	})
}
