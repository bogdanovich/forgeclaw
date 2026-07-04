package agent

// NewPipeline creates a Pipeline from an AgentLoop instance.
func NewPipeline(al *AgentLoop) *Pipeline {
	cfg := al.GetConfig()
	return NewPipelineFromDependencies(PipelineDependencies{
		Cfg: cfg,
		Runtime: pipelineRuntimeServices{
			Bus:            al.bus,
			Events:         al.runtimeEventEmitter(),
			ActiveRequests: al.activeRequestCounter(),
			TurnControl:    al.turnAbortController(),
		},
		Config: pipelineConfigServices{
			ChannelStreaming:  newConfigChannelStreamingProvider(cfg),
			NativeSearch:      newConfigNativeSearchPolicy(cfg),
			LLMRetry:          newConfigLLMRetryPolicy(cfg),
			MediaLimits:       newConfigMediaLimitsProvider(cfg),
			FinalTurnRender:   newConfigFinalTurnRenderPolicy(cfg),
			ModelResolution:   newConfigPipelineModelResolution(cfg),
			PromptBuilder:     newConfigPipelinePromptBuilder(cfg),
			ToolContentFilter: newConfigToolContentFilter(cfg),
		},
		Context: pipelineContextServices{
			Runtime:              al.contextManager,
			BackgroundCompaction: al.backgroundCompactionRunner(),
			ModelExecution:       al.modelExecutionManager(),
			Steering:             al.steering,
			MediaResolver:        al.mediaStore,
		},
		Interaction: pipelineInteractionServices{
			Reasoning:        al.reasoningPublisher(),
			ToolFeedback:     al.toolFeedbackPublisher(),
			SyncToolDelivery: al.syncToolResultDelivery(),
			ToolDelivery:     al.asyncToolCompletionDelivery(),
			Hooks:            al.hooks,
			Fallback:         al.fallback,
		},
	})
}
