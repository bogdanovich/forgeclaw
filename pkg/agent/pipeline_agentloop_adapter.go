package agent

// NewPipeline creates a Pipeline from an AgentLoop instance.
func NewPipeline(al *AgentLoop) *Pipeline {
	cfg := al.GetConfig()
	return NewPipelineFromDependencies(PipelineDependencies{
		Bus:                  al.bus,
		Cfg:                  cfg,
		ChannelStreaming:     newConfigChannelStreamingProvider(cfg),
		NativeSearch:         newConfigNativeSearchPolicy(cfg),
		ContextRuntime:       al.contextManager,
		BackgroundCompaction: al.backgroundCompactionRunner(),
		Events:               al.runtimeEventEmitter(),
		ActiveRequests:       al.activeRequestCounter(),
		ModelExecution:       al.modelExecutionManager(),
		Steering:             al.steering,
		Reasoning:            al.reasoningPublisher(),
		ToolFeedback:         al.toolFeedbackPublisher(),
		ToolContentFilter:    newConfigToolContentFilter(cfg),
		SyncToolDelivery:     al.syncToolResultDelivery(),
		ToolDelivery:         al.asyncToolCompletionDelivery(),
		TurnControl:          al.turnAbortController(),
		Hooks:                al.hooks,
		Fallback:             al.fallback,
		MediaResolver:        al.mediaStore,
	})
}
