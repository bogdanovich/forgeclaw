package agent

import "github.com/sipeed/picoclaw/pkg/tools"

// AsyncDeliveryDecision is the routing plan for a completed async tool result.
//
// This is intentionally decision-only for now. The current runtime still
// performs delivery in pipeline_execute.go, but all routing policy should flow
// through this type so media, duplicate, timeout, and restart handling can move
// behind the same coordinator boundary later.
type AsyncDeliveryDecision struct {
	DeliveryMode  tools.AsyncDeliveryMode
	PublishToUser bool
	QueueParent   bool
	ContentLen    int
	ForUserLen    int
	MediaCount    int
	IsError       bool
}

func decideAsyncToolResultDelivery(result *tools.ToolResult) AsyncDeliveryDecision {
	decision := AsyncDeliveryDecision{
		DeliveryMode: effectiveAsyncToolResultDelivery(result),
	}
	if result == nil {
		return decision
	}

	content := result.ContentForLLM()
	decision.ContentLen = len(content)
	decision.ForUserLen = len(result.ForUser)
	decision.MediaCount = len(result.Media)
	if result.Completion != nil {
		decision.MediaCount += len(result.Completion.Media)
	}
	decision.IsError = result.IsError

	if decision.DeliveryMode != tools.AsyncDeliveryParentOnly {
		decision.PublishToUser = !result.Silent && result.ForUser != ""
	}
	if decision.DeliveryMode != tools.AsyncDeliveryUserOnly {
		decision.QueueParent = content != ""
	}
	return decision
}

func effectiveAsyncToolResultDelivery(result *tools.ToolResult) tools.AsyncDeliveryMode {
	if result == nil || result.AsyncDelivery == "" {
		return tools.AsyncDeliveryUserAndParent
	}
	return result.AsyncDelivery
}
