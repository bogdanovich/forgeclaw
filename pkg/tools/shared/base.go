package toolshared

import (
	"context"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/session"
	"github.com/sipeed/picoclaw/pkg/tools/loopguard"
)

// Tool is the interface that all tools must implement.
type Tool interface {
	Name() string
	Description() string
	Parameters() map[string]any
	Execute(ctx context.Context, args map[string]any) *ToolResult
}

// LoopSemanticsProvider explicitly classifies tool side-effect behavior for
// loop detection. Tools without this optional capability remain unknown.
type LoopSemanticsProvider interface {
	ToolLoopSemantics() loopguard.Semantics
}

const (
	ToolPromptLayerCapability = "capability"
	ToolPromptSlotTooling     = "tooling"
	ToolPromptSlotMCP         = "mcp"
	ToolPromptSourceRegistry  = "tool_registry:native"
	ToolPromptSourceDiscovery = "tool_registry:discovery"
)

type PromptMetadata struct {
	Layer  string
	Slot   string
	Source string
}

type PromptMetadataProvider interface {
	PromptMetadata() PromptMetadata
}

// --- Request-scoped tool context (channel / chatID) ---
//
// Carried via context.Value so that concurrent tool calls each receive
// their own immutable copy — no mutable state on singleton tool instances.
//
// Keys are unexported pointer-typed vars — guaranteed collision-free,
// and only accessible through the helper functions below.

type toolCtxKey struct{ name string }

var (
	ctxKeyChannel          = &toolCtxKey{"channel"}
	ctxKeyChatID           = &toolCtxKey{"chatID"}
	ctxKeyTopicID          = &toolCtxKey{"topicID"}
	ctxKeyMessageID        = &toolCtxKey{"messageID"}
	ctxKeyReplyToMessageID = &toolCtxKey{"replyToMessageID"}
	ctxKeyInboundContext   = &toolCtxKey{"inboundContext"}
	ctxKeyAgentID          = &toolCtxKey{"agentID"}
	ctxKeySessionKey       = &toolCtxKey{"sessionKey"}
	ctxKeyRouteSessionKey  = &toolCtxKey{"routeSessionKey"}
	ctxKeySessionScope     = &toolCtxKey{"sessionScope"}
)

// WithToolContext returns a child context carrying channel and chatID.
func WithToolContext(ctx context.Context, channel, chatID string) context.Context {
	ctx = context.WithValue(ctx, ctxKeyChannel, channel)
	ctx = context.WithValue(ctx, ctxKeyChatID, chatID)
	return ctx
}

// WithToolTopicID returns a child context carrying the inbound topic/thread id.
func WithToolTopicID(ctx context.Context, topicID string) context.Context {
	ctx = context.WithValue(ctx, ctxKeyTopicID, topicID)
	return ctx
}

// WithToolMessageContext returns a child context carrying inbound message IDs.
func WithToolMessageContext(ctx context.Context, messageID, replyToMessageID string) context.Context {
	ctx = context.WithValue(ctx, ctxKeyMessageID, messageID)
	ctx = context.WithValue(ctx, ctxKeyReplyToMessageID, replyToMessageID)
	return ctx
}

// WithToolInboundContext returns a child context carrying channel/chat and inbound IDs.
func WithToolInboundContext(
	ctx context.Context,
	channel, chatID, messageID, replyToMessageID string,
) context.Context {
	ctx = WithToolContext(ctx, channel, chatID)
	ctx = WithToolMessageContext(ctx, messageID, replyToMessageID)
	ctx = WithToolInboundMetadata(ctx, bus.InboundContext{
		Channel:          channel,
		ChatID:           chatID,
		MessageID:        messageID,
		ReplyToMessageID: replyToMessageID,
	})
	return ctx
}

// WithToolInboundMetadata returns a child context carrying the full normalized
// inbound identity/source metadata available to tools.
func WithToolInboundMetadata(ctx context.Context, inbound bus.InboundContext) context.Context {
	return context.WithValue(ctx, ctxKeyInboundContext, cloneToolInboundContext(bus.NormalizeInboundContext(inbound)))
}

// WithToolSessionContext returns a child context carrying turn-scoped session metadata.
func WithToolSessionContext(
	ctx context.Context,
	agentID, sessionKey string,
	scope *session.SessionScope,
) context.Context {
	ctx = context.WithValue(ctx, ctxKeyAgentID, agentID)
	ctx = context.WithValue(ctx, ctxKeySessionKey, sessionKey)
	ctx = context.WithValue(ctx, ctxKeySessionScope, session.CloneScope(scope))
	return ctx
}

// WithToolRouteSessionKey carries the canonical routed conversation key used
// for durable session-scoped state. It can differ from ToolSessionKey after a
// user starts a fresh history session through /new or /reset.
func WithToolRouteSessionKey(ctx context.Context, routeSessionKey string) context.Context {
	return context.WithValue(ctx, ctxKeyRouteSessionKey, routeSessionKey)
}

// ToolChannel extracts the channel from ctx, or "" if unset.
func ToolChannel(ctx context.Context) string {
	v, ok := ctx.Value(ctxKeyChannel).(string)
	if !ok {
		return ""
	}
	return v
}

// ToolChatID extracts the chatID from ctx, or "" if unset.
func ToolChatID(ctx context.Context) string {
	v, ok := ctx.Value(ctxKeyChatID).(string)
	if !ok {
		return ""
	}
	return v
}

// ToolTopicID extracts the inbound topic/thread id from ctx, or "" if unset.
func ToolTopicID(ctx context.Context) string {
	v, ok := ctx.Value(ctxKeyTopicID).(string)
	if !ok {
		return ""
	}
	return v
}

// ToolMessageID extracts the current inbound message ID from ctx, or "" if unset.
func ToolMessageID(ctx context.Context) string {
	v, ok := ctx.Value(ctxKeyMessageID).(string)
	if !ok {
		return ""
	}
	return v
}

// ToolReplyToMessageID extracts the current inbound reply target from ctx, or "" if unset.
func ToolReplyToMessageID(ctx context.Context) string {
	v, ok := ctx.Value(ctxKeyReplyToMessageID).(string)
	if !ok {
		return ""
	}
	return v
}

// ToolInboundContext extracts the full inbound metadata from ctx, or zero value if unset.
func ToolInboundContext(ctx context.Context) bus.InboundContext {
	v, ok := ctx.Value(ctxKeyInboundContext).(bus.InboundContext)
	if !ok {
		return bus.InboundContext{}
	}
	return cloneToolInboundContext(v)
}

func cloneToolInboundContext(inbound bus.InboundContext) bus.InboundContext {
	inbound.ReplyHandles = cloneToolStringMap(inbound.ReplyHandles)
	inbound.Raw = cloneToolStringMap(inbound.Raw)
	return inbound
}

func cloneToolStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// ToolSenderID extracts the transport sender ID from ctx.
func ToolSenderID(ctx context.Context) string {
	return ToolInboundContext(ctx).SenderID
}

// ToolActorID extracts the effective actor ID from ctx. It usually equals sender ID.
func ToolActorID(ctx context.Context) string {
	return ToolInboundContext(ctx).ActorID
}

// ToolOriginID extracts the optional content origin ID from ctx.
func ToolOriginID(ctx context.Context) string {
	return ToolInboundContext(ctx).OriginID
}

// ToolOriginType extracts the optional content origin type from ctx.
func ToolOriginType(ctx context.Context) string {
	return ToolInboundContext(ctx).OriginType
}

// ToolSourceRef extracts the stable inbound source reference from ctx.
func ToolSourceRef(ctx context.Context) string {
	return ToolInboundContext(ctx).SourceRef
}

// ToolAgentID extracts the active turn's agent ID from ctx, or "" if unset.
func ToolAgentID(ctx context.Context) string {
	v, ok := ctx.Value(ctxKeyAgentID).(string)
	if !ok {
		return ""
	}
	return v
}

// ToolSessionKey extracts the active turn's session key from ctx, or "" if unset.
func ToolSessionKey(ctx context.Context) string {
	v, ok := ctx.Value(ctxKeySessionKey).(string)
	if !ok {
		return ""
	}
	return v
}

// ToolRouteSessionKey extracts the canonical routed conversation key from ctx.
func ToolRouteSessionKey(ctx context.Context) string {
	v, ok := ctx.Value(ctxKeyRouteSessionKey).(string)
	if !ok {
		return ""
	}
	return v
}

// ToolSessionScope extracts the active turn's structured session scope from ctx.
func ToolSessionScope(ctx context.Context) *session.SessionScope {
	scope, ok := ctx.Value(ctxKeySessionScope).(*session.SessionScope)
	if !ok {
		return nil
	}
	return session.CloneScope(scope)
}

// AsyncCallback is a function type that async tools use to notify completion.
// When an async tool finishes its work, it calls this callback with the result.
//
// The ctx parameter allows the callback to be canceled if the agent is shutting down.
// The result parameter contains the tool's execution result.
type AsyncCallback func(ctx context.Context, result *ToolResult)

// AsyncExecutor is an optional interface that tools can implement to support
// asynchronous execution with completion callbacks.
//
// Unlike the old AsyncTool pattern (SetCallback + Execute), AsyncExecutor
// receives the callback as a parameter of ExecuteAsync. This eliminates the
// data race where concurrent calls could overwrite each other's callbacks
// on a shared tool instance.
//
// This is useful for:
//   - Long-running operations that shouldn't block the agent loop
//   - Subagent spawns that complete independently
//   - Background tasks that need to report results later
//
// Example:
//
//	func (t *SpawnTool) ExecuteAsync(ctx context.Context, args map[string]any, cb AsyncCallback) *ToolResult {
//	    go func() {
//	        result := t.runSubagent(ctx, args)
//	        if cb != nil { cb(ctx, result) }
//	    }()
//	    return AsyncResult("Subagent spawned, will report back")
//	}
type AsyncExecutor interface {
	Tool
	// ExecuteAsync runs the tool asynchronously. The callback cb will be
	// invoked (possibly from another goroutine) when the async operation
	// completes. cb is guaranteed to be non-nil by the caller (registry).
	ExecuteAsync(ctx context.Context, args map[string]any, cb AsyncCallback) *ToolResult
}

func ToolToSchema(tool Tool) map[string]any {
	return map[string]any{
		"type": "function",
		"function": map[string]any{
			"name":        tool.Name(),
			"description": tool.Description(),
			"parameters":  tool.Parameters(),
		},
	}
}
