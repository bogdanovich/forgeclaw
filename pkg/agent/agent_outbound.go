// PicoClaw - Ultra-lightweight personal AI agent

package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	agentinterfaces "github.com/sipeed/picoclaw/pkg/agent/interfaces"
	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/constants"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/tools"
)

const finalDeliveryFallbackTimeout = 5 * time.Second

type finalResponseDeliveryPolicy uint8

const (
	finalResponseSuppressIfMessageToolSent finalResponseDeliveryPolicy = iota
	finalResponseAlwaysPublish
)

type toolResultDeliveryOutcome uint8

const (
	toolResultDeliveryNone toolResultDeliveryOutcome = iota
	toolResultDeliveryDirect
	toolResultDeliveryQueued
)

func (al *AgentLoop) maybePublishErrorWithPolicy(
	ctx context.Context,
	workspace, agentID string,
	channel, chatID, sessionKey string,
	err error,
	policy finalResponseDeliveryPolicy,
) bool {
	return al.maybePublishErrorWithScopes(
		ctx, workspace, agentID, channel, chatID, sessionKey, err, policy, nil,
	)
}

func (al *AgentLoop) maybePublishErrorWithScopes(
	ctx context.Context,
	workspace, agentID string,
	channel, chatID, sessionKey string,
	err error,
	policy finalResponseDeliveryPolicy,
	traceScopes []runtimeevents.TraceScope,
) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	al.publishResponseWithContextAndScopes(
		ctx,
		workspace,
		agentID,
		channel,
		chatID,
		sessionKey,
		formatUserFacingAgentError(err),
		nil,
		policy,
		traceScopes,
	)
	return true
}

func formatUserFacingAgentError(err error) string {
	if err == nil {
		return "Error processing message."
	}

	base := formatProcessingError(err)
	if strings.TrimSpace(base) == "" {
		base = fmt.Sprintf("Error processing message: %v", err)
	}

	var exhausted *providers.FallbackExhaustedError
	if errors.As(err, &exhausted) && exhausted != nil && len(exhausted.Attempts) > 0 {
		var sb strings.Builder
		sb.WriteString(base)
		sb.WriteString("\n\nFailover details:")
		for i, attempt := range exhausted.Attempts {
			sb.WriteString(fmt.Sprintf(
				"\n%d. %s/%s",
				i+1,
				strings.TrimSpace(attempt.Provider),
				strings.TrimSpace(attempt.Model),
			))
			if attempt.Skipped {
				sb.WriteString(" — skipped")
				if attempt.Error != nil {
					sb.WriteString(": ")
					sb.WriteString(strings.TrimSpace(attempt.Error.Error()))
				}
				continue
			}
			if attempt.Reason != "" {
				sb.WriteString(fmt.Sprintf(" — classification: %s", attempt.Reason))
			}
			if attempt.Error != nil {
				rawErr := attempt.Error
				var failErr *providers.FailoverError
				if errors.As(attempt.Error, &failErr) && failErr != nil && failErr.Wrapped != nil {
					rawErr = failErr.Wrapped
				}
				sb.WriteString("\n   provider error: ")
				sb.WriteString(strings.TrimSpace(rawErr.Error()))
			}
		}
		return sb.String()
	}

	var failErr *providers.FailoverError
	if errors.As(err, &failErr) && failErr != nil {
		var sb strings.Builder
		sb.WriteString(base)
		sb.WriteString(fmt.Sprintf("\n\nFailover classification: %s", failErr.Reason))
		if failErr.Provider != "" || failErr.Model != "" {
			sb.WriteString(fmt.Sprintf(
				"\nFailover target: %s/%s",
				strings.TrimSpace(failErr.Provider),
				strings.TrimSpace(failErr.Model),
			))
		}
		if failErr.Wrapped != nil {
			sb.WriteString("\nProvider error: ")
			sb.WriteString(strings.TrimSpace(failErr.Wrapped.Error()))
		}
		return sb.String()
	}

	return base
}

func (al *AgentLoop) PublishResponseIfNeeded(
	ctx context.Context,
	workspace, agentID, channel, chatID, sessionKey, response string,
) {
	al.publishResponseWithContextIfNeeded(
		ctx,
		workspace,
		agentID,
		channel,
		chatID,
		sessionKey,
		response,
		nil,
		finalResponseSuppressIfMessageToolSent,
	)
}

func (al *AgentLoop) publishResponseWithContextIfNeeded(
	ctx context.Context,
	workspace, agentID string,
	channel, chatID, sessionKey, response string,
	inboundCtx *bus.InboundContext,
	policy finalResponseDeliveryPolicy,
) {
	al.publishResponseWithContextAndScopes(
		ctx, workspace, agentID, channel, chatID, sessionKey, response, inboundCtx, policy, nil,
	)
}

func (al *AgentLoop) publishResponseWithContextAndScopes(
	ctx context.Context,
	workspace, agentID string,
	channel, chatID, sessionKey, response string,
	inboundCtx *bus.InboundContext,
	policy finalResponseDeliveryPolicy,
	traceScopes []runtimeevents.TraceScope,
) {
	if response == "" {
		return
	}

	agent := al.agentForRuntimeScope(newRuntimeSessionScope(workspace, sessionKey), agentID)
	messageToolSentToSameChat := messageToolSentToSameChat(agent, sessionKey, channel, chatID)

	if policy == finalResponseSuppressIfMessageToolSent && messageToolSentToSameChat {
		al.dismissToolFeedbackForSession(ctx, channel, chatID, inboundCtx, sessionKey, traceScopes)
		logger.DebugCF(
			"agent",
			"Skipped outbound (message tool already sent to same chat)",
			map[string]any{"channel": channel, "chat_id": chatID},
		)
		return
	}

	resolvedAgentID := ""
	if agent != nil {
		resolvedAgentID = agent.ID
	}
	msg := bus.OutboundMessage{
		Channel:    channel,
		ChatID:     chatID,
		Context:    outboundContextFromInbound(inboundCtx, channel, chatID, ""),
		AgentID:    resolvedAgentID,
		SessionKey: sessionKey,
		Content:    response,
	}
	if err := bus.SetOutboundTraceScopes(&msg, traceScopes); err != nil {
		logger.ErrorCF("agent", "Rejected aggregated final trace scopes", map[string]any{
			"channel": channel,
			"chat_id": chatID,
			"error":   err.Error(),
		})
		return
	}
	msg.TraceSettlement = len(msg.TraceScopes) > 0
	if policy == finalResponseAlwaysPublish && messageToolSentToSameChat {
		if msg.Context.Raw == nil {
			msg.Context.Raw = make(map[string]string, 1)
		}
		msg.Context.Raw[metadataKeyMessageKind] = messageKindFinalReply
	}
	if sessionKey != "" {
		msg.ContextUsage = computeContextUsage(agent, sessionKey)
	}
	markFinalOutbound(&msg)
	al.bus.PublishOutbound(ctx, msg)
}

func (al *AgentLoop) deliverFinalTurnResult(
	ctx context.Context,
	traceScope runtimeevents.TraceScope,
	agent *AgentInstance,
	opts processOptions,
	result turnResult,
) {
	if al == nil || al.bus == nil || agent == nil {
		return
	}
	if !opts.SendResponse {
		return
	}

	agentID, sessionKey, scope := outboundTurnMetadata(
		agent.ID,
		opts.Dispatch.SessionKey,
		opts.Dispatch.SessionScope,
	)
	outboundCtx := outboundContextFromInbound(
		opts.Dispatch.InboundContext,
		opts.Dispatch.Channel(),
		opts.Dispatch.ChatID(),
		opts.Dispatch.ReplyToMessageID(),
	)
	if result.preferNewOutboundReply || agentMessageToolSentToTurnTarget(agent, sessionKey, opts.Dispatch) {
		outboundCtx = outboundContextWithMessageKind(
			opts.Dispatch.InboundContext,
			opts.Dispatch.Channel(),
			opts.Dispatch.ChatID(),
			opts.Dispatch.ReplyToMessageID(),
			messageKindFinalReply,
		)
	}
	if modelName := strings.TrimSpace(result.modelName); modelName != "" {
		if outboundCtx.Raw == nil {
			outboundCtx.Raw = make(map[string]string, 1)
		}
		outboundCtx.Raw["model_name"] = modelName
	}

	if len(result.completionMedia) > 0 {
		ts := &turnState{
			agent:      agent,
			agentID:    agent.ID,
			turnID:     traceScope.TurnID,
			workspace:  traceScope.Workspace,
			channel:    opts.Dispatch.Channel(),
			chatID:     opts.Dispatch.ChatID(),
			sessionKey: sessionKey,
			opts:       opts,
		}
		outcome, err := al.deliverFinalTurnMedia(ctx, ts, result)
		if err != nil {
			logger.WarnCF("agent", "Failed to deliver final turn media; falling back to text",
				map[string]any{
					"agent_id": agent.ID,
					"channel":  opts.Dispatch.Channel(),
					"chat_id":  opts.Dispatch.ChatID(),
					"error":    err.Error(),
				})
			if !channels.DeliveryDefinitelyNotSent(err) {
				return
			}
		} else if outcome != toolResultDeliveryNone {
			return
		}
	}

	if result.finalContent == "" {
		return
	}
	al.deliverFinalTurnText(
		ctx, traceScope, agent, opts, outboundCtx, agentID, sessionKey, scope, result.finalContent,
	)
}

func (al *AgentLoop) deliverFinalTurnMedia(
	ctx context.Context,
	ts *turnState,
	result turnResult,
) (toolResultDeliveryOutcome, error) {
	mediaResult := (&tools.ToolResult{
		ForLLM:          "Final turn output delivered as media.",
		ForUser:         result.finalContent,
		Silent:          true,
		ResponseHandled: true,
	}).WithCompletion(&tools.CompletionResult{
		Text:  result.finalContent,
		Media: append([]tools.CompletionMedia(nil), result.completionMedia...),
	})
	mediaRefs := completionMediaRefs(result.completionMedia)
	mediaResult.Media = append(mediaResult.Media, mediaRefs...)
	_, outcome, err := al.deliverToolResultToUser(ctx, ts, mediaResult, "final_turn")
	return outcome, err
}

func (al *AgentLoop) deliverFinalTurnText(
	ctx context.Context,
	traceScope runtimeevents.TraceScope,
	agent *AgentInstance,
	opts processOptions,
	outboundCtx bus.InboundContext,
	agentID, sessionKey string,
	scope *bus.OutboundScope,
	content string,
) {
	msg := bus.OutboundMessage{
		Context:      outboundCtx,
		AgentID:      agentID,
		SessionKey:   sessionKey,
		Scope:        scope,
		Content:      content,
		ContextUsage: computeContextUsage(agent, opts.Dispatch.SessionKey),
	}
	if err := bus.SetOutboundTraceScopes(&msg, []runtimeevents.TraceScope{traceScope}); err != nil {
		logger.ErrorCF("agent", "Rejected final turn trace scope", map[string]any{
			"agent_id": agent.ID,
			"error":    err.Error(),
		})
		return
	}
	msg.TraceSettlement = true
	if al.channelManager != nil && opts.Dispatch.Channel() != "" &&
		!constants.IsInternalChannel(opts.Dispatch.Channel()) {
		provisional, ok := al.channelManager.(agentinterfaces.ProvisionalChannelSender)
		if !ok {
			al.publishFinalDeliveryFallback(msg)
			return
		}
		if err := provisional.SendMessageProvisional(ctx, msg); err != nil {
			logger.WarnCF("agent", "Failed to deliver final turn message synchronously; falling back to bus",
				map[string]any{
					"agent_id": agent.ID,
					"channel":  opts.Dispatch.Channel(),
					"chat_id":  opts.Dispatch.ChatID(),
					"error":    err.Error(),
				})
			if !channels.DeliveryDefinitelyNotSent(err) {
				return
			}
		} else {
			return
		}
	}
	al.publishFinalDeliveryFallback(msg)
}

func (al *AgentLoop) publishFinalDeliveryFallback(msg bus.OutboundMessage) {
	if al == nil || al.bus == nil {
		logger.ErrorCF("agent", "Failed to queue final turn fallback", map[string]any{
			"error": "message bus is unavailable",
		})
		return
	}
	deliveryCtx, cancel := context.WithTimeout(context.Background(), finalDeliveryFallbackTimeout)
	defer cancel()
	if err := al.bus.PublishOutbound(deliveryCtx, msg); err != nil {
		logger.ErrorCF("agent", "Failed to queue final turn fallback", map[string]any{
			"channel": msg.Channel,
			"chat_id": msg.ChatID,
			"error":   err.Error(),
		})
	}
}

func (al *AgentLoop) deliverToolResultToUser(
	ctx context.Context,
	ts *turnState,
	result *tools.ToolResult,
	toolName string,
) ([]providers.Attachment, toolResultDeliveryOutcome, error) {
	return al.deliverToolResultToUserWithScopes(ctx, ts, result, toolName, nil)
}

func (al *AgentLoop) deliverToolResultToUserWithScopes(
	ctx context.Context,
	ts *turnState,
	result *tools.ToolResult,
	toolName string,
	traceScopes []runtimeevents.TraceScope,
) ([]providers.Attachment, toolResultDeliveryOutcome, error) {
	if al == nil || ts == nil || result == nil {
		return nil, toolResultDeliveryNone, nil
	}
	if toolName == "final_turn" {
		traceScopes = []runtimeevents.TraceScope{
			runtimeevents.NewTraceScope(ts.workspace, ts.turnID),
		}
	}

	if result.Outbound != nil {
		return al.deliverExplicitToolOutbound(ctx, ts, result, toolName, traceScopes)
	}

	mediaRefs := toolResultMediaRefs(result)
	text := toolResultUserText(result)
	if len(mediaRefs) > 0 {
		parts := al.mediaPartsFromRefs(mediaRefs, result.Completion, text)
		outboundMedia := bus.OutboundMediaMessage{
			Channel: ts.channel,
			ChatID:  ts.chatID,
			Context: outboundContextFromInbound(
				ts.opts.Dispatch.InboundContext,
				ts.channel,
				ts.chatID,
				ts.opts.Dispatch.ReplyToMessageID(),
			),
			AgentID:    ts.agent.ID,
			SessionKey: ts.sessionKey,
			Scope:      outboundScopeFromSessionScope(ts.opts.Dispatch.SessionScope),
			Parts:      parts,
		}
		if err := bus.SetOutboundMediaTraceScopes(&outboundMedia, traceScopes); err != nil {
			return nil, toolResultDeliveryNone, err
		}
		outboundMedia.TraceSettlement = len(outboundMedia.TraceScopes) > 0
		if al.channelManager != nil && ts.channel != "" && !constants.IsInternalChannel(ts.channel) {
			sendMedia := al.channelManager.SendMedia
			if toolName == "final_turn" {
				provisional, ok := al.channelManager.(agentinterfaces.ProvisionalChannelSender)
				if !ok {
					if al.bus != nil {
						if err := al.bus.PublishOutboundMedia(ctx, outboundMedia); err != nil {
							return nil, toolResultDeliveryNone, err
						}
						return nil, toolResultDeliveryQueued, nil
					}
					return nil, toolResultDeliveryNone, nil
				}
				sendMedia = provisional.SendMediaProvisional
			}
			if err := sendMedia(ctx, outboundMedia); err != nil {
				logger.WarnCF("agent", "Failed to deliver tool result media",
					map[string]any{
						"agent_id": ts.agent.ID,
						"tool":     toolName,
						"channel":  ts.channel,
						"chat_id":  ts.chatID,
						"error":    err.Error(),
					})
				return nil, toolResultDeliveryNone, err
			}
			return buildProviderAttachments(al.mediaStore, mediaRefs), toolResultDeliveryDirect, nil
		}
		if al.bus != nil {
			if err := al.bus.PublishOutboundMedia(ctx, outboundMedia); err != nil {
				return nil, toolResultDeliveryNone, err
			}
			return nil, toolResultDeliveryQueued, nil
		}
		return nil, toolResultDeliveryNone, nil
	}

	if strings.TrimSpace(text) == "" {
		return nil, toolResultDeliveryNone, nil
	}
	if result.Silent && result.Completion == nil && result.AsyncDelivery != tools.AsyncDeliveryUserOnly {
		return nil, toolResultDeliveryNone, nil
	}
	if al.bus == nil {
		return nil, toolResultDeliveryNone, nil
	}
	outbound, err := outboundMessageForTraceSettlement(ts, text, traceScopes)
	if err != nil {
		return nil, toolResultDeliveryNone, err
	}
	if err := al.bus.PublishOutbound(ctx, outbound); err != nil {
		return nil, toolResultDeliveryNone, err
	}
	logger.DebugCF("agent", "Sent tool result to user",
		map[string]any{
			"tool":        toolName,
			"content_len": len(text),
		})
	return nil, toolResultDeliveryQueued, nil
}

func (al *AgentLoop) deliverExplicitToolOutbound(
	ctx context.Context,
	ts *turnState,
	result *tools.ToolResult,
	toolName string,
	traceScopes []runtimeevents.TraceScope,
) ([]providers.Attachment, toolResultDeliveryOutcome, error) {
	out := result.Outbound
	if out == nil {
		return nil, toolResultDeliveryNone, nil
	}
	channel := firstNonEmptyString(out.Channel, ts.channel)
	chatID := firstNonEmptyString(out.ChatID, ts.chatID)
	replyToMessageID := firstNonEmptyString(out.ReplyToMessageID, ts.opts.Dispatch.ReplyToMessageID())
	outboundCtx := outboundContextFromInbound(
		ts.opts.Dispatch.InboundContext,
		channel,
		chatID,
		replyToMessageID,
	)
	agentID := ""
	if ts.agent != nil {
		agentID = ts.agent.ID
	}
	if len(out.Media) > 0 {
		outboundMedia := bus.OutboundMediaMessage{
			Channel:    channel,
			ChatID:     chatID,
			Context:    outboundCtx,
			AgentID:    agentID,
			SessionKey: ts.sessionKey,
			Scope:      outboundScopeFromSessionScope(ts.opts.Dispatch.SessionScope),
			Parts:      append([]bus.MediaPart(nil), out.Media...),
		}
		if err := bus.SetOutboundMediaTraceScopes(&outboundMedia, traceScopes); err != nil {
			return nil, toolResultDeliveryNone, err
		}
		outboundMedia.TraceSettlement = len(outboundMedia.TraceScopes) > 0
		if al.channelManager != nil && channel != "" && !constants.IsInternalChannel(channel) {
			if err := al.channelManager.SendMedia(ctx, outboundMedia); err != nil {
				logger.WarnCF("agent", "Failed to deliver explicit tool media",
					map[string]any{
						"agent_id": agentID,
						"tool":     toolName,
						"channel":  channel,
						"chat_id":  chatID,
						"error":    err.Error(),
					})
				return nil, toolResultDeliveryNone, err
			}
			return buildProviderAttachmentsFromMediaParts(out.Media), toolResultDeliveryDirect, nil
		}
		if al.bus != nil {
			if err := al.bus.PublishOutboundMedia(ctx, outboundMedia); err != nil {
				return nil, toolResultDeliveryNone, err
			}
			return nil, toolResultDeliveryQueued, nil
		}
		return nil, toolResultDeliveryNone, nil
	}
	if strings.TrimSpace(out.Text) == "" {
		return nil, toolResultDeliveryNone, nil
	}
	outboundMessage := bus.OutboundMessage{
		Channel:          channel,
		ChatID:           chatID,
		Context:          outboundCtx,
		AgentID:          agentID,
		SessionKey:       ts.sessionKey,
		Scope:            outboundScopeFromSessionScope(ts.opts.Dispatch.SessionScope),
		Content:          out.Text,
		ReplyToMessageID: replyToMessageID,
	}
	if err := bus.SetOutboundTraceScopes(&outboundMessage, traceScopes); err != nil {
		return nil, toolResultDeliveryNone, err
	}
	outboundMessage.TraceSettlement = len(outboundMessage.TraceScopes) > 0
	if al.channelManager != nil && channel != "" && !constants.IsInternalChannel(channel) {
		if err := al.channelManager.SendMessage(ctx, outboundMessage); err != nil {
			return nil, toolResultDeliveryNone, err
		}
		return nil, toolResultDeliveryDirect, nil
	}
	if al.bus != nil {
		if err := al.bus.PublishOutbound(ctx, outboundMessage); err != nil {
			return nil, toolResultDeliveryNone, err
		}
		return nil, toolResultDeliveryQueued, nil
	}
	return nil, toolResultDeliveryNone, nil
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func toolResultUserText(result *tools.ToolResult) string {
	if result == nil {
		return ""
	}
	if text := strings.TrimSpace(result.ForUser); text != "" {
		return result.ForUser
	}
	if result.Completion != nil {
		return result.Completion.Text
	}
	if result.Deliverable != nil {
		return result.Deliverable.Text
	}
	return ""
}

func toolResultMediaRefs(result *tools.ToolResult) []string {
	if result == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(result.Media))
	refs := make([]string, 0, len(result.Media))
	appendRef := func(ref string) {
		ref = strings.TrimSpace(ref)
		if ref == "" {
			return
		}
		if _, ok := seen[ref]; ok {
			return
		}
		seen[ref] = struct{}{}
		refs = append(refs, ref)
	}
	for _, ref := range result.Media {
		appendRef(ref)
	}
	if result.Completion != nil {
		for _, item := range result.Completion.Media {
			appendRef(item.Ref)
		}
	}
	return refs
}

func (al *AgentLoop) mediaPartsFromRefs(
	refs []string,
	completion *tools.CompletionResult,
	caption string,
) []bus.MediaPart {
	hints := make(map[string]tools.CompletionMedia)
	if completion != nil {
		for _, item := range completion.Media {
			ref := strings.TrimSpace(item.Ref)
			if ref != "" {
				hints[ref] = item
			}
		}
	}

	parts := make([]bus.MediaPart, 0, len(refs))
	for i, ref := range refs {
		part := bus.MediaPart{Ref: ref}
		if item, ok := hints[ref]; ok {
			part.Type = item.Type
			part.Filename = item.Filename
			part.ContentType = item.ContentType
		}
		if al != nil && al.mediaStore != nil {
			if _, meta, err := al.mediaStore.ResolveWithMeta(ref); err == nil {
				if part.Filename == "" {
					part.Filename = meta.Filename
				}
				if part.ContentType == "" {
					part.ContentType = meta.ContentType
				}
				if part.Type == "" {
					part.Type = inferMediaType(meta.Filename, meta.ContentType)
				}
			}
		}
		if i == 0 {
			part.Caption = caption
		}
		parts = append(parts, part)
	}
	return parts
}

func messageToolSentToSameChat(
	agent *AgentInstance,
	sessionKey, channel, chatID string,
) bool {
	if strings.TrimSpace(sessionKey) == "" {
		return false
	}
	if agent == nil || agent.Tools == nil {
		return false
	}
	tool, ok := agent.Tools.Get("message")
	if !ok {
		return false
	}
	mt, ok := tool.(*tools.MessageTool)
	return ok && mt.HasSentTo(sessionKey, channel, chatID)
}

func (al *AgentLoop) targetReasoningChannelID(channelName string) (chatID string) {
	return al.reasoningPublisher().targetReasoningChannelID(channelName)
}

func (al *AgentLoop) publishPicoReasoning(
	ctx context.Context,
	reasoningContent, chatID, sessionKey, modelName string,
) {
	al.reasoningPublisher().publishPicoReasoning(ctx, reasoningContent, chatID, sessionKey, modelName)
}

func (al *AgentLoop) publishPicoToolCallInterim(
	ctx context.Context,
	ts *turnState,
	modelName string,
	reasoningContent string,
	content string,
	toolCalls []providers.ToolCall,
) {
	al.reasoningPublisher().publishPicoToolCallInterim(ctx, ts, modelName, reasoningContent, content, toolCalls)
}

func (al *AgentLoop) handleReasoning(
	ctx context.Context,
	reasoningContent, channelName, channelID string,
) {
	al.reasoningPublisher().handleReasoning(ctx, reasoningContent, channelName, channelID)
}
