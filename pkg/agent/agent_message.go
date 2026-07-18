// PicoClaw - Ultra-lightweight personal AI agent

package agent

import (
	"context"
	"fmt"
	"strings"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/constants"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/routing"
	"github.com/sipeed/picoclaw/pkg/session"
	"github.com/sipeed/picoclaw/pkg/utils"
)

func (al *AgentLoop) buildContinuationTarget(msg bus.InboundMessage) (*continuationTarget, error) {
	if msg.Channel == "system" {
		return nil, nil
	}

	route, _, err := al.resolveMessageRoute(msg)
	if err != nil {
		return nil, err
	}
	allocation := al.allocateRouteSession(route, msg)
	allocation, err = al.applySessionLifecycle(allocation, route.SessionPolicy.Lifecycle)
	if err != nil {
		return nil, err
	}

	return &continuationTarget{
		SessionKey: al.resolveEffectiveSessionKey(
			allocation.RouteScopeKey,
			allocation.SessionKey,
			msg.SessionKey,
		),
		Channel: msg.Channel,
		ChatID:  msg.ChatID,
	}, nil
}

func (al *AgentLoop) ProcessDirect(
	ctx context.Context,
	content, sessionKey string,
) (string, error) {
	return al.ProcessDirectWithChannel(ctx, content, sessionKey, "cli", "direct")
}

func (al *AgentLoop) ProcessDirectWithChannel(
	ctx context.Context,
	content, sessionKey, channel, chatID string,
) (string, error) {
	return al.processDirectWithChannel(ctx, content, sessionKey, channel, chatID, false)
}

func (al *AgentLoop) ProcessScheduledWithChannel(
	ctx context.Context,
	content, sessionKey, channel, chatID string,
) (string, error) {
	return al.processDirectWithChannel(ctx, content, sessionKey, channel, chatID, true)
}

func (al *AgentLoop) processDirectWithChannel(
	ctx context.Context,
	content, sessionKey, channel, chatID string,
	scheduled bool,
) (string, error) {
	if err := al.ensureHooksInitialized(ctx); err != nil {
		return "", err
	}
	if err := al.ensureMCPInitialized(ctx); err != nil {
		return "", err
	}

	msg := bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  channel,
			ChatID:   chatID,
			ChatType: "direct",
			SenderID: "cron",
		},
		Content:    content,
		SessionKey: sessionKey,
	}
	if scheduled {
		return al.processScheduledMessage(ctx, msg)
	}

	return al.processMessage(ctx, msg)
}

func (al *AgentLoop) processScheduledMessage(ctx context.Context, msg bus.InboundMessage) (string, error) {
	msg = al.prepareInboundMessageForAgent(ctx, msg)
	route, agent, routeErr := al.resolveMessageRoute(msg)
	if routeErr != nil {
		return "", routeErr
	}
	allocation := al.allocateRouteSession(route, msg)
	allocation, routeErr = al.applySessionLifecycle(allocation, route.SessionPolicy.Lifecycle)
	if routeErr != nil {
		return "", routeErr
	}
	sessionKey := al.resolveEffectiveSessionKey(
		allocation.RouteScopeKey,
		allocation.SessionKey,
		msg.SessionKey,
	)
	modelBinding := al.bindEffectiveModel(allocation.RouteScopeKey, agent)
	defer modelBinding.Cleanup()

	if tool, ok := agent.Tools.Get("message"); ok {
		if resetter, ok := tool.(interface{ ResetSentInRound(sessionKey string) }); ok {
			resetter.ResetSentInRound(sessionKey)
		}
	}

	return al.runAgentLoop(ctx, agent, processOptions{
		Dispatch: DispatchRequest{
			RouteSessionKey: allocation.RouteScopeKey,
			BaseSessionKey:  allocation.SessionKey,
			SessionKey:      sessionKey,
			SessionAliases:  buildSessionAliases(sessionKey, append(allocation.SessionAliases, msg.SessionKey)...),
			InboundContext:  cloneInboundContext(&msg.Context),
			RouteResult:     cloneResolvedRoute(&route),
			SessionScope:    session.CloneScope(&allocation.Scope),
			UserMessage:     msg.Content,
			Media:           append([]string(nil), msg.Media...),
		},
		ModelBinding:         modelBinding,
		SenderID:             msg.SenderID,
		SenderDisplayName:    msg.Sender.DisplayName,
		DefaultResponse:      defaultResponse,
		EnableSummary:        false,
		SendResponse:         false,
		SuppressToolFeedback: true,
		NoHistory:            true,
	})
}

func (al *AgentLoop) ProcessHeartbeat(
	ctx context.Context,
	content, channel, chatID string,
) (string, error) {
	if err := al.ensureHooksInitialized(ctx); err != nil {
		return "", err
	}
	if err := al.ensureMCPInitialized(ctx); err != nil {
		return "", err
	}

	agent := al.GetRegistry().GetDefaultAgent()
	if agent == nil {
		return "", fmt.Errorf("no default agent for heartbeat")
	}
	dispatch := DispatchRequest{
		SessionKey:  "heartbeat",
		UserMessage: content,
	}
	if channel != "" || chatID != "" {
		dispatch.InboundContext = &bus.InboundContext{
			Channel:  channel,
			ChatID:   chatID,
			ChatType: "direct",
			SenderID: "heartbeat",
		}
	}
	return al.runAgentLoop(ctx, agent, processOptions{
		Dispatch:             dispatch,
		DefaultResponse:      defaultResponse,
		EnableSummary:        false,
		SendResponse:         false,
		SuppressToolFeedback: true,
		NoHistory:            true, // Don't load session history for heartbeat
	})
}

func (al *AgentLoop) prepareInboundMessageForAgent(
	ctx context.Context,
	msg bus.InboundMessage,
) bus.InboundMessage {
	msg = bus.NormalizeInboundMessage(msg)

	var hadAudio bool
	msg, hadAudio = al.transcribeAudioInMessage(ctx, msg)

	// For audio messages the placeholder was deferred by the channel.
	// Now that transcription (and optional feedback) is done, send it.
	if hadAudio && al.channelManager != nil {
		al.channelManager.SendPlaceholder(ctx, msg.Channel, msg.ChatID)
	}

	return msg
}

func (al *AgentLoop) processMessage(ctx context.Context, msg bus.InboundMessage) (string, error) {
	turn, turnErr := al.buildInboundMessageTurn(ctx, msg)
	if turnErr != nil {
		return "", turnErr
	}
	return al.processInboundMessageTurn(ctx, turn)
}

func (al *AgentLoop) processInboundMessageTurn(
	ctx context.Context,
	turn inboundMessageTurn,
) (string, error) {
	msg := turn.Message
	msg.SessionKey = turn.SessionKey

	// Add message preview to log (show full content for error messages)
	var logContent string
	if strings.Contains(msg.Content, "Error:") || strings.Contains(msg.Content, "error") {
		logContent = msg.Content // Full content for errors
	} else {
		logContent = utils.Truncate(msg.Content, 80)
	}
	logger.InfoCF(
		"agent",
		fmt.Sprintf("Processing message from %s:%s: %s", msg.Channel, msg.SenderID, logContent),
		map[string]any{
			"channel":     msg.Channel,
			"chat_id":     msg.ChatID,
			"sender_id":   msg.SenderID,
			"session_key": msg.SessionKey,
		},
	)

	// Route system messages to processSystemMessage
	if msg.Channel == "system" {
		return al.processSystemMessage(ctx, msg)
	}

	defer turn.Cleanup()
	turn.resetMessageToolRound()

	logger.InfoCF("agent", "Routed message",
		map[string]any{
			"agent_id":           turn.Agent.ID,
			"effective_agent_id": turn.ModelBinding.ExecutionState().AgentID,
			"scope_key":          turn.ScopeKey,
			"session_key":        turn.SessionKey,
			"matched_by":         turn.Options.Dispatch.RouteResult.MatchedBy,
			"route_agent":        turn.Options.Dispatch.RouteResult.AgentID,
			"route_channel":      turn.Options.Dispatch.RouteResult.Channel,
			"route_main_session": turn.Options.Dispatch.RouteSessionKey,
			"session_epoch":      sessionEpochID(turn.Options.Dispatch.SessionScope),
		})

	opts := turn.Options
	var err error
	opts, err = resolveTurnProfileOptions(al.GetConfig(), opts)
	if err != nil {
		return "", err
	}

	// context-dependent commands check their own Runtime fields and report
	// "unavailable" when the required capability is nil.
	if response, handled := al.handleCommand(ctx, msg, turn.ModelBinding, &opts); handled {
		return response, nil
	}

	if pending := al.takePendingSkills(opts.Dispatch.SessionKey); len(pending) > 0 {
		opts.ForcedSkills = append(opts.ForcedSkills, pending...)
		logger.InfoCF("agent", "Applying pending skill override",
			map[string]any{
				"session_key": opts.Dispatch.SessionKey,
				"skills":      strings.Join(pending, ","),
			})
	}

	return al.runAgentLoop(ctx, turn.Agent, opts)
}

func (al *AgentLoop) observeMessage(ctx context.Context, msg bus.ObservedMessage) {
	msg = bus.NormalizeObservedMessage(msg)
	if strings.TrimSpace(msg.Content) == "" && len(msg.Media) == 0 {
		return
	}

	inbound := bus.InboundMessage{
		Context:    msg.Context,
		Sender:     msg.Sender,
		Content:    msg.Content,
		Media:      append([]string(nil), msg.Media...),
		MediaScope: msg.MediaScope,
		SessionKey: msg.SessionKey,
	}
	route, agent, routeErr := al.resolveMessageRoute(inbound)
	if routeErr != nil {
		logger.WarnCF("agent", "Failed to route observed message", map[string]any{
			"channel": msg.Channel,
			"chat_id": msg.ChatID,
			"error":   routeErr.Error(),
		})
		return
	}

	allocation := al.allocateRouteSession(route, inbound)
	allocation, routeErr = al.applySessionLifecycle(allocation, route.SessionPolicy.Lifecycle)
	if routeErr != nil {
		logger.WarnCF("agent", "Failed to apply session lifecycle for observed message", map[string]any{
			"channel": msg.Channel,
			"chat_id": msg.ChatID,
			"error":   routeErr.Error(),
		})
		return
	}
	sessionKey := al.resolveEffectiveSessionKey(
		allocation.RouteScopeKey,
		allocation.SessionKey,
		msg.SessionKey,
	)
	ensureSessionMetadata(
		agent.Sessions,
		sessionKey,
		session.CloneScope(&allocation.Scope),
		buildSessionAliases(
			sessionKey,
			sessionAliasCandidates(
				allocation.SessionKey,
				sessionKey,
				allocation.SessionAliases,
				msg.SessionKey,
			)...),
	)

	content := formatObservedMessageContent(msg)
	record := providers.Message{
		Role:    "user",
		Content: content,
		Media:   append([]string(nil), msg.Media...),
	}
	var writeErr error
	if len(record.Media) > 0 {
		writeErr = persistFullSessionMessage(agent.Sessions, sessionKey, record)
	} else {
		writeErr = persistSessionMessage(agent.Sessions, sessionKey, record.Role, record.Content)
	}
	if writeErr != nil {
		logger.WarnCF("agent", "Failed to persist observed message", map[string]any{
			"session_key": sessionKey,
			"error":       writeErr.Error(),
		})
	} else if err := agent.Sessions.Save(sessionKey); err != nil {
		logger.WarnCF("agent", "Failed to save observed message", map[string]any{
			"session_key": sessionKey,
			"error":       err.Error(),
		})
	}
	if al.contextManager != nil {
		if err := al.contextManager.Ingest(ctx, &IngestRequest{
			SessionKey:        sessionKey,
			Message:           record,
			CanonicalWriteErr: writeErr,
		}); err != nil {
			logger.WarnCF("agent", "Context manager ingest failed for observed message", map[string]any{
				"session_key": sessionKey,
				"error":       err.Error(),
			})
		}
	}
	logger.DebugCF("agent", "Observed passive message", map[string]any{
		"agent_id":    agent.ID,
		"session_key": sessionKey,
		"channel":     msg.Channel,
		"chat_id":     msg.ChatID,
		"reason":      msg.Reason,
	})
}

func formatObservedMessageContent(msg bus.ObservedMessage) string {
	reason := strings.TrimSpace(msg.Reason)
	if reason == "" {
		reason = "passive context"
	}
	author := strings.TrimSpace(msg.Sender.DisplayName)
	if author == "" {
		author = strings.TrimSpace(msg.Sender.Username)
	}
	if author == "" {
		author = strings.TrimSpace(msg.SenderID)
	}
	content := strings.TrimSpace(msg.Content)
	return fmt.Sprintf("[observed group message from %s; no reply requested; reason: %s]\n%s", author, reason, content)
}

func (al *AgentLoop) resolveMessageRoute(msg bus.InboundMessage) (routing.ResolvedRoute, *AgentInstance, error) {
	registry := al.GetRegistry()
	inboundCtx := normalizedInboundContext(msg)
	route := registry.ResolveRoute(inboundCtx)

	agent, ok := registry.GetAgent(route.AgentID)
	if !ok {
		agent = registry.GetDefaultAgent()
	}
	if agent == nil {
		return routing.ResolvedRoute{}, nil, fmt.Errorf("no agent available for route (agent_id=%s)", route.AgentID)
	}

	return route, agent, nil
}

func (al *AgentLoop) allocateRouteSession(route routing.ResolvedRoute, msg bus.InboundMessage) session.Allocation {
	return session.AllocateRouteSession(session.AllocationInput{
		AgentID:       route.AgentID,
		Context:       normalizedInboundContext(msg),
		SessionPolicy: route.SessionPolicy,
	})
}

func (al *AgentLoop) processSystemMessage(
	ctx context.Context,
	msg bus.InboundMessage,
) (string, error) {
	if msg.Channel != "system" {
		return "", fmt.Errorf(
			"processSystemMessage called with non-system message channel: %s",
			msg.Channel,
		)
	}

	logger.InfoCF("agent", "Processing system message",
		map[string]any{
			"sender_id": msg.SenderID,
			"chat_id":   msg.ChatID,
		})

	// Parse origin channel from chat_id (format: "channel:chat_id")
	var originChannel, originChatID string
	if idx := strings.Index(msg.ChatID, ":"); idx > 0 {
		originChannel = msg.ChatID[:idx]
		originChatID = msg.ChatID[idx+1:]
	} else {
		originChannel = "cli"
		originChatID = msg.ChatID
	}
	originChatType := "direct"
	originTopicID := strings.TrimSpace(msg.Context.TopicID)
	originMessageID := strings.TrimSpace(msg.Context.MessageID)
	originReplyToMessageID := strings.TrimSpace(msg.Context.ReplyToMessageID)
	if raw := msg.Context.Raw; len(raw) > 0 {
		if value := strings.TrimSpace(raw[systemFollowUpOriginChannelKey]); value != "" {
			originChannel = value
		}
		if value := strings.TrimSpace(raw[systemFollowUpOriginChatIDKey]); value != "" {
			originChatID = value
		}
		if value := strings.TrimSpace(raw[systemFollowUpOriginChatTypeKey]); value != "" {
			originChatType = value
		}
		if value := strings.TrimSpace(raw[systemFollowUpOriginTopicIDKey]); value != "" {
			originTopicID = value
		}
		if value := strings.TrimSpace(raw[systemFollowUpOriginMessageIDKey]); value != "" {
			originMessageID = value
		}
		if value := strings.TrimSpace(raw[systemFollowUpOriginReplyToMessageIDKey]); value != "" {
			originReplyToMessageID = value
		}
	}

	if isAsyncCompletionSystemMessage(msg) {
		return al.processAsyncCompletionMessage(ctx, msg, bus.InboundContext{
			Channel:          originChannel,
			ChatID:           originChatID,
			ChatType:         originChatType,
			TopicID:          originTopicID,
			SenderID:         msg.SenderID,
			MessageID:        originMessageID,
			ReplyToMessageID: originReplyToMessageID,
		})
	}

	// Extract subagent result from message content
	// Format: "Task 'label' completed.\n\nResult:\n<actual content>"
	content := msg.Content
	if idx := strings.Index(content, "Result:\n"); idx >= 0 {
		content = content[idx+8:] // Extract just the result part
	}

	// Skip internal channels - only log, don't send to user
	if constants.IsInternalChannel(originChannel) {
		logger.InfoCF("agent", "Subagent completed (internal channel)",
			map[string]any{
				"sender_id":   msg.SenderID,
				"content_len": len(content),
				"channel":     originChannel,
			})
		return "", nil
	}

	// Use default agent for system messages
	agent := al.GetRegistry().GetDefaultAgent()
	if agent == nil {
		return "", fmt.Errorf("no default agent for system message")
	}

	// Use the origin session for context
	sessionKey := session.BuildMainSessionKey(agent.ID)
	dispatch := DispatchRequest{
		SessionKey:  sessionKey,
		UserMessage: fmt.Sprintf("[System: %s] %s", msg.SenderID, msg.Content),
	}
	if originChannel != "" || originChatID != "" {
		dispatch.InboundContext = &bus.InboundContext{
			Channel:          originChannel,
			ChatID:           originChatID,
			ChatType:         originChatType,
			TopicID:          originTopicID,
			SenderID:         msg.SenderID,
			MessageID:        originMessageID,
			ReplyToMessageID: originReplyToMessageID,
		}
	}

	return al.runAgentLoop(ctx, agent, processOptions{
		Dispatch:        dispatch,
		DefaultResponse: "Background task completed.",
		EnableSummary:   false,
		SendResponse:    true,
	})
}

func (al *AgentLoop) processAsyncCompletionMessage(
	ctx context.Context,
	msg bus.InboundMessage,
	origin bus.InboundContext,
) (response string, err error) {
	return al.processAsyncCompletion(ctx, AsyncCompletionInput{
		SourceTool:   strings.TrimPrefix(strings.TrimSpace(msg.SenderID), "async:"),
		CompletionID: strings.TrimSpace(msg.Context.Raw[systemFollowUpIDKey]),
		Content:      msg.Content,
		Origin:       origin,
		SenderID:     msg.SenderID,
	})
}

func (al *AgentLoop) processAsyncCompletion(
	ctx context.Context,
	input AsyncCompletionInput,
) (response string, err error) {
	origin := input.Origin
	if constants.IsInternalChannel(origin.Channel) {
		logger.InfoCF("agent", "Async completion received for internal channel",
			map[string]any{
				"sender_id":   input.SenderID,
				"content_len": len(input.Content),
				"channel":     origin.Channel,
			})
		return "", nil
	}

	agent := al.GetRegistry().GetDefaultAgent()
	if agent == nil {
		return "", fmt.Errorf("no default agent for async completion")
	}

	completionID := strings.TrimSpace(input.CompletionID)
	if completionID != "" {
		if _, loaded := al.asyncCompletions.LoadOrStore(completionID, struct{}{}); loaded {
			logger.InfoCF("agent", "Skipping duplicate async completion",
				map[string]any{
					"completion_id": completionID,
					"sender_id":     input.SenderID,
				})
			return "", nil
		}
		defer func() {
			if err != nil {
				al.asyncCompletions.LoadAndDelete(completionID)
			}
		}()
	}

	sessionKey := session.BuildMainSessionKey(agent.ID)
	dispatch := DispatchRequest{
		SessionKey:     sessionKey,
		UserMessage:    input.Content,
		InboundContext: &origin,
	}

	runCtx, cancel := context.WithTimeout(ctx, asyncCompletionSynthesisTimeout)
	defer cancel()

	return al.runAgentLoop(runCtx, agent, processOptions{
		Dispatch:             dispatch,
		DefaultResponse:      "",
		EnableSummary:        false,
		SendResponse:         true,
		SuppressToolFeedback: true,
		NoHistory:            true,
	})
}
