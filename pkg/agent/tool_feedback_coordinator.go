package agent

import (
	"context"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/agent/interfaces"
	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/utils"
)

type toolFeedbackPublisher struct {
	bus                 interfaces.MessageBus
	cfg                 *config.Config
	channelManager      interfaces.ChannelManager
	getFeedbackOverride func(routeSessionKey string) (bool, bool)
}

func (al *AgentLoop) toolFeedbackPublisher() *toolFeedbackPublisher {
	if al == nil {
		return nil
	}
	return &toolFeedbackPublisher{
		bus:                 al.bus,
		cfg:                 al.GetConfig(),
		channelManager:      al.channelManager,
		getFeedbackOverride: al.getToolFeedbackOverride,
	}
}

func (tf *toolFeedbackPublisher) publishToolFeedbackForCall(
	ctx context.Context,
	ts *turnState,
	response *providers.LLMResponse,
	toolCall providers.ToolCall,
	toolName string,
	toolArgs map[string]any,
	messages []providers.Message,
) {
	if tf == nil || tf.bus == nil || !tf.shouldPublishToolFeedback(ts) || ts.channel == "pico" {
		return
	}
	toolFeedbackMaxLen := tf.toolFeedbackMaxArgsLength()
	toolFeedbackExplanation := toolFeedbackExplanationForToolCall(
		response,
		toolCall,
		messages,
	)
	toolArgsPreview := toolFeedbackArgsPreview(toolArgs, toolFeedbackMaxLen)
	toolFeedbackStyle := tf.toolFeedbackStyle()
	feedbackMsg := utils.FormatToolFeedbackMessageWithStyle(
		toolFeedbackStyle,
		toolName,
		toolFeedbackExplanation,
		toolArgsPreview,
	)
	if title := toolFeedbackTitleForTurn(ts); title != "" {
		feedbackMsg = utils.FormatToolFeedbackMessageWithStyleAndTitle(
			toolFeedbackStyle,
			title,
			toolName,
			toolFeedbackExplanation,
			toolArgsPreview,
		)
	}
	fbCtx, fbCancel := context.WithTimeout(ctx, 3*time.Second)
	_ = tf.bus.PublishOutbound(fbCtx, outboundMessageForTurnWithOptions(
		ts,
		feedbackMsg,
		outboundTurnMessageOptions{kind: messageKindToolFeedback},
	))
	fbCancel()
}

func (tf *toolFeedbackPublisher) dismissToolFeedbackForTurn(ctx context.Context, ts *turnState) {
	if tf == nil || tf.channelManager == nil || ts == nil || ts.channel == "" {
		return
	}
	tf.channelManager.DismissToolFeedback(
		ctx,
		ts.channel,
		ts.chatID,
		ts.opts.InboundContext,
		[]runtimeevents.TraceScope{runtimeevents.NewTraceScope(ts.workspace, ts.turnID)},
	)
}

func (al *AgentLoop) dismissToolFeedbackForSession(
	ctx context.Context,
	channel string,
	chatID string,
	inboundCtx *bus.InboundContext,
	sessionKey string,
	traceScopes []runtimeevents.TraceScope,
) {
	al.toolFeedbackPublisher().dismissToolFeedbackForSession(
		ctx, channel, chatID, inboundCtx, sessionKey, traceScopes,
	)
}

func (tf *toolFeedbackPublisher) dismissToolFeedbackForSession(
	ctx context.Context,
	channel string,
	chatID string,
	inboundCtx *bus.InboundContext,
	sessionKey string,
	traceScopes []runtimeevents.TraceScope,
) {
	if tf == nil || tf.channelManager == nil || channel == "" || chatID == "" {
		return
	}
	dismissCtx, dismissCancel := context.WithTimeout(ctx, 5*time.Second)
	tf.channelManager.DismissToolFeedbackForSession(
		dismissCtx,
		channel,
		chatID,
		inboundCtx,
		sessionKey,
		traceScopes,
	)
	dismissCancel()
}

func (tf *toolFeedbackPublisher) shouldPublishToolFeedback(ts *turnState) bool {
	if tf == nil || ts == nil || ts.channel == "" || ts.opts.SuppressToolFeedback {
		return false
	}
	routeSessionKey := strings.TrimSpace(ts.opts.Dispatch.RouteSessionKey)
	if routeSessionKey != "" && tf.getFeedbackOverride != nil {
		if enabled, ok := tf.getFeedbackOverride(routeSessionKey); ok {
			if !enabled {
				return false
			}
			cfg := tf.cfg
			if cfg != nil && strings.HasPrefix(strings.TrimSpace(ts.sessionKey), "subturn-") &&
				!cfg.Agents.Defaults.IsSubagentToolFeedbackEnabled() {
				return false
			}
			return true
		}
	}
	cfg := tf.cfg
	if cfg == nil || !cfg.Agents.Defaults.IsToolFeedbackEnabled() {
		return false
	}
	if strings.HasPrefix(strings.TrimSpace(ts.sessionKey), "subturn-") &&
		!cfg.Agents.Defaults.IsSubagentToolFeedbackEnabled() {
		return false
	}
	return true
}

func (tf *toolFeedbackPublisher) toolFeedbackMaxArgsLength() int {
	if tf == nil || tf.cfg == nil {
		return 300
	}
	return tf.cfg.Agents.Defaults.GetToolFeedbackMaxArgsLength()
}

func (tf *toolFeedbackPublisher) toolFeedbackStyle() string {
	if tf == nil || tf.cfg == nil {
		return ""
	}
	return tf.cfg.Agents.Defaults.GetToolFeedbackStyle()
}
