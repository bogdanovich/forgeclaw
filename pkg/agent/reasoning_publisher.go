package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/agent/interfaces"
	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/utils"
)

type reasoningPublisherComponent struct {
	bus            interfaces.MessageBus
	cfg            *config.Config
	channelManager interfaces.ChannelManager
}

func (al *AgentLoop) reasoningPublisher() *reasoningPublisherComponent {
	if al == nil {
		return nil
	}
	return &reasoningPublisherComponent{
		bus:            al.bus,
		cfg:            al.GetConfig(),
		channelManager: al.channelManager,
	}
}

func (rp *reasoningPublisherComponent) targetReasoningChannelID(channelName string) (chatID string) {
	if rp == nil || rp.channelManager == nil {
		return ""
	}
	if ch, ok := rp.channelManager.GetChannel(channelName); ok {
		return ch.ReasoningChannelID()
	}
	return ""
}

func (rp *reasoningPublisherComponent) publishPicoReasoning(
	ctx context.Context,
	ts *turnState,
	reasoningContent, modelName string,
) {
	if ts == nil {
		return
	}
	chatID, sessionKey := ts.chatID, ts.sessionKey
	if rp == nil || rp.bus == nil || reasoningContent == "" || chatID == "" {
		return
	}

	if ctx.Err() != nil {
		return
	}

	pubCtx, pubCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pubCancel()

	raw := map[string]string{metadataKeyMessageKind: messageKindThought}
	if trimmedModelName := strings.TrimSpace(modelName); trimmedModelName != "" {
		raw["model_name"] = trimmedModelName
	}

	message := bus.OutboundMessage{
		Context: bus.InboundContext{
			Channel: "pico",
			ChatID:  chatID,
			Raw:     raw,
		},
		SessionKey: sessionKey,
		Content:    reasoningContent,
	}
	message.TraceScopes = []runtimeevents.TraceScope{
		runtimeevents.NewTraceScope(ts.workspace, ts.turnID),
	}
	if err := rp.bus.PublishOutbound(pubCtx, message); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
			errors.Is(err, bus.ErrBusClosed) {
			logger.DebugCF("agent", "Pico reasoning publish skipped (timeout/cancel)", map[string]any{
				"channel": "pico",
				"error":   err.Error(),
			})
		} else {
			logger.WarnCF("agent", "Failed to publish pico reasoning (best-effort)", map[string]any{
				"channel": "pico",
				"error":   err.Error(),
			})
		}
	}
}

func (rp *reasoningPublisherComponent) publishPicoToolCallInterim(
	ctx context.Context,
	ts *turnState,
	modelName string,
	reasoningContent string,
	content string,
	toolCalls []providers.ToolCall,
) {
	if rp == nil || ts == nil || ts.chatID == "" || rp.bus == nil {
		return
	}

	if strings.TrimSpace(reasoningContent) != "" {
		pubCtx, pubCancel := context.WithTimeout(ctx, 3*time.Second)
		err := rp.bus.PublishOutbound(
			pubCtx,
			outboundMessageForTurnWithOptions(
				ts,
				reasoningContent,
				outboundTurnMessageOptions{
					kind:      messageKindThought,
					modelName: modelName,
				},
			),
		)
		pubCancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) &&
			!errors.Is(err, context.Canceled) &&
			!errors.Is(err, bus.ErrBusClosed) {
			logger.WarnCF("agent", "Failed to publish pico reasoning", map[string]any{
				"channel": ts.channel,
				"chat_id": ts.chatID,
				"error":   err.Error(),
			})
		}
	}

	if !ts.opts.AllowInterimPicoPublish {
		return
	}

	toolFeedbackMaxLen := 300
	if rp.cfg != nil {
		toolFeedbackMaxLen = rp.cfg.Agents.Defaults.GetToolFeedbackMaxArgsLength()
	}
	visibleToolCalls := utils.BuildVisibleToolCalls(toolCalls, toolFeedbackMaxLen)
	duplicateToolCallContent := len(visibleToolCalls) > 0 &&
		utils.ToolCallExplanationDuplicatesContent(content, toolCalls)

	if strings.TrimSpace(content) != "" && !duplicateToolCallContent {
		pubCtx, pubCancel := context.WithTimeout(ctx, 3*time.Second)
		err := rp.bus.PublishOutbound(
			pubCtx,
			outboundMessageForTurnWithOptions(ts, content, outboundTurnMessageOptions{
				modelName: modelName,
			}),
		)
		pubCancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) &&
			!errors.Is(err, context.Canceled) &&
			!errors.Is(err, bus.ErrBusClosed) {
			logger.WarnCF("agent", "Failed to publish pico interim assistant content", map[string]any{
				"channel": ts.channel,
				"chat_id": ts.chatID,
				"error":   err.Error(),
			})
		}
	}

	if len(visibleToolCalls) == 0 {
		return
	}

	rawToolCalls, err := json.Marshal(visibleToolCalls)
	if err != nil {
		logger.WarnCF("agent", "Failed to serialize pico tool calls", map[string]any{
			"channel": ts.channel,
			"chat_id": ts.chatID,
			"error":   err.Error(),
		})
		return
	}

	msg := outboundMessageForTurnWithOptions(ts, "", outboundTurnMessageOptions{
		kind:      messageKindToolCalls,
		modelName: modelName,
		raw: map[string]string{
			metadataKeyToolCalls: string(rawToolCalls),
		},
	})

	pubCtx, pubCancel := context.WithTimeout(ctx, 3*time.Second)
	err = rp.bus.PublishOutbound(pubCtx, msg)
	pubCancel()
	if err != nil && !errors.Is(err, context.DeadlineExceeded) &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, bus.ErrBusClosed) {
		logger.WarnCF("agent", "Failed to publish pico tool calls", map[string]any{
			"channel": ts.channel,
			"chat_id": ts.chatID,
			"error":   err.Error(),
		})
	}
}

func (rp *reasoningPublisherComponent) handleReasoning(
	ctx context.Context,
	ts *turnState,
	reasoningContent, channelName, channelID string,
) {
	if rp == nil || rp.bus == nil || ts == nil || reasoningContent == "" ||
		channelName == "" || channelID == "" {
		return
	}

	if ctx.Err() != nil {
		return
	}

	pubCtx, pubCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pubCancel()

	if err := rp.bus.PublishOutbound(pubCtx, bus.OutboundMessage{
		Context: bus.NewOutboundContext(channelName, channelID, ""),
		TraceScopes: []runtimeevents.TraceScope{
			runtimeevents.NewTraceScope(ts.workspace, ts.turnID),
		},
		Content: reasoningContent,
	}); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
			errors.Is(err, bus.ErrBusClosed) {
			logger.DebugCF("agent", "Reasoning publish skipped (timeout/cancel)", map[string]any{
				"channel": channelName,
				"error":   err.Error(),
			})
		} else {
			logger.WarnCF("agent", "Failed to publish reasoning (best-effort)", map[string]any{
				"channel": channelName,
				"error":   err.Error(),
			})
		}
	}
}
