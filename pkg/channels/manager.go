// PicoClaw - Ultra-lightweight personal AI agent
// Inspired by and based on nanobot: https://github.com/HKUDS/nanobot
// License: MIT
//
// Copyright (c) 2026 PicoClaw contributors

package channels

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/constants"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/health"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/media"
	"github.com/sipeed/picoclaw/pkg/utils"
)

const (
	defaultChannelQueueSize = 16
	defaultRateLimit        = 10 // default 10 msg/s
	maxRetries              = 3
	rateLimitDelay          = 1 * time.Second
	baseBackoff             = 500 * time.Millisecond
	maxBackoff              = 8 * time.Second

	janitorInterval = 10 * time.Second
	typingStopTTL   = 5 * time.Minute
	placeholderTTL  = 10 * time.Minute

	streamAuxiliaryTombstoneTTL = 30 * time.Second
)

var errDeliveryClosed = errors.New("channel delivery is closed")

// typingEntry wraps a typing stop function with a creation timestamp for TTL eviction.
type typingEntry struct {
	stop      func()
	createdAt time.Time
}

// reactionEntry wraps a reaction undo function with a creation timestamp for TTL eviction.
type reactionEntry struct {
	undo      func()
	createdAt time.Time
}

// placeholderEntry wraps a placeholder ID with a creation timestamp for TTL eviction.
type placeholderEntry struct {
	id        string
	createdAt time.Time
}

// channelRateConfig maps channel name to per-second rate limit.
var channelRateConfig = map[string]float64{
	"telegram": 20,
	"discord":  1,
	"slack":    1,
	"matrix":   2,
	"line":     10,
	"qq":       5,
	"irc":      2,
}

type channelWorker struct {
	ch         Channel
	queue      chan bus.OutboundMessage
	mediaQueue chan bus.OutboundMediaMessage
	done       chan struct{}
	mediaDone  chan struct{}
	limiter    *rate.Limiter
}

// deliveryOwner is the first narrow ownership boundary around outbound delivery.
// It intentionally owns only Channel+worker enqueue state today. A later
// channelSlot abstraction can wrap this with lifecycle/visibility state for
// safe reload swaps.
type deliveryOwner struct {
	name   string
	ch     Channel
	worker *channelWorker
	mu     sync.Mutex
	closed bool
}

type Manager struct {
	channels                  map[string]Channel
	workers                   map[string]*channelWorker
	deliveryOwners            map[string]*deliveryOwner
	bus                       *bus.MessageBus
	runtimeEvents             runtimeevents.Bus
	config                    *config.Config
	mediaStore                media.MediaStore
	dispatchTask              *asyncTask
	mux                       *dynamicServeMux
	httpServer                *http.Server
	httpListeners             []net.Listener
	mu                        sync.RWMutex
	placeholders              sync.Map // "channel:chatID" → placeholderID (string)
	typingStops               sync.Map // "channel:chatID" → func()
	reactionUndos             sync.Map // "channel:chatID" → reactionEntry
	streamActive              sync.Map // streamSuppressionKey → true (set when streamer.Finalize sent the message)
	streamAuxiliaryTombstones sync.Map // streamSuppressionKey → time.Time (drops late auxiliary messages after stream final)
	toolFeedback              *ToolFeedbackCoordinator
	channelHashes             map[string]string // channel name → config hash
	channelRestartRequired    map[string]string // channel name → desired config hash that needs process restart
}

type mediaStoreSetter interface {
	SetMediaStore(s media.MediaStore)
}

// ManagerOption configures a channel Manager.
type ManagerOption func(*Manager)

// WithRuntimeEvents injects the runtime event bus used for channel observations.
func WithRuntimeEvents(eventBus runtimeevents.Bus) ManagerOption {
	return func(m *Manager) {
		m.runtimeEvents = eventBus
	}
}

// ChannelLifecyclePayload describes channel lifecycle runtime events.
type ChannelLifecyclePayload struct {
	Type  string `json:"type,omitempty"`
	Error string `json:"error,omitempty"`
}

// ChannelOutboundPayload describes channel outbound message runtime events.
type ChannelOutboundPayload struct {
	TraceScopes      []runtimeevents.TraceScope `json:"trace_scopes,omitempty"`
	TraceSettlement  bool                       `json:"trace_settlement,omitempty"`
	Media            bool                       `json:"media,omitempty"`
	ContentLen       int                        `json:"content_len,omitempty"`
	MessageIDs       []string                   `json:"message_ids,omitempty"`
	ReplyToMessageID string                     `json:"reply_to_message_id,omitempty"`
	Error            string                     `json:"error,omitempty"`
	Retries          int                        `json:"retries,omitempty"`
}

type outcomePublication uint8

const (
	publishNoOutcome outcomePublication = iota
	publishDefinitiveOutcome
	publishSuccessOnly
)

func (mode outcomePublication) success() bool {
	return mode == publishDefinitiveOutcome || mode == publishSuccessOnly
}

func (mode outcomePublication) failure(ambiguous bool) bool {
	return mode == publishDefinitiveOutcome || (mode == publishSuccessOnly && ambiguous)
}

type outboundTargetResolver interface {
	ResolveOutboundChatID(chatID string, outboundCtx *bus.InboundContext) string
}

type toolFeedbackMessageTargetResolver interface {
	ToolFeedbackMessageChatID(chatID string, outboundCtx *bus.InboundContext) string
}

type toolFeedbackMessageContentPreparer interface {
	PrepareToolFeedbackMessageContent(content string) string
}

type toolFeedbackMessageEditor interface {
	EditToolFeedbackMessage(ctx context.Context, chatID, messageID, content string) error
}

type toolFeedbackMessageSender interface {
	SendToolFeedbackMessage(ctx context.Context, msg bus.OutboundMessage) ([]string, bool, error)
}

type asyncTask struct {
	cancel context.CancelFunc
}

type deliveryCleanupOptions struct {
	StopTyping          bool
	UndoReaction        bool
	ClearStreamActive   bool
	DismissToolFeedback bool
	DeletePlaceholder   bool
	SessionKey          string
	TraceScopes         []runtimeevents.TraceScope
}

func outboundMessageChannel(msg bus.OutboundMessage) string {
	return msg.Context.Channel
}

func outboundMessageChatID(msg bus.OutboundMessage) string {
	return msg.ChatID
}

func outboundMessageIsToolFeedback(msg bus.OutboundMessage) bool {
	if len(msg.Context.Raw) == 0 {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(msg.Context.Raw["message_kind"]), "tool_feedback")
}

func outboundMessageIsToolCalls(msg bus.OutboundMessage) bool {
	if len(msg.Context.Raw) == 0 {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(msg.Context.Raw["message_kind"]), "tool_calls")
}

func outboundMessageHasAuxiliaryKind(msg bus.OutboundMessage) bool {
	if len(msg.Context.Raw) == 0 {
		return false
	}
	return strings.TrimSpace(msg.Context.Raw["message_kind"]) != ""
}

func outboundMessageIsFinal(msg bus.OutboundMessage) bool {
	if len(msg.Context.Raw) == 0 {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(msg.Context.Raw["outbound_kind"]), "final")
}

func outboundMessageBypassesPlaceholderEdit(msg bus.OutboundMessage) bool {
	if len(msg.Context.Raw) == 0 {
		return false
	}
	kind := strings.TrimSpace(msg.Context.Raw["message_kind"])
	return strings.EqualFold(kind, "thought") ||
		strings.EqualFold(kind, "tool_calls") ||
		strings.EqualFold(kind, "final_reply")
}

func outboundMessageEditPayload(msg bus.OutboundMessage, content string) map[string]any {
	payload := map[string]any{
		"content": content,
	}
	if len(msg.Context.Raw) == 0 {
		return payload
	}
	if modelName := strings.TrimSpace(msg.Context.Raw["model_name"]); modelName != "" {
		payload["model_name"] = modelName
	}
	return payload
}

func outboundMediaChannel(msg bus.OutboundMediaMessage) string {
	return msg.Context.Channel
}

func outboundMediaChatID(msg bus.OutboundMediaMessage) string {
	return msg.ChatID
}

func candidateChatIDs(raw, resolved string) []string {
	raw = strings.TrimSpace(raw)
	resolved = strings.TrimSpace(resolved)
	if raw == "" || raw == resolved {
		return []string{resolved}
	}
	return []string{resolved, raw}
}

func resolveOutboundChatID(ch Channel, chatID string, outboundCtx *bus.InboundContext) string {
	if resolver, ok := ch.(outboundTargetResolver); ok {
		if resolved := strings.TrimSpace(resolver.ResolveOutboundChatID(chatID, outboundCtx)); resolved != "" {
			return resolved
		}
	}
	return strings.TrimSpace(chatID)
}

func traceScopedDeliveryKey(base string, traceScope runtimeevents.TraceScope) (string, bool) {
	traceScope = runtimeevents.NewTraceScope(traceScope.Workspace, traceScope.TurnID)
	if !traceScope.Complete() {
		return base, false
	}
	return base + "\x00turn\x00" + traceScope.Workspace + "\x00" + traceScope.TurnID, true
}

func primaryTraceScope(scopes []runtimeevents.TraceScope) runtimeevents.TraceScope {
	normalized, err := bus.NormalizeTraceScopes(scopes)
	if err != nil || len(normalized) == 0 {
		return runtimeevents.TraceScope{}
	}
	return normalized[0]
}

func streamSuppressionKey(
	channel, chatID, sessionKey string,
	traceScope runtimeevents.TraceScope,
) string {
	key := channel + ":" + chatID
	if strings.TrimSpace(sessionKey) != "" {
		key += ":" + sessionKey
	}
	key, _ = traceScopedDeliveryKey(key, traceScope)
	return key
}

func trackedToolFeedbackMessageChatID(ch Channel, chatID string, outboundCtx *bus.InboundContext) string {
	if resolver, ok := ch.(toolFeedbackMessageTargetResolver); ok {
		if resolved := strings.TrimSpace(resolver.ToolFeedbackMessageChatID(chatID, outboundCtx)); resolved != "" {
			return resolved
		}
	}
	return resolveOutboundChatID(ch, chatID, outboundCtx)
}

func (m *Manager) cleanupDeliveryState(
	ctx context.Context,
	name string,
	chatID string,
	outboundCtx *bus.InboundContext,
	ch Channel,
	opts deliveryCleanupOptions,
) {
	cleanupChatIDs := candidateChatIDs(chatID, resolveOutboundChatID(ch, chatID, outboundCtx))

	if opts.StopTyping {
		for _, cleanupChatID := range cleanupChatIDs {
			if v, loaded := m.typingStops.LoadAndDelete(name + ":" + cleanupChatID); loaded {
				if entry, ok := v.(typingEntry); ok {
					entry.stop()
				}
			}
		}
	}

	if opts.UndoReaction {
		for _, cleanupChatID := range cleanupChatIDs {
			if v, loaded := m.reactionUndos.LoadAndDelete(name + ":" + cleanupChatID); loaded {
				if entry, ok := v.(reactionEntry); ok {
					entry.undo()
				}
			}
		}
	}

	if opts.ClearStreamActive {
		for _, cleanupChatID := range cleanupChatIDs {
			streamKey := streamSuppressionKey(
				name, cleanupChatID, opts.SessionKey, primaryTraceScope(opts.TraceScopes),
			)
			m.streamActive.LoadAndDelete(streamKey)
			m.streamAuxiliaryTombstones.Delete(streamKey)
		}
	}

	if opts.DismissToolFeedback {
		if m.toolFeedback != nil {
			m.dismissToolFeedbackTargets(
				ctx, name, ch, chatID, outboundCtx, opts.SessionKey, opts.TraceScopes,
			)
		}
	}

	if opts.DeletePlaceholder {
		for _, cleanupChatID := range cleanupChatIDs {
			if v, loaded := m.placeholders.LoadAndDelete(name + ":" + cleanupChatID); loaded {
				if entry, ok := v.(placeholderEntry); ok && entry.id != "" {
					if deleter, ok := ch.(MessageDeleter); ok {
						deleter.DeleteMessage(ctx, cleanupChatID, entry.id)
					}
				}
			}
		}
	}
}

func sessionScopedToolFeedbackMessageChatID(chatID, sessionKey string) string {
	chatID = strings.TrimSpace(chatID)
	sessionKey = strings.TrimSpace(sessionKey)
	if chatID == "" || sessionKey == "" {
		return chatID
	}
	return chatID + "#session:" + sessionKey
}

func toolFeedbackCoordinatorKey(channelName, trackedChatID string) string {
	channelName = strings.TrimSpace(channelName)
	trackedChatID = strings.TrimSpace(trackedChatID)
	if channelName == "" || trackedChatID == "" {
		return ""
	}
	return channelName + ":" + trackedChatID
}

func toolFeedbackTarget(
	channelName string,
	ch Channel,
	chatID string,
	outboundCtx *bus.InboundContext,
	sessionKey string,
	traceScope runtimeevents.TraceScope,
) (string, string) {
	deliveryChatID := trackedToolFeedbackMessageChatID(ch, chatID, outboundCtx)
	trackedChatID := sessionScopedToolFeedbackMessageChatID(deliveryChatID, sessionKey)
	key, _ := traceScopedDeliveryKey(
		toolFeedbackCoordinatorKey(channelName, trackedChatID), traceScope,
	)
	return key, deliveryChatID
}

func toolFeedbackTargets(
	channelName string,
	ch Channel,
	chatID string,
	outboundCtx *bus.InboundContext,
	sessionKey string,
	traceScopes []runtimeevents.TraceScope,
) ([]string, bool) {
	base, _ := toolFeedbackTarget(
		channelName, ch, chatID, outboundCtx, sessionKey, runtimeevents.TraceScope{},
	)
	normalized, err := bus.NormalizeTraceScopes(traceScopes)
	if err != nil || len(normalized) == 0 {
		return []string{base}, false
	}
	keys := make([]string, 0, len(normalized))
	for _, traceScope := range normalized {
		key, _ := traceScopedDeliveryKey(base, traceScope)
		keys = append(keys, key)
	}
	return keys, true
}

func toolFeedbackOperationsFor(ch Channel) toolFeedbackOperations {
	operations := toolFeedbackOperations{}
	if editor, ok := ch.(toolFeedbackMessageEditor); ok {
		operations.edit = editor.EditToolFeedbackMessage
	} else if editor, ok := ch.(MessageEditor); ok {
		operations.edit = editor.EditMessage
	}
	if deleter, ok := ch.(MessageDeleter); ok {
		operations.delete = deleter.DeleteMessage
	}
	return operations
}

func (m *Manager) beginToolFeedbackTerminals(
	channelName string,
	ch Channel,
	chatID string,
	outboundCtx *bus.InboundContext,
	sessionKey string,
	traceScopes []runtimeevents.TraceScope,
) []*toolFeedbackTerminal {
	if m == nil || m.toolFeedback == nil {
		return nil
	}
	keys, scoped := toolFeedbackTargets(
		channelName, ch, chatID, outboundCtx, sessionKey, traceScopes,
	)
	terminals := make([]*toolFeedbackTerminal, 0, len(keys))
	for _, key := range keys {
		if scoped {
			terminals = append(terminals, m.toolFeedback.BeginTerminal(key))
		} else {
			terminals = append(terminals, m.toolFeedback.BeginTransientTerminal(key))
		}
	}
	return terminals
}

func (m *Manager) completeToolFeedbackTerminals(
	ctx context.Context,
	terminals []*toolFeedbackTerminal,
	success bool,
) {
	for _, terminal := range terminals {
		m.toolFeedback.CompleteTerminal(ctx, terminal, success)
	}
}

func (m *Manager) deliverToolFeedback(
	ctx context.Context,
	channelName string,
	ch Channel,
	msg bus.OutboundMessage,
	send func(context.Context, bus.OutboundMessage) ([]string, error),
) ([]string, error) {
	key, deliveryChatID := toolFeedbackTarget(
		channelName,
		ch,
		outboundMessageChatID(msg),
		&msg.Context,
		msg.SessionKey,
		primaryTraceScope(msg.TraceScopes),
	)
	content := prepareToolFeedbackMessageContent(ch, msg.Content)
	operations := toolFeedbackOperationsFor(ch)
	return m.toolFeedback.deliver(
		ctx,
		key,
		deliveryChatID,
		content,
		operations,
		func(sendCtx context.Context, prepared string) (toolFeedbackSendResult, error) {
			sendMsg := msg
			sendMsg.Content = prepared
			if sender, ok := ch.(toolFeedbackMessageSender); ok {
				messageIDs, editable, err := sender.SendToolFeedbackMessage(sendCtx, sendMsg)
				return toolFeedbackSendResult{messageIDs: messageIDs, editable: editable}, err
			}
			messageIDs, err := send(sendCtx, sendMsg)
			return toolFeedbackSendResult{messageIDs: messageIDs, editable: operations.edit != nil}, err
		},
	)
}

// DismissToolFeedback clears any tracked tool feedback animation for the
// given channel/chat. This is called when a turn ends without a final
// response (e.g., ResponseHandled tools) to stop orphaned animation goroutines.
// outboundCtx carries topic/thread info for channels that use scoped tracker
// keys (e.g., Telegram forum topics); may be nil for non-topic channels.
func (m *Manager) DismissToolFeedback(
	ctx context.Context,
	channelName, chatID string,
	outboundCtx *bus.InboundContext,
	traceScopes []runtimeevents.TraceScope,
) {
	if m == nil || m.toolFeedback == nil {
		return
	}
	ch, ok := m.GetChannel(channelName)
	if !ok {
		return
	}
	m.dismissToolFeedbackTargets(ctx, channelName, ch, chatID, outboundCtx, "", traceScopes)
}

func (m *Manager) DismissToolFeedbackForSession(
	ctx context.Context,
	channelName, chatID string,
	outboundCtx *bus.InboundContext,
	sessionKey string,
	traceScopes []runtimeevents.TraceScope,
) {
	if m == nil || m.toolFeedback == nil {
		return
	}
	ch, ok := m.GetChannel(channelName)
	if !ok {
		return
	}
	m.dismissToolFeedbackTargets(
		ctx, channelName, ch, chatID, outboundCtx, sessionKey, traceScopes,
	)
}

func (m *Manager) dismissToolFeedbackTargets(
	ctx context.Context,
	channelName string,
	ch Channel,
	chatID string,
	outboundCtx *bus.InboundContext,
	sessionKey string,
	traceScopes []runtimeevents.TraceScope,
) {
	keys, scoped := toolFeedbackTargets(
		channelName, ch, chatID, outboundCtx, sessionKey, traceScopes,
	)
	for _, key := range keys {
		if scoped {
			m.toolFeedback.Dismiss(ctx, key)
		} else {
			m.toolFeedback.DismissTransient(ctx, key)
		}
	}
}

func prepareToolFeedbackMessageContent(ch Channel, content string) string {
	prepared := strings.TrimSpace(content)
	if prepared == "" {
		return ""
	}
	if preparer, ok := ch.(toolFeedbackMessageContentPreparer); ok {
		if candidate := strings.TrimSpace(preparer.PrepareToolFeedbackMessageContent(prepared)); candidate != "" {
			return candidate
		}
	}
	return prepared
}

// RecordPlaceholder registers a placeholder message for later editing.
// Implements PlaceholderRecorder.
func (m *Manager) RecordPlaceholder(channel, chatID, placeholderID string) {
	key := channel + ":" + chatID
	m.placeholders.Store(key, placeholderEntry{id: placeholderID, createdAt: time.Now()})
}

// SendPlaceholder sends a "Thinking…" placeholder for the given channel/chatID
// and records it for later editing. Returns true if a placeholder was sent.
func (m *Manager) SendPlaceholder(ctx context.Context, channel, chatID string) bool {
	m.mu.RLock()
	ch, ok := m.channels[channel]
	m.mu.RUnlock()
	if !ok {
		return false
	}
	pc, ok := ch.(PlaceholderCapable)
	if !ok {
		return false
	}
	phID, err := pc.SendPlaceholder(ctx, chatID)
	if err != nil || phID == "" {
		return false
	}
	m.RecordPlaceholder(channel, chatID, phID)
	return true
}

// RecordTypingStop registers a typing stop function for later invocation.
// Implements PlaceholderRecorder.
func (m *Manager) RecordTypingStop(channel, chatID string, stop func()) {
	key := channel + ":" + chatID
	entry := typingEntry{stop: stop, createdAt: time.Now()}
	if previous, loaded := m.typingStops.Swap(key, entry); loaded {
		if oldEntry, ok := previous.(typingEntry); ok && oldEntry.stop != nil {
			oldEntry.stop()
		}
	}
}

// InvokeTypingStop invokes the registered typing stop function for the given channel and chatID.
// It is safe to call even when no typing indicator is active (no-op).
// Used by the agent loop to stop typing when processing completes (success, error, or panic),
// regardless of whether an outbound message is published.
func (m *Manager) InvokeTypingStop(channel, chatID string) {
	key := channel + ":" + chatID
	if v, loaded := m.typingStops.LoadAndDelete(key); loaded {
		if entry, ok := v.(typingEntry); ok {
			entry.stop()
		}
	}
}

// RecordReactionUndo registers a reaction undo function for later invocation.
// Implements PlaceholderRecorder.
func (m *Manager) RecordReactionUndo(channel, chatID string, undo func()) {
	key := channel + ":" + chatID
	m.reactionUndos.Store(key, reactionEntry{undo: undo, createdAt: time.Now()})
}

// preSend handles typing stop, reaction undo, and placeholder editing before sending a message.
// Returns the delivered message IDs and true when delivery completed before a normal Send.
func (m *Manager) preSend(ctx context.Context, name string, msg bus.OutboundMessage, ch Channel) ([]string, bool) {
	chatID := outboundMessageChatID(msg)
	key := name + ":" + chatID
	traceScope := primaryTraceScope(msg.TraceScopes)
	streamKey := streamSuppressionKey(name, chatID, msg.SessionKey, traceScope)

	m.cleanupDeliveryState(ctx, name, chatID, &msg.Context, ch, deliveryCleanupOptions{
		StopTyping:   true,
		UndoReaction: true,
	})

	isToolFeedback := outboundMessageIsToolFeedback(msg)
	isToolCalls := outboundMessageIsToolCalls(msg)
	isAuxiliaryMessage := outboundMessageHasAuxiliaryKind(msg)
	isFinalMessage := outboundMessageIsFinal(msg)
	// 3. If a stream already finalized this chat, stale auxiliary messages must
	// be dropped without consuming the final-response marker. Streaming
	// finalization bypasses the worker queue, so older queued feedback/thoughts
	// can arrive before the normal final outbound message that cleans up the
	// marker and placeholder.
	// Tool calls must reach the UI, and the queued final must consume the active
	// marker after the streamed copy has already been delivered.
	if isAuxiliaryMessage && !isToolCalls && !isFinalMessage {
		if _, loaded := m.streamActive.Load(streamKey); loaded {
			return nil, true
		}
		if m.streamAuxiliaryTombstoneActive(streamKey) {
			return nil, true
		}
	}

	// 4. If a stream already finalized this turn, skip only the duplicate final
	// outbound. Earlier queued visible messages must still be delivered.
	if isFinalMessage {
		if _, loaded := m.streamActive.LoadAndDelete(streamKey); loaded {
			if v, loaded := m.placeholders.LoadAndDelete(key); loaded {
				if entry, ok := v.(placeholderEntry); ok && entry.id != "" {
					// Prefer deleting the placeholder (cleaner UX than editing to same content)
					if deleter, ok := ch.(MessageDeleter); ok {
						deleter.DeleteMessage(ctx, chatID, entry.id) // best effort
					} else if editor, ok := ch.(MessageEditor); ok {
						if payloadEditor, ok := ch.(MessageEditorWithPayload); ok {
							_ = payloadEditor.EditMessageWithPayload(
								ctx,
								chatID,
								entry.id,
								outboundMessageEditPayload(msg, msg.Content),
							)
						} else {
							editor.EditMessage(ctx, chatID, entry.id, msg.Content) // fallback
						}
					}
				}
			}
			if m.toolFeedback != nil {
				keys, _ := toolFeedbackTargets(
					name, ch, chatID, &msg.Context, msg.SessionKey, msg.TraceScopes,
				)
				for _, key := range keys {
					m.toolFeedback.ReleaseTerminal(key)
				}
			}
			return nil, true
		}
	}

	if _, loaded := m.streamActive.Load(streamKey); loaded {
		return nil, false
	}
	if !traceScope.Complete() && m.streamActiveForChat(name, chatID) {
		return nil, false
	}

	if !isAuxiliaryMessage {
		m.streamAuxiliaryTombstones.Delete(streamKey)
	}

	// 5. Try editing placeholder
	if v, loaded := m.placeholders.LoadAndDelete(key); loaded {
		if entry, ok := v.(placeholderEntry); ok && entry.id != "" {
			logger.InfoCF("channels", "Evaluating placeholder edit bypass",
				map[string]any{
					"channel":          name,
					"chat_id":          chatID,
					"placeholder_id":   entry.id,
					"message_kind":     strings.TrimSpace(msg.Context.Raw["message_kind"]),
					"is_tool_feedback": isToolFeedback,
					"bypass":           outboundMessageBypassesPlaceholderEdit(msg),
				})
			if isToolFeedback {
				if deleter, ok := ch.(MessageDeleter); ok {
					deleter.DeleteMessage(ctx, chatID, entry.id) // best effort
				}
				return nil, false
			}
			if outboundMessageBypassesPlaceholderEdit(msg) {
				if deleter, ok := ch.(MessageDeleter); ok {
					deleter.DeleteMessage(ctx, chatID, entry.id) // best effort
				}
				return nil, false
			}
			if editor, ok := ch.(MessageEditor); ok {
				content := msg.Content
				err := func() error {
					if payloadEditor, ok := ch.(MessageEditorWithPayload); ok {
						return payloadEditor.EditMessageWithPayload(
							ctx,
							chatID,
							entry.id,
							outboundMessageEditPayload(msg, content),
						)
					}
					return editor.EditMessage(ctx, chatID, entry.id, content)
				}()
				if err == nil {
					return []string{entry.id}, true
				}
				// edit failed → fall through to normal Send
			}
		}
	}

	return nil, false
}

// preSendMedia handles typing stop, reaction undo, and placeholder cleanup
// before sending media attachments. Unlike preSend for text messages, media
// delivery never edits the placeholder because there is no text payload to
// replace it with; it only attempts to delete the placeholder when possible.
func (m *Manager) preSendMedia(ctx context.Context, name string, msg bus.OutboundMediaMessage, ch Channel) {
	chatID := outboundMediaChatID(msg)

	m.cleanupDeliveryState(ctx, name, chatID, &msg.Context, ch, deliveryCleanupOptions{
		StopTyping:        true,
		UndoReaction:      true,
		ClearStreamActive: true,
		DeletePlaceholder: true,
		SessionKey:        msg.SessionKey,
		TraceScopes:       msg.TraceScopes,
	})
}

func NewManager(
	cfg *config.Config,
	messageBus *bus.MessageBus,
	store media.MediaStore,
	opts ...ManagerOption,
) (*Manager, error) {
	m := &Manager{
		channels:               make(map[string]Channel),
		workers:                make(map[string]*channelWorker),
		deliveryOwners:         make(map[string]*deliveryOwner),
		bus:                    messageBus,
		config:                 cfg,
		mediaStore:             store,
		channelHashes:          make(map[string]string),
		channelRestartRequired: make(map[string]string),
	}
	if cfg != nil {
		m.toolFeedback = NewToolFeedbackCoordinator(
			ToolFeedbackAnimatorConfig{
				AnimationInterval: cfg.Agents.Defaults.GetToolFeedbackAnimationInterval(),
				MinEditInterval:   cfg.Agents.Defaults.GetToolFeedbackEditMinInterval(),
			},
			cfg.Agents.Defaults.IsToolFeedbackSeparateMessagesEnabled(),
		)
	}
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}

	// Register as streaming delegate so the agent loop can obtain streamers
	messageBus.SetStreamDelegate(m)

	if err := m.initChannels(&cfg.Channels); err != nil {
		return nil, err
	}

	// Store initial config hashes for all channels
	m.channelHashes = toChannelHashes(cfg)

	return m, nil
}

// SetMediaStore updates the store used by the manager and every channel that
// accepts media store injection. Gateway reload creates a fresh store, so
// keeping existing channels on the same store as the agent is required for
// inbound media refs to remain resolvable after reload.
func (m *Manager) SetMediaStore(store media.MediaStore) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.mediaStore = store
	for _, ch := range m.channels {
		if setter, ok := ch.(mediaStoreSetter); ok {
			setter.SetMediaStore(store)
		}
	}
}

func (m *Manager) installDeliveryOwnerLocked(
	ctx context.Context,
	name string,
	channel Channel,
	channelType string,
) *deliveryOwner {
	owner := newDeliveryOwner(name, channel, channelType)
	m.workers[name] = owner.Worker()
	m.deliveryOwners[name] = owner
	owner.StartDelivery(ctx, m)
	return owner
}

func closeWorkerAndWait(w *channelWorker) {
	if w == nil {
		return
	}
	close(w.queue)
	<-w.done
	close(w.mediaQueue)
	<-w.mediaDone
}

// GetStreamer implements bus.StreamDelegate.
// It checks if the named channel supports streaming and returns a Streamer.
func (m *Manager) GetStreamer(
	ctx context.Context,
	channelName, chatID, sessionKey string,
	traceScope runtimeevents.TraceScope,
) (bus.Streamer, bool) {
	m.mu.RLock()
	ch, exists := m.channels[channelName]
	m.mu.RUnlock()

	if !exists {
		return nil, false
	}

	sc, ok := ch.(StreamingCapable)
	if !ok {
		return nil, false
	}

	streamer, err := sc.BeginStream(ctx, chatID)
	if err != nil {
		logger.DebugCF("channels", "Streaming unavailable, falling back to placeholder", map[string]any{
			"channel": channelName,
			"error":   err.Error(),
		})
		return nil, false
	}

	// Mark streamActive on Finalize so preSend knows to clean up the placeholder
	// and late auxiliary messages cannot leak after streaming produced a final.
	streamKey := streamSuppressionKey(channelName, chatID, sessionKey, traceScope)
	placeholderKey := channelName + ":" + chatID
	clearMarker := func() {
		m.streamActive.Delete(streamKey)
	}
	onFinalize := func(finalizeCtx context.Context, finalContent string) {
		if m.toolFeedback != nil {
			m.dismissToolFeedbackTargets(
				finalizeCtx,
				channelName,
				ch,
				chatID,
				&bus.InboundContext{Channel: channelName, ChatID: chatID},
				sessionKey,
				[]runtimeevents.TraceScope{traceScope},
			)
		}
		if v, loaded := m.placeholders.LoadAndDelete(placeholderKey); loaded {
			if entry, ok := v.(placeholderEntry); ok && entry.id != "" {
				if deleter, ok := ch.(MessageDeleter); ok {
					deleter.DeleteMessage(finalizeCtx, chatID, entry.id) // best effort
				} else if editor, ok := ch.(MessageEditor); ok {
					editor.EditMessage(finalizeCtx, chatID, entry.id, finalContent) // best effort fallback
				}
			}
		}
		m.streamActive.Store(streamKey, true)
		m.streamAuxiliaryTombstones.Store(streamKey, time.Now())
	}

	if m.config != nil && m.config.Agents.Defaults.SplitOnMarker {
		return &splitMarkerStreamer{
			current:     streamer,
			reasoning:   reasoningStreamerFrom(streamer),
			begin:       func(beginCtx context.Context) (bus.Streamer, error) { return sc.BeginStream(beginCtx, chatID) },
			onFinalize:  onFinalize,
			clearMarker: clearMarker,
		}, true
	}

	return &finalizeHookStreamer{
		Streamer:    streamer,
		clearMarker: clearMarker,
		onFinalize:  onFinalize,
	}, true
}

func reasoningStreamerFrom(streamer bus.Streamer) bus.ReasoningStreamer {
	if reasoningStreamer, ok := streamer.(bus.ReasoningStreamer); ok {
		return reasoningStreamer
	}
	return nil
}

type modelNameStreamer interface {
	SetModelName(modelName string)
}

func setStreamerModelName(streamer any, modelName string) {
	setter, ok := streamer.(modelNameStreamer)
	if !ok {
		return
	}
	setter.SetModelName(modelName)
}

type turnUsageStreamer interface {
	SetTurnUsage(inputTokens, outputTokens int)
}

// setStreamerTurnUsage forwards real per-turn token usage to a streamer that
// supports it, transparently unwrapping the manager's streamer wrappers.
func setStreamerTurnUsage(streamer any, inputTokens, outputTokens int) {
	setter, ok := streamer.(turnUsageStreamer)
	if !ok {
		return
	}
	setter.SetTurnUsage(inputTokens, outputTokens)
}

// splitMarkerStreamer turns accumulated streaming text containing
// MessageSplitMarker into separate channel stream messages.
type splitMarkerStreamer struct {
	mu               sync.Mutex
	current          bus.Streamer
	reasoning        bus.ReasoningStreamer
	begin            func(context.Context) (bus.Streamer, error)
	completedParts   int
	finalized        bool
	onFinalize       func(context.Context, string)
	clearMarker      func()
	modelName        string
	turnInputTokens  int
	turnOutputTokens int
}

func (s *splitMarkerStreamer) Update(ctx context.Context, content string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.updateLocked(ctx, content)
}

func (s *splitMarkerStreamer) Finalize(ctx context.Context, content string) error {
	return s.FinalizeWithContext(ctx, content, nil)
}

func (s *splitMarkerStreamer) FinalizeWithContext(ctx context.Context, content string, usage *bus.ContextUsage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.finalizeLocked(ctx, content, usage); err != nil {
		return err
	}
	s.runFinalizeHook(ctx, content)
	return nil
}

func (s *splitMarkerStreamer) UpdateReasoning(ctx context.Context, content string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reasoning == nil {
		return nil
	}
	setStreamerModelName(s.reasoning, s.modelName)
	return s.reasoning.UpdateReasoning(ctx, content)
}

func (s *splitMarkerStreamer) FinalizeReasoning(ctx context.Context, content string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reasoning == nil {
		return nil
	}
	setStreamerModelName(s.reasoning, s.modelName)
	return s.reasoning.FinalizeReasoning(ctx, content)
}

func (s *splitMarkerStreamer) SetModelName(modelName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.modelName = strings.TrimSpace(modelName)
	setStreamerModelName(s.current, s.modelName)
	setStreamerModelName(s.reasoning, s.modelName)
}

func (s *splitMarkerStreamer) SetTurnUsage(inputTokens, outputTokens int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.turnInputTokens = inputTokens
	s.turnOutputTokens = outputTokens
	setStreamerTurnUsage(s.current, s.turnInputTokens, s.turnOutputTokens)
}

func (s *splitMarkerStreamer) Cancel(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current != nil {
		s.current.Cancel(ctx)
	}
}

func (s *splitMarkerStreamer) ClearFinalizedStreamMarker() {
	if s.clearMarker != nil {
		s.clearMarker()
	}
}

func (s *splitMarkerStreamer) updateLocked(ctx context.Context, content string) error {
	parts := strings.Split(content, MessageSplitMarker)
	completedLimit := len(parts) - 1
	if err := s.finalizeCompletedPartsLocked(ctx, parts, completedLimit, nil); err != nil {
		return err
	}
	active := strings.TrimSpace(parts[len(parts)-1])
	if active == "" {
		return nil
	}
	if err := s.ensureCurrentLocked(ctx); err != nil {
		return err
	}
	return s.current.Update(ctx, active)
}

func (s *splitMarkerStreamer) finalizeLocked(ctx context.Context, content string, usage *bus.ContextUsage) error {
	parts := strings.Split(content, MessageSplitMarker)
	return s.finalizeCompletedPartsLocked(ctx, parts, len(parts), usage)
}

func (s *splitMarkerStreamer) finalizeCompletedPartsLocked(
	ctx context.Context,
	parts []string,
	limit int,
	usage *bus.ContextUsage,
) error {
	for s.completedParts < limit {
		content := strings.TrimSpace(parts[s.completedParts])
		isLast := s.completedParts == limit-1
		if content != "" {
			if err := s.ensureCurrentLocked(ctx); err != nil {
				return err
			}
			if isLast && usage != nil {
				if contextStreamer, ok := s.current.(bus.ContextUsageStreamer); ok {
					if err := contextStreamer.FinalizeWithContext(ctx, content, usage); err != nil {
						return err
					}
				} else if err := s.current.Finalize(ctx, content); err != nil {
					return err
				}
			} else if err := s.current.Finalize(ctx, content); err != nil {
				return err
			}
			s.current = nil
		}
		s.completedParts++
	}
	return nil
}

func (s *splitMarkerStreamer) ensureCurrentLocked(ctx context.Context) error {
	if s.current != nil {
		return nil
	}
	if s.begin == nil {
		return fmt.Errorf("streamer is not initialized")
	}
	streamer, err := s.begin(ctx)
	if err != nil {
		return err
	}
	s.current = streamer
	setStreamerModelName(s.current, s.modelName)
	setStreamerTurnUsage(s.current, s.turnInputTokens, s.turnOutputTokens)
	return nil
}

func (s *splitMarkerStreamer) runFinalizeHook(ctx context.Context, content string) {
	if s.finalized {
		return
	}
	s.finalized = true
	if s.onFinalize != nil {
		s.onFinalize(ctx, content)
	}
}

func (m *Manager) streamAuxiliaryTombstoneActive(key string) bool {
	v, ok := m.streamAuxiliaryTombstones.Load(key)
	if !ok {
		return false
	}
	createdAt, ok := v.(time.Time)
	if !ok || time.Since(createdAt) > streamAuxiliaryTombstoneTTL {
		m.streamAuxiliaryTombstones.Delete(key)
		return false
	}
	return true
}

func (m *Manager) streamActiveForChat(channel, chatID string) bool {
	chatKey := streamSuppressionKey(channel, chatID, "", runtimeevents.TraceScope{})
	found := false
	m.streamActive.Range(func(key, _ any) bool {
		keyString, ok := key.(string)
		if !ok {
			return true
		}
		if keyString == chatKey || strings.HasPrefix(keyString, chatKey+":") {
			found = true
			return false
		}
		return true
	})
	return found
}

// finalizeHookStreamer wraps a Streamer to run a hook on Finalize.
type finalizeHookStreamer struct {
	Streamer
	onFinalize  func(context.Context, string)
	clearMarker func()
}

func (s *finalizeHookStreamer) Finalize(ctx context.Context, content string) error {
	if err := s.Streamer.Finalize(ctx, content); err != nil {
		return err
	}
	s.runFinalizeHook(ctx, content)
	return nil
}

func (s *finalizeHookStreamer) FinalizeWithContext(ctx context.Context, content string, usage *bus.ContextUsage) error {
	if streamer, ok := s.Streamer.(bus.ContextUsageStreamer); ok {
		if err := streamer.FinalizeWithContext(ctx, content, usage); err != nil {
			return err
		}
	} else if err := s.Streamer.Finalize(ctx, content); err != nil {
		return err
	}
	s.runFinalizeHook(ctx, content)
	return nil
}

func (s *finalizeHookStreamer) UpdateReasoning(ctx context.Context, content string) error {
	if streamer, ok := s.Streamer.(bus.ReasoningStreamer); ok {
		return streamer.UpdateReasoning(ctx, content)
	}
	return nil
}

func (s *finalizeHookStreamer) FinalizeReasoning(ctx context.Context, content string) error {
	if streamer, ok := s.Streamer.(bus.ReasoningStreamer); ok {
		return streamer.FinalizeReasoning(ctx, content)
	}
	return nil
}

func (s *finalizeHookStreamer) SetModelName(modelName string) {
	setStreamerModelName(s.Streamer, strings.TrimSpace(modelName))
}

func (s *finalizeHookStreamer) SetTurnUsage(inputTokens, outputTokens int) {
	setStreamerTurnUsage(s.Streamer, inputTokens, outputTokens)
}

func (s *finalizeHookStreamer) runFinalizeHook(ctx context.Context, content string) {
	if s.onFinalize != nil {
		s.onFinalize(ctx, content)
	}
}

func (s *finalizeHookStreamer) ClearFinalizedStreamMarker() {
	if s.clearMarker != nil {
		s.clearMarker()
	}
}

// initChannel is a helper that looks up a factory by type name and creates the channel.
// typeName is the channel type used for factory lookup (e.g., "telegram").
// channelName is the config map key used as the channel's runtime name (e.g., "my_telegram").
func (m *Manager) initChannel(typeName, channelName string) {
	f, ok := getFactory(typeName)
	if !ok {
		logger.WarnCF("channels", "Factory not registered", map[string]any{
			"channel": channelName,
			"type":    typeName,
		})
		return
	}
	logger.DebugCF("channels", "Attempting to initialize channel", map[string]any{
		"channel": channelName,
		"type":    typeName,
	})
	ch, err := f(channelName, typeName, m.config, m.bus)
	if err != nil {
		logger.ErrorCF("channels", "Failed to initialize channel", map[string]any{
			"channel": channelName,
			"type":    typeName,
			"error":   err.Error(),
		})
	} else {
		// Inject MediaStore if channel supports it
		if m.mediaStore != nil {
			if setter, ok := ch.(mediaStoreSetter); ok {
				setter.SetMediaStore(m.mediaStore)
			}
		}
		// Inject PlaceholderRecorder if channel supports it
		if setter, ok := ch.(interface{ SetPlaceholderRecorder(r PlaceholderRecorder) }); ok {
			setter.SetPlaceholderRecorder(m)
		}
		// Inject owner reference so BaseChannel.HandleMessage can auto-trigger typing/reaction
		if setter, ok := ch.(interface{ SetOwner(ch Channel) }); ok {
			setter.SetOwner(ch)
		}
		m.channels[channelName] = ch
		m.publishChannelEvent(
			runtimeevents.KindChannelLifecycleInitialized,
			channelName,
			runtimeevents.Scope{Channel: channelName},
			runtimeevents.SeverityInfo,
			ChannelLifecyclePayload{Type: typeName},
		)
		logger.InfoCF("channels", "Channel enabled successfully", map[string]any{
			"channel": channelName,
			"type":    typeName,
		})
	}
}

func (m *Manager) getChannelConfigAndEnabled(channelName string) (*config.Channel, bool) {
	bc, ok := m.config.Channels[channelName]
	if !ok || bc == nil {
		return nil, false
	}
	if !bc.Enabled {
		return bc, false
	}

	// Use Type to determine the config struct for validation.
	// The map key (channelName) is the config key, which may differ from the type.
	channelType := bc.Type
	if channelType == "" {
		channelType = channelName
	}

	// Settings have already been decoded by InitChannelList, so we just need to
	// type-assert and check the relevant fields.
	decoded, err := bc.GetDecoded()
	if err != nil {
		return bc, false
	}
	//nolint:revive
	switch settings := decoded.(type) {
	case *config.WhatsAppSettings:
		if channelType == config.ChannelWhatsApp {
			return bc, settings.BridgeURL != ""
		}
		return bc, channelType == config.ChannelWhatsAppNative && settings.UseNative
	case *config.MatrixSettings:
		return bc, settings.Homeserver != "" && settings.UserID != "" && settings.AccessToken.String() != ""
	case *config.WeComSettings:
		return bc, settings.BotID != "" && settings.Secret.String() != ""
	case *config.PicoClientSettings:
		return bc, settings.URL != ""
	case *config.DingTalkSettings:
		return bc, settings.ClientID != ""
	case *config.SlackSettings:
		return bc, settings.BotToken.String() != ""
	case *config.WeixinSettings:
		return bc, settings.Token.String() != ""
	case *config.PicoSettings:
		return bc, settings.Token.String() != ""
	case *config.IRCSettings:
		return bc, settings.Server != ""
	case *config.LINESettings:
		return bc, settings.ChannelAccessToken.String() != ""
	case *config.OneBotSettings:
		return bc, settings.WSUrl != ""
	case *config.QQSettings:
		return bc, settings.AppSecret.String() != ""
	case *config.TelegramSettings:
		return bc, settings.Token.String() != ""
	case *config.FeishuSettings:
		return bc, settings.AppSecret.String() != ""
	case *config.MaixCamSettings:
		return bc, true
	case *config.TeamsWebhookSettings:
		return bc, true
	case *config.SlackWebhookSettings:
		return bc, true
	case *config.DiscordSettings:
		return bc, settings.Token.String() != ""
	case *config.VKSettings:
		return bc, settings.GroupID != 0 && settings.Token.String() != ""
	case *config.MQTTSettings:
		return bc, settings.Broker != "" && settings.AgentID != ""
	}

	return bc, bc.Enabled
}

// initChannels initializes all enabled channels based on the configuration.
// It iterates config entries and uses bc.Type to look up the appropriate factory.
func (m *Manager) initChannels(channels *config.ChannelsConfig) error {
	logger.InfoC("channels", "Initializing channel manager")

	for name, bc := range *channels {
		if !bc.Enabled {
			continue
		}
		_, ready := m.getChannelConfigAndEnabled(name)
		if !ready {
			continue
		}
		typeName := bc.Type
		if typeName == "" {
			typeName = name
		}
		m.initChannel(typeName, name)
	}

	logger.InfoCF("channels", "Channel initialization completed", map[string]any{
		"enabled_channels": len(m.channels),
	})

	return nil
}

// SetupHTTPServer creates a shared HTTP server with the given listen address.
// It registers health endpoints from the health server and discovers channels
// that implement WebhookHandler and/or HealthChecker to register their handlers.
func (m *Manager) SetupHTTPServer(addr string, healthServer *health.Server) {
	m.SetupHTTPServerListeners(nil, addr, healthServer)
}

// SetupHTTPServerListeners creates a shared HTTP server on pre-opened listeners.
// When listeners is empty it falls back to Addr-based ListenAndServe behavior.
func (m *Manager) SetupHTTPServerListeners(listeners []net.Listener, addr string, healthServer *health.Server) {
	m.mux = newDynamicServeMux()

	// Register health endpoints
	if healthServer != nil {
		healthServer.RegisterOnMux(m.mux)
	}

	// Discover and register webhook handlers and health checkers
	m.registerHTTPHandlersLocked()

	m.httpServer = &http.Server{
		Addr:         addr,
		Handler:      m.mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
	m.httpListeners = append([]net.Listener(nil), listeners...)
}

// RegisterHTTPHandler adds a non-channel route to the shared gateway server.
// It must be called after SetupHTTPServerListeners and rejects route collisions.
func (m *Manager) RegisterHTTPHandler(pattern string, handler http.Handler) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mux == nil {
		return errors.New("shared HTTP server is not configured")
	}
	if pattern == "" || handler == nil {
		return errors.New("HTTP handler pattern and implementation are required")
	}
	if err := m.mux.TryHandle(pattern, handler); err != nil {
		return fmt.Errorf("register HTTP handler %q: %w", pattern, err)
	}
	return nil
}

// ReplaceHTTPHandler atomically replaces an existing non-channel route.
func (m *Manager) ReplaceHTTPHandler(pattern string, handler http.Handler) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mux == nil {
		return errors.New("shared HTTP server is not configured")
	}
	if pattern == "" || handler == nil {
		return errors.New("HTTP handler pattern and implementation are required")
	}
	if err := m.mux.Replace(pattern, handler); err != nil {
		return fmt.Errorf("replace HTTP handler %q: %w", pattern, err)
	}
	return nil
}

// UnregisterHTTPHandler removes a non-channel route from the shared gateway server.
func (m *Manager) UnregisterHTTPHandler(pattern string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mux != nil {
		m.mux.Unhandle(pattern)
	}
}

// registerHTTPHandlersLocked registers webhook and health-check handlers for
// all channels currently in m.channels. Caller must hold m.mu (or ensure
// exclusive access).
func (m *Manager) registerHTTPHandlersLocked() {
	for name, ch := range m.channels {
		m.registerChannelHTTPHandler(name, ch)
	}
}

// registerChannelHTTPHandler registers the webhook/health handlers for a
// single channel onto m.mux.
func (m *Manager) registerChannelHTTPHandler(name string, ch Channel) {
	if wh, ok := ch.(WebhookHandler); ok {
		m.mux.Handle(wh.WebhookPath(), wh)
		m.publishChannelEvent(
			runtimeevents.KindChannelWebhookRegistered,
			name,
			runtimeevents.Scope{Channel: name},
			runtimeevents.SeverityInfo,
			ChannelLifecyclePayload{Type: channelTypeForEvent(m, name)},
		)
		logger.InfoCF("channels", "Webhook handler registered", map[string]any{
			"channel": name,
			"path":    wh.WebhookPath(),
		})
	}
	if hc, ok := ch.(HealthChecker); ok {
		m.mux.HandleFunc(hc.HealthPath(), hc.HealthHandler)
		logger.InfoCF("channels", "Health endpoint registered", map[string]any{
			"channel": name,
			"path":    hc.HealthPath(),
		})
	}
}

// unregisterChannelHTTPHandler removes the webhook/health handlers for a
// single channel from m.mux.
func (m *Manager) unregisterChannelHTTPHandler(name string, ch Channel) {
	if wh, ok := ch.(WebhookHandler); ok {
		m.mux.Unhandle(wh.WebhookPath())
		m.publishChannelEvent(
			runtimeevents.KindChannelWebhookUnregistered,
			name,
			runtimeevents.Scope{Channel: name},
			runtimeevents.SeverityInfo,
			ChannelLifecyclePayload{Type: channelTypeForEvent(m, name)},
		)
		logger.InfoCF("channels", "Webhook handler unregistered", map[string]any{
			"channel": name,
			"path":    wh.WebhookPath(),
		})
	}
	if hc, ok := ch.(HealthChecker); ok {
		m.mux.Unhandle(hc.HealthPath())
		logger.InfoCF("channels", "Health endpoint unregistered", map[string]any{
			"channel": name,
			"path":    hc.HealthPath(),
		})
	}
}

func (m *Manager) StartAll(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.channels) == 0 {
		logger.WarnC("channels", "No channels enabled")
	}

	logger.InfoC("channels", "Starting all channels")

	dispatchCtx, cancel := context.WithCancel(ctx)
	m.dispatchTask = &asyncTask{cancel: cancel}
	failedStarts := make([]error, 0, len(m.channels))
	failedNames := make([]string, 0, len(m.channels))

	for name, channel := range m.channels {
		logger.InfoCF("channels", "Starting channel", map[string]any{
			"channel": name,
		})
		if err := channel.Start(ctx); err != nil {
			logger.ErrorCF("channels", "Failed to start channel", map[string]any{
				"channel": name,
				"error":   err.Error(),
			})
			m.publishChannelEvent(
				runtimeevents.KindChannelLifecycleStartFailed,
				name,
				runtimeevents.Scope{Channel: name},
				runtimeevents.SeverityError,
				ChannelLifecyclePayload{Type: channelTypeForEvent(m, name), Error: err.Error()},
			)
			failedStarts = append(failedStarts, fmt.Errorf("channel %s: %w", name, err))
			failedNames = append(failedNames, name)
			continue
		}
		// Lazily create worker only after channel starts successfully
		channelType := name
		if m.config != nil {
			if bc := m.config.Channels.Get(name); bc != nil && bc.Type != "" {
				channelType = bc.Type
			}
		}
		m.installDeliveryOwnerLocked(dispatchCtx, name, channel, channelType)
		m.publishChannelEvent(
			runtimeevents.KindChannelLifecycleStarted,
			name,
			runtimeevents.Scope{Channel: name},
			runtimeevents.SeverityInfo,
			ChannelLifecyclePayload{Type: channelType},
		)
	}

	if len(m.channels) > 0 && len(m.workers) == 0 {
		if m.dispatchTask != nil {
			m.dispatchTask.cancel()
			m.dispatchTask = nil
		}

		sort.Strings(failedNames)
		if len(failedStarts) == 0 {
			return fmt.Errorf("failed to start any enabled channels")
		}

		logger.ErrorCF("channels", "All enabled channels failed to start", map[string]any{
			"failed":          len(failedNames),
			"total":           len(m.channels),
			"failed_channels": failedNames,
		})

		return fmt.Errorf("failed to start any enabled channels: %w", errors.Join(failedStarts...))
	}

	if len(failedNames) > 0 {
		sort.Strings(failedNames)
		logger.WarnCF("channels", "Some channels failed to start", map[string]any{
			"failed":          len(failedNames),
			"started":         len(m.workers),
			"total":           len(m.channels),
			"failed_channels": failedNames,
		})
	}

	// Start the dispatcher that reads from the bus and routes to workers
	go m.dispatchOutbound(dispatchCtx)
	go m.dispatchOutboundMedia(dispatchCtx)

	// Start the TTL janitor that cleans up stale typing/placeholder entries
	go m.runTTLJanitor(dispatchCtx)

	// Start shared HTTP server if configured
	if m.httpServer != nil {
		if len(m.httpListeners) > 0 {
			for _, listener := range m.httpListeners {
				ln := listener
				go func() {
					defer func() {
						if r := recover(); r != nil {
							logger.ErrorCF("channels", "HTTP server goroutine panic recovered",
								map[string]any{
									"addr":  ln.Addr().String(),
									"panic": fmt.Sprintf("%v", r),
									"stack": string(debug.Stack()),
								})
						}
					}()
					logger.InfoCF("channels", "Shared HTTP server listening", map[string]any{
						"addr": ln.Addr().String(),
					})
					if err := m.httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
						logger.FatalCF("channels", "Shared HTTP server error", map[string]any{
							"addr":  ln.Addr().String(),
							"error": err.Error(),
						})
					}
				}()
			}
		} else {
			go func() {
				defer func() {
					if r := recover(); r != nil {
						logger.ErrorCF("channels", "HTTP server goroutine panic recovered",
							map[string]any{
								"addr":  m.httpServer.Addr,
								"panic": fmt.Sprintf("%v", r),
								"stack": string(debug.Stack()),
							})
					}
				}()
				logger.InfoCF("channels", "Shared HTTP server listening", map[string]any{
					"addr": m.httpServer.Addr,
				})
				if err := m.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					logger.FatalCF("channels", "Shared HTTP server error", map[string]any{
						"error": err.Error(),
					})
				}
			}()
		}
	}

	logger.InfoCF("channels", "Channel startup completed", map[string]any{
		"started": len(m.workers),
		"failed":  len(failedNames),
		"total":   len(m.channels),
	})
	return nil
}

func (m *Manager) StopAll(ctx context.Context) error {
	type deliveryCloseTarget struct {
		owner  *deliveryOwner
		worker *channelWorker
	}
	type channelStopTarget struct {
		name        string
		channel     Channel
		channelType string
	}

	m.mu.Lock()
	httpServer := m.httpServer
	m.httpServer = nil
	m.httpListeners = nil

	if m.dispatchTask != nil {
		m.dispatchTask.cancel()
		m.dispatchTask = nil
	}

	deliveries := make([]deliveryCloseTarget, 0, len(m.workers))
	for name, w := range m.workers {
		deliveries = append(deliveries, deliveryCloseTarget{
			owner:  m.deliveryOwners[name],
			worker: w,
		})
	}

	channels := make([]channelStopTarget, 0, len(m.channels))
	for name, channel := range m.channels {
		channels = append(channels, channelStopTarget{
			name:        name,
			channel:     channel,
			channelType: channelTypeForEvent(m, name),
		})
	}
	m.mu.Unlock()

	logger.InfoC("channels", "Stopping all channels")

	// Shutdown shared HTTP server first
	if httpServer != nil {
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			logger.ErrorCF("channels", "Shared HTTP server shutdown error", map[string]any{
				"error": err.Error(),
			})
		}
	}

	// Close delivery queues and wait for accepted work to drain.
	for _, delivery := range deliveries {
		if delivery.owner != nil {
			delivery.owner.CloseDeliveryAndWait()
			continue
		}
		closeWorkerAndWait(delivery.worker)
	}
	if m.toolFeedback != nil {
		m.toolFeedback.StopAll()
	}

	// Stop all channels
	for _, target := range channels {
		logger.InfoCF("channels", "Stopping channel", map[string]any{
			"channel": target.name,
		})
		if err := target.channel.Stop(ctx); err != nil {
			logger.ErrorCF("channels", "Error stopping channel", map[string]any{
				"channel": target.name,
				"error":   err.Error(),
			})
			continue
		}
		m.publishChannelEvent(
			runtimeevents.KindChannelLifecycleStopped,
			target.name,
			runtimeevents.Scope{Channel: target.name},
			runtimeevents.SeverityInfo,
			ChannelLifecyclePayload{Type: target.channelType},
		)
	}

	logger.InfoC("channels", "All channels stopped")
	return nil
}

// newChannelWorker creates a channelWorker with a rate limiter configured
// for the given channel type. channelType is used for rate limit lookup.
func newChannelWorker(name string, ch Channel, channelType string) *channelWorker {
	rateVal := float64(defaultRateLimit)
	if r, ok := channelRateConfig[channelType]; ok {
		rateVal = r
	}
	burst := int(math.Max(1, math.Ceil(rateVal/2)))

	return &channelWorker{
		ch:         ch,
		queue:      make(chan bus.OutboundMessage, defaultChannelQueueSize),
		mediaQueue: make(chan bus.OutboundMediaMessage, defaultChannelQueueSize),
		done:       make(chan struct{}),
		mediaDone:  make(chan struct{}),
		limiter:    rate.NewLimiter(rate.Limit(rateVal), burst),
	}
}

func newDeliveryOwner(name string, ch Channel, channelType string) *deliveryOwner {
	return &deliveryOwner{
		name:   name,
		ch:     ch,
		worker: newChannelWorker(name, ch, channelType),
	}
}

func deliveryOwnerFromWorker(name string, ch Channel, w *channelWorker) *deliveryOwner {
	if ch == nil || w == nil {
		return nil
	}
	return &deliveryOwner{name: name, ch: ch, worker: w}
}

func (o *deliveryOwner) Worker() *channelWorker {
	if o == nil {
		return nil
	}
	return o.worker
}

func (o *deliveryOwner) borrowWorkerForSend() (*channelWorker, func(), error) {
	if o == nil || o.worker == nil {
		return nil, nil, errDeliveryClosed
	}
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return nil, nil, errDeliveryClosed
	}
	return o.worker, o.mu.Unlock, nil
}

func (o *deliveryOwner) StartDelivery(ctx context.Context, m *Manager) {
	if o == nil || o.worker == nil {
		return
	}
	go m.runWorkerOwned(ctx, o.name, o.worker, o.closeAdmission)
	go m.runMediaWorkerOwned(ctx, o.name, o.worker, o.closeAdmission)
}

func (o *deliveryOwner) Enqueue(ctx context.Context, msg bus.OutboundMessage) (bool, error) {
	if o == nil || o.worker == nil {
		return false, errDeliveryClosed
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return false, errDeliveryClosed
	}
	select {
	case o.worker.queue <- msg:
		return true, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (o *deliveryOwner) EnqueueMedia(
	ctx context.Context,
	msg bus.OutboundMediaMessage,
) (bool, error) {
	if o == nil || o.worker == nil {
		return false, errDeliveryClosed
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return false, errDeliveryClosed
	}
	select {
	case o.worker.mediaQueue <- msg:
		return true, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (o *deliveryOwner) CloseDeliveryAndWait() {
	if o == nil || o.worker == nil {
		return
	}
	o.closeAdmission()
	<-o.worker.done
	<-o.worker.mediaDone
}

func (o *deliveryOwner) closeAdmission() {
	if o == nil || o.worker == nil {
		return
	}
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return
	}
	o.closed = true
	close(o.worker.queue)
	close(o.worker.mediaQueue)
	o.mu.Unlock()
}

// runWorker processes outbound messages for a single channel.
// Message processing follows this order:
//  1. SplitByMarker (if enabled in config) - LLM semantic marker-based splitting
//  2. SplitMessage - channel-specific length-based splitting (MaxMessageLength)
func (m *Manager) runWorker(ctx context.Context, name string, w *channelWorker) {
	m.runWorkerOwned(ctx, name, w, nil)
}

func (m *Manager) runWorkerOwned(
	ctx context.Context,
	name string,
	w *channelWorker,
	closeAdmission func(),
) {
	defer close(w.done)
	for {
		select {
		case msg, ok := <-w.queue:
			if !ok {
				return
			}
			maxLen := 0
			if mlp, ok := w.ch.(MessageLengthProvider); ok {
				maxLen = mlp.MaxMessageLength()
			}

			// Collect all message chunks to send
			var chunks []string

			// Step 1: Try marker-based splitting if enabled.
			// Tool feedback must stay a single message, so it skips marker splitting.
			// Stream-final duplicate responses must also stay intact so preSend can
			// consume the whole final message before any marker chunk leaks.
			if m.finalizedStreamActiveForMessage(name, msg) {
				chunks = []string{msg.Content}
			} else if m.config != nil && m.config.Agents.Defaults.SplitOnMarker && !outboundMessageIsToolFeedback(msg) {
				if markerChunks := SplitByMarker(msg.Content); len(markerChunks) > 1 {
					for _, chunk := range markerChunks {
						chunkMsg := msg
						chunkMsg.Content = chunk
						chunks = append(chunks, splitOutboundMessageContent(chunkMsg, maxLen)...)
					}
				}
			}

			// Step 2: Fallback to length-based splitting if no chunks from marker
			if len(chunks) == 0 {
				chunks = splitOutboundMessageContent(msg, maxLen)
			}

			// Step 3: Send all chunks and publish one outcome for the logical message.
			var messageIDs []string
			delivered := true
			for _, chunk := range chunks {
				chunkMsg := msg
				chunkMsg.Content = chunk
				chunkIDs, chunkDelivered, _, sendErr := m.sendWithRetryPolicy(
					ctx, name, w, chunkMsg, true, publishNoOutcome,
				)
				if !chunkDelivered {
					m.publishOutboundFailed(name, msg, sendErr, false)
					delivered = false
					break
				}
				messageIDs = append(messageIDs, chunkIDs...)
			}
			if delivered {
				m.publishOutboundSent(name, msg, messageIDs)
			}
		case <-ctx.Done():
			m.failPendingOutbound(name, w.queue, ctx.Err())
			if closeAdmission != nil {
				closeAdmission()
			}
			m.failPendingOutbound(name, w.queue, ctx.Err())
			return
		}
	}
}

func (m *Manager) failPendingOutbound(
	name string,
	queue <-chan bus.OutboundMessage,
	err error,
) {
	for {
		select {
		case msg, ok := <-queue:
			if !ok {
				return
			}
			m.publishOutboundFailed(name, msg, err, false)
		default:
			return
		}
	}
}

func (m *Manager) finalizedStreamActiveForMessage(channelName string, msg bus.OutboundMessage) bool {
	if m == nil || !outboundMessageIsFinal(msg) {
		return false
	}
	chatID := outboundMessageChatID(msg)
	if strings.TrimSpace(channelName) == "" || strings.TrimSpace(chatID) == "" {
		return false
	}
	_, active := m.streamActive.Load(streamSuppressionKey(
		channelName, chatID, msg.SessionKey, primaryTraceScope(msg.TraceScopes),
	))
	return active
}

// splitOutboundMessageContent splits regular outbound content by maxLen, but
// keeps tool feedback in a single message by truncating the explanation body.
func splitOutboundMessageContent(msg bus.OutboundMessage, maxLen int) []string {
	if maxLen > 0 {
		if outboundMessageIsToolFeedback(msg) {
			animationSafeLen := maxLen - MaxToolFeedbackAnimationFrameLength()
			if animationSafeLen <= 0 {
				animationSafeLen = maxLen
			}
			if len([]rune(msg.Content)) > animationSafeLen {
				return []string{utils.FitToolFeedbackMessage(msg.Content, animationSafeLen)}
			}
			return []string{msg.Content}
		}
		if len([]rune(msg.Content)) > maxLen {
			return SplitMessage(msg.Content, maxLen)
		}
	}
	return []string{msg.Content}
}

// sendWithRetry sends a message through the channel with rate limiting and
// retry logic. It classifies errors to determine the retry strategy:
//   - ErrNotRunning / ErrSendFailed: permanent, no retry
//   - ErrRateLimit: fixed delay retry
//   - ErrTemporary / unknown: exponential backoff retry
func (m *Manager) sendWithRetry(
	ctx context.Context,
	name string,
	w *channelWorker,
	msg bus.OutboundMessage,
) ([]string, bool, bool, error) {
	return m.sendWithRetryPolicy(ctx, name, w, msg, true, publishDefinitiveOutcome)
}

func (m *Manager) sendWithRetryPolicy(
	ctx context.Context,
	name string,
	w *channelWorker,
	msg bus.OutboundMessage,
	retryAmbiguous bool,
	outcome outcomePublication,
) ([]string, bool, bool, error) {
	// Rate limit: wait for token
	if err := w.limiter.Wait(ctx); err != nil {
		// ctx canceled, shutting down
		m.publishChannelEvent(
			runtimeevents.KindChannelRateLimited,
			name,
			scopeFromOutboundContext(msg.Context),
			runtimeevents.SeverityWarn,
			ChannelOutboundPayload{
				TraceScopes:      append([]runtimeevents.TraceScope(nil), msg.TraceScopes...),
				TraceSettlement:  msg.TraceSettlement,
				ContentLen:       len([]rune(msg.Content)),
				ReplyToMessageID: msg.ReplyToMessageID,
				Error:            err.Error(),
			},
		)
		if outcome.failure(false) {
			m.publishOutboundFailed(name, msg, err, false)
		}
		return nil, false, false, err
	}

	isToolFeedback := outboundMessageIsToolFeedback(msg)
	terminalSucceeded := false
	var terminals []*toolFeedbackTerminal
	if m.toolFeedback != nil && !isToolFeedback && OutboundMessageDismissesTrackedToolFeedback(msg) {
		terminals = m.beginToolFeedbackTerminals(
			name,
			w.ch,
			outboundMessageChatID(msg),
			&msg.Context,
			msg.SessionKey,
			msg.TraceScopes,
		)
		defer func() {
			m.completeToolFeedbackTerminals(ctx, terminals, terminalSucceeded)
		}()
	}

	// Pre-send: stop typing and try to edit placeholder
	if msgIDs, handled := m.preSend(ctx, name, msg, w.ch); handled {
		terminalSucceeded = true
		if outcome.success() {
			m.publishOutboundSent(name, msg, msgIDs)
		}
		return msgIDs, true, false, nil
	}

	var lastErr error
	var msgIDs []string
	ambiguous := false
	for attempt := 0; attempt <= maxRetries; attempt++ {
		attemptStart := time.Now()
		if isToolFeedback && m.toolFeedback != nil {
			msgIDs, lastErr = m.deliverToolFeedback(ctx, name, w.ch, msg, w.ch.Send)
		} else {
			msgIDs, lastErr = w.ch.Send(ctx, msg)
		}
		if lastErr == nil {
			terminalSucceeded = true
			if attempt > 0 {
				logger.InfoCF("channels", "Outbound send recovered after retry", map[string]any{
					"channel":        name,
					"chat_id":        outboundMessageChatID(msg),
					"attempt":        attempt + 1,
					"max_attempts":   maxRetries + 1,
					"duration_ms":    time.Since(attemptStart).Milliseconds(),
					"classification": "success_after_retry",
				})
			}
			if outcome.success() {
				m.publishOutboundSent(name, msg, msgIDs)
			}
			return msgIDs, true, false, nil
		}
		if len(msgIDs) > 0 ||
			(!errors.Is(lastErr, ErrNotRunning) &&
				!errors.Is(lastErr, ErrSendFailed) &&
				!errors.Is(lastErr, ErrRateLimit)) {
			ambiguous = true
		}

		classification := classifySendError(lastErr)
		logger.WarnCF("channels", "Outbound send attempt failed", map[string]any{
			"channel":        name,
			"chat_id":        outboundMessageChatID(msg),
			"attempt":        attempt + 1,
			"max_attempts":   maxRetries + 1,
			"duration_ms":    time.Since(attemptStart).Milliseconds(),
			"classification": classification,
			"error":          lastErr.Error(),
		})
		if ambiguous && !retryAmbiguous {
			break
		}

		// Permanent failures — don't retry
		if errors.Is(lastErr, ErrNotRunning) || errors.Is(lastErr, ErrSendFailed) {
			break
		}

		// Last attempt exhausted — don't sleep
		if attempt == maxRetries {
			break
		}

		// Rate limit error — fixed delay
		if errors.Is(lastErr, ErrRateLimit) {
			select {
			case <-time.After(rateLimitDelay):
				continue
			case <-ctx.Done():
				if outcome.failure(ambiguous) {
					m.publishOutboundFailed(name, msg, ctx.Err(), false)
				}
				return nil, false, ambiguous, ctx.Err()
			}
		}

		// ErrTemporary or unknown error — exponential backoff
		backoff := min(time.Duration(float64(baseBackoff)*math.Pow(2, float64(attempt))), maxBackoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			if outcome.failure(ambiguous) {
				m.publishOutboundFailed(name, msg, ctx.Err(), false)
			}
			return nil, false, ambiguous, ctx.Err()
		}
	}

	// All retries exhausted or permanent failure
	logger.ErrorCF("channels", "Send failed", map[string]any{
		"channel": name,
		"chat_id": outboundMessageChatID(msg),
		"error":   lastErr.Error(),
		"retries": maxRetries,
	})
	if outcome.failure(ambiguous) {
		m.publishOutboundFailed(name, msg, lastErr, false)
	}

	return nil, false, ambiguous, lastErr
}

func classifySendError(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, ErrNotRunning):
		return "not_running"
	case errors.Is(err, ErrSendFailed):
		return "permanent"
	case errors.Is(err, ErrRateLimit):
		return "rate_limit"
	case errors.Is(err, ErrTemporary):
		return "temporary"
	default:
		return "unknown"
	}
}

func dispatchLoop[M any](
	ctx context.Context,
	m *Manager,
	ch <-chan M,
	getChannel func(M) string,
	requiresOutcome func(M) bool,
	enqueue func(context.Context, *deliveryOwner, M) bool,
	reject func(M, error),
	startMsg, stopMsg, unknownMsg, noWorkerMsg string,
) {
	logger.InfoC("channels", startMsg)

	for {
		select {
		case <-ctx.Done():
			logger.InfoC("channels", stopMsg)
			return

		case msg, ok := <-ch:
			if !ok {
				logger.InfoC("channels", stopMsg)
				return
			}

			channel := getChannel(msg)

			// Internal traffic has no external delivery owner. Preserve the
			// historical silent skip unless this message explicitly promises a
			// terminal delivery outcome.
			if constants.IsInternalChannel(channel) {
				if requiresOutcome(msg) {
					reject(msg, fmt.Errorf("internal channel %s has no external delivery owner", channel))
				}
				continue
			}

			m.mu.RLock()
			_, exists := m.channels[channel]
			owner := m.deliveryOwnerLocked(channel)
			m.mu.RUnlock()

			if !exists {
				logger.WarnCF("channels", unknownMsg, map[string]any{"channel": channel})
				reject(msg, fmt.Errorf("channel %s not found", channel))
				continue
			}

			if owner != nil && owner.Worker() != nil {
				if !enqueue(ctx, owner, msg) {
					return
				}
			} else if exists {
				logger.WarnCF("channels", noWorkerMsg, map[string]any{"channel": channel})
				reject(msg, fmt.Errorf("channel %s has no active worker", channel))
			}
		}
	}
}

func (m *Manager) dispatchOutbound(ctx context.Context) {
	dispatchLoop(
		ctx, m,
		m.bus.OutboundChan(),
		func(msg bus.OutboundMessage) string { return outboundMessageChannel(msg) },
		func(msg bus.OutboundMessage) bool { return msg.TraceSettlement },
		func(ctx context.Context, owner *deliveryOwner, msg bus.OutboundMessage) bool {
			queued, err := owner.Enqueue(ctx, msg)
			if queued {
				m.publishOutboundQueued(outboundMessageChannel(msg), msg)
				return true
			}
			if err != nil {
				m.publishOutboundFailed(outboundMessageChannel(msg), msg, err, false)
				return errors.Is(err, errDeliveryClosed)
			}
			return false
		},
		func(msg bus.OutboundMessage, err error) {
			m.publishOutboundFailed(outboundMessageChannel(msg), msg, err, false)
		},
		"Outbound dispatcher started",
		"Outbound dispatcher stopped",
		"Unknown channel for outbound message",
		"Channel has no active worker, skipping message",
	)
}

func (m *Manager) dispatchOutboundMedia(ctx context.Context) {
	dispatchLoop(
		ctx, m,
		m.bus.OutboundMediaChan(),
		func(msg bus.OutboundMediaMessage) string { return outboundMediaChannel(msg) },
		func(msg bus.OutboundMediaMessage) bool { return msg.TraceSettlement },
		func(ctx context.Context, owner *deliveryOwner, msg bus.OutboundMediaMessage) bool {
			queued, err := owner.EnqueueMedia(ctx, msg)
			if queued {
				m.publishOutboundMediaQueued(outboundMediaChannel(msg), msg)
				return true
			}
			if err != nil {
				m.publishOutboundMediaFailed(outboundMediaChannel(msg), msg, err)
				return errors.Is(err, errDeliveryClosed)
			}
			return false
		},
		func(msg bus.OutboundMediaMessage, err error) {
			m.publishOutboundMediaFailed(outboundMediaChannel(msg), msg, err)
		},
		"Outbound media dispatcher started",
		"Outbound media dispatcher stopped",
		"Unknown channel for outbound media message",
		"Channel has no active worker, skipping media message",
	)
}

// runMediaWorker processes outbound media messages for a single channel.
func (m *Manager) runMediaWorkerOwned(
	ctx context.Context,
	name string,
	w *channelWorker,
	closeAdmission func(),
) {
	defer close(w.mediaDone)
	for {
		select {
		case msg, ok := <-w.mediaQueue:
			if !ok {
				return
			}
			_, _ = m.sendMediaWithRetry(ctx, name, w, msg)
		case <-ctx.Done():
			m.failPendingOutboundMedia(name, w.mediaQueue, ctx.Err())
			if closeAdmission != nil {
				closeAdmission()
			}
			m.failPendingOutboundMedia(name, w.mediaQueue, ctx.Err())
			return
		}
	}
}

func (m *Manager) failPendingOutboundMedia(
	name string,
	queue <-chan bus.OutboundMediaMessage,
	err error,
) {
	for {
		select {
		case msg, ok := <-queue:
			if !ok {
				return
			}
			m.publishOutboundMediaFailed(name, msg, err)
		default:
			return
		}
	}
}

// sendMediaWithRetry sends a media message through the channel with rate limiting and
// retry logic. It returns the message IDs and nil on success, or nil and the last error
// after retries, including when the channel does not support MediaSender.
func (m *Manager) sendMediaWithRetry(
	ctx context.Context,
	name string,
	w *channelWorker,
	msg bus.OutboundMediaMessage,
) ([]string, error) {
	messageIDs, _, err := m.sendMediaWithRetryPolicy(
		ctx, name, w, msg, publishDefinitiveOutcome,
	)
	return messageIDs, err
}

func (m *Manager) sendMediaWithRetryPolicy(
	ctx context.Context,
	name string,
	w *channelWorker,
	msg bus.OutboundMediaMessage,
	outcome outcomePublication,
) ([]string, bool, error) {
	ms, ok := w.ch.(MediaSender)
	if !ok {
		err := fmt.Errorf("channel %q does not support media sending", name)
		logger.WarnCF("channels", "Channel does not support MediaSender", map[string]any{
			"channel": name,
			"error":   err.Error(),
		})
		if outcome.failure(false) {
			m.publishOutboundMediaFailed(name, msg, err)
		}
		return nil, false, err
	}

	// Rate limit: wait for token
	if err := w.limiter.Wait(ctx); err != nil {
		m.publishChannelEvent(
			runtimeevents.KindChannelRateLimited,
			name,
			scopeFromOutboundContext(msg.Context),
			runtimeevents.SeverityWarn,
			ChannelOutboundPayload{
				TraceScopes:     append([]runtimeevents.TraceScope(nil), msg.TraceScopes...),
				TraceSettlement: msg.TraceSettlement,
				Media:           true,
				Error:           err.Error(),
			},
		)
		if outcome.failure(false) {
			m.publishOutboundMediaFailed(name, msg, err)
		}
		return nil, false, err
	}

	terminalSucceeded := false
	var terminals []*toolFeedbackTerminal
	if m.toolFeedback != nil {
		terminals = m.beginToolFeedbackTerminals(
			name,
			w.ch,
			outboundMediaChatID(msg),
			&msg.Context,
			msg.SessionKey,
			msg.TraceScopes,
		)
		defer func() {
			m.completeToolFeedbackTerminals(ctx, terminals, terminalSucceeded)
		}()
	}

	// Pre-send: stop typing and clean up any placeholder before sending media.
	m.preSendMedia(ctx, name, msg, w.ch)

	var lastErr error
	var msgIDs []string
	ambiguous := false
	for attempt := 0; attempt <= maxRetries; attempt++ {
		msgIDs, lastErr = ms.SendMedia(ctx, msg)
		if lastErr == nil {
			terminalSucceeded = true
			if outcome.success() {
				m.publishOutboundMediaSent(name, msg, msgIDs)
			}
			return msgIDs, false, nil
		}
		if len(msgIDs) > 0 ||
			(!errors.Is(lastErr, ErrNotRunning) &&
				!errors.Is(lastErr, ErrSendFailed) &&
				!errors.Is(lastErr, ErrRateLimit)) {
			ambiguous = true
		}
		if len(msgIDs) > 0 {
			break
		}

		// Permanent failures — don't retry
		if errors.Is(lastErr, ErrNotRunning) || errors.Is(lastErr, ErrSendFailed) {
			break
		}

		// Last attempt exhausted — don't sleep
		if attempt == maxRetries {
			break
		}

		// Rate limit error — fixed delay
		if errors.Is(lastErr, ErrRateLimit) {
			select {
			case <-time.After(rateLimitDelay):
				continue
			case <-ctx.Done():
				if outcome.failure(ambiguous) {
					m.publishOutboundMediaFailed(name, msg, ctx.Err())
				}
				return nil, ambiguous, ctx.Err()
			}
		}

		// ErrTemporary or unknown error — exponential backoff
		backoff := min(time.Duration(float64(baseBackoff)*math.Pow(2, float64(attempt))), maxBackoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			if outcome.failure(ambiguous) {
				m.publishOutboundMediaFailed(name, msg, ctx.Err())
			}
			return nil, ambiguous, ctx.Err()
		}
	}

	// All retries exhausted or permanent failure
	logger.ErrorCF("channels", "SendMedia failed", map[string]any{
		"channel": name,
		"chat_id": outboundMediaChatID(msg),
		"error":   lastErr.Error(),
		"retries": maxRetries,
	})
	if outcome.failure(ambiguous) {
		m.publishOutboundMediaFailed(name, msg, lastErr)
	}
	return nil, ambiguous, lastErr
}

// runTTLJanitor periodically scans the typingStops, placeholders, and stream
// tombstone maps and evicts entries that have exceeded their TTL. This prevents
// memory accumulation when outbound paths fail to trigger preSend (e.g. LLM errors).
func (m *Manager) runTTLJanitor(ctx context.Context) {
	ticker := time.NewTicker(janitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			m.typingStops.Range(func(key, value any) bool {
				if entry, ok := value.(typingEntry); ok {
					if now.Sub(entry.createdAt) > typingStopTTL {
						if _, loaded := m.typingStops.LoadAndDelete(key); loaded {
							entry.stop() // idempotent, safe
						}
					}
				}
				return true
			})
			m.reactionUndos.Range(func(key, value any) bool {
				if entry, ok := value.(reactionEntry); ok {
					if now.Sub(entry.createdAt) > typingStopTTL {
						if _, loaded := m.reactionUndos.LoadAndDelete(key); loaded {
							entry.undo() // idempotent, safe
						}
					}
				}
				return true
			})
			m.placeholders.Range(func(key, value any) bool {
				if entry, ok := value.(placeholderEntry); ok {
					if now.Sub(entry.createdAt) > placeholderTTL {
						m.placeholders.Delete(key)
					}
				}
				return true
			})
			m.streamAuxiliaryTombstones.Range(func(key, value any) bool {
				if createdAt, ok := value.(time.Time); !ok || now.Sub(createdAt) > streamAuxiliaryTombstoneTTL {
					m.streamAuxiliaryTombstones.Delete(key)
				}
				return true
			})
		}
	}
}

func (m *Manager) GetChannel(name string) (Channel, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	channel, ok := m.channels[name]
	return channel, ok
}

func (m *Manager) deliveryOwnerLocked(name string) *deliveryOwner {
	if m.deliveryOwners != nil {
		if owner := m.deliveryOwners[name]; owner != nil {
			return owner
		}
	}
	ch := m.channels[name]
	w := m.workers[name]
	return deliveryOwnerFromWorker(name, ch, w)
}

func (m *Manager) GetStatus() map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := make(map[string]any)
	for name, channel := range m.channels {
		channelStatus := map[string]any{
			"enabled": true,
			"running": channel.IsRunning(),
		}
		if _, ok := m.channelRestartRequired[name]; ok {
			channelStatus["restart_required"] = true
			channelStatus["restart_reason"] = "channel config changed"
		}
		status[name] = channelStatus
	}
	return status
}

func (m *Manager) GetEnabledChannels() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.channels))
	for name := range m.channels {
		names = append(names, name)
	}
	return names
}

// Reload updates the config reference without restarting channels.
// This is used when channel config hasn't changed but other parts of the config have.
func (m *Manager) Reload(ctx context.Context, cfg *config.Config) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Save old config so we can revert on error.
	oldConfig := m.config

	// Update config early: initChannel uses m.config via factory(m.config, m.bus).
	m.config = cfg

	desiredHashes := toChannelHashes(cfg)
	list := make(map[string]string, len(desiredHashes))
	for name, hash := range desiredHashes {
		list[name] = hash
	}
	if m.channelRestartRequired == nil {
		m.channelRestartRequired = make(map[string]string)
	}
	added, removed := compareChannels(m.channelHashes, list)
	inactiveChanged := make(map[string]Channel)
	changed, added, removed := splitChangedChannels(added, removed)
	for _, name := range changed {
		currentHash, ok := m.channelHashes[name]
		if !ok {
			added = append(added, name)
			continue
		}
		if _, ok := m.channels[name]; !ok {
			added = append(added, name)
			continue
		}
		if w, ok := m.workers[name]; !ok || w == nil {
			logger.InfoCF("channels", "Recreating inactive changed channel", map[string]any{
				"channel": name,
			})
			inactiveChanged[name] = m.channels[name]
			added = append(added, name)
			continue
		}
		m.channelRestartRequired[name] = list[name]
		list[name] = currentHash
		logger.WarnCF("channels", "Channel config changed; restart required", map[string]any{
			"channel": name,
		})
	}
	for name := range m.channelRestartRequired {
		desiredHash, ok := desiredHashes[name]
		if !ok || desiredHash == m.channelHashes[name] {
			delete(m.channelRestartRequired, name)
		}
	}

	deferFuncs := make([]func(), 0, len(removed)+len(added))
	for _, name := range removed {
		channel := m.channels[name]
		deferFuncs = append(deferFuncs, func() {
			m.UnregisterChannel(name)
			if channel == nil {
				return
			}
			logger.InfoCF("channels", "Stopping channel", map[string]any{
				"channel": name,
			})
			if err := channel.Stop(ctx); err != nil {
				logger.ErrorCF("channels", "Error stopping channel", map[string]any{
					"channel": name,
					"error":   err.Error(),
				})
			}
		})
	}
	dispatchCtx, cancel := context.WithCancel(ctx)
	m.dispatchTask = &asyncTask{cancel: cancel}
	cc, err := toChannelConfig(cfg, added)
	if err != nil {
		logger.ErrorC("channels", fmt.Sprintf("toChannelConfig error: %v", err))
		m.config = oldConfig
		cancel()
		return err
	}
	err = m.initChannels(cc)
	if err != nil {
		logger.ErrorC("channels", fmt.Sprintf("initChannels error: %v", err))
		m.config = oldConfig
		cancel()
		return err
	}
	for name, oldChannel := range inactiveChanged {
		if m.channels[name] == oldChannel {
			err := fmt.Errorf("replacement channel %s was not initialized", name)
			logger.ErrorCF("channels", "Failed to initialize replacement channel", map[string]any{
				"channel": name,
				"error":   err.Error(),
			})
			m.config = oldConfig
			cancel()
			return err
		}
		if m.toolFeedback != nil {
			m.toolFeedback.RetireChannel(ctx, name)
		}
		if err := oldChannel.Stop(ctx); err != nil {
			logger.ErrorCF("channels", "Error stopping inactive changed channel", map[string]any{
				"channel": name,
				"error":   err.Error(),
			})
		}
	}
	for _, name := range added {
		channel := m.channels[name]
		logger.InfoCF("channels", "Starting channel", map[string]any{
			"channel": name,
		})
		if err := channel.Start(ctx); err != nil {
			logger.ErrorCF("channels", "Failed to start channel", map[string]any{
				"channel": name,
				"error":   err.Error(),
			})
			m.publishChannelEvent(
				runtimeevents.KindChannelLifecycleStartFailed,
				name,
				runtimeevents.Scope{Channel: name},
				runtimeevents.SeverityError,
				ChannelLifecyclePayload{Type: channelTypeForEvent(m, name), Error: err.Error()},
			)
			continue
		}
		// Lazily create worker only after channel starts successfully
		channelType := name
		if m.config != nil {
			if bc := m.config.Channels.Get(name); bc != nil && bc.Type != "" {
				channelType = bc.Type
			}
		}
		m.installDeliveryOwnerLocked(dispatchCtx, name, channel, channelType)
		m.publishChannelEvent(
			runtimeevents.KindChannelLifecycleStarted,
			name,
			runtimeevents.Scope{Channel: name},
			runtimeevents.SeverityInfo,
			ChannelLifecyclePayload{Type: channelType},
		)
		deferFuncs = append(deferFuncs, func() {
			m.RegisterChannel(name, channel)
		})
	}

	// Commit hashes only on full success.
	m.channelHashes = list
	if m.toolFeedback != nil && cfg != nil {
		m.toolFeedback.Configure(
			ToolFeedbackAnimatorConfig{
				AnimationInterval: cfg.Agents.Defaults.GetToolFeedbackAnimationInterval(),
				MinEditInterval:   cfg.Agents.Defaults.GetToolFeedbackEditMinInterval(),
			},
			cfg.Agents.Defaults.IsToolFeedbackSeparateMessagesEnabled(),
		)
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.ErrorCF("channels", "channel registration goroutine panic recovered",
					map[string]any{
						"panic": fmt.Sprintf("%v", r),
						"stack": string(debug.Stack()),
					})
			}
		}()
		for _, f := range deferFuncs {
			f()
		}
	}()
	return nil
}

func (m *Manager) RegisterChannel(name string, channel Channel) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.channels[name] = channel
	if m.mux != nil {
		m.registerChannelHTTPHandler(name, channel)
	}
}

func (m *Manager) UnregisterChannel(name string) {
	m.mu.Lock()
	ch := m.channels[name]
	if ch != nil && m.mux != nil {
		m.unregisterChannelHTTPHandler(name, ch)
	}
	owner := m.deliveryOwners[name]
	w := m.workers[name]
	if owner == nil {
		delete(m.workers, name)
		delete(m.channels, name)
	}
	m.mu.Unlock()

	if owner != nil {
		owner.CloseDeliveryAndWait()
	} else {
		closeWorkerAndWait(w)
	}
	if m.toolFeedback != nil {
		m.toolFeedback.RetireChannel(context.Background(), name)
	}

	m.mu.Lock()
	if owner != nil && m.deliveryOwners[name] == owner {
		delete(m.deliveryOwners, name)
	}
	if w != nil && m.workers[name] == w {
		delete(m.workers, name)
	}
	if ch != nil && m.channels[name] == ch {
		delete(m.channels, name)
	}
	m.mu.Unlock()
}

// SendMessage sends an outbound message synchronously through the channel
// worker's rate limiter and retry logic. It blocks until the message is
// delivered (or all retries are exhausted), which preserves ordering when
// a subsequent operation depends on the message having been sent.
func (m *Manager) SendMessage(ctx context.Context, msg bus.OutboundMessage) error {
	return m.sendMessageWithRetryPolicy(ctx, msg, true, publishDefinitiveOutcome)
}

// SendMessageProvisional suppresses a definitely-not-sent failure outcome so
// the caller can try a fallback. Success and ambiguous failure remain terminal.
// Callers must check DeliveryDefinitelyNotSent before attempting the fallback.
func (m *Manager) SendMessageProvisional(ctx context.Context, msg bus.OutboundMessage) error {
	return m.sendMessageWithRetryPolicy(ctx, msg, true, publishSuccessOnly)
}

// SendMessageDefiniteRetryOnly retries only channel rejections known to occur
// before remote acceptance. It is intended for durable callers that must
// preserve an ambiguous-delivery outcome rather than risk a duplicate send.
func (m *Manager) SendMessageDefiniteRetryOnly(
	ctx context.Context,
	msg bus.OutboundMessage,
) error {
	return m.sendMessageWithRetryPolicy(ctx, msg, false, publishDefinitiveOutcome)
}

func (m *Manager) sendMessageWithRetryPolicy(
	ctx context.Context,
	msg bus.OutboundMessage,
	retryAmbiguous bool,
	outcome outcomePublication,
) error {
	var err error
	msg, err = bus.NormalizeOutboundMessage(msg)
	if err != nil {
		return newDeliveryError(err, false)
	}
	channelName := outboundMessageChannel(msg)

	m.mu.RLock()
	_, exists := m.channels[channelName]
	owner := m.deliveryOwnerLocked(channelName)
	m.mu.RUnlock()

	if !exists {
		return m.rejectMessageBeforeSend(
			outcome, channelName, msg, fmt.Errorf("channel %s not found", channelName),
		)
	}
	var w *channelWorker
	if owner != nil {
		var release func()
		var borrowErr error
		w, release, borrowErr = owner.borrowWorkerForSend()
		if borrowErr != nil {
			return m.rejectMessageBeforeSend(outcome, channelName, msg, borrowErr)
		}
		defer release()
	}
	if w == nil {
		return m.rejectMessageBeforeSend(
			outcome, channelName, msg, fmt.Errorf("channel %s has no active worker", channelName),
		)
	}

	maxLen := 0
	if mlp, ok := w.ch.(MessageLengthProvider); ok {
		maxLen = mlp.MaxMessageLength()
	}
	if chunks := splitOutboundMessageContent(msg, maxLen); len(chunks) > 1 {
		deliveredChunks := 0
		var messageIDs []string
		for _, chunk := range chunks {
			chunkMsg := msg
			chunkMsg.Content = chunk
			if chunkIDs, delivered, ambiguous, sendErr := m.sendWithRetryPolicy(
				ctx, channelName, w, chunkMsg, retryAmbiguous, publishNoOutcome,
			); !delivered {
				logicalAmbiguous := ambiguous || deliveredChunks > 0
				if outcome.failure(logicalAmbiguous) {
					m.publishOutboundFailed(channelName, msg, sendErr, false)
				}
				return newDeliveryError(
					fmt.Errorf("channel %s failed to deliver message: %w", channelName, sendErr),
					logicalAmbiguous,
				)
			} else {
				messageIDs = append(messageIDs, chunkIDs...)
			}
			deliveredChunks++
		}
		if outcome.success() {
			m.publishOutboundSent(channelName, msg, messageIDs)
		}
	} else {
		if len(chunks) == 1 {
			msg.Content = chunks[0]
		}
		if _, delivered, ambiguous, sendErr := m.sendWithRetryPolicy(
			ctx, channelName, w, msg, retryAmbiguous, outcome,
		); !delivered {
			return newDeliveryError(
				fmt.Errorf("channel %s failed to deliver message: %w", channelName, sendErr),
				ambiguous,
			)
		}
	}
	return nil
}

func (m *Manager) rejectMessageBeforeSend(
	outcome outcomePublication,
	channelName string,
	msg bus.OutboundMessage,
	err error,
) error {
	if outcome.failure(false) {
		m.publishOutboundFailed(channelName, msg, err, false)
	}
	return newDeliveryError(err, false)
}

// SendMedia sends outbound media synchronously through the channel worker's
// rate limiter and retry logic. It blocks until the media is delivered (or all
// retries are exhausted), which preserves ordering when later agent behavior
// depends on actual media delivery.
func (m *Manager) SendMedia(ctx context.Context, msg bus.OutboundMediaMessage) error {
	return m.sendMedia(ctx, msg, publishDefinitiveOutcome)
}

// SendMediaProvisional suppresses a definitely-not-sent failure outcome so the
// caller can try a fallback. Success and ambiguous failure remain terminal.
// Callers must check DeliveryDefinitelyNotSent before attempting the fallback.
func (m *Manager) SendMediaProvisional(ctx context.Context, msg bus.OutboundMediaMessage) error {
	return m.sendMedia(ctx, msg, publishSuccessOnly)
}

func (m *Manager) sendMedia(
	ctx context.Context,
	msg bus.OutboundMediaMessage,
	outcome outcomePublication,
) error {
	var err error
	msg, err = bus.NormalizeOutboundMediaMessage(msg)
	if err != nil {
		return newDeliveryError(err, false)
	}
	channelName := outboundMediaChannel(msg)

	m.mu.RLock()
	_, exists := m.channels[channelName]
	owner := m.deliveryOwnerLocked(channelName)
	m.mu.RUnlock()

	if !exists {
		return m.rejectMediaBeforeSend(
			outcome, channelName, msg, fmt.Errorf("channel %s not found", channelName),
		)
	}
	var w *channelWorker
	if owner != nil {
		var release func()
		var borrowErr error
		w, release, borrowErr = owner.borrowWorkerForSend()
		if borrowErr != nil {
			return m.rejectMediaBeforeSend(outcome, channelName, msg, borrowErr)
		}
		defer release()
	}
	if w == nil {
		return m.rejectMediaBeforeSend(
			outcome, channelName, msg, fmt.Errorf("channel %s has no active worker", channelName),
		)
	}

	_, ambiguous, err := m.sendMediaWithRetryPolicy(ctx, channelName, w, msg, outcome)
	if err != nil {
		return newDeliveryError(err, ambiguous)
	}
	return nil
}

func (m *Manager) rejectMediaBeforeSend(
	outcome outcomePublication,
	channelName string,
	msg bus.OutboundMediaMessage,
	err error,
) error {
	if outcome.failure(false) {
		m.publishOutboundMediaFailed(channelName, msg, err)
	}
	return newDeliveryError(err, false)
}

func (m *Manager) SendToChannel(ctx context.Context, channelName, chatID, content string) error {
	m.mu.RLock()
	channel, exists := m.channels[channelName]
	owner := m.deliveryOwnerLocked(channelName)
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("channel %s not found", channelName)
	}

	msg := bus.OutboundMessage{
		Context: bus.NewOutboundContext(channelName, chatID, ""),
		Content: content,
	}
	msg, err := bus.NormalizeOutboundMessage(msg)
	if err != nil {
		return err
	}

	if owner != nil && owner.Worker() != nil {
		queued, enqueueErr := owner.Enqueue(ctx, msg)
		if queued {
			m.publishOutboundQueued(channelName, msg)
			return nil
		}
		if enqueueErr != nil {
			return enqueueErr
		}
		return fmt.Errorf("channel %s has closed delivery", channelName)
	}

	// Fallback: direct send (should not happen)
	_, err = channel.Send(ctx, msg)
	return err
}
