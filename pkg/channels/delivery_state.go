package channels

import (
	"context"
	"strings"
	"sync"
	"time"

	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
)

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

// deliveryInteractionState owns transient UI state associated with outbound
// delivery. It is embedded in Manager while callers migrate to narrower APIs.
type deliveryInteractionState struct {
	placeholders  sync.Map // "channel:chatID" -> placeholderEntry
	typingStops   sync.Map // "channel:chatID" -> typingEntry
	reactionUndos sync.Map // "channel:chatID" -> reactionEntry
	toolFeedback  *ToolFeedbackCoordinator
}

func (s *deliveryInteractionState) initializeToolFeedback(
	config ToolFeedbackAnimatorConfig,
	separateMessages bool,
) {
	s.toolFeedback = NewToolFeedbackCoordinator(config, separateMessages)
}

func (s *deliveryInteractionState) hasToolFeedback() bool {
	return s != nil && s.toolFeedback != nil
}

func (s *deliveryInteractionState) beginToolFeedbackTerminals(
	keys []string,
	scoped bool,
) []*toolFeedbackTerminal {
	if !s.hasToolFeedback() {
		return nil
	}
	terminals := make([]*toolFeedbackTerminal, 0, len(keys))
	for _, key := range keys {
		if scoped {
			terminals = append(terminals, s.toolFeedback.BeginTerminal(key))
		} else {
			terminals = append(terminals, s.toolFeedback.BeginTransientTerminal(key))
		}
	}
	return terminals
}

func (s *deliveryInteractionState) completeToolFeedbackTerminals(
	ctx context.Context,
	terminals []*toolFeedbackTerminal,
	success bool,
) {
	if !s.hasToolFeedback() {
		return
	}
	for _, terminal := range terminals {
		s.toolFeedback.CompleteTerminal(ctx, terminal, success)
	}
}

func (s *deliveryInteractionState) deliverToolFeedback(
	ctx context.Context,
	key, chatID, content string,
	operations toolFeedbackOperations,
	send func(context.Context, string) (toolFeedbackSendResult, error),
) ([]string, error) {
	if !s.hasToolFeedback() {
		result, err := send(ctx, content)
		return result.messageIDs, err
	}
	return s.toolFeedback.deliver(ctx, key, chatID, content, operations, send)
}

func (s *deliveryInteractionState) dismissToolFeedback(
	ctx context.Context,
	keys []string,
	scoped bool,
) {
	if !s.hasToolFeedback() {
		return
	}
	for _, key := range keys {
		if scoped {
			s.toolFeedback.Dismiss(ctx, key)
		} else {
			s.toolFeedback.DismissTransient(ctx, key)
		}
	}
}

func (s *deliveryInteractionState) singleActiveScopedToolFeedbackKey(base string) (string, bool) {
	if !s.hasToolFeedback() {
		return "", false
	}
	return s.toolFeedback.singleActiveScopedKey(base)
}

func (s *deliveryInteractionState) releaseToolFeedbackTerminals(keys []string) {
	if !s.hasToolFeedback() {
		return
	}
	for _, key := range keys {
		s.toolFeedback.ReleaseTerminal(key)
	}
}

func (s *deliveryInteractionState) stopToolFeedback() {
	if s.hasToolFeedback() {
		s.toolFeedback.StopAll()
	}
}

func (s *deliveryInteractionState) retireToolFeedbackChannel(ctx context.Context, channel string) {
	if s.hasToolFeedback() {
		s.toolFeedback.RetireChannel(ctx, channel)
	}
}

func (s *deliveryInteractionState) configureToolFeedback(
	config ToolFeedbackAnimatorConfig,
	separateMessages bool,
) {
	if s.hasToolFeedback() {
		s.toolFeedback.Configure(config, separateMessages)
	}
}

func (s *deliveryInteractionState) expire(now time.Time) {
	s.typingStops.Range(func(key, value any) bool {
		if entry, ok := value.(typingEntry); ok && now.Sub(entry.createdAt) > typingStopTTL {
			if _, loaded := s.typingStops.LoadAndDelete(key); loaded {
				entry.stop()
			}
		}
		return true
	})
	s.reactionUndos.Range(func(key, value any) bool {
		if entry, ok := value.(reactionEntry); ok && now.Sub(entry.createdAt) > typingStopTTL {
			if _, loaded := s.reactionUndos.LoadAndDelete(key); loaded {
				entry.undo()
			}
		}
		return true
	})
	s.placeholders.Range(func(key, value any) bool {
		if entry, ok := value.(placeholderEntry); ok && now.Sub(entry.createdAt) > placeholderTTL {
			s.placeholders.Delete(key)
		}
		return true
	})
}

// streamDeliveryState owns final-stream suppression state independently from
// channel lifecycle and queue ownership.
type streamDeliveryState struct {
	streamActive              sync.Map // streamSuppressionKey -> true
	streamAuxiliaryTombstones sync.Map // streamSuppressionKey -> time.Time
}

func (s *streamDeliveryState) activeKey(
	channel, chatID, sessionKey string,
	traceScope runtimeevents.TraceScope,
) (string, bool) {
	key := streamSuppressionKey(channel, chatID, sessionKey, traceScope)
	if _, active := s.streamActive.Load(key); active {
		return key, true
	}
	if traceScope.Complete() {
		return "", false
	}
	return singleScopedStreamStateKey(
		&s.streamActive,
		streamSuppressionBaseKey(channel, chatID, sessionKey),
		nil,
	)
}

func (s *streamDeliveryState) consumeActive(key string) bool {
	_, loaded := s.streamActive.LoadAndDelete(key)
	return loaded
}

func (s *streamDeliveryState) clear(key string) {
	s.streamActive.Delete(key)
	s.streamAuxiliaryTombstones.Delete(key)
}

func (s *streamDeliveryState) markFinalized(key string, now time.Time) {
	s.streamActive.Store(key, true)
	s.streamAuxiliaryTombstones.Store(key, now)
}

func (s *streamDeliveryState) clearTombstone(key string) {
	s.streamAuxiliaryTombstones.Delete(key)
}

func (s *streamDeliveryState) tombstoneActive(key string, now time.Time) bool {
	value, ok := s.streamAuxiliaryTombstones.Load(key)
	if !ok {
		return false
	}
	createdAt, ok := value.(time.Time)
	if !ok || now.Sub(createdAt) > streamAuxiliaryTombstoneTTL {
		s.streamAuxiliaryTombstones.Delete(key)
		return false
	}
	return true
}

func (s *streamDeliveryState) tombstoneActiveForMessage(
	channel, chatID, sessionKey string,
	traceScope runtimeevents.TraceScope,
	now time.Time,
) bool {
	key := streamSuppressionKey(channel, chatID, sessionKey, traceScope)
	if s.tombstoneActive(key, now) {
		return true
	}
	if traceScope.Complete() {
		return false
	}
	key, ok := singleScopedStreamStateKey(
		&s.streamAuxiliaryTombstones,
		streamSuppressionBaseKey(channel, chatID, sessionKey),
		func(key string, value any) bool {
			createdAt, ok := value.(time.Time)
			if !ok || now.Sub(createdAt) > streamAuxiliaryTombstoneTTL {
				s.streamAuxiliaryTombstones.Delete(key)
				return false
			}
			return true
		},
	)
	return ok && s.tombstoneActive(key, now)
}

func (s *streamDeliveryState) activeForChat(channel, chatID string) bool {
	chatKey := streamSuppressionBaseKey(channel, chatID, "")
	found := false
	s.streamActive.Range(func(key, _ any) bool {
		keyString, ok := key.(string)
		if !ok {
			return true
		}
		if keyString == chatKey || strings.HasPrefix(keyString, chatKey+":") ||
			strings.HasPrefix(keyString, chatKey+"\x00turn\x00") {
			found = true
			return false
		}
		return true
	})
	return found
}

func (s *streamDeliveryState) expire(now time.Time) {
	s.streamAuxiliaryTombstones.Range(func(key, value any) bool {
		if createdAt, ok := value.(time.Time); !ok || now.Sub(createdAt) > streamAuxiliaryTombstoneTTL {
			s.streamAuxiliaryTombstones.Delete(key)
		}
		return true
	})
}

func singleScopedStreamStateKey(
	state *sync.Map,
	baseKey string,
	valid func(string, any) bool,
) (string, bool) {
	prefix := baseKey + "\x00turn\x00"
	matched := ""
	ambiguous := false
	state.Range(func(key, value any) bool {
		keyString, ok := key.(string)
		if !ok || !strings.HasPrefix(keyString, prefix) {
			return true
		}
		if valid != nil && !valid(keyString, value) {
			return true
		}
		if matched != "" {
			ambiguous = true
			return false
		}
		matched = keyString
		return true
	})
	return matched, matched != "" && !ambiguous
}
