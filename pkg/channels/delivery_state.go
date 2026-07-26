package channels

import (
	"sync"
	"time"
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

func (s *streamDeliveryState) expire(now time.Time) {
	s.streamAuxiliaryTombstones.Range(func(key, value any) bool {
		if createdAt, ok := value.(time.Time); !ok || now.Sub(createdAt) > streamAuxiliaryTombstoneTTL {
			s.streamAuxiliaryTombstones.Delete(key)
		}
		return true
	})
}
