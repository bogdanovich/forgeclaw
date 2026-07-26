package channels

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestDeliveryInteractionStateExpire(t *testing.T) {
	var expiredStops atomic.Int32
	var currentStops atomic.Int32
	state := deliveryInteractionState{}
	now := time.Now()

	state.typingStops.Store("expired-typing", typingEntry{
		stop:      func() { expiredStops.Add(1) },
		createdAt: now.Add(-typingStopTTL - time.Second),
	})
	state.typingStops.Store("current-typing", typingEntry{
		stop:      func() { currentStops.Add(1) },
		createdAt: now,
	})
	state.reactionUndos.Store("expired-reaction", reactionEntry{
		undo:      func() { expiredStops.Add(1) },
		createdAt: now.Add(-typingStopTTL - time.Second),
	})
	state.reactionUndos.Store("current-reaction", reactionEntry{
		undo:      func() { currentStops.Add(1) },
		createdAt: now,
	})
	state.placeholders.Store("expired-placeholder", placeholderEntry{
		id:        "old",
		createdAt: now.Add(-placeholderTTL - time.Second),
	})
	state.placeholders.Store("current-placeholder", placeholderEntry{
		id:        "current",
		createdAt: now,
	})

	state.expire(now)

	if expiredStops.Load() != 2 {
		t.Fatalf("expired callbacks = %d, want 2", expiredStops.Load())
	}
	if currentStops.Load() != 0 {
		t.Fatalf("current callbacks = %d, want 0", currentStops.Load())
	}
	for _, key := range []string{"expired-typing", "expired-reaction", "expired-placeholder"} {
		if _, ok := stateEntry(&state, key); ok {
			t.Fatalf("expired entry %q was not removed", key)
		}
	}
	for _, key := range []string{"current-typing", "current-reaction", "current-placeholder"} {
		if _, ok := stateEntry(&state, key); !ok {
			t.Fatalf("current entry %q was removed", key)
		}
	}
}

func TestStreamDeliveryStateExpire(t *testing.T) {
	state := streamDeliveryState{}
	now := time.Now()
	state.streamActive.Store("active", true)
	state.streamAuxiliaryTombstones.Store(
		"expired",
		now.Add(-streamAuxiliaryTombstoneTTL-time.Second),
	)
	state.streamAuxiliaryTombstones.Store("current", now)
	state.streamAuxiliaryTombstones.Store("malformed", "not-a-time")

	state.expire(now)

	if _, ok := state.streamActive.Load("active"); !ok {
		t.Fatal("stream activity must not be TTL-evicted with tombstones")
	}
	if _, ok := state.streamAuxiliaryTombstones.Load("current"); !ok {
		t.Fatal("current tombstone was removed")
	}
	for _, key := range []string{"expired", "malformed"} {
		if _, ok := state.streamAuxiliaryTombstones.Load(key); ok {
			t.Fatalf("stale tombstone %q was not removed", key)
		}
	}
}

func stateEntry(state *deliveryInteractionState, key string) (any, bool) {
	if value, ok := state.typingStops.Load(key); ok {
		return value, true
	}
	if value, ok := state.reactionUndos.Load(key); ok {
		return value, true
	}
	return state.placeholders.Load(key)
}
