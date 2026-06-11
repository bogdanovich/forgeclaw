package channels

import (
	"testing"
	"time"
)

func TestDedupStoreSeen_EmptyKeyIsNeverDuplicate(t *testing.T) {
	store := NewDedupStore(time.Minute, 10)
	if store.Seen("") {
		t.Fatal("empty key should not be treated as duplicate")
	}
	if store.Seen("") {
		t.Fatal("empty key should not become duplicate on subsequent calls")
	}
}

func TestDedupStoreSeen_DetectsDuplicateWithinTTL(t *testing.T) {
	store := NewDedupStore(time.Minute, 10)
	if store.Seen("msg-1") {
		t.Fatal("first key should not be duplicate")
	}
	if !store.Seen("msg-1") {
		t.Fatal("second key within ttl should be duplicate")
	}
}

func TestDedupStoreSeen_ExpiresKeysAfterTTL(t *testing.T) {
	store := NewDedupStore(10*time.Millisecond, 10)
	if store.Seen("msg-1") {
		t.Fatal("first key should not be duplicate")
	}
	time.Sleep(20 * time.Millisecond)
	if store.Seen("msg-1") {
		t.Fatal("expired key should not be duplicate")
	}
}

func TestDedupStoreSeen_EvictsOldestAtCapacity(t *testing.T) {
	store := NewDedupStore(time.Minute, 2)
	if store.Seen("msg-1") || store.Seen("msg-2") {
		t.Fatal("initial keys should not be duplicate")
	}
	if store.Seen("msg-3") {
		t.Fatal("new key at capacity should not be duplicate")
	}
	if !store.Seen("msg-3") {
		t.Fatal("newly inserted key should be duplicate")
	}
	if store.Seen("msg-1") {
		t.Fatal("oldest key should have been evicted")
	}
}

func TestDedupStoreSeen_UnboundedCapacityDoesNotEvict(t *testing.T) {
	store := NewDedupStore(time.Minute, 0)
	for i := 0; i < 5000; i++ {
		key := time.Unix(0, int64(i)).Format(time.RFC3339Nano)
		if store.Seen(key) {
			t.Fatalf("first sighting for %q should not be duplicate", key)
		}
	}
	if !store.Seen(time.Unix(0, 0).Format(time.RFC3339Nano)) {
		t.Fatal("old entry should remain duplicate when capacity is unbounded")
	}
}
