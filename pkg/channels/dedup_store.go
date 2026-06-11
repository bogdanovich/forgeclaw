package channels

import (
	"sync"
	"time"
)

// DedupStore tracks recently seen keys and reports whether a key is a duplicate.
// Entries expire after ttl when ttl > 0. maxEntries, when > 0, bounds memory by
// evicting the oldest entry before inserting a new one at capacity.
type DedupStore struct {
	mu         sync.Mutex
	ttl        time.Duration
	maxEntries int
	entries    map[string]time.Time
}

func NewDedupStore(ttl time.Duration, maxEntries int) *DedupStore {
	return &DedupStore{
		ttl:        ttl,
		maxEntries: maxEntries,
		entries:    make(map[string]time.Time),
	}
}

// Seen reports whether key has already been seen within the configured TTL
// window. Empty keys are never considered duplicates.
func (d *DedupStore) Seen(key string) bool {
	if d == nil || key == "" {
		return false
	}

	now := time.Now()

	d.mu.Lock()
	defer d.mu.Unlock()

	d.pruneExpiredLocked(now)

	if ts, ok := d.entries[key]; ok {
		if d.ttl <= 0 || now.Sub(ts) < d.ttl {
			return true
		}
		delete(d.entries, key)
	}

	if d.maxEntries > 0 && len(d.entries) >= d.maxEntries {
		d.evictOldestLocked()
	}

	d.entries[key] = now
	return false
}

func (d *DedupStore) pruneExpiredLocked(now time.Time) {
	if d.ttl <= 0 {
		return
	}
	for key, ts := range d.entries {
		if now.Sub(ts) >= d.ttl {
			delete(d.entries, key)
		}
	}
}

func (d *DedupStore) evictOldestLocked() {
	var oldestKey string
	var oldestTS time.Time
	for key, ts := range d.entries {
		if oldestKey == "" || ts.Before(oldestTS) {
			oldestKey = key
			oldestTS = ts
		}
	}
	if oldestKey != "" {
		delete(d.entries, oldestKey)
	}
}
