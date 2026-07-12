package memory

import (
	"context"
	"testing"

	"github.com/sipeed/picoclaw/pkg/providers"
)

func TestJSONLHistoryRevisionTracksLogicalMutations(t *testing.T) {
	store, err := NewJSONLStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := "revision"
	initial, err := store.GetHistoryRevision(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddMessage(ctx, key, "user", "one"); err != nil {
		t.Fatal(err)
	}
	appended, _ := store.GetHistoryRevision(ctx, key)
	if appended.Revision != initial.Revision+1 || appended.Count != 1 || appended.Dirty {
		t.Fatalf("append revision = %+v", appended)
	}
	if err := store.TruncateHistory(ctx, key, 0); err != nil {
		t.Fatal(err)
	}
	truncated, _ := store.GetHistoryRevision(ctx, key)
	if truncated.Revision != appended.Revision+1 {
		t.Fatalf("truncate revision = %+v", truncated)
	}
	if err := store.Compact(ctx, key); err != nil {
		t.Fatal(err)
	}
	compacted, _ := store.GetHistoryRevision(ctx, key)
	if compacted.Revision != truncated.Revision || compacted.Skip != 0 {
		t.Fatalf("compact revision = %+v", compacted)
	}
	if err := store.SetHistory(ctx, key, []providers.Message{{Role: "user", Content: "replacement"}}); err != nil {
		t.Fatal(err)
	}
	replaced, _ := store.GetHistoryRevision(ctx, key)
	if replaced.Revision != compacted.Revision+1 || replaced.Count != 1 {
		t.Fatalf("replace revision = %+v", replaced)
	}
}

func TestJSONLHistoryRevisionRepairsDirtyMetadata(t *testing.T) {
	store, err := NewJSONLStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := "dirty"
	if err := store.AddMessage(ctx, key, "user", "one"); err != nil {
		t.Fatal(err)
	}
	meta, _ := store.readMeta(key)
	meta.HistoryDirty = true
	meta.Count = 99
	if err := store.writeMeta(key, meta); err != nil {
		t.Fatal(err)
	}
	revision, err := store.GetHistoryRevision(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if revision.Dirty || revision.Count != 1 {
		t.Fatalf("recovered revision = %+v", revision)
	}
}

func TestJSONLHistoryRevisionRestoresMetadataAfterInterruptedCompact(t *testing.T) {
	store, err := NewJSONLStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := "interrupted-compact"
	for _, content := range []string{"one", "two", "three"} {
		if err := store.AddMessage(ctx, key, "user", content); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.TruncateHistory(ctx, key, 1); err != nil {
		t.Fatal(err)
	}
	previous, _ := store.readMeta(key)
	interrupted := previous
	interrupted.Count = 1
	interrupted.Skip = 0
	interrupted.HistoryDirty = true
	interrupted.HistoryHasPrevious = true
	interrupted.HistoryPreviousCount = previous.Count
	interrupted.HistoryPreviousSkip = previous.Skip
	if err := store.writeMeta(key, interrupted); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetHistoryRevision(ctx, key); err != nil {
		t.Fatal(err)
	}
	history, err := store.GetHistory(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].Content != "three" {
		t.Fatalf("interrupted compact exposed history: %#v", history)
	}
}
