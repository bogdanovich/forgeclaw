//go:build !mipsle && !netbsd && !(freebsd && arm)

package agent

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/sipeed/picoclaw/pkg/memory"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/seahorse"
	"github.com/sipeed/picoclaw/pkg/session"
)

func newReconciliationTestManager(t *testing.T) (*seahorseContextManager, *memory.JSONLStore) {
	t.Helper()
	dir := t.TempDir()
	canonical, storeErr := memory.NewJSONLStore(dir + "/sessions")
	if storeErr != nil {
		t.Fatal(storeErr)
	}
	engine, err := seahorse.NewEngine(seahorse.Config{DBPath: dir + "/seahorse.db"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	return &seahorseContextManager{
		engine: engine, sessions: session.NewJSONLBackend(canonical),
	}, canonical
}

func TestSeahorseReconciliationCleanRevisionSkipsDeepComparison(t *testing.T) {
	mgr, canonical := newReconciliationTestManager(t)
	ctx := context.Background()
	key := "clean"
	if err := canonical.AddMessage(ctx, key, "user", "canonical"); err != nil {
		t.Fatal(err)
	}
	if err := mgr.ensureReconciled(ctx, key, mgr.sessions); err != nil {
		t.Fatal(err)
	}
	before := mgr.reconciliations.Load()
	if err := mgr.ensureReconciled(ctx, key, mgr.sessions); err != nil {
		t.Fatal(err)
	}
	if got := mgr.reconciliations.Load(); got != before {
		t.Fatalf("unchanged revision reconciliations = %d, want %d", got, before)
	}
}

func TestSeahorseReconciliationCleanRestartUsesDurableWatermark(t *testing.T) {
	dir := t.TempDir()
	canonical, err := memory.NewJSONLStore(dir + "/sessions")
	if err != nil {
		t.Fatal(err)
	}
	backend := session.NewJSONLBackend(canonical)
	ctx := context.Background()
	key := "restart"
	if err := canonical.AddMessage(ctx, key, "user", "persisted"); err != nil {
		t.Fatal(err)
	}
	engine1, engineErr := seahorse.NewEngine(seahorse.Config{DBPath: dir + "/seahorse.db"}, nil)
	if engineErr != nil {
		t.Fatal(engineErr)
	}
	mgr1 := &seahorseContextManager{engine: engine1, sessions: backend}
	if err := mgr1.ensureReconciled(ctx, key, backend); err != nil {
		t.Fatal(err)
	}
	if err := engine1.Close(); err != nil {
		t.Fatal(err)
	}
	engine2, reopenErr := seahorse.NewEngine(seahorse.Config{DBPath: dir + "/seahorse.db"}, nil)
	if reopenErr != nil {
		t.Fatal(reopenErr)
	}
	defer engine2.Close()
	mgr2 := &seahorseContextManager{engine: engine2, sessions: backend}
	if err := mgr2.ensureReconciled(ctx, key, backend); err != nil {
		t.Fatal(err)
	}
	if got := mgr2.reconciliations.Load(); got != 0 {
		t.Fatalf("clean restart performed %d full reconciliations", got)
	}
}

func TestSeahorseReconciliationAppendAndReplace(t *testing.T) {
	mgr, canonical := newReconciliationTestManager(t)
	ctx := context.Background()
	key := "mutations"
	first := providers.Message{Role: "user", Content: "one"}
	if err := canonical.AddFullMessage(ctx, key, first); err != nil {
		t.Fatal(err)
	}
	if err := mgr.ensureReconciled(ctx, key, mgr.sessions); err != nil {
		t.Fatal(err)
	}
	second := providers.Message{Role: "assistant", Content: "two"}
	if err := canonical.AddFullMessage(ctx, key, second); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Ingest(ctx, &IngestRequest{SessionKey: key, Message: second}); err != nil {
		t.Fatal(err)
	}
	if err := canonical.SetHistory(ctx, key, []providers.Message{{Role: "user", Content: "replacement"}}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.ensureReconciled(ctx, key, mgr.sessions); err != nil {
		t.Fatal(err)
	}
	conv, _ := mgr.engine.GetRetrieval().Store().GetConversationBySessionKey(ctx, key)
	messages, _ := mgr.engine.GetRetrieval().Store().GetMessages(ctx, conv.ConversationID, 0, 0)
	if len(messages) != 1 || messages[0].Content != "replacement" {
		t.Fatalf("reconciled messages = %#v", messages)
	}
}

func TestSeahorseReconciliationGenerationAndFailureRetry(t *testing.T) {
	mgr, canonical := newReconciliationTestManager(t)
	key := "retry"
	if err := canonical.AddMessage(context.Background(), key, "user", "source"); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := mgr.ensureReconciled(canceled, key, mgr.sessions); err == nil {
		t.Fatal("expected canceled reconciliation to fail")
	}
	store := mgr.engine.GetRetrieval().Store()
	state, _ := store.GetReconciliationState(context.Background(), key)
	if state != nil {
		t.Fatal("failed reconciliation advanced watermark")
	}
	if err := mgr.ensureReconciled(context.Background(), key, mgr.sessions); err != nil {
		t.Fatal(err)
	}
	state, _ = store.GetReconciliationState(context.Background(), key)
	state.SchemaGeneration--
	if err := store.SetReconciliationState(context.Background(), *state); err != nil {
		t.Fatal(err)
	}
	before := mgr.reconciliations.Load()
	if err := mgr.ensureReconciled(context.Background(), key, mgr.sessions); err != nil {
		t.Fatal(err)
	}
	if mgr.reconciliations.Load() != before+1 {
		t.Fatal("schema generation change did not force reconciliation")
	}
	state, _ = store.GetReconciliationState(context.Background(), key)
	if state.SchemaGeneration != seahorseReconciliationGeneration {
		t.Fatalf("generation = %d", state.SchemaGeneration)
	}
}

func TestSeahorseConcurrentLiveIngestDoesNotDuplicate(t *testing.T) {
	mgr, canonical := newReconciliationTestManager(t)
	ctx := context.Background()
	key := "concurrent"
	const count = 20
	messages := make([]providers.Message, count)
	for i := range messages {
		messages[i] = providers.Message{Role: "user", Content: fmt.Sprintf("message-%d", i)}
		if err := canonical.AddFullMessage(ctx, key, messages[i]); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for i := range messages {
		wg.Add(1)
		go func(msg providers.Message) {
			defer wg.Done()
			if err := mgr.Ingest(ctx, &IngestRequest{SessionKey: key, Message: msg}); err != nil {
				t.Errorf("Ingest: %v", err)
			}
		}(messages[i])
	}
	wg.Wait()
	conv, _ := mgr.engine.GetRetrieval().Store().GetConversationBySessionKey(ctx, key)
	stored, _ := mgr.engine.GetRetrieval().Store().GetMessages(ctx, conv.ConversationID, 0, 0)
	if len(stored) != count {
		t.Fatalf("stored %d messages, want %d", len(stored), count)
	}
}

func TestSeahorseReconciliationUsesRoutedSessionOwner(t *testing.T) {
	mgr, _ := newReconciliationTestManager(t)
	mainStore := session.NewSessionManager("")
	supportStore := session.NewSessionManager("")
	key := "agent:support:direct:42"
	supportStore.AddMessage(key, "user", "owned by support")
	mgr.sessions = mainStore
	mgr.al = &AgentLoop{registry: &AgentRegistry{
		agents: map[string]*AgentInstance{
			"main":    {ID: "main", Sessions: mainStore},
			"support": {ID: "support", Sessions: supportStore},
		},
	}}
	if err := mgr.ensureReconciled(context.Background(), key, mgr.sessionStore(key)); err != nil {
		t.Fatal(err)
	}
	conv, _ := mgr.engine.GetRetrieval().Store().GetConversationBySessionKey(context.Background(), key)
	stored, _ := mgr.engine.GetRetrieval().Store().GetMessages(context.Background(), conv.ConversationID, 0, 0)
	if len(stored) != 1 || stored[0].Content != "owned by support" {
		t.Fatalf("routed messages = %#v", stored)
	}
}

func BenchmarkSeahorseCleanRevisionCheck(b *testing.B) {
	dir := b.TempDir()
	canonical, _ := memory.NewJSONLStore(dir + "/sessions")
	backend := session.NewJSONLBackend(canonical)
	engine, _ := seahorse.NewEngine(seahorse.Config{DBPath: dir + "/seahorse.db"}, nil)
	defer engine.Close()
	mgr := &seahorseContextManager{engine: engine, sessions: backend}
	ctx := context.Background()
	_ = canonical.AddMessage(ctx, "bench", "user", "hello")
	_ = mgr.ensureReconciled(ctx, "bench", backend)
	b.ResetTimer()
	for range b.N {
		if err := mgr.ensureReconciled(ctx, "bench", backend); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSeahorseForcedReconciliation100Messages(b *testing.B) {
	dir := b.TempDir()
	canonical, _ := memory.NewJSONLStore(dir + "/sessions")
	backend := session.NewJSONLBackend(canonical)
	engine, _ := seahorse.NewEngine(seahorse.Config{DBPath: dir + "/seahorse.db"}, nil)
	defer engine.Close()
	mgr := &seahorseContextManager{engine: engine, sessions: backend}
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_ = canonical.AddMessage(ctx, "bench-full", "user", fmt.Sprintf("message-%d", i))
	}
	_ = mgr.ensureReconciled(ctx, "bench-full", backend)
	store := engine.GetRetrieval().Store()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		state, _ := store.GetReconciliationState(ctx, "bench-full")
		state.SchemaGeneration--
		_ = store.SetReconciliationState(ctx, *state)
		b.StartTimer()
		if err := mgr.ensureReconciled(ctx, "bench-full", backend); err != nil {
			b.Fatal(err)
		}
	}
}
