package tools

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
)

func TestMemoryToolAddDeduplicatesAndAuditsWithoutRawContent(t *testing.T) {
	workspace := t.TempDir()
	eventBus := runtimeevents.NewBus()
	sub, eventsCh, err := eventBus.Channel().OfKind(runtimeevents.KindAgentMemoryMutation).SubscribeChan(
		t.Context(),
		runtimeevents.SubscribeOptions{Name: "memory-tool-test", Buffer: 4},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	var invalidations atomic.Int32
	tool := NewMemoryTool(workspace, func() { invalidations.Add(1) }, eventBus)
	secret := "User's favorite tea is jasmine"
	ctx := WithToolSessionContext(t.Context(), "main", "telegram:chat-1", nil)
	ctx = WithToolInboundContext(ctx, "telegram", "chat-1", "message-1", "")
	ctx = WithToolInboundMetadata(ctx, bus.InboundContext{
		Channel:   "telegram",
		ChatID:    "chat-1",
		SenderID:  "user-1",
		MessageID: "message-1",
	})

	result := tool.Execute(ctx, map[string]any{
		"operation": "add",
		"content":   secret,
	})
	if result.IsError || len(result.WriteAudit) != 1 {
		t.Fatalf("add result = %#v", result)
	}
	if invalidations.Load() != 1 {
		t.Fatalf("cache invalidations = %d, want 1", invalidations.Load())
	}

	memoryPath := filepath.Join(workspace, "memory", "MEMORY.md")
	data, err := os.ReadFile(memoryPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != secret+"\n" {
		t.Fatalf("memory contents = %q", data)
	}
	info, err := os.Stat(memoryPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("memory mode = %o, want 600", info.Mode().Perm())
	}

	evt := receiveMemoryMutationEvent(t, eventsCh)
	payload, ok := evt.Payload.(MemoryMutationPayload)
	if !ok || payload.Outcome != "added" || payload.ContentHash == "" || payload.ContentBytes != len(secret) {
		t.Fatalf("event payload = %#v", evt.Payload)
	}
	if evt.Scope.AgentID != "main" || evt.Scope.SenderID != "user-1" || evt.Scope.MessageID != "message-1" {
		t.Fatalf("event scope = %#v", evt.Scope)
	}
	encoded, err := json.Marshal(evt)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), secret) {
		t.Fatalf("runtime event leaked raw memory content: %s", encoded)
	}

	duplicate := tool.Execute(ctx, map[string]any{
		"operation": "add",
		"content":   "  USER'S   FAVORITE TEA IS JASMINE  ",
	})
	if duplicate.IsError || len(duplicate.WriteAudit) != 0 ||
		!strings.Contains(duplicate.ForLLM, `"status":"duplicate"`) {
		t.Fatalf("duplicate result = %#v", duplicate)
	}
	if invalidations.Load() != 1 {
		t.Fatalf("duplicate invalidated cache; count = %d", invalidations.Load())
	}
	_ = receiveMemoryMutationEvent(t, eventsCh)
}

func TestMemoryToolReplaceAndRemoveRequireOneExactMatch(t *testing.T) {
	workspace := t.TempDir()
	memoryPath := filepath.Join(workspace, "memory", "MEMORY.md")
	if err := os.MkdirAll(filepath.Dir(memoryPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(memoryPath, []byte("# Preferences\n\n- User lives in Oakland\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var invalidations atomic.Int32
	tool := NewMemoryTool(workspace, func() { invalidations.Add(1) }, nil)
	replaced := tool.Execute(t.Context(), map[string]any{
		"operation":   "replace",
		"content":     "- User lives in Oakland",
		"replacement": "- User lives in San Mateo",
	})
	if replaced.IsError || !strings.Contains(replaced.ForLLM, `"status":"replaced"`) {
		t.Fatalf("replace result = %#v", replaced)
	}
	assertMemoryFile(t, memoryPath, "# Preferences\n\n- User lives in San Mateo\n")

	removed := tool.Execute(t.Context(), map[string]any{
		"operation": "remove",
		"content":   "- User lives in San Mateo",
	})
	if removed.IsError || !strings.Contains(removed.ForLLM, `"status":"removed"`) {
		t.Fatalf("remove result = %#v", removed)
	}
	assertMemoryFile(t, memoryPath, "# Preferences\n")
	if invalidations.Load() != 2 {
		t.Fatalf("cache invalidations = %d, want 2", invalidations.Load())
	}
}

func TestMemoryToolRejectsAmbiguousMutationAndSubstringDedup(t *testing.T) {
	workspace := t.TempDir()
	memoryPath := filepath.Join(workspace, "memory", "MEMORY.md")
	if err := os.MkdirAll(filepath.Dir(memoryPath), 0o755); err != nil {
		t.Fatal(err)
	}
	original := "tea\ntea\nteam\n"
	if err := os.WriteFile(memoryPath, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}

	tool := NewMemoryTool(workspace, nil, nil)
	ambiguous := tool.Execute(t.Context(), map[string]any{
		"operation": "remove",
		"content":   "tea",
	})
	if !ambiguous.IsError || !strings.Contains(ambiguous.ForLLM, "more than one location") {
		t.Fatalf("ambiguous result = %#v", ambiguous)
	}
	assertMemoryFile(t, memoryPath, original)

	added := tool.Execute(t.Context(), map[string]any{
		"operation": "add",
		"content":   "te",
	})
	if added.IsError || !strings.Contains(added.ForLLM, `"status":"added"`) {
		t.Fatalf("substring add result = %#v", added)
	}

	_, _, _, _, err := applyCuratedMemoryMutation("team\n", "remove", "tea", "")
	if err == nil {
		t.Fatal("remove must not match content inside a larger entry")
	}
}

func TestMemoryToolSerializesConcurrentAdds(t *testing.T) {
	workspace := t.TempDir()
	tool := NewMemoryTool(workspace, nil, nil)

	const count = 24
	var wg sync.WaitGroup
	errs := make(chan string, count)
	for i := range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			content := fmt.Sprintf("fact-%02d", i)
			result := tool.Execute(t.Context(), map[string]any{"operation": "add", "content": content})
			if result.IsError {
				errs <- result.ForLLM
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent add failed: %s", err)
	}

	data, err := os.ReadFile(filepath.Join(workspace, "memory", "MEMORY.md"))
	if err != nil {
		t.Fatal(err)
	}
	for i := range count {
		fact := fmt.Sprintf("fact-%02d", i)
		if strings.Count(string(data), fact) != 1 {
			t.Errorf("memory contains %q %d times", fact, strings.Count(string(data), fact))
		}
	}
}

func receiveMemoryMutationEvent(t *testing.T, ch <-chan runtimeevents.Event) runtimeevents.Event {
	t.Helper()
	select {
	case evt := <-ch:
		return evt
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for memory mutation event")
		return runtimeevents.Event{}
	}
}

func assertMemoryFile(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != want {
		t.Fatalf("memory file = %q, want %q", data, want)
	}
}
