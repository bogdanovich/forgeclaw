package tools

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
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

	if err := os.WriteFile(memoryPath, []byte("team\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	substringRemove := tool.Execute(t.Context(), map[string]any{
		"operation": "remove",
		"content":   "tea",
	})
	if !substringRemove.IsError {
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

func TestMemoryToolAppendDailySelectsDateDeduplicatesAndAudits(t *testing.T) {
	workspace := t.TempDir()
	fixedNow := time.Date(2026, time.July, 18, 23, 30, 0, 0, time.FixedZone("PDT", -7*60*60))
	eventBus := runtimeevents.NewBus()
	sub, eventsCh, err := eventBus.Channel().OfKind(runtimeevents.KindAgentMemoryMutation).SubscribeChan(
		t.Context(),
		runtimeevents.SubscribeOptions{Name: "daily-memory-test", Buffer: 4},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	var invalidations atomic.Int32
	tool := newMemoryTool(workspace, func() { invalidations.Add(1) }, eventBus, func() time.Time {
		return fixedNow
	})
	properties := tool.Parameters()["properties"].(map[string]any)
	operations := properties["operation"].(map[string]any)["enum"].([]string)
	if !slices.Contains(operations, appendDailyMemoryOperation) {
		t.Fatalf("memory operation enum = %#v", operations)
	}
	firstContent := "- Finished the private deployment"
	secondContent := "- Follow up on the rollout tomorrow"
	first := tool.Execute(t.Context(), map[string]any{
		"operation": appendDailyMemoryOperation,
		"content":   firstContent,
	})
	second := tool.Execute(t.Context(), map[string]any{
		"operation": appendDailyMemoryOperation,
		"content":   secondContent,
	})
	duplicate := tool.Execute(t.Context(), map[string]any{
		"operation": appendDailyMemoryOperation,
		"content":   "  FINISHED   THE PRIVATE DEPLOYMENT  ",
	})
	for _, result := range []*ToolResult{first, second, duplicate} {
		if result.IsError {
			t.Fatalf("append_daily result = %#v", result)
		}
	}
	if len(first.WriteAudit) != 1 || len(second.WriteAudit) != 1 || len(duplicate.WriteAudit) != 0 {
		t.Fatalf(
			"write audits = first:%#v second:%#v duplicate:%#v",
			first.WriteAudit,
			second.WriteAudit,
			duplicate.WriteAudit,
		)
	}
	const target = "memory/202607/20260718.md"
	if !strings.Contains(first.ForLLM, `"status":"appended"`) ||
		!strings.Contains(first.ForLLM, `"target":"`+target+`"`) ||
		!strings.Contains(duplicate.ForLLM, `"status":"duplicate"`) {
		t.Fatalf("unexpected results: first=%s duplicate=%s", first.ForLLM, duplicate.ForLLM)
	}
	if invalidations.Load() != 2 {
		t.Fatalf("cache invalidations = %d, want 2", invalidations.Load())
	}

	dailyPath := filepath.Join(workspace, filepath.FromSlash(target))
	assertMemoryFile(t, dailyPath, "# 2026-07-18\n\n"+firstContent+"\n\n"+secondContent+"\n")
	info, err := os.Stat(dailyPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("daily memory mode = %o, want 600", info.Mode().Perm())
	}

	wantOutcomes := []string{"appended", "appended", "duplicate"}
	for index, wantOutcome := range wantOutcomes {
		evt := receiveMemoryMutationEvent(t, eventsCh)
		payload, ok := evt.Payload.(MemoryMutationPayload)
		if !ok || payload.Operation != appendDailyMemoryOperation || payload.Outcome != wantOutcome ||
			payload.Target != target || payload.ContentHash == "" {
			t.Fatalf("event %d payload = %#v", index, evt.Payload)
		}
		encoded, marshalErr := json.Marshal(evt)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		for _, raw := range []string{firstContent, secondContent, "FINISHED   THE PRIVATE DEPLOYMENT"} {
			if strings.Contains(string(encoded), raw) {
				t.Fatalf("runtime event leaked raw daily content: %s", encoded)
			}
		}
	}
}

func TestMemoryToolSerializesConcurrentDailyAppends(t *testing.T) {
	workspace := t.TempDir()
	fixedNow := time.Date(2026, time.July, 18, 12, 0, 0, 0, time.UTC)
	tool := newMemoryTool(workspace, nil, nil, func() time.Time { return fixedNow })

	const count = 24
	var wg sync.WaitGroup
	errs := make(chan string, count)
	for i := range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			content := fmt.Sprintf("daily-event-%02d", i)
			result := tool.Execute(t.Context(), map[string]any{
				"operation": appendDailyMemoryOperation,
				"content":   content,
			})
			if result.IsError {
				errs <- result.ForLLM
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent daily append failed: %s", err)
	}

	data, err := os.ReadFile(filepath.Join(workspace, "memory", "202607", "20260718.md"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(data), "# 2026-07-18") != 1 {
		t.Fatalf("daily header count = %d, want 1", strings.Count(string(data), "# 2026-07-18"))
	}
	for i := range count {
		content := fmt.Sprintf("daily-event-%02d", i)
		if strings.Count(string(data), content) != 1 {
			t.Errorf("daily memory contains %q %d times", content, strings.Count(string(data), content))
		}
	}
}

func TestMemoryToolAppendDailyRejectsMonthSymlinkEscape(t *testing.T) {
	workspace := t.TempDir()
	outside := t.TempDir()
	memoryDir := filepath.Join(workspace, "memory")
	if err := os.MkdirAll(memoryDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(memoryDir, "202607")); err != nil {
		t.Skipf("symlinks not supported in this environment: %v", err)
	}

	fixedNow := time.Date(2026, time.July, 18, 12, 0, 0, 0, time.UTC)
	tool := newMemoryTool(workspace, nil, nil, func() time.Time { return fixedNow })
	result := tool.Execute(t.Context(), map[string]any{
		"operation": appendDailyMemoryOperation,
		"content":   "must-not-escape",
	})
	if !result.IsError || !strings.Contains(result.ForLLM, "symlink resolves outside workspace") {
		t.Fatalf("daily symlink escape result = %#v", result)
	}
	if _, err := os.Stat(filepath.Join(outside, "20260718.md")); !os.IsNotExist(err) {
		t.Fatalf("outside daily file was created through symlink: %v", err)
	}
}

func TestMemoryToolRejectsFileSymlinkEscape(t *testing.T) {
	workspace := t.TempDir()
	outside := t.TempDir()
	outsideFile := filepath.Join(outside, "secret.md")
	if err := os.WriteFile(outsideFile, []byte("outside-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	memoryDir := filepath.Join(workspace, "memory")
	if err := os.MkdirAll(memoryDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outsideFile, filepath.Join(memoryDir, "MEMORY.md")); err != nil {
		t.Skipf("symlinks not supported in this environment: %v", err)
	}

	tool := NewMemoryTool(workspace, nil, nil)
	result := tool.Execute(t.Context(), map[string]any{
		"operation": "add",
		"content":   "must-not-escape",
	})
	if !result.IsError || !strings.Contains(result.ForLLM, "symlink resolves outside workspace") {
		t.Fatalf("file symlink escape result = %#v", result)
	}
	assertMemoryFile(t, outsideFile, "outside-secret\n")
}

func TestMemoryToolRejectsDirectorySymlinkEscape(t *testing.T) {
	workspace := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(workspace, "memory")); err != nil {
		t.Skipf("symlinks not supported in this environment: %v", err)
	}

	tool := NewMemoryTool(workspace, nil, nil)
	result := tool.Execute(t.Context(), map[string]any{
		"operation": "add",
		"content":   "must-not-escape",
	})
	if !result.IsError || !strings.Contains(result.ForLLM, "symlink resolves outside workspace") {
		t.Fatalf("directory symlink escape result = %#v", result)
	}
	if _, err := os.Stat(filepath.Join(outside, "MEMORY.md")); !os.IsNotExist(err) {
		t.Fatalf("outside memory file was created through directory symlink: %v", err)
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
