package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/tools/loopguard"
)

type loopGuardTestTool struct {
	mu       sync.Mutex
	executed int
	fail     bool
}

func (t *loopGuardTestTool) Name() string        { return "loop_test" }
func (t *loopGuardTestTool) Description() string { return "test loop behavior" }
func (t *loopGuardTestTool) ToolLoopSemantics() loopguard.Semantics {
	return loopguard.SemanticsReadOnlyIdempotent
}

func (t *loopGuardTestTool) Parameters() map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": map[string]any{"value": map[string]any{"type": "string"}},
	}
}

func (t *loopGuardTestTool) Execute(context.Context, map[string]any) *ToolResult {
	t.mu.Lock()
	t.executed++
	t.mu.Unlock()
	if t.fail {
		return ErrorResult("stable failure")
	}
	return SilentResult("stable result")
}

func (t *loopGuardTestTool) count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.executed
}

type loopGuardScriptedProvider struct {
	mu        sync.Mutex
	calls     int
	batch     bool
	snapshots [][]providers.Message
}

func (p *loopGuardScriptedProvider) GetDefaultModel() string { return "loop-test-model" }
func (p *loopGuardScriptedProvider) Chat(
	_ context.Context,
	messages []providers.Message,
	_ []providers.ToolDefinition,
	_ string,
	_ map[string]any,
) (*providers.LLMResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	p.snapshots = append(p.snapshots, append([]providers.Message(nil), messages...))
	if p.batch && p.calls == 1 {
		return &providers.LLMResponse{ToolCalls: []providers.ToolCall{
			{ID: "batch-1", Name: "loop_test", Arguments: map[string]any{"value": "one"}},
			{ID: "batch-2", Name: "loop_test", Arguments: map[string]any{"value": "two"}},
		}}, nil
	}
	if p.batch || p.calls > 3 {
		return &providers.LLMResponse{Content: "done"}, nil
	}
	return &providers.LLMResponse{ToolCalls: []providers.ToolCall{{
		ID: fmt.Sprintf("call-%d", p.calls), Name: "loop_test",
		Arguments: map[string]any{"value": "same"},
	}}}, nil
}

func TestRunToolLoopBlocksWithoutBreakingToolCallPairing(t *testing.T) {
	registry := NewToolRegistry()
	tool := &loopGuardTestTool{fail: true}
	registry.Register(tool)
	provider := &loopGuardScriptedProvider{}
	config := loopguard.DefaultConfig()
	config.HardStopsEnabled = true
	config.ExactFailureWarn = 1
	config.ExactFailureBlock = 2
	config.SameToolFailureHalt = 99

	result, err := RunToolLoop(context.Background(), ToolLoopConfig{
		Provider: provider, Model: "test", Tools: registry, MaxIterations: 6,
		LoopDetection: config,
	}, []providers.Message{{Role: "user", Content: "test"}}, "cli", "direct")
	if err != nil {
		t.Fatal(err)
	}
	if result.Content != "done" || tool.count() != 2 {
		t.Fatalf("result=%#v executions=%d", result, tool.count())
	}
	last := provider.snapshots[len(provider.snapshots)-1]
	paired := map[string]bool{}
	for _, message := range last {
		if message.Role == "tool" {
			paired[message.ToolCallID] = true
		}
	}
	for _, id := range []string{"call-1", "call-2", "call-3"} {
		if !paired[id] {
			t.Fatalf("missing tool result for %s in %#v", id, last)
		}
	}
	blocked := last[len(last)-1]
	if blocked.ToolCallID != "call-3" || !strings.Contains(blocked.Content, "repeated_exact_failure_block") {
		t.Fatalf("blocked result = %#v", blocked)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(blocked.Content), &payload); err != nil {
		t.Fatalf("blocked result is not structured JSON: %v", err)
	}
}

func TestRunToolLoopRecordsParallelResultsInProviderOrder(t *testing.T) {
	registry := NewToolRegistry()
	tool := &loopGuardTestTool{fail: true}
	registry.Register(tool)
	provider := &loopGuardScriptedProvider{batch: true}
	config := loopguard.DefaultConfig()
	config.SameToolFailureWarn = 2
	config.ExactFailureWarn = 99

	_, err := RunToolLoop(context.Background(), ToolLoopConfig{
		Provider: provider, Model: "test", Tools: registry, MaxIterations: 3,
		LoopDetection: config,
	}, []providers.Message{{Role: "user", Content: "test"}}, "cli", "direct")
	if err != nil {
		t.Fatal(err)
	}
	if tool.count() != 2 {
		t.Fatalf("executions = %d, want 2", tool.count())
	}
	snapshot := provider.snapshots[1]
	var results []providers.Message
	for _, message := range snapshot {
		if message.Role == "tool" {
			results = append(results, message)
		}
	}
	if len(results) != 2 || results[0].ToolCallID != "batch-1" || results[1].ToolCallID != "batch-2" {
		t.Fatalf("result order = %#v", results)
	}
	if strings.Contains(results[0].Content, "same_tool_failure_warning") ||
		!strings.Contains(results[1].Content, "same_tool_failure_warning") {
		t.Fatalf("deterministic warning order not preserved: %#v", results)
	}
}
