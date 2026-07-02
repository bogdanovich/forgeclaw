package tools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	taskregistry "github.com/sipeed/picoclaw/pkg/tasks"
)

func TestTaskBoardExecuteNextTool_ExecutesDelegateBackedStep(t *testing.T) {
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(t.TempDir()))
	toolRegistry := NewToolRegistry()
	toolRegistry.Register(NewTaskBoardTool(registry))

	spawner := &delegateMockSpawner{}
	delegateTool := NewDelegateTool()
	delegateTool.SetSpawner(spawner)
	delegateTool.SetTaskRegistry(registry)
	toolRegistry.Register(delegateTool)

	executor := NewTaskBoardExecuteNextTool(registry, toolRegistry)
	toolRegistry.Register(executor)

	ctx := WithToolContext(context.Background(), "telegram", "chat-1")
	add := toolRegistry.ExecuteWithContext(ctx, "task_board", map[string]any{
		"action":         "add_step",
		"board_id":       "workflow-1",
		"step_id":        "research",
		"step_title":     "Research recipe",
		"owner":          "research",
		"task":           "Research the recipe context.",
		"execution_tool": "delegate",
	}, "telegram", "chat-1", nil)
	if add.IsError {
		t.Fatalf("add_step failed: %s", add.ForLLM)
	}

	result := toolRegistry.ExecuteWithContext(ctx, "task_board_execute_next", map[string]any{
		"board_id": "workflow-1",
	}, "telegram", "chat-1", nil)
	if result.IsError {
		t.Fatalf("execute_next failed: %s", result.ForLLM)
	}

	var payload struct {
		Action          string         `json:"action"`
		StepID          string         `json:"step_id"`
		Executed        bool           `json:"executed"`
		RecommendedTool string         `json:"recommended_tool"`
		DelegateArgs    map[string]any `json:"delegate_args"`
		Result          string         `json:"result"`
	}
	if err := json.Unmarshal([]byte(result.ForLLM), &payload); err != nil {
		t.Fatalf("execute_next JSON error = %v\n%s", err, result.ForLLM)
	}
	if payload.Action != "execute_next" ||
		payload.StepID != "research" ||
		!payload.Executed ||
		payload.RecommendedTool != "delegate" ||
		payload.DelegateArgs["agent_id"] != "research" ||
		!strings.Contains(payload.Result, `[Response from agent "research"]`) {
		t.Fatalf("unexpected execute payload: %+v\n%s", payload, result.ForLLM)
	}
	if spawner.lastCfg.TargetAgentID != "research" {
		t.Fatalf("delegate target = %q, want research", spawner.lastCfg.TargetAgentID)
	}
}

func TestTaskBoardExecuteNextTool_DoesNotAutoRunSpawnStep(t *testing.T) {
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(t.TempDir()))
	toolRegistry := NewToolRegistry()
	toolRegistry.Register(NewTaskBoardTool(registry))
	toolRegistry.Register(NewTaskBoardExecuteNextTool(registry, toolRegistry))

	ctx := WithToolContext(context.Background(), "telegram", "chat-1")
	add := toolRegistry.ExecuteWithContext(ctx, "task_board", map[string]any{
		"action":         "add_step",
		"board_id":       "workflow-1",
		"step_id":        "background",
		"step_title":     "Background work",
		"owner":          "research",
		"task":           "Do background work.",
		"execution_tool": "spawn",
	}, "telegram", "chat-1", nil)
	if add.IsError {
		t.Fatalf("add_step failed: %s", add.ForLLM)
	}

	result := toolRegistry.ExecuteWithContext(ctx, "task_board_execute_next", map[string]any{
		"board_id": "workflow-1",
		"step_id":  "background",
	}, "telegram", "chat-1", nil)
	if result.IsError {
		t.Fatalf("execute_next should return a non-executed plan, got error: %s", result.ForLLM)
	}
	if !strings.Contains(result.ForLLM, `"executed": false`) ||
		!strings.Contains(result.ForLLM, `"recommended_tool": "spawn"`) ||
		!strings.Contains(result.ForLLM, "not delegate-backed") {
		t.Fatalf("unexpected spawn execute response:\n%s", result.ForLLM)
	}
}

func TestTaskBoardExecuteAllTool_ExecutesDelegateBackedChain(t *testing.T) {
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(t.TempDir()))
	toolRegistry := NewToolRegistry()
	toolRegistry.Register(NewTaskBoardTool(registry))

	spawner := &delegateMockSpawner{}
	delegateTool := NewDelegateTool()
	delegateTool.SetSpawner(spawner)
	delegateTool.SetTaskRegistry(registry)
	toolRegistry.Register(delegateTool)
	toolRegistry.Register(NewTaskBoardExecuteAllTool(registry, toolRegistry))

	ctx := WithToolContext(context.Background(), "telegram", "chat-1")
	for _, step := range []map[string]any{
		{
			"action":         "add_step",
			"board_id":       "workflow-1",
			"step_id":        "media-extract",
			"step_title":     "Extract media",
			"owner":          "media",
			"task":           "Extract media.",
			"execution_tool": "delegate",
		},
		{
			"action":         "add_step",
			"board_id":       "workflow-1",
			"step_id":        "polish",
			"step_title":     "Polish result",
			"owner":          "research",
			"task":           "Polish the extracted result.",
			"execution_tool": "delegate",
			"depends_on":     []any{"media-extract"},
		},
	} {
		add := toolRegistry.ExecuteWithContext(ctx, "task_board", step, "telegram", "chat-1", nil)
		if add.IsError {
			t.Fatalf("add_step failed: %s", add.ForLLM)
		}
	}

	result := toolRegistry.ExecuteWithContext(ctx, "task_board_execute_all", map[string]any{
		"board_id": "workflow-1",
	}, "telegram", "chat-1", nil)
	if result.IsError {
		t.Fatalf("execute_all failed: %s", result.ForLLM)
	}

	var payload struct {
		Action        string `json:"action"`
		Executed      bool   `json:"executed"`
		ExecutedCount int    `json:"executed_count"`
		StopReason    string `json:"stop_reason"`
		Steps         []struct {
			StepID       string         `json:"step_id"`
			Executed     bool           `json:"executed"`
			DelegateArgs map[string]any `json:"delegate_args"`
		} `json:"steps"`
		FinalStatus struct {
			Counts map[string]int `json:"counts"`
		} `json:"final_status"`
	}
	if err := json.Unmarshal([]byte(result.ForLLM), &payload); err != nil {
		t.Fatalf("execute_all JSON error = %v\n%s", err, result.ForLLM)
	}
	if payload.Action != "execute_all" ||
		!payload.Executed ||
		payload.ExecutedCount != 2 ||
		payload.StopReason != "complete_or_no_ready_steps" ||
		len(payload.Steps) != 2 ||
		payload.Steps[0].StepID != "media-extract" ||
		payload.Steps[1].StepID != "polish" ||
		payload.FinalStatus.Counts["done"] != 2 {
		t.Fatalf("unexpected execute_all payload: %+v\n%s", payload, result.ForLLM)
	}
	if len(spawner.calls) != 2 ||
		spawner.calls[0].TargetAgentID != "media" ||
		spawner.calls[1].TargetAgentID != "research" {
		t.Fatalf("unexpected delegate calls: %+v", spawner.calls)
	}
}

func TestTaskBoardExecuteAllTool_StopsAtNonDelegateReadyStep(t *testing.T) {
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(t.TempDir()))
	toolRegistry := NewToolRegistry()
	toolRegistry.Register(NewTaskBoardTool(registry))

	spawner := &delegateMockSpawner{}
	delegateTool := NewDelegateTool()
	delegateTool.SetSpawner(spawner)
	delegateTool.SetTaskRegistry(registry)
	toolRegistry.Register(delegateTool)
	toolRegistry.Register(NewTaskBoardExecuteAllTool(registry, toolRegistry))

	ctx := WithToolContext(context.Background(), "telegram", "chat-1")
	add := toolRegistry.ExecuteWithContext(ctx, "task_board", map[string]any{
		"action":         "add_step",
		"board_id":       "workflow-1",
		"step_id":        "manual-review",
		"step_title":     "Manual review",
		"task":           "Review locally.",
		"execution_tool": "manual",
	}, "telegram", "chat-1", nil)
	if add.IsError {
		t.Fatalf("add_step failed: %s", add.ForLLM)
	}

	result := toolRegistry.ExecuteWithContext(ctx, "task_board_execute_all", map[string]any{
		"board_id": "workflow-1",
	}, "telegram", "chat-1", nil)
	if result.IsError {
		t.Fatalf("execute_all should stop cleanly, got error: %s", result.ForLLM)
	}
	if !strings.Contains(result.ForLLM, `"executed": false`) ||
		!strings.Contains(result.ForLLM, `"stop_reason": "no_delegate_backed_ready_step"`) ||
		!strings.Contains(result.ForLLM, `"recommended_tool": "task_board.update"`) {
		t.Fatalf("unexpected execute_all response:\n%s", result.ForLLM)
	}
	if len(spawner.calls) != 0 {
		t.Fatalf("manual step should not delegate, got calls: %+v", spawner.calls)
	}
}
