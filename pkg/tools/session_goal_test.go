package tools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/state"
)

func goalToolContext(routeSessionKey string) context.Context {
	return WithToolRouteSessionKey(context.Background(), routeSessionKey)
}

func decodeSessionGoalToolResponse(t *testing.T, result *ToolResult) sessionGoalToolResponse {
	t.Helper()
	if result == nil {
		t.Fatal("expected tool result")
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %s", result.ContentForLLM())
	}
	var response sessionGoalToolResponse
	if err := json.Unmarshal([]byte(result.ContentForLLM()), &response); err != nil {
		t.Fatalf("decode tool response: %v\n%s", err, result.ContentForLLM())
	}
	return response
}

func TestGetGoalToolReturnsNotSetForCurrentRoute(t *testing.T) {
	tool := NewGetGoalTool(state.NewManager(t.TempDir()))
	response := decodeSessionGoalToolResponse(t, tool.Execute(goalToolContext("route-a"), map[string]any{}))
	if response.Status != "not_set" || response.Goal != nil {
		t.Fatalf("response = %+v, want not_set without goal", response)
	}
}

func TestSessionGoalToolsCreateDuplicateAndUpdate(t *testing.T) {
	manager := state.NewManager(t.TempDir())
	createTool := NewCreateGoalTool(manager)
	getTool := NewGetGoalTool(manager)
	updateTool := NewUpdateGoalTool(manager)
	ctx := goalToolContext("route-a")

	created := decodeSessionGoalToolResponse(t, createTool.Execute(ctx, map[string]any{
		"objective": "finish the release",
	}))
	if created.Status != "created" || created.Goal == nil || created.Goal.Status != state.SessionGoalActive {
		t.Fatalf("created response = %+v", created)
	}

	duplicate := createTool.Execute(ctx, map[string]any{"objective": "replace the release"})
	if duplicate == nil || !duplicate.IsError || !strings.Contains(duplicate.ContentForLLM(), "already exists") {
		t.Fatalf("duplicate create result = %+v", duplicate)
	}

	completed := decodeSessionGoalToolResponse(t, updateTool.Execute(ctx, map[string]any{
		"status": "complete",
		"note":   "release shipped",
	}))
	if completed.Status != "updated" || completed.Goal == nil || completed.Goal.Status != state.SessionGoalComplete {
		t.Fatalf("completed response = %+v", completed)
	}
	if completed.Goal.Note != "release shipped" || completed.Goal.CompletedAt == nil {
		t.Fatalf("completed goal = %+v", completed.Goal)
	}

	blocked := decodeSessionGoalToolResponse(t, updateTool.Execute(ctx, map[string]any{
		"status": "blocked",
		"note":   "waiting for approval",
	}))
	if blocked.Goal == nil || blocked.Goal.Status != state.SessionGoalBlocked || blocked.Goal.BlockedAt == nil {
		t.Fatalf("blocked response = %+v", blocked)
	}
	if blocked.Goal.CompletedAt == nil {
		t.Fatalf("blocked goal lost completed timestamp: %+v", blocked.Goal)
	}

	got := decodeSessionGoalToolResponse(t, getTool.Execute(ctx, map[string]any{}))
	if got.Status != "found" || got.Goal == nil || got.Goal.Objective != "finish the release" {
		t.Fatalf("get response = %+v", got)
	}
}

func TestUpdateGoalToolRejectsNonTerminalStatus(t *testing.T) {
	manager := state.NewManager(t.TempDir())
	ctx := goalToolContext("route-a")
	if _, err := manager.CreateSessionGoal("route-a", "finish the release"); err != nil {
		t.Fatalf("CreateSessionGoal failed: %v", err)
	}

	result := NewUpdateGoalTool(manager).Execute(ctx, map[string]any{"status": "paused"})
	if result == nil || !result.IsError || !strings.Contains(result.ContentForLLM(), "complete, blocked") {
		t.Fatalf("non-terminal update result = %+v", result)
	}
}

func TestSessionGoalToolsRequireRouteSessionContext(t *testing.T) {
	manager := state.NewManager(t.TempDir())
	tests := []struct {
		name string
		tool Tool
		args map[string]any
	}{
		{name: "get", tool: NewGetGoalTool(manager), args: map[string]any{}},
		{name: "create", tool: NewCreateGoalTool(manager), args: map[string]any{"objective": "finish"}},
		{name: "update", tool: NewUpdateGoalTool(manager), args: map[string]any{"status": "complete"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.tool.Execute(context.Background(), tt.args)
			if result == nil || !result.IsError || !strings.Contains(result.ContentForLLM(), "route session context") {
				t.Fatalf("missing-context result = %+v", result)
			}
		})
	}
}

func TestSessionGoalToolsUseRouteSessionKeyInsteadOfHistorySession(t *testing.T) {
	manager := state.NewManager(t.TempDir())
	ctx := WithToolSessionContext(context.Background(), "main", "history-session", nil)
	ctx = WithToolRouteSessionKey(ctx, "route-session")

	response := decodeSessionGoalToolResponse(t, NewCreateGoalTool(manager).Execute(ctx, map[string]any{
		"objective": "persist on route session",
	}))
	if response.Goal == nil || response.Goal.Objective != "persist on route session" {
		t.Fatalf("create response = %+v", response)
	}
	if _, found := manager.GetSessionGoal("history-session"); found {
		t.Fatal("goal should not use the effective history session key")
	}
	if _, found := manager.GetSessionGoal("route-session"); !found {
		t.Fatal("goal should use the canonical route session key")
	}
}
