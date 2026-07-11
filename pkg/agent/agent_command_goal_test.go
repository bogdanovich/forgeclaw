package agent

import (
	"context"
	"testing"

	"github.com/sipeed/picoclaw/pkg/commands"
)

func TestBuildCommandsRuntime_GoalCallbacksUseRouteSessionKey(t *testing.T) {
	al, cfg, _, _, cleanup := newTestAgentLoop(t)
	t.Cleanup(cleanup)
	if cfg == nil {
		t.Fatal("expected test config")
	}
	workspaceAgent := al.registry.GetDefaultAgent()
	if workspaceAgent == nil {
		t.Fatal("expected default agent")
	}
	for _, toolName := range []string{"get_goal", "create_goal", "update_goal"} {
		if _, ok := workspaceAgent.Tools.Get(toolName); !ok {
			t.Fatalf("expected %s to be registered", toolName)
		}
	}

	buildRuntime := func(routeSessionKey string) *commands.Runtime {
		return al.buildCommandsRuntime(context.Background(), effectiveModelBinding{
			RouteSessionKey: routeSessionKey,
			WorkspaceAgent:  workspaceAgent,
		}, &processOptions{Dispatch: DispatchRequest{RouteSessionKey: routeSessionKey}})
	}

	runtimeA := buildRuntime("route-a")
	if runtimeA.CreateGoal == nil || runtimeA.GetGoal == nil || runtimeA.ClearGoal == nil {
		t.Fatal("expected goal callbacks")
	}
	created, err := runtimeA.CreateGoal("finish command support")
	if err != nil {
		t.Fatalf("CreateGoal failed: %v", err)
	}
	if created.Objective != "finish command support" || created.Status != "active" {
		t.Fatalf("created goal = %+v", created)
	}

	runtimeB := buildRuntime("route-b")
	goalB, foundB, getErr := runtimeB.GetGoal()
	if getErr != nil || foundB || goalB.Objective != "" {
		t.Fatalf("route-b goal = (%+v, %v, %v), want no goal", goalB, foundB, getErr)
	}

	goalA, foundA, getErr := runtimeA.GetGoal()
	if getErr != nil || !foundA || goalA.Objective != "finish command support" {
		t.Fatalf("route-a goal = (%+v, %v, %v)", goalA, foundA, getErr)
	}
}
