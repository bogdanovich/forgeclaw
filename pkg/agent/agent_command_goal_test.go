package agent

import (
	"context"
	"testing"

	"github.com/sipeed/picoclaw/pkg/commands"
)

func TestBuildCommandsRuntime_GoalCallbacksUseRouteSessionKey(t *testing.T) {
	al, _, _, _, cleanup := newTestAgentLoop(t)
	t.Cleanup(cleanup)
	workspaceAgent := al.registry.GetDefaultAgent()
	if workspaceAgent == nil {
		t.Fatal("expected default agent")
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
	if goal, found, err := runtimeB.GetGoal(); err != nil || found || goal.Objective != "" {
		t.Fatalf("route-b goal = (%+v, %v, %v), want no goal", goal, found, err)
	}

	goal, found, err := runtimeA.GetGoal()
	if err != nil || !found || goal.Objective != "finish command support" {
		t.Fatalf("route-a goal = (%+v, %v, %v)", goal, found, err)
	}
}
