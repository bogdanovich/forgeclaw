package commands

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

type commandGoalStore struct {
	goal  GoalInfo
	found bool
}

func (s *commandGoalStore) runtime() *Runtime {
	return &Runtime{
		GetGoal: func() (GoalInfo, bool, error) {
			return s.goal, s.found, nil
		},
		CreateGoal: func(objective string) (GoalInfo, error) {
			if s.found {
				return GoalInfo{}, fmt.Errorf("session goal already exists")
			}
			s.goal = GoalInfo{
				Objective: objective,
				Status:    "active",
				CreatedAt: time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC),
				UpdatedAt: time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC),
			}
			s.found = true
			return s.goal, nil
		},
		EditGoal: func(objective string) (GoalInfo, error) {
			if !s.found {
				return GoalInfo{}, fmt.Errorf("session goal not found")
			}
			s.goal.Objective = objective
			return s.goal, nil
		},
		SetGoalStatus: func(status, note string) (GoalInfo, error) {
			if !s.found {
				return GoalInfo{}, fmt.Errorf("session goal not found")
			}
			s.goal.Status = status
			s.goal.Note = note
			return s.goal, nil
		},
		ClearGoal: func() error {
			s.goal = GoalInfo{}
			s.found = false
			return nil
		},
	}
}

func executeGoalCommand(t *testing.T, rt *Runtime, text string) string {
	t.Helper()
	executor := NewExecutor(NewRegistry(BuiltinDefinitions()), rt)
	var reply string
	result := executor.Execute(context.Background(), Request{
		Text: text,
		Reply: func(text string) error {
			reply = text
			return nil
		},
	})
	if result.Outcome != OutcomeHandled {
		t.Fatalf("%s outcome = %v, want handled", text, result.Outcome)
	}
	if result.Err != nil {
		t.Fatalf("%s command error: %v", text, result.Err)
	}
	return reply
}

func TestGoalCommandLifecycle(t *testing.T) {
	store := &commandGoalStore{}
	rt := store.runtime()

	if got := executeGoalCommand(t, rt, "/goal"); !strings.Contains(got, "No goal is set") {
		t.Fatalf("empty goal status = %q", got)
	}

	if got := executeGoalCommand(
		t,
		rt,
		"/goal ship the release checklist",
	); !strings.Contains(got, "Goal started.") {
		t.Fatalf("implicit start reply = %q", got)
	}
	if store.goal.Objective != "ship the release checklist" || store.goal.Status != "active" {
		t.Fatalf("implicit start goal = %+v", store.goal)
	}

	if got := executeGoalCommand(
		t,
		rt,
		"/goal edit ship the verified release checklist",
	); !strings.Contains(got, "Goal updated.") {
		t.Fatalf("edit reply = %q", got)
	}
	if store.goal.Objective != "ship the verified release checklist" {
		t.Fatalf("edited objective = %q", store.goal.Objective)
	}

	if got := executeGoalCommand(t, rt, "/goal pause waiting for CI"); !strings.Contains(got, "Status: paused") {
		t.Fatalf("pause reply = %q", got)
	}
	if store.goal.Status != "paused" || store.goal.Note != "waiting for CI" {
		t.Fatalf("paused goal = %+v", store.goal)
	}

	if got := executeGoalCommand(t, rt, "/goal resume continuing"); !strings.Contains(got, "Status: active") {
		t.Fatalf("resume reply = %q", got)
	}
	if store.goal.Status != "active" || store.goal.Note != "continuing" {
		t.Fatalf("resumed goal = %+v", store.goal)
	}

	if got := executeGoalCommand(t, rt, "/goal done shipped"); !strings.Contains(got, "Status: complete") {
		t.Fatalf("done reply = %q", got)
	}
	if store.goal.Status != "complete" || store.goal.Note != "shipped" {
		t.Fatalf("completed goal = %+v", store.goal)
	}

	if got := executeGoalCommand(t, rt, "/goal clear"); got != "Goal cleared for this conversation." {
		t.Fatalf("clear reply = %q", got)
	}
	if store.found {
		t.Fatal("expected clear to remove goal")
	}
}

func TestGoalCommandAliasesAndValidation(t *testing.T) {
	store := &commandGoalStore{}
	rt := store.runtime()

	if got := executeGoalCommand(t, rt, "/goal start"); got != "Usage: /goal start <objective>" {
		t.Fatalf("start without objective = %q", got)
	}
	if got := executeGoalCommand(t, rt, "/goal create first goal"); !strings.Contains(got, "Goal started.") {
		t.Fatalf("create reply = %q", got)
	}
	if got := executeGoalCommand(
		t,
		rt,
		"/goal set second goal",
	); !strings.Contains(got, "session goal already exists") {
		t.Fatalf("duplicate set reply = %q", got)
	}
	if got := executeGoalCommand(
		t,
		rt,
		"/goal block external dependency",
	); !strings.Contains(got, "Status: blocked") {
		t.Fatalf("block reply = %q", got)
	}
	if got := executeGoalCommand(t, rt, "/goal clear extra"); got != "Usage: /goal clear" {
		t.Fatalf("clear with argument = %q", got)
	}
}

func TestGoalCommandUnavailableRuntime(t *testing.T) {
	got := executeGoalCommand(t, nil, "/goal status")
	if got != unavailableMsg {
		t.Fatalf("unavailable reply = %q", got)
	}
}
