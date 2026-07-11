package commands

import (
	"context"
	"fmt"
	"strings"
)

var goalActions = map[string]struct{}{
	"status":   {},
	"start":    {},
	"create":   {},
	"set":      {},
	"edit":     {},
	"pause":    {},
	"resume":   {},
	"complete": {},
	"done":     {},
	"block":    {},
	"blocked":  {},
	"clear":    {},
}

const goalUsage = "/goal [status|start <objective>|edit <objective>|pause [note]|resume [note]|complete [note]|block [note]|clear]"

func goalCommand() Definition {
	return Definition{
		Name:        "goal",
		Description: "Show or manage the durable goal for this conversation",
		Usage:       goalUsage,
		Handler: func(_ context.Context, req Request, rt *Runtime) error {
			if rt == nil || rt.GetGoal == nil || rt.CreateGoal == nil || rt.EditGoal == nil ||
				rt.SetGoalStatus == nil || rt.ClearGoal == nil {
				return req.Reply(unavailableMsg)
			}

			args := strings.Fields(strings.TrimSpace(req.Text))
			if len(args) <= 1 || strings.EqualFold(args[1], "status") {
				return replyGoalStatus(req, rt)
			}

			action := strings.ToLower(args[1])
			argument := strings.TrimSpace(strings.Join(args[2:], " "))
			if _, known := goalActions[action]; !known {
				return createGoal(req, rt, strings.TrimSpace(strings.Join(args[1:], " ")))
			}

			switch action {
			case "start", "create", "set":
				return createGoal(req, rt, argument)
			case "edit":
				if argument == "" {
					return req.Reply("Usage: /goal edit <objective>")
				}
				goal, err := rt.EditGoal(argument)
				if err != nil {
					return req.Reply("Failed to update goal: " + err.Error())
				}
				return req.Reply(formatGoalChange("Goal updated.", goal))
			case "pause", "resume", "complete", "done", "block", "blocked":
				status := action
				switch action {
				case "pause":
					status = "paused"
				case "resume":
					status = "active"
				case "done":
					status = "complete"
				case "block":
					status = "blocked"
				}
				goal, err := rt.SetGoalStatus(status, argument)
				if err != nil {
					return req.Reply("Failed to update goal: " + err.Error())
				}
				return req.Reply(formatGoalChange("Goal "+status+".", goal))
			case "clear":
				if argument != "" {
					return req.Reply("Usage: /goal clear")
				}
				if err := rt.ClearGoal(); err != nil {
					return req.Reply("Failed to clear goal: " + err.Error())
				}
				return req.Reply("Goal cleared for this conversation.")
			default:
				return req.Reply("Usage: " + goalUsage)
			}
		},
	}
}

func createGoal(req Request, rt *Runtime, objective string) error {
	if objective == "" {
		return req.Reply("Usage: /goal start <objective>")
	}
	goal, err := rt.CreateGoal(objective)
	if err != nil {
		return req.Reply("Failed to start goal: " + err.Error())
	}
	return req.Reply(formatGoalChange("Goal started.", goal))
}

func replyGoalStatus(req Request, rt *Runtime) error {
	goal, found, err := rt.GetGoal()
	if err != nil {
		return req.Reply("Failed to read goal: " + err.Error())
	}
	if !found {
		return req.Reply("No goal is set for this conversation.\nUse /goal start <objective> to create one.")
	}
	return req.Reply(formatGoalStatus(goal))
}

func formatGoalChange(prefix string, goal GoalInfo) string {
	return prefix + "\n" + formatGoalStatus(goal)
}

func formatGoalStatus(goal GoalInfo) string {
	lines := []string{
		"Objective: " + goal.Objective,
		"Status: " + goal.Status,
	}
	if goal.Note != "" {
		lines = append(lines, "Note: "+goal.Note)
	}
	if !goal.CreatedAt.IsZero() {
		lines = append(lines, fmt.Sprintf("Created: %s", goal.CreatedAt.Format("2006-01-02 15:04 MST")))
	}
	if !goal.UpdatedAt.IsZero() {
		lines = append(lines, fmt.Sprintf("Updated: %s", goal.UpdatedAt.Format("2006-01-02 15:04 MST")))
	}
	return strings.Join(lines, "\n")
}
