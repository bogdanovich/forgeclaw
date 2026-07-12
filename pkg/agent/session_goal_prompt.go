package agent

import (
	"strings"

	"github.com/sipeed/picoclaw/pkg/state"
	"github.com/sipeed/picoclaw/pkg/utils"
)

const maxActiveGoalObjectiveRunes = 480

func (al *AgentLoop) applyActiveGoalPrompt(opts *processOptions) {
	if opts == nil {
		return
	}
	opts.ActiveGoal = ""
	if al == nil || al.state == nil || opts.NoHistory {
		return
	}

	routeSessionKey := strings.TrimSpace(opts.Dispatch.RouteSessionKey)
	if routeSessionKey == "" {
		routeSessionKey = strings.TrimSpace(opts.ModelBinding.RouteSessionKey)
	}
	if routeSessionKey == "" {
		return
	}

	goal, found := al.state.GetSessionGoal(routeSessionKey)
	if !found || goal.Status != state.SessionGoalActive {
		return
	}

	objective := strings.Join(strings.Fields(goal.Objective), " ")
	objective = utils.Truncate(objective, maxActiveGoalObjectiveRunes)
	if objective == "" {
		return
	}
	opts.ActiveGoal = "Active goal: " + objective +
		" - advance it or update its status (get_goal/update_goal)."
}
