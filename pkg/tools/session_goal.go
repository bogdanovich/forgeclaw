package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/state"
)

// GetGoalTool exposes the durable goal for the current routed conversation.
type GetGoalTool struct {
	state *state.Manager
}

// CreateGoalTool creates the one durable goal allowed for a routed conversation.
type CreateGoalTool struct {
	state *state.Manager
}

// UpdateGoalTool lets the model record a terminal goal state.
type UpdateGoalTool struct {
	state *state.Manager
}

type sessionGoalToolView struct {
	Objective   string                  `json:"objective"`
	Status      state.SessionGoalStatus `json:"status"`
	Note        string                  `json:"note,omitempty"`
	CreatedAt   time.Time               `json:"created_at"`
	UpdatedAt   time.Time               `json:"updated_at"`
	BlockedAt   *time.Time              `json:"blocked_at,omitempty"`
	CompletedAt *time.Time              `json:"completed_at,omitempty"`
}

type sessionGoalToolResponse struct {
	Status string               `json:"status"`
	Goal   *sessionGoalToolView `json:"goal,omitempty"`
}

func NewGetGoalTool(manager *state.Manager) *GetGoalTool {
	return &GetGoalTool{state: manager}
}

func NewCreateGoalTool(manager *state.Manager) *CreateGoalTool {
	return &CreateGoalTool{state: manager}
}

func NewUpdateGoalTool(manager *state.Manager) *UpdateGoalTool {
	return &UpdateGoalTool{state: manager}
}

func (t *GetGoalTool) Name() string {
	return "get_goal"
}

func (t *GetGoalTool) Description() string {
	return "Read the durable goal for the current conversation. Use this to check the objective and its current status."
}

func (t *GetGoalTool) Parameters() map[string]any {
	return emptySessionGoalParameters()
}

func (t *GetGoalTool) Execute(ctx context.Context, _ map[string]any) *ToolResult {
	manager, routeSessionKey, err := sessionGoalToolContext(t.state, ctx)
	if err != nil {
		return ErrorResult(err.Error()).WithError(err)
	}
	goal, found := manager.GetSessionGoal(routeSessionKey)
	if !found {
		return sessionGoalToolResult(sessionGoalToolResponse{Status: "not_set"})
	}
	return sessionGoalToolResult(sessionGoalToolResponse{
		Status: "found",
		Goal:   sessionGoalToolViewFor(goal),
	})
}

func (t *CreateGoalTool) Name() string {
	return "create_goal"
}

func (t *CreateGoalTool) Description() string {
	return "Create one durable goal for the current conversation. Use only when the user or system explicitly asks to set a goal. Fails if a goal already exists."
}

func (t *CreateGoalTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"objective": map[string]any{
				"type":        "string",
				"description": "The explicit objective for this conversation.",
			},
		},
		"required": []string{"objective"},
	}
}

func (t *CreateGoalTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	manager, routeSessionKey, err := sessionGoalToolContext(t.state, ctx)
	if err != nil {
		return ErrorResult(err.Error()).WithError(err)
	}
	objective, err := requiredStringArg(args, "objective", "objective")
	if err != nil {
		return ErrorResult(err.Error()).WithError(err)
	}
	goal, err := manager.CreateSessionGoal(routeSessionKey, objective)
	if err != nil {
		return ErrorResult(fmt.Sprintf("failed to create goal: %v", err)).WithError(err)
	}
	return sessionGoalToolResult(sessionGoalToolResponse{
		Status: "created",
		Goal:   sessionGoalToolViewFor(goal),
	})
}

func (t *UpdateGoalTool) Name() string {
	return "update_goal"
}

func (t *UpdateGoalTool) Description() string {
	return "Record that the current goal is complete or blocked. Do not pause, resume, clear, replace, or otherwise change goals without an explicit user action."
}

func (t *UpdateGoalTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"status": map[string]any{
				"type":        "string",
				"enum":        []string{string(state.SessionGoalComplete), string(state.SessionGoalBlocked)},
				"description": "Terminal status to record for the current goal.",
			},
			"note": map[string]any{
				"type":        "string",
				"description": "Optional concise completion result or blocking reason.",
			},
		},
		"required": []string{"status"},
	}
}

func (t *UpdateGoalTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	manager, routeSessionKey, err := sessionGoalToolContext(t.state, ctx)
	if err != nil {
		return ErrorResult(err.Error()).WithError(err)
	}
	status, err := requiredStringArg(args, "status", "status")
	if err != nil {
		return ErrorResult(err.Error()).WithError(err)
	}
	if status != string(state.SessionGoalComplete) && status != string(state.SessionGoalBlocked) {
		return ErrorResult("status must be one of complete, blocked")
	}
	note, err := optionalStringArg(args, "note")
	if err != nil {
		return ErrorResult(err.Error()).WithError(err)
	}
	goal, err := manager.SetSessionGoalStatus(routeSessionKey, state.SessionGoalStatus(status), note)
	if err != nil {
		return ErrorResult(fmt.Sprintf("failed to update goal: %v", err)).WithError(err)
	}
	return sessionGoalToolResult(sessionGoalToolResponse{
		Status: "updated",
		Goal:   sessionGoalToolViewFor(goal),
	})
}

func emptySessionGoalParameters() map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": map[string]any{},
		"required":   []string{},
	}
}

func sessionGoalToolContext(manager *state.Manager, ctx context.Context) (*state.Manager, string, error) {
	if manager == nil {
		return nil, "", fmt.Errorf("session goal store not configured")
	}
	routeSessionKey := strings.TrimSpace(ToolRouteSessionKey(ctx))
	if routeSessionKey == "" {
		return nil, "", fmt.Errorf("route session context not available")
	}
	return manager, routeSessionKey, nil
}

func sessionGoalToolViewFor(goal state.SessionGoal) *sessionGoalToolView {
	return &sessionGoalToolView{
		Objective:   goal.Objective,
		Status:      goal.Status,
		Note:        goal.Note,
		CreatedAt:   goal.CreatedAt,
		UpdatedAt:   goal.UpdatedAt,
		BlockedAt:   goal.BlockedAt,
		CompletedAt: goal.CompletedAt,
	}
}

func sessionGoalToolResult(response sessionGoalToolResponse) *ToolResult {
	data, err := json.Marshal(response)
	if err != nil {
		return ErrorResult(fmt.Sprintf("failed to encode goal response: %v", err)).WithError(err)
	}
	return NewToolResult(string(data))
}
