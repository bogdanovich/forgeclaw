package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	taskregistry "github.com/sipeed/picoclaw/pkg/tasks"
)

// TaskBoardExecuteNextTool executes one delegate-backed ready step from a board.
//
// It intentionally does not auto-run spawn/manual steps. Spawn requires async
// callback ownership, and manual steps require parent/local work outside this
// tool. Keeping this first executor delegate-only preserves delivery semantics.
type TaskBoardExecuteNextTool struct {
	registry *taskregistry.Registry
	tools    *ToolRegistry
}

func NewTaskBoardExecuteNextTool(registry *taskregistry.Registry, tools *ToolRegistry) *TaskBoardExecuteNextTool {
	return &TaskBoardExecuteNextTool{registry: registry, tools: tools}
}

// TaskBoardExecuteAllTool executes delegate-backed ready steps from a board
// until no more conservative auto-executable steps remain.
//
// It uses the same planner as task_board next and the same execution primitive
// as task_board_execute_next. It intentionally stops at spawn/manual steps
// rather than changing async delivery semantics or pretending local work was
// performed.
type TaskBoardExecuteAllTool struct {
	registry *taskregistry.Registry
	tools    *ToolRegistry
}

func NewTaskBoardExecuteAllTool(registry *taskregistry.Registry, tools *ToolRegistry) *TaskBoardExecuteAllTool {
	return &TaskBoardExecuteAllTool{registry: registry, tools: tools}
}

func (t *TaskBoardExecuteNextTool) Name() string {
	return "task_board_execute_next"
}

func (t *TaskBoardExecuteAllTool) Name() string {
	return "task_board_execute_all"
}

func (t *TaskBoardExecuteNextTool) Description() string {
	return "Execute one delegate-backed ready step from a task_board next plan. " +
		"This is conservative: it does not execute spawn or manual steps; use task_board next for those plans."
}

func (t *TaskBoardExecuteAllTool) Description() string {
	return "Execute delegate-backed ready steps from a task_board until the board is complete, blocked, active, waiting, or reaches a non-delegate/manual step. " +
		"This is conservative: it does not execute spawn or manual steps."
}

func (t *TaskBoardExecuteNextTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"board_id": map[string]any{
				"type":        "string",
				"description": "Workflow/task-board ID to execute one ready step from.",
			},
			"step_id": map[string]any{
				"type":        "string",
				"description": "Optional step_id to execute. If omitted, the first delegate-backed ready step from task_board next is used.",
			},
		},
		"required": []string{"board_id"},
	}
}

func (t *TaskBoardExecuteAllTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"board_id": map[string]any{
				"type":        "string",
				"description": "Workflow/task-board ID to execute ready delegate-backed steps from.",
			},
			"max_steps": map[string]any{
				"type":        "integer",
				"minimum":     1,
				"maximum":     50,
				"description": "Safety cap for the number of delegate-backed steps to execute in one call. Defaults to 10.",
			},
		},
		"required": []string{"board_id"},
	}
}

func (t *TaskBoardExecuteNextTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t == nil || t.registry == nil || t.tools == nil {
		return ErrorResult("task_board_execute_next not configured")
	}
	boardID, err := requiredStringArg(args, "board_id", "board_id")
	if err != nil {
		return ErrorResult(err.Error())
	}
	stepID, err := optionalStringArg(args, "step_id")
	if err != nil {
		return ErrorResult(err.Error())
	}

	records := visibleTaskBoardRecordsForToolContext(ctx, t.registry, boardID)
	plan := buildTaskBoardNextView(boardID, records, time.Now())
	selected, ok := selectTaskBoardExecutableStep(plan.Plan, stepID)
	if !ok {
		return taskBoardJSONResult(map[string]any{
			"action":            "execute_next",
			"board_id":          boardID,
			"executed":          false,
			"error":             "no delegate-backed ready step found",
			"next_plan":         plan.Plan,
			"active":            plan.ActiveSteps,
			"waiting":           plan.WaitingSteps,
			"blocked":           plan.BlockedSteps,
			"requested_step_id": stepID,
		})
	}
	if selected.RecommendedTool != "delegate" || selected.DelegateArgs == nil {
		return taskBoardJSONResult(map[string]any{
			"action":           "execute_next",
			"board_id":         boardID,
			"step_id":          selected.StepID,
			"executed":         false,
			"recommended_tool": selected.RecommendedTool,
			"error":            "selected step is not delegate-backed; use task_board next and execute it explicitly",
			"plan":             selected,
		})
	}
	if _, ok := t.tools.Get("delegate"); !ok {
		return ErrorResult("task_board_execute_next requires delegate tool")
	}

	result := t.tools.ExecuteWithContext(
		ctx,
		"delegate",
		selected.DelegateArgs,
		ToolChannel(ctx),
		ToolChatID(ctx),
		nil,
	)
	if result == nil {
		return ErrorResult("delegate returned nil result")
	}
	payload := map[string]any{
		"action":           "execute_next",
		"board_id":         boardID,
		"step_id":          selected.StepID,
		"executed":         !result.IsError,
		"recommended_tool": selected.RecommendedTool,
		"delegate_args":    selected.DelegateArgs,
		"result":           result.ContentForLLM(),
	}
	if result.IsError {
		payload["error"] = result.ContentForLLM()
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return ErrorResult(fmt.Sprintf("failed to encode task board execution result: %v", err)).WithError(err)
	}
	if result.IsError {
		delegateErr := result.Err
		if delegateErr == nil {
			delegateErr = fmt.Errorf("delegate step failed")
		}
		return ErrorResult(string(data)).WithError(delegateErr)
	}
	return NewToolResult(string(data))
}

func (t *TaskBoardExecuteAllTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t == nil || t.registry == nil || t.tools == nil {
		return ErrorResult("task_board_execute_all not configured")
	}
	boardID, err := requiredStringArg(args, "board_id", "board_id")
	if err != nil {
		return ErrorResult(err.Error())
	}
	maxSteps, err := optionalBoundedIntArg(args, "max_steps", 10, 1, 50)
	if err != nil {
		return ErrorResult(err.Error())
	}
	if _, ok := t.tools.Get("delegate"); !ok {
		return ErrorResult("task_board_execute_all requires delegate tool")
	}

	payload := map[string]any{
		"action":       "execute_all",
		"board_id":     boardID,
		"max_steps":    maxSteps,
		"executed":     false,
		"steps":        []map[string]any{},
		"stop_reason":  "",
		"final_status": map[string]any{},
	}
	steps := make([]map[string]any, 0, maxSteps)
	var finalPlan taskBoardNextView

	for len(steps) < maxSteps {
		records := visibleTaskBoardRecordsForToolContext(ctx, t.registry, boardID)
		finalPlan = buildTaskBoardNextView(boardID, records, time.Now())

		selected, ok := selectTaskBoardExecutableStep(finalPlan.Plan, "")
		if !ok {
			payload["stop_reason"] = taskBoardExecuteAllStopReason(finalPlan)
			break
		}
		if selected.RecommendedTool != "delegate" || selected.DelegateArgs == nil {
			payload["stop_reason"] = "non_delegate_ready_step"
			break
		}

		result := t.tools.ExecuteWithContext(
			ctx,
			"delegate",
			selected.DelegateArgs,
			ToolChannel(ctx),
			ToolChatID(ctx),
			nil,
		)
		if result == nil {
			return ErrorResult("delegate returned nil result")
		}

		stepResult := map[string]any{
			"step_id":          selected.StepID,
			"executed":         !result.IsError,
			"recommended_tool": selected.RecommendedTool,
			"delegate_args":    selected.DelegateArgs,
			"result":           result.ContentForLLM(),
		}
		if result.IsError {
			stepResult["error"] = result.ContentForLLM()
		}
		steps = append(steps, stepResult)

		if result.IsError {
			payload["steps"] = steps
			payload["executed"] = len(steps) > 0
			payload["executed_count"] = len(steps)
			payload["stop_reason"] = "delegate_error"
			payload["final_status"] = taskBoardExecuteAllFinalStatus(boardID, t.registry, ctx)
			data, marshalErr := json.MarshalIndent(payload, "", "  ")
			if marshalErr != nil {
				return ErrorResult(fmt.Sprintf("failed to encode task board execution result: %v", marshalErr)).
					WithError(marshalErr)
			}
			delegateErr := result.Err
			if delegateErr == nil {
				delegateErr = fmt.Errorf("delegate step failed")
			}
			return ErrorResult(string(data)).WithError(delegateErr)
		}
	}

	if len(steps) >= maxSteps && payload["stop_reason"] == "" {
		payload["stop_reason"] = "max_steps_reached"
	}
	finalPlan = buildTaskBoardNextView(
		boardID,
		visibleTaskBoardRecordsForToolContext(ctx, t.registry, boardID),
		time.Now(),
	)
	payload["steps"] = steps
	payload["executed"] = len(steps) > 0
	payload["executed_count"] = len(steps)
	payload["next_plan"] = finalPlan.Plan
	payload["active"] = finalPlan.ActiveSteps
	payload["waiting"] = finalPlan.WaitingSteps
	payload["blocked"] = finalPlan.BlockedSteps
	payload["final_status"] = taskBoardExecuteAllFinalStatus(boardID, t.registry, ctx)

	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return ErrorResult(fmt.Sprintf("failed to encode task board execution result: %v", err)).WithError(err)
	}
	return NewToolResult(string(data))
}

func selectTaskBoardExecutableStep(plan []taskBoardNextStepView, stepID string) (taskBoardNextStepView, bool) {
	for _, step := range plan {
		if stepID != "" && step.StepID != stepID {
			continue
		}
		if step.RecommendedTool == "delegate" && step.DelegateArgs != nil {
			return step, true
		}
		if stepID != "" {
			return step, true
		}
	}
	return taskBoardNextStepView{}, false
}

func taskBoardExecuteAllStopReason(plan taskBoardNextView) string {
	if len(plan.Plan) > 0 {
		return "no_delegate_backed_ready_step"
	}
	if len(plan.ActiveSteps) > 0 {
		return "active_steps"
	}
	if len(plan.BlockedSteps) > 0 {
		return "blocked_steps"
	}
	if len(plan.WaitingSteps) > 0 {
		return "waiting_steps"
	}
	return "complete_or_no_ready_steps"
}

func taskBoardExecuteAllFinalStatus(
	boardID string,
	registry *taskregistry.Registry,
	ctx context.Context,
) taskBoardReadyView {
	records := visibleTaskBoardRecordsForToolContext(ctx, registry, boardID)
	return buildTaskBoardReadyView(boardID, records, time.Now())
}

func optionalBoundedIntArg(args map[string]any, key string, defaultValue, minValue, maxValue int) (int, error) {
	value, ok := args[key]
	if !ok || value == nil {
		return defaultValue, nil
	}
	var parsed int
	switch v := value.(type) {
	case int:
		parsed = v
	case int64:
		if v > int64(math.MaxInt) || v < int64(math.MinInt) {
			return 0, fmt.Errorf("%s is outside integer range", key)
		}
		parsed = int(v)
	case float64:
		if math.Trunc(v) != v {
			return 0, fmt.Errorf("%s must be an integer", key)
		}
		if v > float64(math.MaxInt) || v < float64(math.MinInt) {
			return 0, fmt.Errorf("%s is outside integer range", key)
		}
		parsed = int(v)
	default:
		return 0, fmt.Errorf("%s must be an integer", key)
	}
	if parsed < minValue || parsed > maxValue {
		return 0, fmt.Errorf("%s must be between %d and %d", key, minValue, maxValue)
	}
	return parsed, nil
}

func visibleTaskBoardRecordsForToolContext(
	ctx context.Context,
	registry *taskregistry.Registry,
	boardID string,
) []taskregistry.Record {
	channel := ToolChannel(ctx)
	chatID := ToolChatID(ctx)
	topicID := ToolTopicID(ctx)
	records := registry.ListBoard(boardID)
	filtered := make([]taskregistry.Record, 0, len(records))
	for _, rec := range records {
		if taskRecordVisibleToCaller(rec, channel, chatID, topicID) {
			filtered = append(filtered, rec)
		}
	}
	return filtered
}
