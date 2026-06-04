package tools

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	taskregistry "github.com/sipeed/picoclaw/pkg/tasks"
)

// SpawnStatusTool reports the status of subagents that were spawned via the
// spawn tool. It can query a specific task by ID, or list every known task with
// a summary count broken-down by status.
type SpawnStatusTool struct {
	manager  *SubagentManager
	registry *taskregistry.Registry
}

// NewSpawnStatusTool creates a SpawnStatusTool backed by the given manager.
func NewSpawnStatusTool(manager *SubagentManager) *SpawnStatusTool {
	var registry *taskregistry.Registry
	if manager != nil {
		registry = manager.taskRegistry
	}
	return &SpawnStatusTool{manager: manager, registry: registry}
}

func (t *SpawnStatusTool) Name() string {
	return "spawn_status"
}

func (t *SpawnStatusTool) Description() string {
	return "Get durable status for subagents started specifically with the spawn tool. " +
		"This is a spawn-only compatibility view over the task registry; use task_status for general task history, " +
		"delegate runs, or restart-persistent completed task checks. " +
		"Returns a list of all subagents and their current state " +
		"(running, completed, failed, or canceled), or retrieves details " +
		"for a specific subagent task when task_id is provided. " +
		"Results are scoped to the current conversation's channel and chat ID; " +
		"all tasks are listed only when no channel/chat context is injected " +
		"(e.g. direct programmatic calls via Execute)."
}

func (t *SpawnStatusTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"task_id": map[string]any{
				"type": "string",
				"description": "Optional task ID (e.g. \"subagent-1\") to inspect a specific " +
					"subagent. When omitted, all visible subagents are listed.",
			},
		},
		"required": []string{},
	}
}

func (t *SpawnStatusTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.manager == nil && t.registry == nil {
		return ErrorResult("Subagent manager not configured")
	}

	// Derive the calling conversation's identity so we can scope results to the
	// current chat only — preventing cross-conversation task leakage in
	// multi-user deployments.
	callerChannel := ToolChannel(ctx)
	callerChatID := ToolChatID(ctx)

	var taskID string
	if rawTaskID, ok := args["task_id"]; ok && rawTaskID != nil {
		taskIDStr, ok := rawTaskID.(string)
		if !ok {
			return ErrorResult("task_id must be a string")
		}
		taskID = strings.TrimSpace(taskIDStr)
	}

	if t.registry != nil {
		result, found := t.executeFromRegistry(taskID, callerChannel, callerChatID)
		if found {
			return result
		}
		// Fall through to the legacy in-memory manager for ad-hoc tests and
		// non-registry callers. Normal gateway execution should be satisfied by
		// the durable task registry path above.
	}

	if t.manager == nil {
		if taskID != "" {
			return ErrorResult(fmt.Sprintf("No subagent found with task ID: %s", taskID))
		}
		return NewToolResult(
			"No visible spawned subagents are registered. This tool only reports tasks started with the spawn tool. For delegate runs, other child-run mechanisms, or broader task checks, use task_status.",
		)
	}

	if taskID != "" {
		// GetTaskCopy returns a consistent snapshot under the manager lock,
		// eliminating any data race with the concurrent subagent goroutine.
		taskCopy, ok := t.manager.GetTaskCopy(taskID)
		if !ok {
			return ErrorResult(fmt.Sprintf("No subagent found with task ID: %s", taskID))
		}

		// Restrict lookup to tasks that belong to this conversation.
		if callerChannel != "" && taskCopy.OriginChannel != "" && taskCopy.OriginChannel != callerChannel {
			return ErrorResult(fmt.Sprintf("No subagent found with task ID: %s", taskID))
		}
		if callerChatID != "" && taskCopy.OriginChatID != "" && taskCopy.OriginChatID != callerChatID {
			return ErrorResult(fmt.Sprintf("No subagent found with task ID: %s", taskID))
		}

		return NewToolResult(spawnStatusFormatTask(&taskCopy))
	}

	// ListTaskCopies returns consistent snapshots under the manager lock.
	origTasks := t.manager.ListTaskCopies()
	if len(origTasks) == 0 {
		return NewToolResult(
			"No visible spawned subagents are registered in the current process. This tool only reports tasks started with the spawn tool. For delegate runs, other child-run mechanisms, or restart-persistent completed task checks, use task_status.",
		)
	}

	tasks := make([]*SubagentTask, 0, len(origTasks))
	for i := range origTasks {
		cpy := &origTasks[i]

		// Filter to tasks that originate from the current conversation only.
		if callerChannel != "" && cpy.OriginChannel != "" && cpy.OriginChannel != callerChannel {
			continue
		}
		if callerChatID != "" && cpy.OriginChatID != "" && cpy.OriginChatID != callerChatID {
			continue
		}

		tasks = append(tasks, cpy)
	}

	if len(tasks) == 0 {
		return NewToolResult(
			"No spawned subagents found for this conversation. This tool only reports tasks started with the spawn tool. For delegate runs or other child-run mechanisms, use task_status.",
		)
	}

	// Order by creation time (ascending) so spawning order is preserved.
	// Fall back to ID string for tasks created in the same millisecond.
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].Created != tasks[j].Created {
			return tasks[i].Created < tasks[j].Created
		}
		return tasks[i].ID < tasks[j].ID
	})

	counts := map[string]int{}
	for _, task := range tasks {
		counts[task.Status]++
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Subagent status report (%d total):\n", len(tasks)))
	for _, status := range []string{"running", "completed", "failed", "canceled"} {
		if n := counts[status]; n > 0 {
			label := strings.ToUpper(status[:1]) + status[1:] + ":"
			sb.WriteString(fmt.Sprintf("  %-10s %d\n", label, n))
		}
	}
	sb.WriteString("\n")

	for _, task := range tasks {
		sb.WriteString(spawnStatusFormatTask(task))
		sb.WriteString("\n\n")
	}

	return NewToolResult(strings.TrimRight(sb.String(), "\n"))
}

func (t *SpawnStatusTool) executeFromRegistry(taskID, callerChannel, callerChatID string) (*ToolResult, bool) {
	if taskID != "" {
		rec, ok := t.registry.Get(taskID)
		if !ok {
			return nil, false
		}
		if !spawnRecordVisibleToCaller(rec, callerChannel, callerChatID) {
			return ErrorResult(fmt.Sprintf("No subagent found with task ID: %s", taskID)), true
		}
		return NewToolResult(spawnStatusFormatRecord(rec)), true
	}

	records := t.registry.List()
	filtered := make([]taskregistry.Record, 0, len(records))
	for _, rec := range records {
		if !spawnRecordVisibleToCaller(rec, callerChannel, callerChatID) {
			continue
		}
		filtered = append(filtered, rec)
	}
	if len(filtered) == 0 {
		return nil, false
	}

	counts := map[string]int{}
	for _, rec := range filtered {
		counts[spawnStatusFromRecord(rec)]++
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Subagent status report (%d total):\n", len(filtered)))
	for _, status := range []string{"running", "completed", "failed", "canceled"} {
		if n := counts[status]; n > 0 {
			label := strings.ToUpper(status[:1]) + status[1:] + ":"
			sb.WriteString(fmt.Sprintf("  %-10s %d\n", label, n))
		}
	}
	sb.WriteString("\n")

	for _, rec := range filtered {
		sb.WriteString(spawnStatusFormatRecord(rec))
		sb.WriteString("\n\n")
	}

	return NewToolResult(strings.TrimRight(sb.String(), "\n")), true
}

func spawnRecordVisibleToCaller(rec taskregistry.Record, channel, chatID string) bool {
	if rec.Runtime != taskregistry.RuntimeSubagent || rec.TaskKind != "spawn" {
		return false
	}
	if channel != "" && rec.Channel != "" && rec.Channel != channel {
		return false
	}
	if chatID != "" && rec.ChatID != "" && rec.ChatID != chatID {
		return false
	}
	return true
}

func spawnStatusFromRecord(rec taskregistry.Record) string {
	switch rec.Status {
	case taskregistry.StatusSucceeded:
		return "completed"
	case taskregistry.StatusFailed:
		return "failed"
	case taskregistry.StatusCancelled, taskregistry.StatusTimedOut:
		return "canceled"
	case taskregistry.StatusRunning, taskregistry.StatusQueued:
		return "running"
	default:
		return string(rec.Status)
	}
}

func spawnStatusFormatRecord(rec taskregistry.Record) string {
	task := &SubagentTask{
		ID:            rec.TaskID,
		Task:          rec.Task,
		Label:         rec.Label,
		AgentID:       rec.AgentID,
		OriginChannel: rec.Channel,
		OriginChatID:  rec.ChatID,
		Status:        spawnStatusFromRecord(rec),
		Result:        rec.TerminalSummary,
		Created:       rec.CreatedAt,
	}
	out := spawnStatusFormatTask(task)
	if rec.DeliveryStatus != "" {
		out += fmt.Sprintf("\n  delivery: %s", rec.DeliveryStatus)
		if rec.DeliveryMode != "" {
			out += fmt.Sprintf(" (%s)", rec.DeliveryMode)
		}
	}
	if rec.DeliveryError != "" {
		out += fmt.Sprintf("\n  delivery_error: %s", truncateTaskText(rec.DeliveryError, 300))
	}
	return out
}

// spawnStatusFormatTask renders a single SubagentTask as a human-readable block.
func spawnStatusFormatTask(task *SubagentTask) string {
	var sb strings.Builder

	header := fmt.Sprintf("[%s] status=%s", task.ID, task.Status)
	if task.Label != "" {
		header += fmt.Sprintf("  label=%q", task.Label)
	}
	if task.AgentID != "" {
		header += fmt.Sprintf("  agent=%s", task.AgentID)
	}
	if task.Created > 0 {
		created := time.UnixMilli(task.Created).UTC().Format("2006-01-02 15:04:05 UTC")
		header += fmt.Sprintf("  created=%s", created)
	}
	sb.WriteString(header)

	if task.Task != "" {
		sb.WriteString(fmt.Sprintf("\n  task:   %s", task.Task))
	}
	if task.Result != "" {
		result := task.Result
		const maxResultLen = 300
		runes := []rune(result)
		if len(runes) > maxResultLen {
			result = string(runes[:maxResultLen]) + "…"
		}
		sb.WriteString(fmt.Sprintf("\n  result: %s", result))
	}

	return sb.String()
}
