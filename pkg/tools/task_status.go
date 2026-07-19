package tools

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	taskregistry "github.com/sipeed/picoclaw/pkg/tasks"
)

const taskStatusActiveStaleAfter = 30 * time.Minute

const (
	defaultTaskStatusListLimit = 12
	maxTaskStatusListLimit     = 25
)

// TaskStatusTool reports durable runtime task/run records across spawn,
// delegate, cron, and future background runtimes.
type TaskStatusTool struct {
	registry *taskregistry.Registry
}

func NewTaskStatusTool(registry *taskregistry.Registry) *TaskStatusTool {
	return &TaskStatusTool{registry: registry}
}

func (t *TaskStatusTool) Name() string {
	return "task_status"
}

func (t *TaskStatusTool) Description() string {
	return "Get durable runtime task status for spawn/delegate/cron/subtask runs. " +
		"Prefer this for general task history, completed task checks, and after service restarts. " +
		"Use this instead of spawn_status when the task may have used delegate or another child-run mechanism. " +
		"Results are scoped to the current conversation's channel/chat when available. " +
		"Without task_id, returns a compact list of the most recent tasks; use task_id for a full task record."
}

func (t *TaskStatusTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"task_id": map[string]any{
				"type":        "string",
				"description": "Optional durable task ID, e.g. subagent-1 or delegate-...",
			},
			"task_kind": map[string]any{
				"type":        "string",
				"description": "Optional task kind filter, e.g. spawn or delegate.",
			},
			"limit": map[string]any{
				"type":        "integer",
				"minimum":     1,
				"maximum":     maxTaskStatusListLimit,
				"description": "Maximum recent tasks to return in list mode (default 12, maximum 25). Ignored with task_id.",
			},
			"include_events": map[string]any{
				"type":        "boolean",
				"description": "Include typed task event details. With task_id, shows that task's event stream. With list output, shows recent events per visible task.",
			},
			"include_deliverable": map[string]any{
				"type":        "boolean",
				"description": "Return the complete durable deliverable text. Requires an exact task_id. Use this to recover or present a completed task's full result.",
			},
		},
		"required": []string{},
	}
}

func (t *TaskStatusTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t == nil || t.registry == nil {
		return ErrorResult("task registry not configured")
	}
	if _, err := t.registry.MarkStaleActiveLost(
		taskStatusActiveStaleAfter,
		"active task did not report progress before task_status stale timeout",
	); err != nil {
		return ErrorResult(fmt.Sprintf("failed to reconcile stale active tasks: %v", err)).WithError(err)
	}
	taskID, err := optionalTaskStatusStringArg(args, "task_id")
	if err != nil {
		return ErrorResult(err.Error())
	}
	taskKind, err := optionalTaskStatusStringArg(args, "task_kind")
	if err != nil {
		return ErrorResult(err.Error())
	}
	includeEvents, err := optionalTaskStatusBoolArg(args, "include_events")
	if err != nil {
		return ErrorResult(err.Error())
	}
	includeDeliverable, err := optionalTaskStatusBoolArg(args, "include_deliverable")
	if err != nil {
		return ErrorResult(err.Error())
	}
	if includeDeliverable && taskID == "" {
		return ErrorResult("include_deliverable requires task_id")
	}
	callerChannel := ToolChannel(ctx)
	callerChatID := ToolChatID(ctx)
	callerTopicID := ToolTopicID(ctx)

	if taskID != "" {
		rec, ok := t.registry.Get(taskID)
		if !ok || !taskRecordVisibleToCaller(rec, callerChannel, callerChatID, callerTopicID) {
			return ErrorResult(fmt.Sprintf("No task found with task ID: %s", taskID))
		}
		out := formatTaskRecord(rec)
		if includeDeliverable {
			out += formatCompleteTaskDeliverable(rec)
		}
		if includeEvents {
			out = out + "\n" + formatTaskEvents(t.registry.ListEvents(taskID))
		}
		return NewToolResult(out)
	}
	limit, err := optionalTaskStatusLimitArg(args)
	if err != nil {
		return ErrorResult(err.Error())
	}

	records := t.registry.List()
	filtered := make([]taskregistry.Record, 0, len(records))
	for _, rec := range records {
		if taskKind != "" && rec.TaskKind != taskKind {
			continue
		}
		if !taskRecordVisibleToCaller(rec, callerChannel, callerChatID, callerTopicID) {
			continue
		}
		filtered = append(filtered, rec)
	}
	if len(filtered) == 0 {
		if taskKind != "" {
			return NewToolResult(fmt.Sprintf("No visible tasks found for task_kind %q.", taskKind))
		}
		return NewToolResult("No visible durable tasks are registered for this conversation.")
	}

	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].CreatedAt != filtered[j].CreatedAt {
			return filtered[i].CreatedAt > filtered[j].CreatedAt
		}
		return filtered[i].TaskID > filtered[j].TaskID
	})

	counts := map[taskregistry.Status]int{}
	for _, rec := range filtered {
		counts[rec.Status]++
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Task status report (%d total):\n", len(filtered)))
	for _, status := range []taskregistry.Status{
		taskregistry.StatusQueued,
		taskregistry.StatusRunning,
		taskregistry.StatusWaitingForInput,
		taskregistry.StatusSucceeded,
		taskregistry.StatusFailed,
		taskregistry.StatusTimedOut,
		taskregistry.StatusCancelled,
		taskregistry.StatusLost,
	} {
		if n := counts[status]; n > 0 {
			sb.WriteString(fmt.Sprintf("  %-10s %d\n", status+":", n))
		}
	}
	sb.WriteString("\n")
	visible := filtered
	if len(visible) > limit {
		visible = visible[:limit]
	}
	for _, rec := range visible {
		sb.WriteString(formatTaskListRecord(rec))
		if includeEvents {
			sb.WriteString("\n")
			sb.WriteString(formatRecentTaskEvents(t.registry.ListEvents(rec.TaskID), 3))
		}
		sb.WriteString("\n")
	}
	if omitted := len(filtered) - len(visible); omitted > 0 {
		sb.WriteString(fmt.Sprintf(
			"... %d older task(s) omitted. Use task_id for a full task record or limit to show more.\n",
			omitted,
		))
	}
	return NewToolResult(strings.TrimSpace(sb.String()))
}

func formatCompleteTaskDeliverable(rec taskregistry.Record) string {
	text := ""
	if rec.Deliverable != nil {
		text = rec.Deliverable.Text
	} else if rec.Completion != nil {
		text = rec.Completion.Text
	}
	if strings.TrimSpace(text) == "" {
		return "\n\nComplete deliverable: no durable text is available."
	}
	return "\n\nComplete deliverable:\n" + text
}

func optionalTaskStatusLimitArg(args map[string]any) (int, error) {
	const key = "limit"
	raw, ok := args[key]
	if !ok || raw == nil {
		return defaultTaskStatusListLimit, nil
	}

	var limit int
	switch value := raw.(type) {
	case int:
		limit = value
	case int64:
		limit = int(value)
	case float64:
		if value != float64(int(value)) {
			return 0, fmt.Errorf("%s must be an integer", key)
		}
		limit = int(value)
	default:
		return 0, fmt.Errorf("%s must be an integer", key)
	}
	if limit < 1 || limit > maxTaskStatusListLimit {
		return 0, fmt.Errorf("%s must be between 1 and %d", key, maxTaskStatusListLimit)
	}
	return limit, nil
}

func optionalTaskStatusBoolArg(args map[string]any, key string) (bool, error) {
	raw, ok := args[key]
	if !ok || raw == nil {
		return false, nil
	}
	value, ok := raw.(bool)
	if !ok {
		return false, fmt.Errorf("%s must be a boolean", key)
	}
	return value, nil
}

func optionalTaskStatusStringArg(args map[string]any, key string) (string, error) {
	raw, ok := args[key]
	if !ok || raw == nil {
		return "", nil
	}
	value, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("%s must be a string", key)
	}
	return strings.TrimSpace(value), nil
}

func taskRecordVisibleToCaller(rec taskregistry.Record, channel, chatID, topicID string) bool {
	if channel != "" && rec.Channel != "" && rec.Channel != channel {
		return false
	}
	if chatID != "" && rec.ChatID != "" && rec.ChatID != chatID {
		return false
	}
	if topicID != "" && rec.TopicID != "" && rec.TopicID != topicID {
		return false
	}
	return true
}

func formatTaskRecord(rec taskregistry.Record) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Task %s [%s/%s]\n", rec.TaskID, rec.Runtime, rec.TaskKind))
	sb.WriteString(fmt.Sprintf("  Status: %s\n", rec.Status))
	sb.WriteString(fmt.Sprintf("  Delivery: %s", rec.DeliveryStatus))
	if rec.DeliveryMode != "" {
		sb.WriteString(fmt.Sprintf(" (%s)", rec.DeliveryMode))
	}
	sb.WriteString("\n")
	if rec.LastCompletionID != "" {
		sb.WriteString(fmt.Sprintf("  Completion ID: %s\n", rec.LastCompletionID))
	}
	if rec.DeliveredAt > 0 {
		sb.WriteString(fmt.Sprintf("  Delivered: %s\n", formatTaskTime(rec.DeliveredAt)))
	}
	if rec.DeliveryError != "" {
		sb.WriteString(fmt.Sprintf("  Delivery error: %s\n", truncateTaskText(rec.DeliveryError, 500)))
	}
	if rec.AgentID != "" {
		sb.WriteString(fmt.Sprintf("  Agent: %s\n", rec.AgentID))
	}
	if rec.Channel != "" || rec.ChatID != "" || rec.TopicID != "" {
		sb.WriteString(fmt.Sprintf("  Scope: %s/%s", rec.Channel, rec.ChatID))
		if rec.TopicID != "" {
			sb.WriteString(fmt.Sprintf(" topic=%s", rec.TopicID))
		}
		sb.WriteString("\n")
	}
	if rec.CreatedAt > 0 {
		sb.WriteString(fmt.Sprintf("  Created: %s\n", formatTaskTime(rec.CreatedAt)))
	}
	if rec.EndedAt > 0 {
		sb.WriteString(fmt.Sprintf("  Ended: %s\n", formatTaskTime(rec.EndedAt)))
	}
	if rec.Task != "" {
		sb.WriteString(fmt.Sprintf("  Task: %s\n", truncateTaskText(rec.Task, 240)))
	}
	appendTaskInteractionStatus(&sb, rec, "  ")
	if rec.TerminalSummary != "" {
		sb.WriteString(fmt.Sprintf("  Result: %s\n", truncateTaskText(rec.TerminalSummary, 500)))
	}
	if rec.Error != "" {
		sb.WriteString(fmt.Sprintf("  Error: %s\n", truncateTaskText(rec.Error, 500)))
	}
	if rec.Deliverable != nil {
		sb.WriteString(
			fmt.Sprintf(
				"  Deliverable: text=%t artifacts=%d report=%t\n",
				rec.Deliverable.Text != "",
				len(rec.Deliverable.Artifacts),
				rec.Deliverable.Report != nil,
			),
		)
		if rec.Deliverable.Report != nil {
			sb.WriteString(formatDeliverableReport(rec.Deliverable.Report))
		}
	}
	if rec.Completion != nil && rec.Deliverable == nil {
		sb.WriteString(
			fmt.Sprintf(
				"  Legacy completion: text=%t media=%d\n",
				rec.Completion.Text != "",
				len(rec.Completion.Media),
			),
		)
	}
	return strings.TrimRight(sb.String(), "\n")
}

func formatTaskListRecord(rec taskregistry.Record) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(
		"Task %s [%s/%s] status=%s delivery=%s",
		rec.TaskID,
		rec.Runtime,
		rec.TaskKind,
		rec.Status,
		rec.DeliveryStatus,
	))
	if rec.AgentID != "" {
		sb.WriteString(fmt.Sprintf(" agent=%s", rec.AgentID))
	}
	if rec.CreatedAt > 0 {
		sb.WriteString(fmt.Sprintf(" created=%s", formatTaskTime(rec.CreatedAt)))
	}
	if rec.Task != "" {
		sb.WriteString(fmt.Sprintf("\n  Task: %s", truncateTaskText(rec.Task, 160)))
	}
	appendTaskInteractionStatus(&sb, rec, "\n  ")
	if rec.TerminalSummary != "" {
		sb.WriteString(fmt.Sprintf("\n  Result: %s", truncateTaskText(rec.TerminalSummary, 240)))
	} else if rec.Error != "" {
		sb.WriteString(fmt.Sprintf("\n  Error: %s", truncateTaskText(rec.Error, 240)))
	}
	if rec.Deliverable != nil {
		sb.WriteString(
			fmt.Sprintf(
				"\n  Deliverable: text=%t artifacts=%d report=%t",
				rec.Deliverable.Text != "",
				len(rec.Deliverable.Artifacts),
				rec.Deliverable.Report != nil,
			),
		)
	}
	return sb.String()
}

func appendTaskInteractionStatus(
	sb *strings.Builder,
	rec taskregistry.Record,
	prefix string,
) {
	if sb == nil || rec.Status != taskregistry.StatusWaitingForInput {
		return
	}
	requestID := strings.TrimSpace(rec.InteractionShortID)
	if requestID == "" {
		requestID = "unknown"
	}
	sb.WriteString(fmt.Sprintf("%sInput required: request=%s", prefix, requestID))
	if summary := strings.TrimSpace(rec.InteractionSummary); summary != "" {
		sb.WriteString(" summary=" + truncateTaskText(summary, 240))
	}
	if prefix == "  " {
		sb.WriteString("\n")
	}
}

func formatDeliverableReport(report *taskregistry.DeliverableReport) string {
	if report == nil {
		return ""
	}
	var sb strings.Builder
	schema := strings.TrimSpace(report.SchemaVersion)
	if schema == "" {
		schema = "unknown"
	}
	sb.WriteString(fmt.Sprintf("  Report: %s", schema))
	if report.ReportID != "" {
		sb.WriteString(fmt.Sprintf(" id=%s", truncateTaskText(report.ReportID, 96)))
	}
	if report.ContentHash != "" {
		sb.WriteString(fmt.Sprintf(" hash=%s", truncateTaskText(report.ContentHash, 12)))
	}
	sb.WriteString("\n")
	if report.Summary != "" {
		sb.WriteString(fmt.Sprintf("    Summary: %s\n", truncateTaskText(report.Summary, 280)))
	}
	if status := report.Metadata["result_status"]; status != "" {
		sb.WriteString(fmt.Sprintf("    Status: %s\n", status))
	}
	if len(report.Claims) > 0 {
		sb.WriteString(fmt.Sprintf("    Claims: %d\n", len(report.Claims)))
		for i, claim := range report.Claims {
			if i >= 3 {
				sb.WriteString(fmt.Sprintf("      ...and %d more\n", len(report.Claims)-i))
				break
			}
			sb.WriteString(fmt.Sprintf("      - %s\n", formatReportClaim(claim)))
		}
	}
	if len(report.FieldDeltas) > 0 {
		sb.WriteString(fmt.Sprintf("    Field deltas: %d\n", len(report.FieldDeltas)))
	}
	return sb.String()
}

func formatReportClaim(claim taskregistry.ReportClaim) string {
	kind := strings.TrimSpace(claim.Kind)
	if kind == "" {
		kind = "claim"
	}
	text := truncateTaskText(claim.Text, 220)
	if claim.Confidence != "" {
		return fmt.Sprintf("%s [%s]: %s", kind, claim.Confidence, text)
	}
	return fmt.Sprintf("%s: %s", kind, text)
}

func formatTaskEvents(events []taskregistry.TaskEvent) string {
	if len(events) == 0 {
		return "Events: none"
	}
	var sb strings.Builder
	sb.WriteString("Events:\n")
	for _, evt := range events {
		sb.WriteString("  ")
		sb.WriteString(formatTaskEventLine(evt))
		sb.WriteString("\n")
	}
	return strings.TrimRight(sb.String(), "\n")
}

func formatRecentTaskEvents(events []taskregistry.TaskEvent, limit int) string {
	if len(events) == 0 {
		return "  Recent events: none"
	}
	if limit <= 0 || limit > len(events) {
		limit = len(events)
	}
	start := len(events) - limit
	var sb strings.Builder
	sb.WriteString("  Recent events:\n")
	for _, evt := range events[start:] {
		sb.WriteString("  ")
		sb.WriteString(formatTaskEventLine(evt))
		sb.WriteString("\n")
	}
	return strings.TrimRight(sb.String(), "\n")
}

func formatTaskEventLine(evt taskregistry.TaskEvent) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(
		"#%d %s runtime=%s producer=%s source=%s status=%s delivery=%s at=%s",
		evt.Seq,
		evt.Type,
		evt.Runtime,
		firstNonEmptyTaskStatus(evt.Producer, "unknown"),
		firstNonEmptyTaskStatus(evt.Source, "unknown"),
		evt.Status,
		evt.DeliveryStatus,
		formatTaskTime(evt.EmittedAt),
	))
	if payloadKind := strings.TrimSpace(evt.Payload["payload_kind"]); payloadKind != "" {
		sb.WriteString(fmt.Sprintf(" payload_kind=%s", payloadKind))
	}
	deliveryMode := firstNonEmptyTaskStatus(evt.Payload["delivery_mode"], evt.Payload["mode"])
	if deliveryMode != "" {
		sb.WriteString(fmt.Sprintf(" delivery_mode=%s", deliveryMode))
	}
	if completionID := strings.TrimSpace(evt.Payload["completion_id"]); completionID != "" {
		sb.WriteString(fmt.Sprintf(" completion_id=%s", completionID))
	}
	if evt.Fingerprint != "" {
		sb.WriteString(fmt.Sprintf(" fingerprint=%s", truncateTaskText(evt.Fingerprint, 12)))
	}
	if len(evt.Payload) > 0 {
		sb.WriteString(fmt.Sprintf(" payload=%s", formatTaskEventPayload(evt.Payload)))
	}
	return sb.String()
}

func formatTaskEventPayload(payload map[string]string) string {
	if len(payload) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(payload))
	for key := range payload {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%q", key, payload[key]))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func firstNonEmptyTaskStatus(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func formatTaskTime(ms int64) string {
	return time.UnixMilli(ms).Format(time.RFC3339)
}

func truncateTaskText(s string, limit int) string {
	s = strings.TrimSpace(s)
	if limit <= 0 || len(s) <= limit {
		return s
	}
	return s[:limit] + "..."
}
