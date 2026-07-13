package tools

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	taskregistry "github.com/sipeed/picoclaw/pkg/tasks"
)

func TestTaskStatusTool_ListsVisibleRecords(t *testing.T) {
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(t.TempDir()))
	now := time.Now().UnixMilli()
	if err := registry.Upsert(taskregistry.Record{
		TaskID:         "delegate-1",
		Runtime:        taskregistry.RuntimeDelegate,
		TaskKind:       "delegate",
		ParentTaskID:   "root-1",
		Channel:        "telegram",
		ChatID:         "chat-1",
		TopicID:        "topic-1",
		AgentID:        "media",
		Task:           "download reel",
		Status:         taskregistry.StatusSucceeded,
		DeliveryStatus: taskregistry.DeliverySessionQueued,
		DeliveryMode:   string(AsyncDeliveryParentOnly),
		CreatedAt:      now,
		StartedAt:      now,
		EndedAt:        now,
		Deliverable: &taskregistry.DeliverablePayload{
			Text: "video downloaded",
			Artifacts: []taskregistry.DeliverableItem{{
				Ref:  "media://video",
				Kind: "video",
			}},
		},
	}); err != nil {
		t.Fatalf("Upsert(delegate) error = %v", err)
	}
	if err := registry.Upsert(taskregistry.Record{
		TaskID:         "delegate-other",
		Runtime:        taskregistry.RuntimeDelegate,
		TaskKind:       "delegate",
		Channel:        "telegram",
		ChatID:         "chat-2",
		Task:           "other chat",
		Status:         taskregistry.StatusSucceeded,
		DeliveryStatus: taskregistry.DeliverySessionQueued,
		CreatedAt:      now,
	}); err != nil {
		t.Fatalf("Upsert(other) error = %v", err)
	}

	tool := NewTaskStatusTool(registry)
	result := tool.Execute(WithToolContext(context.Background(), "telegram", "chat-1"), nil)

	if result.IsError {
		t.Fatalf("expected success, got error: %s", result.ForLLM)
	}
	for _, want := range []string{
		"Task status report (1 total)",
		"delegate-1",
		"delegate/delegate",
		"agent=media",
		"Deliverable: text=true artifacts=1 report=true",
		"Task: download reel",
	} {
		if !strings.Contains(result.ForLLM, want) {
			t.Fatalf("result missing %q:\n%s", want, result.ForLLM)
		}
	}
	if strings.Contains(result.ForLLM, "delegate-other") {
		t.Fatalf("result leaked other chat task:\n%s", result.ForLLM)
	}
}

func TestTaskStatusTool_ListReturnsNewestRecordsWithinDefaultLimit(t *testing.T) {
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(t.TempDir()))
	now := time.Now().UnixMilli()
	for i := 1; i <= defaultTaskStatusListLimit+2; i++ {
		if err := registry.Upsert(taskregistry.Record{
			TaskID:         fmt.Sprintf("delegate-%02d", i),
			Runtime:        taskregistry.RuntimeDelegate,
			TaskKind:       "delegate",
			Channel:        "telegram",
			ChatID:         "chat-1",
			Task:           strings.Repeat("x", 500),
			Status:         taskregistry.StatusSucceeded,
			DeliveryStatus: taskregistry.DeliveryDelivered,
			CreatedAt:      now + int64(i),
		}); err != nil {
			t.Fatalf("Upsert(delegate-%02d) error = %v", i, err)
		}
	}

	tool := NewTaskStatusTool(registry)
	result := tool.Execute(WithToolContext(context.Background(), "telegram", "chat-1"), nil)
	if result.IsError {
		t.Fatalf("expected success, got error: %s", result.ForLLM)
	}
	if strings.Contains(result.ForLLM, "delegate-01") || strings.Contains(result.ForLLM, "delegate-02") {
		t.Fatalf("expected oldest records to be omitted:\n%s", result.ForLLM)
	}
	if !strings.Contains(result.ForLLM, "delegate-14") {
		t.Fatalf("expected newest record:\n%s", result.ForLLM)
	}
	if !strings.Contains(result.ForLLM, "... 2 older task(s) omitted.") {
		t.Fatalf("expected omission notice:\n%s", result.ForLLM)
	}
	if strings.Count(result.ForLLM, "Task delegate-") != defaultTaskStatusListLimit {
		t.Fatalf("visible record count = %d, want %d:\n%s", strings.Count(result.ForLLM, "Task delegate-"), defaultTaskStatusListLimit, result.ForLLM)
	}
	if strings.Contains(result.ForLLM, strings.Repeat("x", 241)) {
		t.Fatalf("list mode included an unbounded task payload:\n%s", result.ForLLM)
	}
}

func TestTaskStatusTool_ListLimitValidation(t *testing.T) {
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(t.TempDir()))
	tool := NewTaskStatusTool(registry)

	for _, args := range []map[string]any{
		{"limit": 0},
		{"limit": maxTaskStatusListLimit + 1},
		{"limit": 1.5},
		{"limit": "12"},
	} {
		result := tool.Execute(context.Background(), args)
		if !result.IsError {
			t.Fatalf("expected invalid limit %v to fail", args["limit"])
		}
	}
}

func TestTaskStatusTool_TaskID(t *testing.T) {
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(t.TempDir()))
	if err := registry.Upsert(taskregistry.Record{
		TaskID:         "subagent-1",
		Runtime:        taskregistry.RuntimeSubagent,
		TaskKind:       "spawn",
		Task:           "background work",
		Status:         taskregistry.StatusRunning,
		DeliveryStatus: taskregistry.DeliveryPending,
	}); err != nil {
		t.Fatalf("Upsert(subagent) error = %v", err)
	}

	tool := NewTaskStatusTool(registry)
	result := tool.Execute(context.Background(), map[string]any{"task_id": "subagent-1"})

	if result.IsError {
		t.Fatalf("expected success, got error: %s", result.ForLLM)
	}
	if !strings.Contains(result.ForLLM, "Task subagent-1 [subagent/spawn]") {
		t.Fatalf("unexpected result:\n%s", result.ForLLM)
	}
}

func TestTaskStatusTool_TaskIDIncludesEvents(t *testing.T) {
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(t.TempDir()))
	now := time.Now().UnixMilli()
	if err := registry.Upsert(taskregistry.Record{
		TaskID:           "subagent-1",
		Runtime:          taskregistry.RuntimeSubagent,
		TaskKind:         "spawn",
		Task:             "background work",
		Status:           taskregistry.StatusRunning,
		DeliveryStatus:   taskregistry.DeliveryPending,
		DeliveryMode:     "user_only",
		LastCompletionID: "completion-1",
		DeliveredAt:      now,
	}); err != nil {
		t.Fatalf("Upsert(subagent) error = %v", err)
	}
	if err := registry.AppendEvent("subagent-1", taskregistry.EventTaskDeliveryDecision, map[string]string{
		"completion_id": "completion-1",
		"mode":          "user_only",
		"payload_kind":  "subagent_result",
	}); err != nil {
		t.Fatalf("AppendEvent(subagent) error = %v", err)
	}
	if err := registry.Update("subagent-1", func(rec *taskregistry.Record) {
		rec.Status = taskregistry.StatusSucceeded
		rec.DeliveryStatus = taskregistry.DeliveryDelivered
	}); err != nil {
		t.Fatalf("Update(subagent) error = %v", err)
	}

	tool := NewTaskStatusTool(registry)
	result := tool.Execute(context.Background(), map[string]any{
		"task_id":        "subagent-1",
		"include_events": true,
	})

	if result.IsError {
		t.Fatalf("expected success, got error: %s", result.ForLLM)
	}
	for _, want := range []string{
		"Completion ID: completion-1",
		"Delivered:",
		"Events:",
		"#1 task.upserted runtime=subagent producer=subagent source=task_registry",
		"#2 task.delivery_decision runtime=subagent producer=subagent source=task_registry",
		"payload_kind=subagent_result",
		"delivery_mode=user_only",
		"completion_id=completion-1",
		"#3 task.status_changed",
		"#4 task.delivery_changed",
		"payload={from=\"running\", to=\"succeeded\"}",
	} {
		if !strings.Contains(result.ForLLM, want) {
			t.Fatalf("result missing %q:\n%s", want, result.ForLLM)
		}
	}
}

func TestTaskStatusTool_ListIncludesRecentEvents(t *testing.T) {
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(t.TempDir()))
	if err := registry.Upsert(taskregistry.Record{
		TaskID:         "cron-job-1-run",
		Runtime:        taskregistry.RuntimeCron,
		TaskKind:       "cron",
		Task:           "send reminder",
		Status:         taskregistry.StatusRunning,
		DeliveryStatus: taskregistry.DeliveryPending,
		DeliveryMode:   "deliver_text",
	}); err != nil {
		t.Fatalf("Upsert(cron) error = %v", err)
	}
	if err := registry.AppendEvent("cron-job-1-run", taskregistry.EventTaskDeliveryDecision, map[string]string{
		"job_id":        "job-1",
		"payload_kind":  "deliver_text",
		"delivery_mode": "deliver_text",
	}); err != nil {
		t.Fatalf("AppendEvent(cron) error = %v", err)
	}

	tool := NewTaskStatusTool(registry)
	result := tool.Execute(context.Background(), map[string]any{"include_events": true})

	if result.IsError {
		t.Fatalf("expected success, got error: %s", result.ForLLM)
	}
	for _, want := range []string{
		"Recent events:",
		"task.delivery_decision runtime=cron producer=cron source=task_registry",
		"payload_kind=deliver_text",
		"delivery_mode=deliver_text",
	} {
		if !strings.Contains(result.ForLLM, want) {
			t.Fatalf("result missing %q:\n%s", want, result.ForLLM)
		}
	}
}

func TestTaskStatusTool_ListsSpawnAndDelegateRecords(t *testing.T) {
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(t.TempDir()))
	now := time.Now().UnixMilli()
	records := []taskregistry.Record{
		{
			TaskID:         "subagent-1",
			Runtime:        taskregistry.RuntimeSubagent,
			TaskKind:       "spawn",
			Channel:        "telegram",
			ChatID:         "chat-1",
			TopicID:        "topic-1",
			AgentID:        "research",
			Task:           "background research",
			Status:         taskregistry.StatusRunning,
			DeliveryStatus: taskregistry.DeliveryPending,
			CreatedAt:      now,
		},
		{
			TaskID:         "delegate-1",
			Runtime:        taskregistry.RuntimeDelegate,
			TaskKind:       "delegate",
			Channel:        "telegram",
			ChatID:         "chat-1",
			TopicID:        "topic-1",
			AgentID:        "media",
			Task:           "download media",
			Status:         taskregistry.StatusSucceeded,
			DeliveryStatus: taskregistry.DeliverySessionQueued,
			CreatedAt:      now + 1,
		},
	}
	for _, rec := range records {
		if err := registry.Upsert(rec); err != nil {
			t.Fatalf("Upsert(%s) error = %v", rec.TaskID, err)
		}
	}

	tool := NewTaskStatusTool(registry)
	ctx := WithToolTopicID(WithToolContext(context.Background(), "telegram", "chat-1"), "topic-1")
	result := tool.Execute(ctx, nil)

	if result.IsError {
		t.Fatalf("expected success, got error: %s", result.ForLLM)
	}
	for _, want := range []string{
		"Task status report (2 total)",
		"subagent-1",
		"subagent/spawn",
		"delegate-1",
		"delegate/delegate",
	} {
		if !strings.Contains(result.ForLLM, want) {
			t.Fatalf("result missing %q:\n%s", want, result.ForLLM)
		}
	}
}

func TestTaskStatusTool_TaskKindFilter(t *testing.T) {
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(t.TempDir()))
	for _, rec := range []taskregistry.Record{
		{
			TaskID:         "subagent-1",
			Runtime:        taskregistry.RuntimeSubagent,
			TaskKind:       "spawn",
			Channel:        "telegram",
			ChatID:         "chat-1",
			Status:         taskregistry.StatusRunning,
			DeliveryStatus: taskregistry.DeliveryPending,
		},
		{
			TaskID:         "delegate-1",
			Runtime:        taskregistry.RuntimeDelegate,
			TaskKind:       "delegate",
			Channel:        "telegram",
			ChatID:         "chat-1",
			Status:         taskregistry.StatusSucceeded,
			DeliveryStatus: taskregistry.DeliverySessionQueued,
		},
	} {
		if err := registry.Upsert(rec); err != nil {
			t.Fatalf("Upsert(%s) error = %v", rec.TaskID, err)
		}
	}

	tool := NewTaskStatusTool(registry)
	result := tool.Execute(WithToolContext(context.Background(), "telegram", "chat-1"), map[string]any{
		"task_kind": "delegate",
	})

	if result.IsError {
		t.Fatalf("expected success, got error: %s", result.ForLLM)
	}
	if !strings.Contains(result.ForLLM, "delegate-1") {
		t.Fatalf("expected delegate record:\n%s", result.ForLLM)
	}
	if strings.Contains(result.ForLLM, "subagent-1") {
		t.Fatalf("spawn record leaked through delegate filter:\n%s", result.ForLLM)
	}
}

func TestTaskStatusTool_TopicScoping(t *testing.T) {
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(t.TempDir()))
	for _, rec := range []taskregistry.Record{
		{
			TaskID:         "delegate-topic-1",
			Runtime:        taskregistry.RuntimeDelegate,
			TaskKind:       "delegate",
			Channel:        "telegram",
			ChatID:         "chat-1",
			TopicID:        "topic-1",
			Status:         taskregistry.StatusSucceeded,
			DeliveryStatus: taskregistry.DeliverySessionQueued,
		},
		{
			TaskID:         "delegate-topic-2",
			Runtime:        taskregistry.RuntimeDelegate,
			TaskKind:       "delegate",
			Channel:        "telegram",
			ChatID:         "chat-1",
			TopicID:        "topic-2",
			Status:         taskregistry.StatusSucceeded,
			DeliveryStatus: taskregistry.DeliverySessionQueued,
		},
	} {
		if err := registry.Upsert(rec); err != nil {
			t.Fatalf("Upsert(%s) error = %v", rec.TaskID, err)
		}
	}

	tool := NewTaskStatusTool(registry)
	ctx := WithToolTopicID(WithToolContext(context.Background(), "telegram", "chat-1"), "topic-1")
	result := tool.Execute(ctx, nil)

	if result.IsError {
		t.Fatalf("expected success, got error: %s", result.ForLLM)
	}
	if !strings.Contains(result.ForLLM, "delegate-topic-1") {
		t.Fatalf("expected topic-1 record:\n%s", result.ForLLM)
	}
	if strings.Contains(result.ForLLM, "delegate-topic-2") {
		t.Fatalf("topic-2 record leaked into topic-1 status:\n%s", result.ForLLM)
	}
}
