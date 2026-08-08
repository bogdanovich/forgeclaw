package tools

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/providers"
	taskregistry "github.com/bogdanovich/mintclaw/pkg/tasks"
	toolshared "github.com/bogdanovich/mintclaw/pkg/tools/shared"
)

func NewSubagentManager(
	provider providers.LLMProvider,
	defaultModel, workspace string,
) *SubagentManager {
	return NewSubagentManagerWithRegistry(
		provider,
		defaultModel,
		workspace,
		taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace)),
	)
}

func TestSubagentManagersSharingRegistryAllocateDistinctTaskIDs(t *testing.T) {
	workspace := t.TempDir()
	registry := taskregistry.NewRegistry(
		taskregistry.WorkspaceStorePath(workspace),
	)
	first := NewSubagentManagerWithRegistry(
		&MockLLMProvider{}, "model", workspace, registry,
	)
	second := NewSubagentManagerWithRegistry(
		&MockLLMProvider{},
		"model",
		workspace+string(os.PathSeparator)+".",
		registry,
	)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := first.Spawn(
		ctx, "first", "", "main", "telegram", "chat",
		toolshared.AsyncDeliveryUserOnly, nil,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Spawn(
		ctx, "second", "", "main", "telegram", "chat",
		toolshared.AsyncDeliveryUserOnly, nil,
	); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		records := registry.List()
		if len(records) == 2 &&
			records[0].Status == taskregistry.StatusCancelled &&
			records[1].Status == taskregistry.StatusCancelled {
			if records[0].TaskID == records[1].TaskID {
				t.Fatalf("managers reused task ID %q", records[0].TaskID)
			}
			for _, record := range records {
				if !strings.HasPrefix(record.TaskID, "subagent-") ||
					record.GenerationID == "" {
					t.Fatalf("invalid durable task identity: %#v", record)
				}
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("shared registry tasks = %#v, want 2", records)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSubagentSpawnFailsBeforeLaunchWhenTaskCreateFails(t *testing.T) {
	root := t.TempDir()
	blocked := filepath.Join(root, "blocked")
	if err := os.WriteFile(blocked, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	registry := taskregistry.NewRegistry(filepath.Join(blocked, "tasks.json"))
	manager := NewSubagentManagerWithRegistry(
		&MockLLMProvider{}, "model", root, registry,
	)

	_, err := manager.Spawn(
		context.Background(),
		"must not launch",
		"",
		"main",
		"telegram",
		"chat",
		toolshared.AsyncDeliveryUserOnly,
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "persist spawned subagent") {
		t.Fatalf("Spawn() error = %v", err)
	}
	if tasks := manager.ListTaskCopies(); len(tasks) != 0 {
		t.Fatalf("failed durable create published in-memory tasks: %#v", tasks)
	}
}

func TestSubagentStatusUpdatePreservesDurableGeneration(t *testing.T) {
	registry := taskregistry.NewRegistry(filepath.Join(t.TempDir(), "tasks.json"))
	manager := NewSubagentManagerWithRegistry(
		&MockLLMProvider{}, "model", t.TempDir(), registry,
	)
	task := &SubagentTask{
		ID: "subagent-" + strings.Repeat("a", 36), Task: "test",
		Status: "running", Created: time.Now().UnixMilli(),
	}
	if err := manager.createTask(task); err != nil {
		t.Fatal(err)
	}
	created, _ := registry.Get(task.ID)
	task.Created = created.CreatedAt + int64(time.Hour/time.Millisecond)
	if err := registry.Update(task.ID, func(record *taskregistry.Record) {
		record.InteractionID = "interaction-1"
		record.Completion = &taskregistry.CompletionPayload{Text: "existing"}
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.recordTask(
		task,
		taskregistry.StatusSucceeded,
		taskregistry.DeliveryDelivered,
		"done",
	); err != nil {
		t.Fatal(err)
	}
	completed, _ := registry.Get(task.ID)
	if completed.GenerationID != created.GenerationID ||
		completed.CreatedAt != created.CreatedAt ||
		completed.StartedAt != created.StartedAt ||
		completed.Status != taskregistry.StatusSucceeded ||
		completed.TerminalSummary != "done" ||
		completed.InteractionID != "interaction-1" ||
		completed.Completion == nil ||
		completed.Completion.Text != "existing" {
		t.Fatalf("updated durable task = %#v, created = %#v", completed, created)
	}
}

func TestSubagentResultPersistsTerminalStateAndPayloadTogether(t *testing.T) {
	registry := taskregistry.NewRegistry(filepath.Join(t.TempDir(), "tasks.json"))
	manager := NewSubagentManagerWithRegistry(
		&MockLLMProvider{}, "model", t.TempDir(), registry,
	)
	task := &SubagentTask{
		ID: "subagent-" + strings.Repeat("b", 36), Task: "test",
		Status: "running", Created: time.Now().UnixMilli(),
	}
	if err := manager.createTask(task); err != nil {
		t.Fatal(err)
	}

	manager.recordTaskResult(task, &toolshared.ToolResult{
		ForLLM: "done",
		Deliverable: &toolshared.DeliverableResult{
			Text: "structured result",
		},
	})

	completed, ok := registry.Get(task.ID)
	if !ok {
		t.Fatal("completed task not found")
	}
	if completed.Status != taskregistry.StatusSucceeded ||
		!strings.Contains(completed.TerminalSummary, "done") ||
		completed.Deliverable == nil ||
		completed.Deliverable.Text != "structured result" {
		t.Fatalf("completed durable task = %#v", completed)
	}
	events := registry.ListEvents(task.ID)
	statusChanges := 0
	for _, event := range events {
		if event.Type == taskregistry.EventTaskStatusChanged {
			statusChanges++
		}
	}
	if statusChanges != 1 {
		t.Fatalf("terminal events = %#v", events)
	}
}
