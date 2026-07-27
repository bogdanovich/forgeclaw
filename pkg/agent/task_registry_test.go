package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/config"
	taskregistry "github.com/bogdanovich/mintclaw/pkg/tasks"
)

func TestTaskRegistryForWorkspaceCanonicalizesAliases(t *testing.T) {
	workspace := t.TempDir()
	parent := t.TempDir()
	symlink := filepath.Join(parent, "workspace-link")
	if err := os.Symlink(workspace, symlink); err != nil {
		t.Skipf("create workspace symlink: %v", err)
	}
	current, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd() error = %v", err)
	}
	relative, err := filepath.Rel(current, workspace)
	if err != nil {
		t.Fatalf("filepath.Rel() error = %v", err)
	}
	al := &AgentLoop{}

	registry := al.taskRegistryForWorkspace(workspace)
	for name, alias := range map[string]string{
		"dot":      workspace + string(os.PathSeparator) + ".",
		"relative": relative,
		"symlink":  symlink,
	} {
		if aliased := al.taskRegistryForWorkspace(alias); aliased != registry {
			t.Fatalf("%s workspace alias created a distinct task registry", name)
		}
	}

	for _, taskID := range []string{"task-real", "task-symlink"} {
		if err := registry.Create(taskregistry.Record{TaskID: taskID}); err != nil {
			t.Fatalf("Create(%q) error = %v", taskID, err)
		}
	}
	reloaded := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(workspace))
	for _, taskID := range []string{"task-real", "task-symlink"} {
		if _, ok := reloaded.Get(taskID); !ok {
			t.Fatalf("reloaded registry lost %q", taskID)
		}
	}
}

func TestTaskRegistryForWorkspaceUsesConfiguredRetentionLimits(t *testing.T) {
	workspace := t.TempDir()
	al := &AgentLoop{cfg: &config.Config{Tasks: config.TaskConfig{
		TerminalRetentionHours: 12,
		MaxRecords:             12,
		MaxEvents:              34,
		MaxSnapshotBytes:       5678,
	}}}
	stats := al.taskRegistryForWorkspace(workspace).Stats()
	if stats.TerminalRetention != 12*time.Hour || stats.MaxRecords != 12 || stats.MaxEvents != 34 ||
		stats.MaxSnapshotBytes != 5678 {
		t.Fatalf("unexpected registry stats: %#v", stats)
	}
}

func TestTaskRegistryForWorkspace_ReconcilesRestoredActiveTasksAsLost(t *testing.T) {
	workspace := t.TempDir()
	store := taskregistry.WorkspaceStorePath(workspace)
	registry := taskregistry.NewRegistry(store)
	if err := registry.Upsert(taskregistry.Record{
		TaskID:         "subagent-1",
		Runtime:        taskregistry.RuntimeSubagent,
		TaskKind:       "spawn",
		Task:           "old background task",
		Status:         taskregistry.StatusRunning,
		DeliveryStatus: taskregistry.DeliveryPending,
		CreatedAt:      time.Now().Add(-time.Hour).UnixMilli(),
		LastEventAt:    time.Now().Add(-time.Hour).UnixMilli(),
	}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	al := &AgentLoop{}
	reconciled := al.taskRegistryForWorkspace(workspace)
	rec, ok := reconciled.Get("subagent-1")
	if !ok {
		t.Fatal("expected task")
	}
	if rec.Status != taskregistry.StatusLost {
		t.Fatalf("Status = %q, want %q", rec.Status, taskregistry.StatusLost)
	}
	if rec.DeliveryStatus != taskregistry.DeliveryNotApplicable {
		t.Fatalf(
			"DeliveryStatus = %q, want %q",
			rec.DeliveryStatus,
			taskregistry.DeliveryNotApplicable,
		)
	}
	if rec.EndedAt == 0 {
		t.Fatal("expected EndedAt to be stamped")
	}
	if !strings.Contains(rec.Error, "previous runtime owner") {
		t.Fatalf("Error = %q, want previous runtime owner note", rec.Error)
	}
}

func TestTaskRegistryForWorkspace_ReconcilesRecentRestoredActiveTaskAsLost(t *testing.T) {
	workspace := t.TempDir()
	store := taskregistry.WorkspaceStorePath(workspace)
	registry := taskregistry.NewRegistry(store)
	now := time.Now().UnixMilli()
	if err := registry.Upsert(taskregistry.Record{
		TaskID:         "delegate-1",
		Runtime:        taskregistry.RuntimeDelegate,
		TaskKind:       "delegate",
		Task:           "recent delegate task",
		Status:         taskregistry.StatusRunning,
		DeliveryStatus: taskregistry.DeliveryPending,
		CreatedAt:      now,
		LastEventAt:    now,
	}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	al := &AgentLoop{}
	reconciled := al.taskRegistryForWorkspace(workspace)
	rec, ok := reconciled.Get("delegate-1")
	if !ok {
		t.Fatal("expected task")
	}
	if rec.Status != taskregistry.StatusLost {
		t.Fatalf("Status = %q, want %q", rec.Status, taskregistry.StatusLost)
	}
	if rec.DeliveryStatus != taskregistry.DeliveryNotApplicable {
		t.Fatalf(
			"DeliveryStatus = %q, want %q",
			rec.DeliveryStatus,
			taskregistry.DeliveryNotApplicable,
		)
	}
}
