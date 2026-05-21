package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/config"
	taskregistry "github.com/sipeed/picoclaw/pkg/tasks"
)

func TestHandleListTasks(t *testing.T) {
	configPath, cleanup := setupOAuthTestEnv(t)
	defer cleanup()

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(cfg.WorkspacePath()))
	now := time.Now().UnixMilli()
	for _, rec := range []taskregistry.Record{
		{
			TaskID:         "subagent-1",
			Runtime:        taskregistry.RuntimeSubagent,
			TaskKind:       "spawn",
			Task:           "background research",
			Status:         taskregistry.StatusRunning,
			DeliveryStatus: taskregistry.DeliveryPending,
			CreatedAt:      now,
		},
		{
			TaskID:         "delegate-1",
			Runtime:        taskregistry.RuntimeDelegate,
			TaskKind:       "delegate",
			Task:           "download media",
			Status:         taskregistry.StatusSucceeded,
			DeliveryStatus: taskregistry.DeliverySessionQueued,
			CreatedAt:      now + 1,
		},
	} {
		if err := registry.Upsert(rec); err != nil {
			t.Fatalf("Upsert(%s) error = %v", rec.TaskID, err)
		}
	}

	h := NewHandler(configPath)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/tasks", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var resp taskListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if resp.Workspace != cfg.WorkspacePath() {
		t.Fatalf("workspace = %q, want %q", resp.Workspace, cfg.WorkspacePath())
	}
	if resp.Count != 2 || len(resp.Tasks) != 2 {
		t.Fatalf("count=%d len=%d, want 2", resp.Count, len(resp.Tasks))
	}
	if resp.Tasks[0].TaskID != "delegate-1" || resp.Tasks[1].TaskID != "subagent-1" {
		t.Fatalf("tasks order = %#v, want newest first", resp.Tasks)
	}
	if resp.Counts[taskregistry.StatusSucceeded] != 1 || resp.Counts[taskregistry.StatusRunning] != 1 {
		t.Fatalf("counts = %#v", resp.Counts)
	}
}

func TestHandleListTasksFiltersAndLimits(t *testing.T) {
	configPath, cleanup := setupOAuthTestEnv(t)
	defer cleanup()

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	registry := taskregistry.NewRegistry(taskregistry.WorkspaceStorePath(cfg.WorkspacePath()))
	now := time.Now().UnixMilli()
	for _, rec := range []taskregistry.Record{
		{
			TaskID:         "delegate-1",
			Runtime:        taskregistry.RuntimeDelegate,
			TaskKind:       "delegate",
			Status:         taskregistry.StatusSucceeded,
			DeliveryStatus: taskregistry.DeliverySessionQueued,
			CreatedAt:      now,
		},
		{
			TaskID:         "subagent-1",
			Runtime:        taskregistry.RuntimeSubagent,
			TaskKind:       "spawn",
			Status:         taskregistry.StatusRunning,
			DeliveryStatus: taskregistry.DeliveryPending,
			CreatedAt:      now + 1,
		},
		{
			TaskID:         "delegate-2",
			Runtime:        taskregistry.RuntimeDelegate,
			TaskKind:       "delegate",
			Status:         taskregistry.StatusFailed,
			DeliveryStatus: taskregistry.DeliveryFailed,
			CreatedAt:      now + 2,
		},
	} {
		if err := registry.Upsert(rec); err != nil {
			t.Fatalf("Upsert(%s) error = %v", rec.TaskID, err)
		}
	}

	h := NewHandler(configPath)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/tasks?task_kind=delegate&limit=1", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var resp taskListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if resp.Count != 1 || len(resp.Tasks) != 1 || resp.Tasks[0].TaskID != "delegate-2" {
		t.Fatalf("response = %#v, want latest delegate only", resp)
	}
	if resp.Counts[taskregistry.StatusSucceeded] != 1 || resp.Counts[taskregistry.StatusFailed] != 1 {
		t.Fatalf("counts should cover filtered records before limit, got %#v", resp.Counts)
	}
}

func TestHandleListTasksRejectsInvalidLimit(t *testing.T) {
	configPath, cleanup := setupOAuthTestEnv(t)
	defer cleanup()

	h := NewHandler(configPath)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/tasks?limit=-1", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}
