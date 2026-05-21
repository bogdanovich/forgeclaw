package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/sipeed/picoclaw/pkg/config"
	taskregistry "github.com/sipeed/picoclaw/pkg/tasks"
)

const defaultTaskListLimit = 50
const maxTaskListLimit = 500

type taskListResponse struct {
	Workspace string                      `json:"workspace"`
	StorePath string                      `json:"store_path"`
	Count     int                         `json:"count"`
	Tasks     []taskregistry.Record       `json:"tasks"`
	Counts    map[taskregistry.Status]int `json:"counts"`
}

func (h *Handler) registerTaskRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/tasks", h.handleListTasks)
}

func (h *Handler) handleListTasks(w http.ResponseWriter, r *http.Request) {
	cfg, err := config.LoadConfig(h.configPath)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to load config: %v", err), http.StatusInternalServerError)
		return
	}

	limit, err := parseTaskListLimit(r.URL.Query().Get("limit"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	taskKind := strings.TrimSpace(r.URL.Query().Get("task_kind"))
	workspace := cfg.WorkspacePath()
	storePath := taskregistry.WorkspaceStorePath(workspace)
	registry := taskregistry.NewRegistry(storePath)
	if err := registry.LastLoadError(); err != nil {
		http.Error(w, fmt.Sprintf("Failed to load task registry: %v", err), http.StatusInternalServerError)
		return
	}

	records := registry.List()
	filtered := make([]taskregistry.Record, 0, len(records))
	counts := make(map[taskregistry.Status]int)
	for _, rec := range records {
		if taskKind != "" && rec.TaskKind != taskKind {
			continue
		}
		filtered = append(filtered, rec)
		counts[rec.Status]++
	}
	if limit > 0 && len(filtered) > limit {
		filtered = filtered[len(filtered)-limit:]
	}
	reverseTaskRecords(filtered)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(taskListResponse{
		Workspace: workspace,
		StorePath: storePath,
		Count:     len(filtered),
		Tasks:     filtered,
		Counts:    counts,
	}); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}
}

func parseTaskListLimit(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return defaultTaskListLimit, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 0 {
		return 0, fmt.Errorf("limit must be a non-negative integer")
	}
	if limit > maxTaskListLimit {
		return maxTaskListLimit, nil
	}
	return limit, nil
}

func reverseTaskRecords(records []taskregistry.Record) {
	for i, j := 0, len(records)-1; i < j; i, j = i+1, j-1 {
		records[i], records[j] = records[j], records[i]
	}
}
