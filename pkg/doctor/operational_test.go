package doctor

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/tasks"
)

func writeOperationalJSON(t *testing.T, path string, value any) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func findingsHaveID(findings []Finding, id string) bool {
	for _, finding := range findings {
		if finding.ID == id {
			return true
		}
	}
	return false
}

func TestOperationalTaskThresholdsAndRedaction(t *testing.T) {
	workspace := t.TempDir()
	now := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	old := now.Add(-time.Hour).UnixMilli()
	recent := now.Add(-time.Minute).UnixMilli()
	snapshot := tasks.Snapshot{Tasks: []tasks.Record{
		{TaskID: "secret-task-id", Task: "secret task text", Status: tasks.StatusRunning, LastEventAt: old},
		{TaskID: "delivery-id", Status: tasks.StatusSucceeded, DeliveryStatus: tasks.DeliveryPending, EndedAt: old},
		{
			TaskID: "failed-id", Status: tasks.StatusFailed, DeliveryStatus: tasks.DeliveryFailed, EndedAt: recent,
			Error: "secret failure",
		},
	}}
	path := filepath.Join(workspace, "state", "task_registry.json")
	writeOperationalJSON(t, path, snapshot)
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = workspace
	findings := runOperationalChecks(cfg, Options{Now: now})
	for _, id := range []string{
		CheckTaskStaleActive, CheckDeliveryPendingTerminal, CheckTaskRecentFailure, CheckDeliveryRecentFailure,
	} {
		if !findingsHaveID(findings, id) {
			t.Fatalf("missing %s: %+v", id, findings)
		}
	}
	encoded, err := json.Marshal(findings)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"secret-task-id", "secret task text", "secret failure", workspace} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("finding leaked %q: %s", secret, encoded)
		}
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if before.ModTime() != after.ModTime() || before.Size() != after.Size() {
		t.Fatal("operational audit mutated task registry")
	}
}

func TestOperationalAbsentAndHistoricalStateAreClean(t *testing.T) {
	workspace := t.TempDir()
	now := time.Now().UTC()
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = workspace
	if findings := runOperationalChecks(cfg, Options{Now: now}); len(findings) != 0 {
		t.Fatalf("absent state findings: %+v", findings)
	}
	writeOperationalJSON(
		t,
		filepath.Join(workspace, "state", "task_registry.json"),
		tasks.Snapshot{Tasks: []tasks.Record{
			{
				Status:         tasks.StatusFailed,
				DeliveryStatus: tasks.DeliveryFailed,
				EndedAt:        now.Add(-48 * time.Hour).UnixMilli(),
			},
		}},
	)
	if findings := runOperationalChecks(cfg, Options{Now: now}); len(findings) != 0 {
		t.Fatalf("historical state findings: %+v", findings)
	}
}

func TestOperationalCorruptStateAndWorkspaceDeduplication(t *testing.T) {
	workspace := t.TempDir()
	path := filepath.Join(workspace, "state", "task_registry.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"task":"sensitive"`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = workspace
	cfg.Agents.List = []config.AgentConfig{{ID: "same", Workspace: workspace}}
	findings := runOperationalChecks(cfg, Options{Now: time.Now()})
	if len(findings) != 1 || findings[0].ID != CheckOperationalStateUnreadable {
		t.Fatalf("findings = %+v", findings)
	}
}

func TestOperationalRestartResidue(t *testing.T) {
	workspace := t.TempDir()
	now := time.Now().UTC()
	writeOperationalJSON(t, filepath.Join(workspace, "state", "gateway-restart", "restart-sentinel.json"),
		handoffSentinel{Kind: "restart", Status: "running", UpdatedAt: now.Add(-time.Hour)})
	writeOperationalJSON(t, filepath.Join(workspace, "state", "gateway-deploy", "deploy-sentinel.json"),
		handoffSentinel{Kind: "deploy", Status: "failed", UpdatedAt: now.Add(-time.Hour)})
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = workspace
	findings := runOperationalChecks(cfg, Options{Now: now})
	for _, id := range []string{CheckRestartReconciliation, CheckHandoffContinuation, CheckHandoffFailure} {
		if !findingsHaveID(findings, id) {
			t.Fatalf("missing %s: %+v", id, findings)
		}
	}
}

func TestOperationalHandoffsUseDefaultWorkspace(t *testing.T) {
	defaultWorkspace := filepath.Join(t.TempDir(), "z-default")
	agentWorkspace := filepath.Join(t.TempDir(), "a-agent")
	now := time.Now().UTC()
	writeOperationalJSON(t, filepath.Join(defaultWorkspace, "state", "gateway-restart", "restart-sentinel.json"),
		handoffSentinel{Kind: "restart", Status: "running", UpdatedAt: now.Add(-time.Hour)})
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = defaultWorkspace
	cfg.Agents.List = []config.AgentConfig{{ID: "agent", Workspace: agentWorkspace}}
	findings := runOperationalChecks(cfg, Options{Now: now})
	if !findingsHaveID(findings, CheckRestartReconciliation) {
		t.Fatalf("default workspace handoff was not audited: %+v", findings)
	}
}

func TestOperationalAuditsInheritedNamedAgentWorkspace(t *testing.T) {
	root := t.TempDir()
	defaultWorkspace := filepath.Join(root, "workspace")
	namedWorkspace := filepath.Join(root, "workspace-coding")
	now := time.Now().UTC()
	writeOperationalJSON(
		t,
		filepath.Join(namedWorkspace, "state", "task_registry.json"),
		tasks.Snapshot{Tasks: []tasks.Record{{
			Status: tasks.StatusRunning, LastEventAt: now.Add(-time.Hour).UnixMilli(),
		}}},
	)
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = defaultWorkspace
	cfg.Agents.List = []config.AgentConfig{{ID: "Coding"}}
	findings := runOperationalChecks(cfg, Options{Now: now})
	if !findingsHaveID(findings, CheckTaskStaleActive) {
		t.Fatalf("inherited named-agent workspace was not audited: %+v", findings)
	}
}
