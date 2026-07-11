package state

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestAtomicSave(t *testing.T) {
	// Create temp workspace
	tmpDir, err := os.MkdirTemp("", "state-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sm := NewManager(tmpDir)

	// Test SetLastChannel
	err = sm.SetLastChannel("test-channel")
	if err != nil {
		t.Fatalf("SetLastChannel failed: %v", err)
	}

	// Verify the channel was saved
	lastChannel := sm.GetLastChannel()
	if lastChannel != "test-channel" {
		t.Errorf("Expected channel 'test-channel', got '%s'", lastChannel)
	}

	// Verify timestamp was updated
	if sm.GetTimestamp().IsZero() {
		t.Error("Expected timestamp to be updated")
	}

	// Verify state file exists
	stateFile := filepath.Join(tmpDir, "state", "state.json")
	if _, err := os.Stat(stateFile); os.IsNotExist(err) {
		t.Error("Expected state file to exist")
	}

	// Create a new manager to verify persistence
	sm2 := NewManager(tmpDir)
	if sm2.GetLastChannel() != "test-channel" {
		t.Errorf("Expected persistent channel 'test-channel', got '%s'", sm2.GetLastChannel())
	}
}

func TestSetLastChatID(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "state-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sm := NewManager(tmpDir)

	// Test SetLastChatID
	err = sm.SetLastChatID("test-chat-id")
	if err != nil {
		t.Fatalf("SetLastChatID failed: %v", err)
	}

	// Verify the chat ID was saved
	lastChatID := sm.GetLastChatID()
	if lastChatID != "test-chat-id" {
		t.Errorf("Expected chat ID 'test-chat-id', got '%s'", lastChatID)
	}

	// Verify timestamp was updated
	if sm.GetTimestamp().IsZero() {
		t.Error("Expected timestamp to be updated")
	}

	// Create a new manager to verify persistence
	sm2 := NewManager(tmpDir)
	if sm2.GetLastChatID() != "test-chat-id" {
		t.Errorf("Expected persistent chat ID 'test-chat-id', got '%s'", sm2.GetLastChatID())
	}
}

func TestAtomicity_NoCorruptionOnInterrupt(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "state-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sm := NewManager(tmpDir)

	// Write initial state
	err = sm.SetLastChannel("initial-channel")
	if err != nil {
		t.Fatalf("SetLastChannel failed: %v", err)
	}

	// Simulate a crash scenario by manually creating a corrupted temp file
	tempFile := filepath.Join(tmpDir, "state", "state.json.tmp")
	err = os.WriteFile(tempFile, []byte("corrupted data"), 0o644)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	// Verify that the original state is still intact
	lastChannel := sm.GetLastChannel()
	if lastChannel != "initial-channel" {
		t.Errorf("Expected channel 'initial-channel' after corrupted temp file, got '%s'", lastChannel)
	}

	// Clean up the temp file manually
	os.Remove(tempFile)

	// Now do a proper save
	err = sm.SetLastChannel("new-channel")
	if err != nil {
		t.Fatalf("SetLastChannel failed: %v", err)
	}

	// Verify the new state was saved
	if sm.GetLastChannel() != "new-channel" {
		t.Errorf("Expected channel 'new-channel', got '%s'", sm.GetLastChannel())
	}
}

func TestConcurrentAccess(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "state-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sm := NewManager(tmpDir)

	// Test concurrent writes
	done := make(chan bool, 10)
	for i := range 10 {
		go func(idx int) {
			channel := fmt.Sprintf("channel-%d", idx)
			sm.SetLastChannel(channel)
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for range 10 {
		<-done
	}

	// Verify the final state is consistent
	lastChannel := sm.GetLastChannel()
	if lastChannel == "" {
		t.Error("Expected non-empty channel after concurrent writes")
	}

	// Verify state file is valid JSON
	stateFile := filepath.Join(tmpDir, "state", "state.json")
	data, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("Failed to read state file: %v", err)
	}

	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		t.Errorf("State file contains invalid JSON: %v", err)
	}
}

func TestNewManager_ExistingState(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "state-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create initial state
	sm1 := NewManager(tmpDir)
	sm1.SetLastChannel("existing-channel")
	sm1.SetLastChatID("existing-chat-id")

	// Create new manager with same workspace
	sm2 := NewManager(tmpDir)

	// Verify state was loaded
	if sm2.GetLastChannel() != "existing-channel" {
		t.Errorf("Expected channel 'existing-channel', got '%s'", sm2.GetLastChannel())
	}

	if sm2.GetLastChatID() != "existing-chat-id" {
		t.Errorf("Expected chat ID 'existing-chat-id', got '%s'", sm2.GetLastChatID())
	}
}

func TestNewManager_EmptyWorkspace(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "state-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sm := NewManager(tmpDir)

	// Verify default state
	if sm.GetLastChannel() != "" {
		t.Errorf("Expected empty channel, got '%s'", sm.GetLastChannel())
	}

	if sm.GetLastChatID() != "" {
		t.Errorf("Expected empty chat ID, got '%s'", sm.GetLastChatID())
	}

	if !sm.GetTimestamp().IsZero() {
		t.Error("Expected zero timestamp for new state")
	}
}

func TestSessionOverridesPersist(t *testing.T) {
	tmpDir := t.TempDir()

	sm := NewManager(tmpDir)
	if err := sm.SetSessionOverride("route-session", "reset-session"); err != nil {
		t.Fatalf("SetSessionOverride failed: %v", err)
	}

	if got := sm.GetSessionOverride("route-session"); got != "reset-session" {
		t.Fatalf("GetSessionOverride() = %q, want %q", got, "reset-session")
	}

	sm2 := NewManager(tmpDir)
	if got := sm2.GetSessionOverride("route-session"); got != "reset-session" {
		t.Fatalf("persisted GetSessionOverride() = %q, want %q", got, "reset-session")
	}

	if err := sm2.ClearSessionOverride("route-session"); err != nil {
		t.Fatalf("ClearSessionOverride failed: %v", err)
	}
	if got := sm2.GetSessionOverride("route-session"); got != "" {
		t.Fatalf("GetSessionOverride() after clear = %q, want empty", got)
	}

	sm3 := NewManager(tmpDir)
	if got := sm3.GetSessionOverride("route-session"); got != "" {
		t.Fatalf("persisted GetSessionOverride() after clear = %q, want empty", got)
	}
}

func TestToolFeedbackOverridesPersist(t *testing.T) {
	tmpDir := t.TempDir()

	sm := NewManager(tmpDir)
	if err := sm.SetToolFeedbackOverride("route-session", false); err != nil {
		t.Fatalf("SetToolFeedbackOverride failed: %v", err)
	}

	if got, ok := sm.GetToolFeedbackOverride("route-session"); !ok || got {
		t.Fatalf("GetToolFeedbackOverride() = (%v, %v), want (false, true)", got, ok)
	}

	sm2 := NewManager(tmpDir)
	if got, ok := sm2.GetToolFeedbackOverride("route-session"); !ok || got {
		t.Fatalf("persisted GetToolFeedbackOverride() = (%v, %v), want (false, true)", got, ok)
	}

	if err := sm2.ClearToolFeedbackOverride("route-session"); err != nil {
		t.Fatalf("ClearToolFeedbackOverride failed: %v", err)
	}
	if got, ok := sm2.GetToolFeedbackOverride("route-session"); ok {
		t.Fatalf("GetToolFeedbackOverride() after clear = (%v, %v), want (_, false)", got, ok)
	}

	sm3 := NewManager(tmpDir)
	if got, ok := sm3.GetToolFeedbackOverride("route-session"); ok {
		t.Fatalf("persisted GetToolFeedbackOverride() after clear = (%v, %v), want (_, false)", got, ok)
	}
}

func TestSessionModelOverridesPersist(t *testing.T) {
	tmpDir := t.TempDir()

	sm := NewManager(tmpDir)
	if err := sm.SetSessionModelOverride("route-session", "deepseek"); err != nil {
		t.Fatalf("SetSessionModelOverride failed: %v", err)
	}

	got, ok := sm.GetSessionModelOverride("route-session")
	if !ok || got.Model != "deepseek" {
		t.Fatalf("GetSessionModelOverride() = (%+v, %v), want (deepseek, true)", got, ok)
	}
	if got.UpdatedAt.IsZero() {
		t.Fatal("expected UpdatedAt to be populated")
	}

	sm2 := NewManager(tmpDir)
	got2, ok := sm2.GetSessionModelOverride("route-session")
	if !ok || got2.Model != "deepseek" {
		t.Fatalf("persisted GetSessionModelOverride() = (%+v, %v), want (deepseek, true)", got2, ok)
	}

	if err := sm2.ClearSessionModelOverride("route-session"); err != nil {
		t.Fatalf("ClearSessionModelOverride failed: %v", err)
	}
	if _, ok := sm2.GetSessionModelOverride("route-session"); ok {
		t.Fatal("expected session model override to be cleared")
	}

	sm3 := NewManager(tmpDir)
	if _, ok := sm3.GetSessionModelOverride("route-session"); ok {
		t.Fatal("expected persisted session model override to be cleared")
	}
}

func TestAutoModelSelectionsPersist(t *testing.T) {
	tmpDir := t.TempDir()

	selection := AutoModelSelection{
		SelectedProvider: "openai",
		SelectedModel:    "gpt-5.4",
		ActiveProvider:   "anthropic",
		ActiveModel:      "claude-opus-4.1",
		Reason:           "rate_limit",
		ExpiresAt:        time.Now().Add(15 * time.Minute).Round(time.Second),
	}

	sm := NewManager(tmpDir)
	if err := sm.SetAutoModelSelection("route-session", selection); err != nil {
		t.Fatalf("SetAutoModelSelection failed: %v", err)
	}

	got, ok := sm.GetAutoModelSelection("route-session")
	if !ok {
		t.Fatal("GetAutoModelSelection() missing value")
	}
	if got.SelectedProvider != selection.SelectedProvider ||
		got.SelectedModel != selection.SelectedModel ||
		got.ActiveProvider != selection.ActiveProvider ||
		got.ActiveModel != selection.ActiveModel ||
		got.Reason != selection.Reason ||
		!got.ExpiresAt.Equal(selection.ExpiresAt) {
		t.Fatalf("GetAutoModelSelection() = %+v, want %+v", got, selection)
	}
	if got.UpdatedAt.IsZero() {
		t.Fatal("expected UpdatedAt to be populated")
	}

	sm2 := NewManager(tmpDir)
	got2, ok := sm2.GetAutoModelSelection("route-session")
	if !ok {
		t.Fatal("persisted GetAutoModelSelection() missing value")
	}
	if got2.SelectedProvider != selection.SelectedProvider ||
		got2.SelectedModel != selection.SelectedModel ||
		got2.ActiveProvider != selection.ActiveProvider ||
		got2.ActiveModel != selection.ActiveModel ||
		got2.Reason != selection.Reason ||
		!got2.ExpiresAt.Equal(selection.ExpiresAt) {
		t.Fatalf("persisted GetAutoModelSelection() = %+v, want %+v", got2, selection)
	}

	if err := sm2.ClearAutoModelSelection("route-session"); err != nil {
		t.Fatalf("ClearAutoModelSelection failed: %v", err)
	}
	if _, ok := sm2.GetAutoModelSelection("route-session"); ok {
		t.Fatal("expected auto model selection to be cleared")
	}

	sm3 := NewManager(tmpDir)
	if _, ok := sm3.GetAutoModelSelection("route-session"); ok {
		t.Fatal("expected persisted auto model selection to be cleared")
	}
}

func TestSessionGoalsPersist(t *testing.T) {
	tmpDir := t.TempDir()

	sm := NewManager(tmpDir)
	goal, err := sm.CreateSessionGoal("route-session", "  finish the PR  ")
	if err != nil {
		t.Fatalf("CreateSessionGoal failed: %v", err)
	}
	if goal.Objective != "finish the PR" {
		t.Fatalf("CreateSessionGoal objective = %q, want trimmed objective", goal.Objective)
	}
	if goal.Status != SessionGoalActive {
		t.Fatalf("CreateSessionGoal status = %q, want %q", goal.Status, SessionGoalActive)
	}
	if goal.CreatedAt.IsZero() || goal.UpdatedAt.IsZero() {
		t.Fatal("expected goal timestamps to be populated")
	}

	got, ok := sm.GetSessionGoal("route-session")
	if !ok {
		t.Fatal("GetSessionGoal missing value")
	}
	if got.Objective != "finish the PR" || got.Status != SessionGoalActive {
		t.Fatalf("GetSessionGoal() = %+v, want active finish the PR", got)
	}

	sm2 := NewManager(tmpDir)
	got2, ok := sm2.GetSessionGoal("route-session")
	if !ok {
		t.Fatal("persisted GetSessionGoal missing value")
	}
	if got2.Objective != "finish the PR" || got2.Status != SessionGoalActive {
		t.Fatalf("persisted GetSessionGoal() = %+v, want active finish the PR", got2)
	}
}

func TestSessionGoalsRejectDuplicateAndInvalidInput(t *testing.T) {
	sm := NewManager(t.TempDir())

	if _, err := sm.CreateSessionGoal("", "objective"); err == nil {
		t.Fatal("expected empty session key to fail")
	}
	if _, err := sm.CreateSessionGoal("route-session", " "); err == nil {
		t.Fatal("expected empty objective to fail")
	}

	if _, err := sm.CreateSessionGoal("route-session", "first"); err != nil {
		t.Fatalf("CreateSessionGoal failed: %v", err)
	}
	if _, err := sm.CreateSessionGoal("route-session", "second"); err == nil {
		t.Fatal("expected duplicate goal create to fail")
	}
}

func TestSessionGoalsAreSessionScoped(t *testing.T) {
	sm := NewManager(t.TempDir())

	if _, err := sm.CreateSessionGoal("route-a", "goal a"); err != nil {
		t.Fatalf("CreateSessionGoal route-a failed: %v", err)
	}
	if _, err := sm.CreateSessionGoal("route-b", "goal b"); err != nil {
		t.Fatalf("CreateSessionGoal route-b failed: %v", err)
	}

	gotA, okA := sm.GetSessionGoal("route-a")
	gotB, okB := sm.GetSessionGoal("route-b")
	if !okA || gotA.Objective != "goal a" {
		t.Fatalf("route-a goal = (%+v, %v), want goal a", gotA, okA)
	}
	if !okB || gotB.Objective != "goal b" {
		t.Fatalf("route-b goal = (%+v, %v), want goal b", gotB, okB)
	}
}

func TestSessionGoalEditStatusAndClear(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewManager(tmpDir)

	created, err := sm.CreateSessionGoal("route-session", "first objective")
	if err != nil {
		t.Fatalf("CreateSessionGoal failed: %v", err)
	}

	edited, err := sm.EditSessionGoal("route-session", "updated objective")
	if err != nil {
		t.Fatalf("EditSessionGoal failed: %v", err)
	}
	if edited.Objective != "updated objective" {
		t.Fatalf("EditSessionGoal objective = %q, want updated objective", edited.Objective)
	}
	if edited.Status != SessionGoalActive {
		t.Fatalf("EditSessionGoal status = %q, want active", edited.Status)
	}
	if !edited.CreatedAt.Equal(created.CreatedAt) {
		t.Fatalf("EditSessionGoal changed CreatedAt: got %v want %v", edited.CreatedAt, created.CreatedAt)
	}

	blocked, err := sm.SetSessionGoalStatus("route-session", SessionGoalBlocked, " waiting on CI ")
	if err != nil {
		t.Fatalf("SetSessionGoalStatus blocked failed: %v", err)
	}
	if blocked.Status != SessionGoalBlocked || blocked.Note != "waiting on CI" || blocked.BlockedAt == nil {
		t.Fatalf("blocked goal = %+v, want blocked note and timestamp", blocked)
	}
	if blocked.CompletedAt != nil {
		t.Fatalf("blocked goal CompletedAt = %v, want nil", blocked.CompletedAt)
	}
	blockedAt := *blocked.BlockedAt

	blockedAgain, err := sm.SetSessionGoalStatus("route-session", SessionGoalBlocked, "still waiting on CI")
	if err != nil {
		t.Fatalf("SetSessionGoalStatus blocked again failed: %v", err)
	}
	if !blockedAgain.BlockedAt.Equal(blockedAt) {
		t.Fatalf("blocked goal changed BlockedAt: got %v want %v", blockedAgain.BlockedAt, blockedAt)
	}

	complete, err := sm.SetSessionGoalStatus("route-session", SessionGoalComplete, "done")
	if err != nil {
		t.Fatalf("SetSessionGoalStatus complete failed: %v", err)
	}
	if complete.Status != SessionGoalComplete || complete.CompletedAt == nil {
		t.Fatalf("complete goal = %+v, want complete timestamp", complete)
	}
	if complete.BlockedAt == nil || !complete.BlockedAt.Equal(blockedAt) {
		t.Fatalf("complete goal changed BlockedAt: got %v want %v", complete.BlockedAt, blockedAt)
	}
	completedAt := *complete.CompletedAt

	completeAgain, err := sm.SetSessionGoalStatus("route-session", SessionGoalComplete, "still done")
	if err != nil {
		t.Fatalf("SetSessionGoalStatus complete again failed: %v", err)
	}
	if !completeAgain.CompletedAt.Equal(completedAt) {
		t.Fatalf("complete goal changed CompletedAt: got %v want %v", completeAgain.CompletedAt, completedAt)
	}

	paused, err := sm.SetSessionGoalStatus("route-session", SessionGoalPaused, "waiting for input")
	if err != nil {
		t.Fatalf("SetSessionGoalStatus pause failed: %v", err)
	}
	if paused.BlockedAt == nil || paused.CompletedAt == nil ||
		!paused.BlockedAt.Equal(blockedAt) || !paused.CompletedAt.Equal(completedAt) {
		t.Fatalf("paused goal lost terminal history: %+v", paused)
	}

	resumed, err := sm.SetSessionGoalStatus("route-session", SessionGoalActive, "continuing")
	if err != nil {
		t.Fatalf("SetSessionGoalStatus resume failed: %v", err)
	}
	if resumed.BlockedAt == nil || resumed.CompletedAt == nil ||
		!resumed.BlockedAt.Equal(blockedAt) || !resumed.CompletedAt.Equal(completedAt) {
		t.Fatalf("resumed goal lost terminal history: %+v", resumed)
	}

	sm2 := NewManager(tmpDir)
	persisted, ok := sm2.GetSessionGoal("route-session")
	if !ok || persisted.Status != SessionGoalActive || persisted.CompletedAt == nil || persisted.BlockedAt == nil {
		t.Fatalf("persisted resumed goal = (%+v, %v), want active with terminal history", persisted, ok)
	}

	if err := sm2.ClearSessionGoal("route-session"); err != nil {
		t.Fatalf("ClearSessionGoal failed: %v", err)
	}
	if _, ok := sm2.GetSessionGoal("route-session"); ok {
		t.Fatal("expected goal to be cleared")
	}
	sm3 := NewManager(tmpDir)
	if _, ok := sm3.GetSessionGoal("route-session"); ok {
		t.Fatal("expected cleared goal to stay cleared")
	}
}

func TestSessionGoalStatusValidation(t *testing.T) {
	sm := NewManager(t.TempDir())
	if _, err := sm.SetSessionGoalStatus("route-session", SessionGoalActive, ""); err == nil {
		t.Fatal("expected missing goal status update to fail")
	}
	if _, err := sm.CreateSessionGoal("route-session", "objective"); err != nil {
		t.Fatalf("CreateSessionGoal failed: %v", err)
	}
	if _, err := sm.SetSessionGoalStatus("route-session", SessionGoalStatus("unknown"), ""); err == nil {
		t.Fatal("expected invalid status to fail")
	}
	if _, err := sm.EditSessionGoal("missing", "objective"); err == nil {
		t.Fatal("expected edit of missing goal to fail")
	}
}

func TestSessionGoalGetReturnsCopy(t *testing.T) {
	sm := NewManager(t.TempDir())
	if _, err := sm.CreateSessionGoal("route-session", "objective"); err != nil {
		t.Fatalf("CreateSessionGoal failed: %v", err)
	}
	blocked, err := sm.SetSessionGoalStatus("route-session", SessionGoalBlocked, "blocked")
	if err != nil {
		t.Fatalf("SetSessionGoalStatus failed: %v", err)
	}
	if blocked.BlockedAt == nil {
		t.Fatal("expected BlockedAt to be set")
	}

	got, ok := sm.GetSessionGoal("route-session")
	if !ok {
		t.Fatal("GetSessionGoal missing value")
	}
	*got.BlockedAt = got.BlockedAt.Add(24 * time.Hour)

	again, ok := sm.GetSessionGoal("route-session")
	if !ok {
		t.Fatal("GetSessionGoal missing value after mutation")
	}
	if !again.BlockedAt.Equal(*blocked.BlockedAt) {
		t.Fatalf("GetSessionGoal returned shared timestamp pointer: got %v want %v", again.BlockedAt, blocked.BlockedAt)
	}
}

func TestNewManager_MkdirFailureDoesNotCrash(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		tmpDir := os.Getenv("CRASH_DIR")

		statePath := filepath.Join(tmpDir, "state")
		if err := os.WriteFile(statePath, []byte("I'm a file, not a folder"), 0o644); err != nil {
			fmt.Printf("setup failed: %v", err)
			os.Exit(0)
		}

		NewManager(tmpDir)
		os.Exit(0)
	}

	tmpDir, err := os.MkdirTemp("", "state-crash-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cmd := exec.Command(os.Args[0], "-test.run=TestNewManager_MkdirFailureDoesNotCrash")
	cmd.Env = append(os.Environ(), "BE_CRASHER=1", "CRASH_DIR="+tmpDir)

	err = cmd.Run()
	if err != nil {
		t.Fatalf("NewManager should not crash when state dir creation fails, got: %v", err)
	}
}
