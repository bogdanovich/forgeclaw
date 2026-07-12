package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/state"
)

func newSessionGoalPromptLoop(t *testing.T) (*AgentLoop, *AgentInstance, *turnProfileCaptureProvider) {
	t.Helper()
	provider := &turnProfileCaptureProvider{}
	al := newTurnProfileAgentLoop(t, &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         t.TempDir(),
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 4,
			},
		},
	}, provider)
	agent := al.GetRegistry().GetDefaultAgent()
	if agent == nil {
		t.Fatal("expected default agent")
	}
	return al, agent, provider
}

func sessionGoalPromptOptions(routeSessionKey, sessionKey string) processOptions {
	return processOptions{
		Dispatch: DispatchRequest{
			RouteSessionKey: routeSessionKey,
			SessionKey:      sessionKey,
			UserMessage:     "continue the work",
		},
		DefaultResponse: defaultResponse,
		EnableSummary:   false,
	}
}

func TestRunAgentLoopInjectsActiveGoalWithoutPersistingItInHistory(t *testing.T) {
	al, agent, provider := newSessionGoalPromptLoop(t)
	const routeSessionKey = "route-goal"
	const sessionKey = "history-goal"
	if _, err := al.state.CreateSessionGoal(routeSessionKey, "finish the release checklist"); err != nil {
		t.Fatalf("CreateSessionGoal failed: %v", err)
	}

	if _, err := al.runAgentLoop(
		context.Background(),
		agent,
		sessionGoalPromptOptions(routeSessionKey, sessionKey),
	); err != nil {
		t.Fatalf("runAgentLoop failed: %v", err)
	}
	if len(provider.messages) == 0 {
		t.Fatal("expected provider messages")
	}
	const reminder = "Active goal: finish the release checklist - " +
		"advance it or update its status (get_goal/update_goal)."
	if !strings.Contains(provider.messages[0].Content, reminder) {
		t.Fatalf("system prompt missing goal reminder:\n%s", provider.messages[0].Content)
	}

	for _, message := range agent.Sessions.GetHistory(sessionKey) {
		if strings.Contains(message.Content, "Active goal:") {
			t.Fatalf("goal reminder leaked into stored history: %#v", message)
		}
	}
}

func TestApplyActiveGoalPromptSkipsInactiveAndNoHistoryTurns(t *testing.T) {
	statuses := []struct {
		name      string
		status    string
		noHistory bool
	}{
		{name: "paused", status: "paused"},
		{name: "blocked", status: "blocked"},
		{name: "complete", status: "complete"},
		{name: "no_history", status: "active", noHistory: true},
	}

	for _, tt := range statuses {
		t.Run(tt.name, func(t *testing.T) {
			al, _, _ := newSessionGoalPromptLoop(t)
			if _, err := al.state.CreateSessionGoal("route-goal", "finish the release"); err != nil {
				t.Fatalf("CreateSessionGoal failed: %v", err)
			}
			if tt.status != "active" {
				if _, err := al.state.SetSessionGoalStatus(
					"route-goal",
					state.SessionGoalStatus(tt.status),
					"state test",
				); err != nil {
					t.Fatalf("SetSessionGoalStatus failed: %v", err)
				}
			}

			opts := sessionGoalPromptOptions("route-goal", "history-goal")
			opts.NoHistory = tt.noHistory
			al.applyActiveGoalPrompt(&opts)
			if opts.ActiveGoal != "" {
				t.Fatalf("ActiveGoal = %q, want empty", opts.ActiveGoal)
			}
		})
	}
}

func TestApplyActiveGoalPromptTruncatesObjectiveAndHandlesMissingStore(t *testing.T) {
	al, _, _ := newSessionGoalPromptLoop(t)
	objective := strings.Repeat("a", maxActiveGoalObjectiveRunes+100)
	if _, err := al.state.CreateSessionGoal("route-goal", objective); err != nil {
		t.Fatalf("CreateSessionGoal failed: %v", err)
	}

	opts := sessionGoalPromptOptions("route-goal", "history-goal")
	al.applyActiveGoalPrompt(&opts)
	if !strings.HasPrefix(opts.ActiveGoal, "Active goal: ") || !strings.Contains(opts.ActiveGoal, "...") {
		t.Fatalf("ActiveGoal = %q, want truncated reminder", opts.ActiveGoal)
	}

	missingStore := &AgentLoop{}
	missingStore.applyActiveGoalPrompt(&opts)
}

func TestSideQuestionDoesNotReceiveActiveGoalPrompt(t *testing.T) {
	al, agent, _ := newSessionGoalPromptLoop(t)
	sideProvider := &turnProfileSideQuestionCaptureProvider{}
	useTestSideQuestionProvider(al, sideProvider)
	if _, err := al.state.CreateSessionGoal("route-goal", "finish the release checklist"); err != nil {
		t.Fatalf("CreateSessionGoal failed: %v", err)
	}

	opts := sessionGoalPromptOptions("route-goal", "history-goal")
	if _, err := al.askSideQuestion(context.Background(), agent, &opts, "what changed?"); err != nil {
		t.Fatalf("askSideQuestion failed: %v", err)
	}
	for _, message := range sideProvider.messages {
		if strings.Contains(message.Content, "Active goal:") {
			t.Fatalf("side question received active goal prompt: %#v", message)
		}
	}
}

func TestActiveGoalPromptCountsTowardPromptReserve(t *testing.T) {
	al, agent, _ := newSessionGoalPromptLoop(t)
	if _, err := al.state.CreateSessionGoal("route-goal", "finish the release checklist"); err != nil {
		t.Fatalf("CreateSessionGoal failed: %v", err)
	}

	withoutGoal := sessionGoalPromptOptions("route-goal", "history-goal")
	withGoal := withoutGoal
	al.applyActiveGoalPrompt(&withGoal)

	withoutGoalTokens := estimateNonHistoryPromptReserveForProcessOptions(al.GetConfig(), agent, withoutGoal, "")
	withGoalTokens := estimateNonHistoryPromptReserveForProcessOptions(al.GetConfig(), agent, withGoal, "")
	if withGoalTokens <= withoutGoalTokens {
		t.Fatalf("goal prompt reserve = %d, want > %d", withGoalTokens, withoutGoalTokens)
	}
}
