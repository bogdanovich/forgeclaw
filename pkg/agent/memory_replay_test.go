//go:build !mipsle && !netbsd && !(freebsd && arm)

package agent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/evalreplay"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/seahorse"
	"github.com/sipeed/picoclaw/pkg/session"
	"github.com/sipeed/picoclaw/pkg/tools"
)

func TestMemoryReplayCorrectionDeletionAndAudit(t *testing.T) {
	first := replayCuratedMemoryLifecycle(t)
	second := replayCuratedMemoryLifecycle(t)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("memory replay is not deterministic:\nfirst:  %#v\nsecond: %#v", first, second)
	}
}

type curatedMemoryReplay struct {
	Statuses                  []string
	EventOutcomes             []string
	CorrectedVisible          bool
	StaleVisible              bool
	DuplicateCountAfterAdd    int
	DuplicateCountAfterRemove int
	RawContentInEvent         bool
}

func replayCuratedMemoryLifecycle(t *testing.T) curatedMemoryReplay {
	t.Helper()
	workspace := t.TempDir()
	memoryPath := filepath.Join(workspace, "memory", "MEMORY.md")
	if err := os.MkdirAll(filepath.Dir(memoryPath), 0o755); err != nil {
		t.Fatal(err)
	}
	const staleFact = "- Home: Oakland"
	const correctedFact = "- Home: San Mateo"
	const duplicateInput = "HOME: SAN MATEO"
	if err := os.WriteFile(memoryPath, []byte("# Stable facts\n\n"+staleFact+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	eventBus := runtimeevents.NewBus()
	subscription, eventCh, err := eventBus.Channel().OfKind(runtimeevents.KindAgentMemoryMutation).SubscribeChan(
		t.Context(),
		runtimeevents.SubscribeOptions{Name: "memory-replay", Buffer: 4},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()

	builder := NewContextBuilder(workspace)
	_ = builder.BuildSystemPromptWithCache()
	tool := tools.NewMemoryTool(workspace, builder.InvalidateCache, eventBus)
	results := make([]*tools.ToolResult, 0, 3)
	results = append(results, tool.Execute(t.Context(), map[string]any{
		"operation": "replace", "content": staleFact, "replacement": correctedFact,
	}))
	correctedPrompt := builder.BuildSystemPromptWithCache()
	results = append(results, tool.Execute(t.Context(), map[string]any{
		"operation": "add", "content": duplicateInput,
	}))
	duplicatePrompt := builder.BuildSystemPromptWithCache()
	results = append(results, tool.Execute(t.Context(), map[string]any{
		"operation": "remove", "content": correctedFact,
	}))
	forgottenPrompt := builder.BuildSystemPromptWithCache()

	observation := curatedMemoryReplay{
		CorrectedVisible:          strings.Contains(correctedPrompt, correctedFact),
		StaleVisible:              strings.Contains(correctedPrompt, staleFact),
		DuplicateCountAfterAdd:    strings.Count(strings.ToLower(duplicatePrompt), "home: san mateo"),
		DuplicateCountAfterRemove: strings.Count(strings.ToLower(forgottenPrompt), "home: san mateo"),
	}
	for _, result := range results {
		if result.IsError {
			t.Fatalf("memory mutation failed: %s", result.ContentForLLM())
		}
		var response struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal([]byte(result.ContentForLLM()), &response); err != nil {
			t.Fatal(err)
		}
		observation.Statuses = append(observation.Statuses, response.Status)
	}
	for range results {
		select {
		case event := <-eventCh:
			payload, ok := event.Payload.(tools.MemoryMutationPayload)
			if !ok {
				t.Fatalf("memory event payload = %#v", event.Payload)
			}
			observation.EventOutcomes = append(observation.EventOutcomes, payload.Outcome)
			encoded, marshalErr := json.Marshal(event)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			for _, input := range []string{staleFact, correctedFact, duplicateInput} {
				observation.RawContentInEvent = observation.RawContentInEvent ||
					strings.Contains(string(encoded), input)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for memory replay event")
		}
	}

	if !observation.CorrectedVisible || observation.StaleVisible ||
		observation.DuplicateCountAfterAdd != 1 || observation.DuplicateCountAfterRemove != 0 {
		t.Fatalf("unexpected correction/deletion observation: %#v", observation)
	}
	if !reflect.DeepEqual(observation.Statuses, []string{"replaced", "duplicate", "removed"}) ||
		!reflect.DeepEqual(observation.EventOutcomes, observation.Statuses) || observation.RawContentInEvent {
		t.Fatalf("unexpected mutation/audit observation: %#v", observation)
	}
	return observation
}

func TestMemoryReplayBoundsAndDailyRollover(t *testing.T) {
	first := replayBoundedPromptRollover(t)
	second := replayBoundedPromptRollover(t)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("prompt-memory replay is not deterministic:\nfirst:  %#v\nsecond: %#v", first, second)
	}
}

type promptMemoryReplay struct {
	StableHeadBefore bool
	StableTailBefore bool
	StableCutBefore  bool
	StableHeadAfter  bool
	StableTailAfter  bool
	StableCutAfter   bool
	DayOneBefore     bool
	DayTwoBefore     bool
	DayOneAfter      bool
	DayTwoAfter      bool
	DailyTruncated   bool
}

func replayBoundedPromptRollover(t *testing.T) promptMemoryReplay {
	t.Helper()
	workspace := t.TempDir()
	clock := evalreplay.NewVirtualClock(time.Date(2026, time.July, 18, 23, 59, 0, 0, time.UTC))
	writeReplayMemoryFile(t, workspace, "memory/MEMORY.md",
		"STABLE-HEAD\n"+strings.Repeat("stable-middle ", 80)+"\nSTABLE-TAIL")
	writeReplayMemoryFile(t, workspace, "memory/202607/20260718.md",
		"DAY-ONE-HEAD\n"+strings.Repeat("day-one ", 80)+"\nDAY-ONE-TAIL")
	writeReplayMemoryFile(t, workspace, "memory/202607/20260719.md",
		"DAY-TWO-HEAD\n"+strings.Repeat("day-two ", 80)+"\nDAY-TWO-TAIL")

	builder := NewContextBuilder(workspace)
	builder.memory = newMemoryStore(workspace, config.PromptMemoryConfig{
		LongTermMaxBytes: 180, DailyNotesMaxBytes: 180, RecentDays: 1,
	}, clock.Now)
	before := builder.BuildSystemPromptWithCache()
	clock.Advance(2 * time.Minute)
	after := builder.BuildSystemPromptWithCache()

	observation := promptMemoryReplay{
		StableHeadBefore: strings.Contains(before, "STABLE-HEAD"),
		StableTailBefore: strings.Contains(before, "STABLE-TAIL"),
		StableCutBefore:  strings.Contains(before, "stable memory truncated"),
		StableHeadAfter:  strings.Contains(after, "STABLE-HEAD"),
		StableTailAfter:  strings.Contains(after, "STABLE-TAIL"),
		StableCutAfter:   strings.Contains(after, "stable memory truncated"),
		DayOneBefore:     strings.Contains(before, "DAY-ONE-TAIL"),
		DayTwoBefore:     strings.Contains(before, "DAY-TWO-TAIL"),
		DayOneAfter:      strings.Contains(after, "DAY-ONE-TAIL"),
		DayTwoAfter:      strings.Contains(after, "DAY-TWO-TAIL"),
		DailyTruncated: strings.Contains(before, "daily notes truncated") &&
			strings.Contains(after, "daily notes truncated"),
	}
	if !observation.StableHeadBefore || !observation.StableTailBefore || !observation.StableCutBefore ||
		!observation.StableHeadAfter || !observation.StableTailAfter || !observation.StableCutAfter ||
		!observation.DayOneBefore || observation.DayTwoBefore || observation.DayOneAfter ||
		!observation.DayTwoAfter || !observation.DailyTruncated {
		t.Fatalf("unexpected bounded rollover observation: %#v", observation)
	}
	return observation
}

func writeReplayMemoryFile(t *testing.T, workspace, relative, content string) {
	t.Helper()
	path := filepath.Join(workspace, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryReplayCrossUserRecallIsolation(t *testing.T) {
	first := replayRecallIsolation(t)
	second := replayRecallIsolation(t)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("recall replay is not deterministic:\nfirst:  %#v\nsecond: %#v", first, second)
	}
}

type recallReplay struct {
	GrepContents     []string
	ExpandedContents []string
	RejectedCount    int
	WorkspaceDenied  bool
}

func replayRecallIsolation(t *testing.T) recallReplay {
	t.Helper()
	engine, err := seahorse.NewEngine(seahorse.Config{DBPath: filepath.Join(t.TempDir(), "seahorse.db")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	store := engine.GetRetrieval().Store()
	ctx := context.Background()
	seed := func(sessionKey, routeKey, agentID, content string) int64 {
		conversation, createErr := store.GetOrCreateConversation(ctx, sessionKey)
		if createErr != nil {
			t.Fatal(createErr)
		}
		if provenanceErr := store.SetConversationProvenance(ctx, sessionKey, routeKey, agentID); provenanceErr != nil {
			t.Fatal(provenanceErr)
		}
		message, addErr := store.AddMessage(ctx, conversation.ConversationID, "user", content+" memoryneedle", 5)
		if addErr != nil {
			t.Fatal(addErr)
		}
		return message.ID
	}

	currentID := seed("epoch:a:current", "route:chat:sender-a", "main", "current")
	previousID := seed("epoch:a:previous", "route:chat:sender-a", "main", "previous")
	otherSenderID := seed("epoch:b", "route:chat:sender-b", "main", "other-sender")
	otherAgentID := seed("epoch:reviewer", "route:chat:sender-a", "reviewer", "other-agent")
	toolCtx := tools.WithToolSessionContext(ctx, "main", "epoch:a:current", &session.SessionScope{
		Version: session.ScopeVersionV2, AgentID: "main", RouteScopeKey: "route:chat:sender-a",
	})

	grepResult := seahorse.NewGrepTool(engine.GetRetrieval()).Execute(toolCtx, map[string]any{
		"pattern": "memoryneedle", "scope": "message", "retrieval_scope": "conversation",
	})
	if grepResult.IsError {
		t.Fatal(grepResult.ContentForLLM())
	}
	var grepOutput struct {
		Messages []struct {
			Snippet string `json:"snippet"`
		} `json:"messages"`
	}
	if err := json.Unmarshal([]byte(grepResult.ContentForLLM()), &grepOutput); err != nil {
		t.Fatal(err)
	}

	expandResult := seahorse.NewExpandTool(engine.GetRetrieval()).Execute(toolCtx, map[string]any{
		"message_ids": []any{
			float64(currentID), float64(previousID), float64(otherSenderID), float64(otherAgentID),
		},
		"retrieval_scope": "conversation",
	})
	if expandResult.IsError {
		t.Fatal(expandResult.ContentForLLM())
	}
	var expandOutput struct {
		Messages []struct {
			Content string `json:"content"`
		} `json:"messages"`
		Rejected []int64 `json:"rejectedMessageIds"`
	}
	if err := json.Unmarshal([]byte(expandResult.ContentForLLM()), &expandOutput); err != nil {
		t.Fatal(err)
	}

	observation := recallReplay{RejectedCount: len(expandOutput.Rejected)}
	for _, message := range grepOutput.Messages {
		observation.GrepContents = append(observation.GrepContents, message.Snippet)
	}
	for _, message := range expandOutput.Messages {
		observation.ExpandedContents = append(observation.ExpandedContents, message.Content)
	}
	sort.Strings(observation.GrepContents)
	sort.Strings(observation.ExpandedContents)
	workspaceResult := seahorse.NewGrepTool(engine.GetRetrieval()).Execute(toolCtx, map[string]any{
		"pattern": "memoryneedle", "retrieval_scope": "workspace",
	})
	observation.WorkspaceDenied = workspaceResult.IsError &&
		strings.Contains(workspaceResult.ContentForLLM(), "exceeds operator maximum")

	if !reflect.DeepEqual(observation.GrepContents, []string{
		"current memoryneedle", "previous memoryneedle",
	}) ||
		!reflect.DeepEqual(observation.ExpandedContents, []string{
			"current memoryneedle", "previous memoryneedle",
		}) || observation.RejectedCount != 2 || !observation.WorkspaceDenied {
		t.Fatalf("unexpected recall isolation observation: %#v", observation)
	}
	return observation
}

func TestMemoryReplayWithoutSeahorse(t *testing.T) {
	first := replayWithoutSeahorse(t)
	second := replayWithoutSeahorse(t)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("Seahorse-disabled replay is not deterministic:\nfirst:  %#v\nsecond: %#v", first, second)
	}
	if !first.LegacyContext || !first.MemoryTool || first.GrepTool || first.ExpandTool ||
		!first.FactVisible || first.SeahorseDB {
		t.Fatalf("unexpected Seahorse-disabled observation: %#v", first)
	}
}

type disabledSeahorseReplay struct {
	LegacyContext bool
	MemoryTool    bool
	GrepTool      bool
	ExpandTool    bool
	FactVisible   bool
	SeahorseDB    bool
}

func replayWithoutSeahorse(t *testing.T) disabledSeahorseReplay {
	t.Helper()
	workspace := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = workspace
	messageBus := bus.NewMessageBus()
	loop := NewAgentLoop(cfg, messageBus, &mockProvider{})
	defer func() {
		loop.Close()
		messageBus.Close()
	}()
	instance := loop.registry.GetDefaultAgent()
	if instance == nil {
		t.Fatal("default agent is missing")
	}
	memoryTool, hasMemory := instance.Tools.Get("memory")
	_, hasGrep := instance.Tools.Get("short_grep")
	_, hasExpand := instance.Tools.Get("short_expand")
	if !hasMemory {
		t.Fatal("memory tool is missing without Seahorse")
	}
	result := memoryTool.Execute(t.Context(), map[string]any{
		"operation": "add", "content": "offline-stable-fact",
	})
	if result.IsError {
		t.Fatal(result.ContentForLLM())
	}
	_, dbErr := os.Stat(filepath.Join(workspace, "sessions", "seahorse.db"))
	return disabledSeahorseReplay{
		LegacyContext: func() bool { _, ok := loop.contextManager.(*legacyContextManager); return ok }(),
		MemoryTool:    hasMemory,
		GrepTool:      hasGrep,
		ExpandTool:    hasExpand,
		FactVisible:   strings.Contains(instance.ContextBuilder.BuildSystemPromptWithCache(), "offline-stable-fact"),
		SeahorseDB:    dbErr == nil,
	}
}
