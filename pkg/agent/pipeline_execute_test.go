package agent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/routing"
	"github.com/sipeed/picoclaw/pkg/session"
	"github.com/sipeed/picoclaw/pkg/tools"
)

func TestInferSkillNamesFromToolCall_ReadFileSkillMarkdown(t *testing.T) {
	workspace := t.TempDir()
	skillDir := filepath.Join(workspace, "skills", "three-one")
	if err := os.MkdirAll(skillDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(
		filepath.Join(skillDir, "SKILL.md"),
		[]byte("---\nname: three-one\ndescription: test\n---\n# Three One\n"),
		0o644,
	); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cb := NewContextBuilder(workspace)
	ts := &turnState{
		workspace: workspace,
		agent: &AgentInstance{
			Workspace:      workspace,
			ContextBuilder: cb,
		},
	}

	got := inferSkillNamesFromToolCall(ts, "read_file", map[string]any{
		"path": filepath.Join(workspace, "skills", "three-one", "SKILL.md"),
	})
	if len(got) != 1 || got[0] != "three-one" {
		t.Fatalf("inferSkillNamesFromToolCall = %v, want [three-one]", got)
	}
}

func TestInferSkillNamesFromToolCall_NonSkillFileIgnored(t *testing.T) {
	workspace := t.TempDir()
	ts := &turnState{workspace: workspace}

	got := inferSkillNamesFromToolCall(ts, "read_file", map[string]any{
		"path": filepath.Join(workspace, "README.md"),
	})
	if len(got) != 0 {
		t.Fatalf("inferSkillNamesFromToolCall = %v, want empty", got)
	}
}

func TestIsFatalMCPTransportErrorSummary(t *testing.T) {
	if !isFatalMCPTransportErrorSummary(
		`MCP tool execution failed: failed to call tool: connection closed: calling "tools/call": client is closing: invalid character 'ð' looking for beginning of value`,
	) {
		t.Fatal("expected fatal MCP transport error to match")
	}
	if isFatalMCPTransportErrorSummary("MCP tool returned error: rate limited, retry later") {
		t.Fatal("expected normal MCP server error not to match fatal transport classifier")
	}
}

func TestPipelineAppendToolMessage_PersistsWithoutIngest(t *testing.T) {
	sessionStore := session.NewSessionManager("")
	cm := &trackingContextManager{}
	pipeline := &Pipeline{Context: PipelineContextServices{Runtime: cm}}
	ts := &turnState{
		agent:      &AgentInstance{Sessions: sessionStore},
		sessionKey: "session-tool-message",
	}
	runner := &toolLoopRunner{
		p:       pipeline,
		turnCtx: context.Background(),
		ts:      ts,
	}
	msg := providers.Message{
		Role:       "tool",
		Content:    "skipped",
		ToolCallID: "call-1",
	}

	runner.appendToolMessage(msg, toolMessagePersistOnly)
	if len(runner.messages) != 1 || runner.messages[0].Content != "skipped" {
		t.Fatalf("messages = %#v, want appended message", runner.messages)
	}
	history := sessionStore.GetHistory(ts.sessionKey)
	if len(history) != 1 || history[0].Content != "skipped" {
		t.Fatalf("session history = %#v, want persisted message", history)
	}
	if got := cm.ingestCalls.Load(); got != 0 {
		t.Fatalf("ingest calls = %d, want 0", got)
	}
}

func TestPipelineAppendToolMessage_PersistsAndIngests(t *testing.T) {
	sessionStore := session.NewSessionManager("")
	cm := &trackingContextManager{}
	pipeline := &Pipeline{Context: PipelineContextServices{Runtime: cm}}
	ts := &turnState{
		agent:      &AgentInstance{Sessions: sessionStore},
		sessionKey: "session-tool-result",
	}
	runner := &toolLoopRunner{
		p:       pipeline,
		turnCtx: context.Background(),
		ts:      ts,
	}
	msg := providers.Message{
		Role:       "tool",
		Content:    "result",
		ToolCallID: "call-2",
	}

	runner.appendToolMessage(msg, toolMessagePersistAndIngest)
	if len(runner.messages) != 1 || runner.messages[0].Content != "result" {
		t.Fatalf("messages = %#v, want appended message", runner.messages)
	}
	history := sessionStore.GetHistory(ts.sessionKey)
	if len(history) != 1 || history[0].Content != "result" {
		t.Fatalf("session history = %#v, want persisted message", history)
	}
	if got := cm.ingestCalls.Load(); got != 1 {
		t.Fatalf("ingest calls = %d, want 1", got)
	}
	if cm.lastIngest == nil || cm.lastIngest.Message.Content != "result" {
		t.Fatalf("last ingest = %#v, want result message", cm.lastIngest)
	}
}

func TestPipelineAppendSkippedToolMessages_PersistsRemainingWithoutIngest(t *testing.T) {
	sessionStore := session.NewSessionManager("")
	cm := &trackingContextManager{}
	pipeline := &Pipeline{Context: PipelineContextServices{Runtime: cm}}
	ts := &turnState{
		agent:      &AgentInstance{Sessions: sessionStore},
		sessionKey: "session-skipped-tool",
	}
	toolCalls := []providers.ToolCall{
		{ID: "call-complete", Name: "done_tool"},
		{ID: "call-skip-1", Name: "expensive_tool"},
		{ID: "call-skip-2", Name: "slow_tool"},
	}
	runner := &toolLoopRunner{
		p:         pipeline,
		turnCtx:   context.Background(),
		ts:        ts,
		toolCalls: toolCalls,
	}

	runner.appendSkippedToolMessages(
		1,
		"queued user steering message",
		"Skipped due to queued user message.",
	)
	if len(runner.messages) != 2 {
		t.Fatalf("messages len = %d, want 2", len(runner.messages))
	}
	if runner.messages[0].ToolCallID != "call-skip-1" ||
		runner.messages[1].ToolCallID != "call-skip-2" ||
		runner.messages[0].Content != "Skipped due to queued user message." {
		t.Fatalf("messages = %#v, want skipped tool messages", runner.messages)
	}
	history := sessionStore.GetHistory(ts.sessionKey)
	if len(history) != 2 ||
		history[0].ToolCallID != "call-skip-1" ||
		history[1].ToolCallID != "call-skip-2" {
		t.Fatalf("session history = %#v, want skipped messages persisted", history)
	}
	if got := cm.ingestCalls.Load(); got != 0 {
		t.Fatalf("ingest calls = %d, want 0", got)
	}
}

func TestToolLoopRunnerAppendPendingSubTurnResult_PersistsAndIngests(t *testing.T) {
	sessionStore := session.NewSessionManager("")
	cm := &trackingContextManager{}
	pipeline := &Pipeline{Context: PipelineContextServices{Runtime: cm}}
	ts := &turnState{
		agent: &AgentInstance{
			Sessions: sessionStore,
		},
		sessionKey:     "session-subturn-result",
		pendingResults: make(chan *tools.ToolResult, 1),
	}
	ts.pendingResults <- &tools.ToolResult{ForLLM: "child result"}
	runner := &toolLoopRunner{
		p:       pipeline,
		turnCtx: context.Background(),
		ts:      ts,
	}

	runner.appendPendingSubTurnResult()
	if len(runner.messages) != 1 ||
		!strings.Contains(runner.messages[0].Content, "child result") {
		t.Fatalf("messages = %#v, want subturn result message", runner.messages)
	}
	history := sessionStore.GetHistory(ts.sessionKey)
	if len(history) != 1 || !strings.Contains(history[0].Content, "child result") {
		t.Fatalf("session history = %#v, want persisted subturn result", history)
	}
	persisted := ts.persistedMessagesSnapshot()
	if len(persisted) != 1 || !strings.Contains(persisted[0].Content, "child result") {
		t.Fatalf("persisted messages = %#v, want subturn result", persisted)
	}
	if got := cm.ingestCalls.Load(); got != 1 {
		t.Fatalf("ingest calls = %d, want 1", got)
	}
	if cm.lastIngest == nil || !strings.Contains(cm.lastIngest.Message.Content, "child result") {
		t.Fatalf("last ingest = %#v, want subturn result", cm.lastIngest)
	}
}

type repeatingFatalToolProvider struct {
	calls int
}

func (p *repeatingFatalToolProvider) Chat(
	ctx context.Context,
	messages []providers.Message,
	toolDefs []providers.ToolDefinition,
	model string,
	opts map[string]any,
) (*providers.LLMResponse, error) {
	p.calls++
	return &providers.LLMResponse{
		ToolCalls: []providers.ToolCall{{
			ID:        "call-fatal",
			Name:      "mcp_gpt_researcher_quick_search",
			Arguments: map[string]any{"query": "modere refresh"},
		}},
	}, nil
}

func (p *repeatingFatalToolProvider) GetDefaultModel() string {
	return "fatal-loop-model"
}

type repeatingFatalTool struct{}

func (t *repeatingFatalTool) Name() string { return "mcp_gpt_researcher_quick_search" }
func (t *repeatingFatalTool) Description() string {
	return "Always fails with a fatal MCP transport error"
}

func (t *repeatingFatalTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query": map[string]any{
				"type": "string",
			},
		},
	}
}

func (t *repeatingFatalTool) Execute(ctx context.Context, args map[string]any) *tools.ToolResult {
	err := `MCP tool execution failed: failed to call tool: connection closed: calling "tools/call": client is closing: invalid character 'ð' looking for beginning of value`
	return tools.ErrorResult(err)
}

func TestRunAgentLoop_AbortsRepeatedFatalToolTransportErrors(t *testing.T) {
	workspace := t.TempDir()
	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         workspace,
				ModelName:         "test-model",
				MaxTokens:         2048,
				MaxToolIterations: 20,
			},
		},
	}

	msgBus := bus.NewMessageBus()
	provider := &repeatingFatalToolProvider{}
	al := NewAgentLoop(cfg, msgBus, provider)
	al.RegisterTool(&repeatingFatalTool{})

	defaultAgent := al.registry.GetDefaultAgent()
	if defaultAgent == nil {
		t.Fatal("expected default agent")
	}

	response, err := al.runAgentLoop(context.Background(), defaultAgent, processOptions{
		SessionKey:      "session-fatal-tool-loop",
		Channel:         "cli",
		ChatID:          "direct",
		UserMessage:     "run research",
		DefaultResponse: defaultResponse,
		EnableSummary:   false,
		SendResponse:    false,
		InboundContext: &bus.InboundContext{
			Channel:  "cli",
			ChatID:   "direct",
			ChatType: "direct",
			SenderID: "tester",
		},
		RouteResult: &routing.ResolvedRoute{
			AgentID:   "main",
			Channel:   "cli",
			AccountID: routing.DefaultAccountID,
			SessionPolicy: routing.SessionPolicy{
				Dimensions: []string{"sender"},
			},
			MatchedBy: "default",
		},
		SessionScope: &session.SessionScope{
			Version:    session.ScopeVersionV1,
			AgentID:    "main",
			Channel:    "cli",
			Account:    routing.DefaultAccountID,
			Dimensions: []string{"sender"},
			Values: map[string]string{
				"sender": "tester",
			},
		},
	})
	if err != nil {
		t.Fatalf("runAgentLoop() error = %v", err)
	}
	if got, want := response, "I hit repeated backend tool transport errors while using `mcp_gpt_researcher_quick_search` and stopped instead of retrying indefinitely. Please try again."; got != want {
		t.Fatalf("runAgentLoop() response = %q, want %q", got, want)
	}
	if provider.calls != repeatedFatalToolErrorStreakLimit {
		t.Fatalf("provider calls = %d, want %d", provider.calls, repeatedFatalToolErrorStreakLimit)
	}
}

type fatalMCPServerTool struct{}

func (t *fatalMCPServerTool) Name() string { return "mcp_gpt_researcher_quick_search" }
func (t *fatalMCPServerTool) Description() string {
	return "Fails with a fatal MCP server transport error"
}

func (t *fatalMCPServerTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query": map[string]any{
				"type": "string",
			},
		},
	}
}
func (t *fatalMCPServerTool) MCPServerName() string { return "gpt_researcher" }
func (t *fatalMCPServerTool) Execute(ctx context.Context, args map[string]any) *tools.ToolResult {
	err := `MCP tool execution failed: failed to call tool: connection closed: calling "tools/call": client is closing: invalid character 'ð' looking for beginning of value`
	return tools.ErrorResult(err)
}

func TestRunAgentLoop_AbortsFatalMCPServerTransportErrorImmediately(t *testing.T) {
	workspace := t.TempDir()
	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         workspace,
				ModelName:         "test-model",
				MaxTokens:         2048,
				MaxToolIterations: 20,
			},
		},
	}

	msgBus := bus.NewMessageBus()
	provider := &repeatingFatalToolProvider{}
	al := NewAgentLoop(cfg, msgBus, provider)
	al.RegisterTool(&fatalMCPServerTool{})

	defaultAgent := al.registry.GetDefaultAgent()
	if defaultAgent == nil {
		t.Fatal("expected default agent")
	}

	response, err := al.runAgentLoop(context.Background(), defaultAgent, processOptions{
		SessionKey:      "session-fatal-mcp-server",
		Channel:         "cli",
		ChatID:          "direct",
		UserMessage:     "run research",
		DefaultResponse: defaultResponse,
		EnableSummary:   false,
		SendResponse:    false,
		InboundContext: &bus.InboundContext{
			Channel:  "cli",
			ChatID:   "direct",
			ChatType: "direct",
			SenderID: "tester",
		},
		RouteResult: &routing.ResolvedRoute{
			AgentID:   "main",
			Channel:   "cli",
			AccountID: routing.DefaultAccountID,
			SessionPolicy: routing.SessionPolicy{
				Dimensions: []string{"sender"},
			},
			MatchedBy: "default",
		},
		SessionScope: &session.SessionScope{
			Version:    session.ScopeVersionV1,
			AgentID:    "main",
			Channel:    "cli",
			Account:    routing.DefaultAccountID,
			Dimensions: []string{"sender"},
			Values: map[string]string{
				"sender": "tester",
			},
		},
	})
	if err != nil {
		t.Fatalf("runAgentLoop() error = %v", err)
	}
	want := "I hit a backend MCP transport error while using the `gpt_researcher` server and stopped instead of trying workarounds. Please restart or fix that MCP server, then try again."
	if response != want {
		t.Fatalf("runAgentLoop() response = %q, want %q", response, want)
	}
	if provider.calls != 1 {
		t.Fatalf("provider calls = %d, want 1", provider.calls)
	}
}
