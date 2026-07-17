package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/routing"
	"github.com/sipeed/picoclaw/pkg/session"
	"github.com/sipeed/picoclaw/pkg/tools"
	"github.com/sipeed/picoclaw/pkg/tools/loopguard"
)

type pipelineLoopGuardTool struct {
	executions int
}

type pipelineLoopGuardReadTool struct {
	executions int
}

type steeringSafetyTestTool struct {
	name       string
	safety     tools.SteeringSafety
	executions int
}

func (t *steeringSafetyTestTool) Name() string        { return t.name }
func (t *steeringSafetyTestTool) Description() string { return "steering safety test" }
func (t *steeringSafetyTestTool) Parameters() map[string]any {
	return map[string]any{"type": "object"}
}

func (t *steeringSafetyTestTool) ToolSteeringSafety(map[string]any) tools.SteeringSafety {
	return t.safety
}

func (t *steeringSafetyTestTool) Execute(context.Context, map[string]any) *tools.ToolResult {
	t.executions++
	return tools.SilentResult(t.name + " complete")
}

type unknownSteeringSafetyTestTool struct {
	executions int
}

func (*unknownSteeringSafetyTestTool) Name() string        { return "unknown" }
func (*unknownSteeringSafetyTestTool) Description() string { return "unknown steering safety test" }
func (*unknownSteeringSafetyTestTool) Parameters() map[string]any {
	return map[string]any{"type": "object"}
}

func (t *unknownSteeringSafetyTestTool) Execute(context.Context, map[string]any) *tools.ToolResult {
	t.executions++
	return tools.SilentResult("unknown complete")
}

func (t *pipelineLoopGuardReadTool) Name() string        { return "loop_hook_test" }
func (t *pipelineLoopGuardReadTool) Description() string { return "hook loop test" }
func (t *pipelineLoopGuardReadTool) Parameters() map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": map[string]any{"text": map[string]any{"type": "string"}},
	}
}

func (t *pipelineLoopGuardReadTool) ToolLoopSemantics() loopguard.Semantics {
	return loopguard.SemanticsReadOnlyIdempotent
}

func (t *pipelineLoopGuardReadTool) Execute(_ context.Context, args map[string]any) *tools.ToolResult {
	t.executions++
	text, _ := args["text"].(string)
	return tools.SilentResult(text)
}

type capturedRuntimeEvent struct {
	kind    runtimeevents.Kind
	payload any
}

type captureRuntimeEmitter struct {
	events []capturedRuntimeEvent
}

type oneShotLoopGuardSteering struct {
	messages []providers.Message
}

type delayedSteering struct {
	polls    int
	messages []providers.Message
}

func (s *delayedSteering) dequeueSteeringMessagesForTurn(string, string) []providers.Message {
	s.polls++
	if s.polls < 2 {
		return nil
	}
	messages := s.messages
	s.messages = nil
	return messages
}

func (s *oneShotLoopGuardSteering) dequeueSteeringMessagesForTurn(string, string) []providers.Message {
	messages := s.messages
	s.messages = nil
	return messages
}

func (e *captureRuntimeEmitter) emitEvent(kind runtimeevents.Kind, _ HookMeta, payload any) {
	e.events = append(e.events, capturedRuntimeEvent{kind: kind, payload: payload})
}

func (t *pipelineLoopGuardTool) Name() string        { return "pipeline_loop_test" }
func (t *pipelineLoopGuardTool) Description() string { return "pipeline loop test" }
func (t *pipelineLoopGuardTool) Parameters() map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": map[string]any{"value": map[string]any{"type": "string"}},
	}
}

func (t *pipelineLoopGuardTool) ToolLoopSemantics() loopguard.Semantics {
	return loopguard.SemanticsReadOnlyIdempotent
}

func (t *pipelineLoopGuardTool) Execute(context.Context, map[string]any) *tools.ToolResult {
	t.executions++
	return tools.ErrorResult("stable pipeline failure")
}

func TestPipelineLoopGuardBlocksAndPreservesToolCallResults(t *testing.T) {
	registry := tools.NewToolRegistry()
	tool := &pipelineLoopGuardTool{}
	registry.Register(tool)
	guardConfig := loopguard.DefaultConfig()
	guardConfig.HardStopsEnabled = true
	guardConfig.ExactFailureWarn = 1
	guardConfig.ExactFailureBlock = 2
	guardConfig.SameToolFailureHalt = 99
	agent := &AgentInstance{
		ID: "main", Tools: registry, Sessions: session.NewSessionManager(""),
		ToolLoopDetection: guardConfig,
	}
	ts := &turnState{
		agent: agent, agentID: "main", turnID: "turn-loop-guard",
		sessionKey: "session-loop-guard", opts: processOptions{NoHistory: true},
	}
	exec := newTurnExecution(agent, ts.opts, nil, "", nil)
	emitter := &captureRuntimeEmitter{}
	pipeline := &Pipeline{Runtime: PipelineRuntimeServices{Events: emitter}}

	for i := 1; i <= 3; i++ {
		exec.normalizedToolCalls = []providers.ToolCall{{
			ID: fmt.Sprintf("call-%d", i), Name: tool.Name(),
			Arguments: map[string]any{"value": "same"},
		}}
		if i == 3 {
			exec.normalizedToolCalls = append(exec.normalizedToolCalls, providers.ToolCall{
				ID: "call-3-skipped", Name: tool.Name(), Arguments: map[string]any{"value": "other"},
			})
			pipeline.Context.Steering = &delayedSteering{
				messages: []providers.Message{{Role: "user", Content: "change course"}},
			}
		}
		exec.allResponsesHandled = true
		if got := pipeline.ExecuteTools(
			context.Background(),
			context.Background(),
			ts,
			exec,
			i,
		); got != ToolControlContinue {
			t.Fatalf("iteration %d control = %v", i, got)
		}
	}

	if tool.executions != 2 {
		t.Fatalf("tool executions = %d, want 2", tool.executions)
	}
	if len(exec.messages) != 4 {
		t.Fatalf("tool messages = %d, want 4", len(exec.messages))
	}
	for i, message := range exec.messages[:3] {
		wantID := fmt.Sprintf("call-%d", i+1)
		if message.Role != "tool" || message.ToolCallID != wantID {
			t.Fatalf("message %d = %#v, want tool result for %s", i, message, wantID)
		}
	}
	if !strings.Contains(exec.messages[2].Content, "repeated_exact_failure_block") {
		t.Fatalf("blocked content = %q", exec.messages[2].Content)
	}
	if exec.messages[3].ToolCallID != "call-3-skipped" ||
		!strings.Contains(exec.messages[3].Content, "newer user message arrived") {
		t.Fatalf("steering-skipped result = %#v", exec.messages[3])
	}
	var decisions []ToolLoopDecisionPayload
	for _, event := range emitter.events {
		if event.kind == runtimeevents.KindAgentToolLoopDecision {
			decisions = append(decisions, event.payload.(ToolLoopDecisionPayload))
		}
	}
	if len(decisions) != 3 || decisions[len(decisions)-1].Action != "block" {
		t.Fatalf("loop decision events = %#v", decisions)
	}
	encoded, err := json.Marshal(decisions)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "same") || strings.Contains(string(encoded), "stable pipeline failure") {
		t.Fatalf("decision events exposed arguments/results: %s", encoded)
	}
}

func TestTurnExecutionsHaveIsolatedLoopGuardState(t *testing.T) {
	config := loopguard.DefaultConfig()
	config.ExactFailureWarn = 1
	agent := &AgentInstance{ToolLoopDetection: config}
	first := newTurnExecution(agent, processOptions{}, nil, "", nil)
	second := newTurnExecution(agent, processOptions{}, nil, "", nil)
	observation := loopguard.Observation{
		Tool: "read_file", Args: map[string]any{"path": "x"}, Failed: true,
	}
	if got := first.loopGuard.After(observation); got.Action != loopguard.ActionWarn {
		t.Fatalf("first decision = %#v", got)
	}
	if got := second.loopGuard.Before(
		"read_file",
		observation.Args,
		loopguard.SemanticsReadOnlyIdempotent,
	); !got.AllowsExecution() ||
		got.Count != 0 {
		t.Fatalf("second turn inherited state: %#v", got)
	}
}

func TestPipelineLoopGuardUsesHookModifiedArgumentsAndResults(t *testing.T) {
	registry := tools.NewToolRegistry()
	tool := &pipelineLoopGuardReadTool{}
	registry.Register(tool)
	config := loopguard.DefaultConfig()
	config.NoProgressWarn = 2
	agent := &AgentInstance{
		ID:                "main",
		Tools:             registry,
		Sessions:          session.NewSessionManager(""),
		ToolLoopDetection: config,
	}
	ts := &turnState{
		agent:      agent,
		agentID:    "main",
		turnID:     "turn-hook-loop",
		sessionKey: "hook-loop",
		opts:       processOptions{NoHistory: true},
	}
	exec := newTurnExecution(agent, ts.opts, nil, "", nil)
	hooks := NewHookManager(nil)
	defer hooks.Close()
	if err := hooks.Mount(NamedHook("rewrite", &toolRewriteHook{})); err != nil {
		t.Fatal(err)
	}
	pipeline := &Pipeline{Interaction: PipelineInteractionServices{Hooks: hooks}}

	for i, value := range []string{"original-one", "original-two"} {
		exec.normalizedToolCalls = []providers.ToolCall{{
			ID: fmt.Sprintf("hook-%d", i), Name: tool.Name(), Arguments: map[string]any{"text": value},
		}}
		exec.allResponsesHandled = true
		pipeline.ExecuteTools(context.Background(), context.Background(), ts, exec, i+1)
	}
	if tool.executions != 2 {
		t.Fatalf("executions = %d", tool.executions)
	}
	if !strings.Contains(exec.messages[1].Content, "read_only_no_progress_warning") ||
		!strings.Contains(exec.messages[1].Content, "after:modified") {
		t.Fatalf("second hook result = %q", exec.messages[1].Content)
	}
}

func TestPipelineLoopGuardDoesNotCountPolicyDenials(t *testing.T) {
	registry := tools.NewToolRegistry()
	tool := &pipelineLoopGuardTool{}
	registry.Register(tool)
	config := loopguard.DefaultConfig()
	config.HardStopsEnabled = true
	config.ExactFailureWarn = 1
	config.ExactFailureBlock = 1
	config.SameToolFailureHalt = 99
	agent := &AgentInstance{
		ID:                "main",
		Tools:             registry,
		Sessions:          session.NewSessionManager(""),
		ToolLoopDetection: config,
	}
	ts := &turnState{
		agent:      agent,
		agentID:    "main",
		turnID:     "turn-denial-loop",
		sessionKey: "denial-loop",
		opts:       processOptions{NoHistory: true},
	}
	exec := newTurnExecution(agent, ts.opts, nil, "", nil)
	hooks := NewHookManager(nil)
	defer hooks.Close()
	if err := hooks.Mount(NamedHook("deny", &denyToolHook{denyTools: map[string]bool{tool.Name(): true}})); err != nil {
		t.Fatal(err)
	}
	pipeline := &Pipeline{Interaction: PipelineInteractionServices{Hooks: hooks}}
	call := providers.ToolCall{ID: "denied", Name: tool.Name(), Arguments: map[string]any{"value": "same"}}
	exec.normalizedToolCalls = []providers.ToolCall{call}
	pipeline.ExecuteTools(context.Background(), context.Background(), ts, exec, 1)
	hooks.Unmount("deny")
	call.ID = "executed"
	exec.normalizedToolCalls = []providers.ToolCall{call}
	pipeline.ExecuteTools(context.Background(), context.Background(), ts, exec, 2)
	if tool.executions != 1 {
		t.Fatalf("policy denial affected loop state; executions = %d", tool.executions)
	}
	if strings.Contains(exec.messages[1].Content, "_block") {
		t.Fatalf("first executed failure was incorrectly blocked: %q", exec.messages[1].Content)
	}
}

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
		queuedSteeringDeferredToolResult,
	)
	if len(runner.messages) != 2 {
		t.Fatalf("messages len = %d, want 2", len(runner.messages))
	}
	if runner.messages[0].ToolCallID != "call-skip-1" ||
		runner.messages[1].ToolCallID != "call-skip-2" ||
		runner.messages[0].Content != queuedSteeringDeferredToolResult {
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

func TestPipelineSteeringClassifiesEveryPendingToolAndPreservesPairing(t *testing.T) {
	registry := tools.NewToolRegistry()
	readOnly := &steeringSafetyTestTool{name: "read", safety: tools.SteeringSafetyReadOnly}
	cancellable := &steeringSafetyTestTool{name: "write", safety: tools.SteeringSafetyCancellable}
	nonCancellable := &steeringSafetyTestTool{name: "commit", safety: tools.SteeringSafetyNonCancellable}
	unknown := &unknownSteeringSafetyTestTool{}
	for _, tool := range []tools.Tool{readOnly, cancellable, nonCancellable, unknown} {
		registry.Register(tool)
	}
	agent := &AgentInstance{ID: "main", Tools: registry, Sessions: session.NewSessionManager("")}
	ts := &turnState{
		agent: agent, agentID: "main", turnID: "turn-steering-safety",
		sessionKey: "session-steering-safety", opts: processOptions{NoHistory: true},
	}
	exec := newTurnExecution(agent, ts.opts, nil, "", nil)
	exec.normalizedToolCalls = []providers.ToolCall{
		{ID: "call-read", Name: "read"},
		{ID: "call-write", Name: "write"},
		{ID: "call-commit", Name: "commit"},
		{ID: "call-unknown", Name: "unknown"},
	}
	emitter := &captureRuntimeEmitter{}
	pipeline := &Pipeline{
		Context: PipelineContextServices{Steering: &oneShotLoopGuardSteering{
			messages: []providers.Message{{Role: "user", Content: "change course"}},
		}},
		Runtime: PipelineRuntimeServices{Events: emitter},
	}

	if got := pipeline.ExecuteTools(
		context.Background(),
		context.Background(),
		ts,
		exec,
		1,
	); got != ToolControlContinue {
		t.Fatalf("control = %v, want continue", got)
	}
	if readOnly.executions != 1 || nonCancellable.executions != 1 {
		t.Fatalf("safe executions = read:%d commit:%d, want 1 each", readOnly.executions, nonCancellable.executions)
	}
	if cancellable.executions != 0 || unknown.executions != 0 {
		t.Fatalf("unsafe executions = write:%d unknown:%d, want 0", cancellable.executions, unknown.executions)
	}
	if len(exec.messages) != 4 {
		t.Fatalf("tool results = %d, want one per call", len(exec.messages))
	}
	for i, call := range exec.normalizedToolCalls {
		if exec.messages[i].Role != "tool" || exec.messages[i].ToolCallID != call.ID {
			t.Fatalf("result[%d] = %#v, want source-ordered result for %s", i, exec.messages[i], call.ID)
		}
	}
	if !strings.Contains(exec.messages[1].Content, "reissue it if it is still requested") ||
		!strings.Contains(exec.messages[3].Content, "omit it only if the user canceled or replaced it") {
		t.Fatalf("deferred results do not explain reconciliation: %#v", exec.messages)
	}

	decisions := make(map[string]ToolSteeringDecisionPayload)
	for _, event := range emitter.events {
		if event.kind != runtimeevents.KindAgentToolSteeringDecision {
			continue
		}
		payload := event.payload.(ToolSteeringDecisionPayload)
		decisions[payload.ToolCallID] = payload
	}
	if len(decisions) != 4 || decisions["call-read"].Decision != "finish" ||
		decisions["call-write"].Decision != "skip" || decisions["call-commit"].Decision != "finish" ||
		decisions["call-unknown"].Classification != string(tools.SteeringSafetyUnknown) {
		t.Fatalf("decisions = %#v", decisions)
	}
}

func TestPipelineSteeringArrivingDuringBatchDoesNotCancelCompletedCall(t *testing.T) {
	registry := tools.NewToolRegistry()
	first := &steeringSafetyTestTool{name: "first-write", safety: tools.SteeringSafetyCancellable}
	second := &steeringSafetyTestTool{name: "second-write", safety: tools.SteeringSafetyCancellable}
	registry.Register(first)
	registry.Register(second)
	agent := &AgentInstance{ID: "main", Tools: registry, Sessions: session.NewSessionManager("")}
	ts := &turnState{
		agent: agent, agentID: "main", turnID: "turn-delayed-steering",
		sessionKey: "session-delayed-steering", opts: processOptions{NoHistory: true},
	}
	exec := newTurnExecution(agent, ts.opts, nil, "", nil)
	exec.normalizedToolCalls = []providers.ToolCall{
		{ID: "call-first", Name: first.Name()},
		{ID: "call-second", Name: second.Name()},
	}
	pipeline := &Pipeline{Context: PipelineContextServices{Steering: &delayedSteering{
		messages: []providers.Message{{Role: "user", Content: "stop the second write"}},
	}}}

	pipeline.ExecuteTools(context.Background(), context.Background(), ts, exec, 1)
	if first.executions != 1 || second.executions != 0 {
		t.Fatalf("executions = first:%d second:%d, want 1 and 0", first.executions, second.executions)
	}
	if len(exec.messages) != 2 || exec.messages[0].ToolCallID != "call-first" ||
		exec.messages[1].ToolCallID != "call-second" {
		t.Fatalf("tool results = %#v, want one source-ordered result per call", exec.messages)
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
