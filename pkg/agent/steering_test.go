package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/agent/interfaces"
	"github.com/sipeed/picoclaw/pkg/audio/asr"
	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/media"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/routing"
	"github.com/sipeed/picoclaw/pkg/session"
	"github.com/sipeed/picoclaw/pkg/tools"
)

func userMessageContains(msg providers.Message, text string) bool {
	return msg.Role == "user" && strings.Contains(msg.Content, text)
}

// --- steeringQueue unit tests ---

func TestSteeringQueue_PushDequeue_OneAtATime(t *testing.T) {
	sq := newSteeringQueue(SteeringOneAtATime)

	scope := testSessionScope("queue")
	sq.pushScopeWithSender(scope, providers.Message{Role: "user", Content: "msg1"}, "")
	sq.pushScopeWithSender(scope, providers.Message{Role: "user", Content: "msg2"}, "")
	sq.pushScopeWithSender(scope, providers.Message{Role: "user", Content: "msg3"}, "")

	if sq.len() != 3 {
		t.Fatalf("expected 3 messages, got %d", sq.len())
	}

	msgs := sq.dequeueScope(scope)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message in one-at-a-time mode, got %d", len(msgs))
	}
	if msgs[0].Content != "msg1" {
		t.Fatalf("expected 'msg1', got %q", msgs[0].Content)
	}
	if sq.len() != 2 {
		t.Fatalf("expected 2 remaining, got %d", sq.len())
	}

	msgs = sq.dequeueScope(scope)
	if len(msgs) != 1 || msgs[0].Content != "msg2" {
		t.Fatalf("expected 'msg2', got %v", msgs)
	}

	msgs = sq.dequeueScope(scope)
	if len(msgs) != 1 || msgs[0].Content != "msg3" {
		t.Fatalf("expected 'msg3', got %v", msgs)
	}

	msgs = sq.dequeueScope(scope)
	if msgs != nil {
		t.Fatalf("expected nil from empty queue, got %v", msgs)
	}
}

func TestSteeringQueue_PushDequeue_All(t *testing.T) {
	sq := newSteeringQueue(SteeringAll)

	scope := testSessionScope("queue")
	sq.pushScopeWithSender(scope, providers.Message{Role: "user", Content: "msg1"}, "")
	sq.pushScopeWithSender(scope, providers.Message{Role: "user", Content: "msg2"}, "")
	sq.pushScopeWithSender(scope, providers.Message{Role: "user", Content: "msg3"}, "")

	msgs := sq.dequeueScope(scope)
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages in all mode, got %d", len(msgs))
	}
	if msgs[0].Content != "msg1" || msgs[1].Content != "msg2" || msgs[2].Content != "msg3" {
		t.Fatalf("unexpected messages: %v", msgs)
	}

	if sq.len() != 0 {
		t.Fatalf("expected 0 remaining, got %d", sq.len())
	}

	msgs = sq.dequeueScope(scope)
	if msgs != nil {
		t.Fatalf("expected nil from empty queue, got %v", msgs)
	}
}

func TestSteeringQueue_EmptyDequeue(t *testing.T) {
	sq := newSteeringQueue(SteeringOneAtATime)
	if msgs := sq.dequeueScope(testSessionScope("queue")); msgs != nil {
		t.Fatalf("expected nil, got %v", msgs)
	}
}

func TestSteeringQueue_SetMode(t *testing.T) {
	sq := newSteeringQueue(SteeringOneAtATime)
	if sq.getMode() != SteeringOneAtATime {
		t.Fatalf("expected one-at-a-time, got %v", sq.getMode())
	}

	sq.setMode(SteeringAll)
	if sq.getMode() != SteeringAll {
		t.Fatalf("expected all, got %v", sq.getMode())
	}

	// Push two messages and verify all-mode drains them
	scope := testSessionScope("queue")
	sq.pushScopeWithSender(scope, providers.Message{Role: "user", Content: "a"}, "")
	sq.pushScopeWithSender(scope, providers.Message{Role: "user", Content: "b"}, "")

	msgs := sq.dequeueScope(scope)
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages after mode switch, got %d", len(msgs))
	}
}

func TestSteeringQueue_ConcurrentAccess(t *testing.T) {
	sq := newSteeringQueue(SteeringOneAtATime)

	var wg sync.WaitGroup
	const n = MaxQueueSize

	// Push from multiple goroutines
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sq.pushScopeWithSender(
				testSessionScope("queue"),
				providers.Message{Role: "user", Content: fmt.Sprintf("msg%d", i)},
				"",
			)
		}(i)
	}
	wg.Wait()

	if sq.len() != n {
		t.Fatalf("expected %d messages, got %d", n, sq.len())
	}

	// Drain from multiple goroutines
	var drained int
	var mu sync.Mutex
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if msgs := sq.dequeueScope(testSessionScope("queue")); len(msgs) > 0 {
				mu.Lock()
				drained += len(msgs)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if drained != n {
		t.Fatalf("expected to drain %d messages, got %d", n, drained)
	}
}

func TestSteeringQueue_Overflow(t *testing.T) {
	sq := newSteeringQueue(SteeringOneAtATime)

	// Fill the queue up to its maximum capacity
	for i := 0; i < MaxQueueSize; i++ {
		err := sq.pushScopeWithSender(
			testSessionScope("queue"),
			providers.Message{Role: "user", Content: fmt.Sprintf("msg%d", i)},
			"",
		)
		if err != nil {
			t.Fatalf("unexpected error pushing message %d: %v", i, err)
		}
	}

	// Sanity check: ensure the queue is actually full
	if sq.len() != MaxQueueSize {
		t.Fatalf("expected queue length %d, got %d", MaxQueueSize, sq.len())
	}

	// Attempt to push one more message, which MUST fail
	err := sq.pushScopeWithSender(
		testSessionScope("queue"), providers.Message{Role: "user", Content: "overflow_msg"}, "",
	)

	// Assert the error happened and is the exact one we expect
	if err == nil {
		t.Fatal("expected an error when pushing to a full queue, but got nil")
	}

	expectedErr := "steering queue is full"
	if err.Error() != expectedErr {
		t.Errorf("expected error message %q, got %q", expectedErr, err.Error())
	}
}

func TestSteeringQueue_DequeueForTurnPrefersCurrentSenderAndKeepsOthersQueued(t *testing.T) {
	sq := newSteeringQueue(SteeringOneAtATime)

	if err := sq.pushScopeWithSender(
		testSessionScope("session-1"),
		providers.Message{Role: "user", Content: "b1"},
		"user-b",
	); err != nil {
		t.Fatalf("push b1: %v", err)
	}
	if err := sq.pushScopeWithSender(
		testSessionScope("session-1"),
		providers.Message{Role: "user", Content: "a1"},
		"user-a",
	); err != nil {
		t.Fatalf("push a1: %v", err)
	}
	if err := sq.pushScopeWithSender(
		testSessionScope("session-1"),
		providers.Message{Role: "user", Content: "a2"},
		"user-a",
	); err != nil {
		t.Fatalf("push a2: %v", err)
	}

	msgs := sq.dequeueScopeForTurn(testSessionScope("session-1"), "user-a")
	if len(msgs) != 2 {
		t.Fatalf("dequeued messages = %d, want 2", len(msgs))
	}
	if msgs[0].Content != "a1" || msgs[1].Content != "a2" {
		t.Fatalf("dequeued contents = [%q, %q], want [a1 a2]", msgs[0].Content, msgs[1].Content)
	}

	remaining := sq.dequeueScope(testSessionScope("session-1"))
	if len(remaining) != 1 || remaining[0].Content != "b1" {
		t.Fatalf("remaining queue = %#v, want only b1", remaining)
	}
}

func TestSteeringQueue_DequeueContinuationBatchesOldestDeferredSender(t *testing.T) {
	sq := newSteeringQueue(SteeringOneAtATime)

	if err := sq.pushScopeWithSender(
		testSessionScope("session-1"),
		providers.Message{Role: "user", Content: "b1"},
		"user-b",
	); err != nil {
		t.Fatalf("push b1: %v", err)
	}
	if err := sq.pushScopeWithSender(
		testSessionScope("session-1"),
		providers.Message{Role: "user", Content: "c1"},
		"user-c",
	); err != nil {
		t.Fatalf("push c1: %v", err)
	}
	if err := sq.pushScopeWithSender(
		testSessionScope("session-1"),
		providers.Message{Role: "user", Content: "b2"},
		"user-b",
	); err != nil {
		t.Fatalf("push b2: %v", err)
	}

	msgs := sq.dequeueScopeForContinuation(testSessionScope("session-1"))
	if len(msgs) != 2 {
		t.Fatalf("dequeued messages = %d, want 2", len(msgs))
	}
	if msgs[0].Content != "b1" || msgs[1].Content != "b2" {
		t.Fatalf("dequeued contents = [%q, %q], want [b1 b2]", msgs[0].Content, msgs[1].Content)
	}

	remaining := sq.dequeueScope(testSessionScope("session-1"))
	if len(remaining) != 1 || remaining[0].Content != "c1" {
		t.Fatalf("remaining queue = %#v, want only c1", remaining)
	}
}

func TestSteeringQueue_DequeueContinuationWithSenderlessHeadBatchesOldestSenderCohort(
	t *testing.T,
) {
	sq := newSteeringQueue(SteeringOneAtATime)

	if err := sq.pushScopeWithSender(
		testSessionScope("session-1"),
		providers.Message{Role: "user", Content: "legacy"},
		"",
	); err != nil {
		t.Fatalf("push legacy: %v", err)
	}
	if err := sq.pushScopeWithSender(
		testSessionScope("session-1"),
		providers.Message{Role: "user", Content: "b1"},
		"user-b",
	); err != nil {
		t.Fatalf("push b1: %v", err)
	}
	if err := sq.pushScopeWithSender(
		testSessionScope("session-1"),
		providers.Message{Role: "user", Content: "c1"},
		"user-c",
	); err != nil {
		t.Fatalf("push c1: %v", err)
	}
	if err := sq.pushScopeWithSender(
		testSessionScope("session-1"),
		providers.Message{Role: "user", Content: "b2"},
		"user-b",
	); err != nil {
		t.Fatalf("push b2: %v", err)
	}

	msgs := sq.dequeueScopeForContinuation(testSessionScope("session-1"))
	if len(msgs) != 3 {
		t.Fatalf("dequeued messages = %d, want 3", len(msgs))
	}
	if msgs[0].Content != "legacy" || msgs[1].Content != "b1" || msgs[2].Content != "b2" {
		t.Fatalf(
			"dequeued contents = [%q, %q, %q], want [legacy b1 b2]",
			msgs[0].Content,
			msgs[1].Content,
			msgs[2].Content,
		)
	}

	remaining := sq.dequeueScope(testSessionScope("session-1"))
	if len(remaining) != 1 || remaining[0].Content != "c1" {
		t.Fatalf("remaining queue = %#v, want only c1", remaining)
	}
}

func TestParseSteeringMode(t *testing.T) {
	tests := []struct {
		input    string
		expected SteeringMode
	}{
		{"", SteeringOneAtATime},
		{"one-at-a-time", SteeringOneAtATime},
		{"all", SteeringAll},
		{"unknown", SteeringOneAtATime},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := parseSteeringMode(tt.input); got != tt.expected {
				t.Fatalf("parseSteeringMode(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

// --- AgentLoop steering integration tests ---

func TestAgentLoop_Steer_Enqueues(t *testing.T) {
	al, cfg, msgBus, provider, cleanup := newTestAgentLoop(t)
	defer cleanup()

	if cfg == nil {
		t.Fatal("expected config to be initialized")
	}
	if msgBus == nil {
		t.Fatal("expected message bus to be initialized")
	}
	if provider == nil {
		t.Fatal("expected provider to be initialized")
	}

	agent := al.GetRegistry().GetDefaultAgent()
	if err := al.enqueueSteeringMessageWithSender(
		newRuntimeSessionScope(agent.Workspace, "test-session"), agent.ID, "",
		providers.Message{Role: "user", Content: "interrupt me"},
	); err != nil {
		t.Fatal(err)
	}

	if al.steering.len() != 1 {
		t.Fatalf("expected 1 steering message, got %d", al.steering.len())
	}

	msgs := al.dequeueSteeringMessagesForScope(
		newRuntimeSessionScope(agent.Workspace, "test-session"),
	)
	if len(msgs) != 1 || !userMessageContains(msgs[0], "interrupt me") {
		t.Fatalf("unexpected dequeued message: %v", msgs)
	}
}

func TestAgentLoop_SteeringMode_GetSet(t *testing.T) {
	al, cfg, msgBus, provider, cleanup := newTestAgentLoop(t)
	defer cleanup()

	if cfg == nil {
		t.Fatal("expected config to be initialized")
	}
	if msgBus == nil {
		t.Fatal("expected message bus to be initialized")
	}
	if provider == nil {
		t.Fatal("expected provider to be initialized")
	}

	if al.SteeringMode() != SteeringOneAtATime {
		t.Fatalf("expected default mode one-at-a-time, got %v", al.SteeringMode())
	}

	al.SetSteeringMode(SteeringAll)
	if al.SteeringMode() != SteeringAll {
		t.Fatalf("expected all mode, got %v", al.SteeringMode())
	}
}

func TestAgentLoop_SteeringMode_ConfiguredFromConfig(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
				SteeringMode:      "all",
			},
		},
	}

	msgBus := bus.NewMessageBus()
	provider := &mockProvider{}
	al := NewAgentLoop(cfg, msgBus, provider)

	if al.SteeringMode() != SteeringAll {
		t.Fatalf("expected 'all' mode from config, got %v", al.SteeringMode())
	}
}

func TestAgentLoop_DequeueSteeringDoesNotAckInboundSpool(t *testing.T) {
	tmpDir := t.TempDir()
	msgBus := bus.NewMessageBus()
	defer msgBus.Close()
	spool, err := bus.NewInboundSpool(filepath.Join(tmpDir, "spool"))
	if err != nil {
		t.Fatalf("NewInboundSpool failed: %v", err)
	}
	msgBus.SetInboundSpool(spool)
	al := NewAgentLoop(&config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}, msgBus, &mockProvider{})

	if err := msgBus.PublishInbound(context.Background(), bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  "test",
			ChatID:   "chat1",
			SenderID: "user1",
		},
		Content: "queued follow-up",
	}); err != nil {
		t.Fatalf("PublishInbound failed: %v", err)
	}
	inbound := <-msgBus.InboundChan()
	if inbound.SpoolID == "" {
		t.Fatal("expected inbound SpoolID")
	}
	agent := al.GetRegistry().GetDefaultAgent()
	if err := al.enqueueSteeringMessageWithSender(
		newRuntimeSessionScope(agent.Workspace, "session-1"), agent.ID, "", providers.Message{
			Role:           "user",
			Content:        inbound.Content,
			InboundSpoolID: inbound.SpoolID,
		}); err != nil {
		t.Fatalf("enqueueSteeringMessage failed: %v", err)
	}
	msgs := al.dequeueSteeringMessagesForScope(testRuntimeSessionScope(al, "session-1"))
	if len(msgs) != 1 {
		t.Fatalf("dequeued messages = %d, want 1", len(msgs))
	}
	waitForSpoolEntries(t, spool.Dir(), "*.processing", 1)
	al.ackAcceptedSteeringMessages(context.Background(), msgs)
	waitForSpoolEntries(t, spool.Dir(), "*.processing", 0)
}

func TestAgentLoop_Continue_NoMessages(t *testing.T) {
	al, cfg, msgBus, provider, cleanup := newTestAgentLoop(t)
	defer cleanup()

	if cfg == nil {
		t.Fatal("expected config to be initialized")
	}
	if msgBus == nil {
		t.Fatal("expected message bus to be initialized")
	}
	if provider == nil {
		t.Fatal("expected provider to be initialized")
	}

	resp, err := al.Continue(
		context.Background(), cfg.Agents.Defaults.Workspace, "test-session", "test", "chat1",
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != "" {
		t.Fatalf("expected empty response for no steering messages, got %q", resp)
	}
}

func TestAgentLoop_Continue_WithMessages(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	msgBus := bus.NewMessageBus()
	provider := &simpleMockProvider{response: "continued response"}
	al := NewAgentLoop(cfg, msgBus, provider)

	agent := al.GetRegistry().GetDefaultAgent()
	if enqueueErr := al.enqueueSteeringMessageWithSender(
		newRuntimeSessionScope(agent.Workspace, "test-session"), agent.ID, "",
		providers.Message{Role: "user", Content: "new direction"},
	); enqueueErr != nil {
		t.Fatal(enqueueErr)
	}

	resp, err := al.Continue(context.Background(), tmpDir, "test-session", "test", "chat1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != "continued response" {
		t.Fatalf("expected 'continued response', got %q", resp)
	}
}

// slowTool simulates a tool that takes some time to execute.
type slowTool struct {
	name     string
	duration time.Duration
	execCh   chan struct{} // closed when Execute starts
}

func (t *slowTool) Name() string        { return t.name }
func (t *slowTool) Description() string { return "slow tool for testing" }
func (t *slowTool) Parameters() map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": map[string]any{},
	}
}

func (t *slowTool) Execute(ctx context.Context, args map[string]any) *tools.ToolResult {
	if t.execCh != nil {
		close(t.execCh)
	}
	time.Sleep(t.duration)
	return tools.SilentResult(fmt.Sprintf("executed %s", t.name))
}

// toolCallProvider returns an LLM response with tool calls on the first call,
// then a direct response on subsequent calls.
type toolCallProvider struct {
	mu        sync.Mutex
	calls     int
	toolCalls []providers.ToolCall
	finalResp string
}

func (m *toolCallProvider) Chat(
	ctx context.Context,
	messages []providers.Message,
	tools []providers.ToolDefinition,
	model string,
	opts map[string]any,
) (*providers.LLMResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++

	if m.calls == 1 && len(m.toolCalls) > 0 {
		return &providers.LLMResponse{
			Content:   "",
			ToolCalls: m.toolCalls,
		}, nil
	}

	return &providers.LLMResponse{
		Content:   m.finalResp,
		ToolCalls: []providers.ToolCall{},
	}, nil
}

func (m *toolCallProvider) GetDefaultModel() string {
	return "tool-call-mock"
}

type gracefulCaptureProvider struct {
	mu                 sync.Mutex
	calls              int
	toolCalls          []providers.ToolCall
	finalResp          string
	terminalMessages   []providers.Message
	terminalToolsCount int
}

func (p *gracefulCaptureProvider) Chat(
	ctx context.Context,
	messages []providers.Message,
	tools []providers.ToolDefinition,
	model string,
	opts map[string]any,
) (*providers.LLMResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++

	if p.calls == 1 {
		return &providers.LLMResponse{
			ToolCalls: p.toolCalls,
		}, nil
	}

	p.terminalMessages = append([]providers.Message(nil), messages...)
	p.terminalToolsCount = len(tools)
	return &providers.LLMResponse{
		Content: p.finalResp,
	}, nil
}

func (p *gracefulCaptureProvider) GetDefaultModel() string {
	return "graceful-capture-mock"
}

type lateSteeringProvider struct {
	mu                 sync.Mutex
	calls              int
	firstCallStarted   chan struct{}
	releaseFirstCall   chan struct{}
	firstStartOnce     sync.Once
	secondCallMessages []providers.Message
}

func (p *lateSteeringProvider) Chat(
	ctx context.Context,
	messages []providers.Message,
	tools []providers.ToolDefinition,
	model string,
	opts map[string]any,
) (*providers.LLMResponse, error) {
	p.mu.Lock()
	p.calls++
	call := p.calls
	p.mu.Unlock()

	if call == 1 {
		p.firstStartOnce.Do(func() { close(p.firstCallStarted) })
		<-p.releaseFirstCall
		return &providers.LLMResponse{Content: "first response"}, nil
	}

	p.mu.Lock()
	p.secondCallMessages = append([]providers.Message(nil), messages...)
	p.mu.Unlock()
	return &providers.LLMResponse{Content: "continued response"}, nil
}

func (p *lateSteeringProvider) GetDefaultModel() string {
	return "late-steering-mock"
}

type stagedContinuationProvider struct {
	mu                sync.Mutex
	calls             int
	firstCallStarted  chan struct{}
	releaseFirstCall  chan struct{}
	secondCallStarted chan struct{}
	releaseSecondCall chan struct{}
	secondMessages    []providers.Message
	thirdMessages     []providers.Message
}

func (p *stagedContinuationProvider) Chat(
	ctx context.Context,
	messages []providers.Message,
	tools []providers.ToolDefinition,
	model string,
	opts map[string]any,
) (*providers.LLMResponse, error) {
	p.mu.Lock()
	p.calls++
	call := p.calls
	firstStarted := p.firstCallStarted
	releaseFirst := p.releaseFirstCall
	secondStarted := p.secondCallStarted
	releaseSecond := p.releaseSecondCall
	if call == 2 {
		p.secondMessages = append([]providers.Message(nil), messages...)
	}
	if call == 3 {
		p.thirdMessages = append([]providers.Message(nil), messages...)
	}
	p.mu.Unlock()

	switch call {
	case 1:
		if firstStarted != nil {
			close(firstStarted)
		}
		<-releaseFirst
		return &providers.LLMResponse{Content: "first response"}, nil
	case 2:
		if secondStarted != nil {
			close(secondStarted)
		}
		<-releaseSecond
		return &providers.LLMResponse{Content: "response for B"}, nil
	default:
		return &providers.LLMResponse{Content: "response for C"}, nil
	}
}

func (p *stagedContinuationProvider) GetDefaultModel() string {
	return "staged-continuation-mock"
}

type saveFailOnContentSessionStore struct {
	session.SessionStore
	mu      sync.Mutex
	content string
	err     error
	failed  bool
}

func (s *saveFailOnContentSessionStore) Save(sessionKey string) error {
	s.mu.Lock()
	if !s.failed {
		for _, msg := range s.SessionStore.GetHistory(sessionKey) {
			if strings.Contains(msg.Content, s.content) {
				s.failed = true
				s.mu.Unlock()
				return s.err
			}
		}
	}
	s.mu.Unlock()
	return s.SessionStore.Save(sessionKey)
}

type recordingMessageBus struct {
	interfaces.MessageBus

	mu          sync.Mutex
	releasedIDs []string
}

func (b *recordingMessageBus) ReleaseInbound(
	ctx context.Context,
	msg bus.InboundMessage,
	cause error,
) error {
	b.mu.Lock()
	b.releasedIDs = append(b.releasedIDs, msg.SpoolID)
	b.mu.Unlock()
	return b.MessageBus.ReleaseInbound(ctx, msg, cause)
}

func (b *recordingMessageBus) releaseCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.releasedIDs)
}

type fixedTranscriber struct {
	text string
}

func (f *fixedTranscriber) Name() string { return "fixed" }

func (f *fixedTranscriber) Transcribe(
	ctx context.Context,
	audioFilePath string,
) (*asr.TranscriptionResponse, error) {
	return &asr.TranscriptionResponse{Text: f.text}, nil
}

type blockingDirectProvider struct {
	mu             sync.Mutex
	firstStartOnce sync.Once
	calls          int
	firstStarted   chan struct{}
	releaseFirst   chan struct{}
	firstResp      string
	finalResp      string
	secondMessages []providers.Message
}

func (p *blockingDirectProvider) Chat(
	ctx context.Context,
	messages []providers.Message,
	tools []providers.ToolDefinition,
	model string,
	opts map[string]any,
) (*providers.LLMResponse, error) {
	p.mu.Lock()
	p.calls++
	call := p.calls
	firstStarted := p.firstStarted
	releaseFirst := p.releaseFirst
	firstResp := p.firstResp
	finalResp := p.finalResp
	if call == 1 && p.firstStarted != nil {
		p.firstStartOnce.Do(func() { close(p.firstStarted) })
	}
	if call == 2 {
		p.secondMessages = append([]providers.Message(nil), messages...)
	}
	p.mu.Unlock()

	if call == 1 {
		select {
		case <-releaseFirst:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &providers.LLMResponse{Content: firstResp}, nil
	}

	_ = firstStarted
	return &providers.LLMResponse{Content: finalResp}, nil
}

func (p *blockingDirectProvider) GetDefaultModel() string {
	return "blocking-direct-mock"
}

type interruptibleTool struct {
	name    string
	started chan struct{}
	once    sync.Once
}

func (t *interruptibleTool) Name() string        { return t.name }
func (t *interruptibleTool) Description() string { return "interruptible tool for testing" }
func (t *interruptibleTool) Parameters() map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": map[string]any{},
	}
}

func (t *interruptibleTool) Execute(ctx context.Context, args map[string]any) *tools.ToolResult {
	if t.started != nil {
		t.once.Do(func() { close(t.started) })
	}
	<-ctx.Done()
	return tools.ErrorResult(ctx.Err().Error()).WithError(ctx.Err())
}

func TestAgentLoop_Steering_SkipsRemainingTools(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	tool1ExecCh := make(chan struct{})
	tool1 := &slowTool{name: "tool_one", duration: 50 * time.Millisecond, execCh: tool1ExecCh}
	tool2 := &slowTool{name: "tool_two", duration: 50 * time.Millisecond}

	provider := &toolCallProvider{
		toolCalls: []providers.ToolCall{
			{
				ID:   "call_1",
				Type: "function",
				Name: "tool_one",
				Function: &providers.FunctionCall{
					Name:      "tool_one",
					Arguments: "{}",
				},
				Arguments: map[string]any{},
			},
			{
				ID:   "call_2",
				Type: "function",
				Name: "tool_two",
				Function: &providers.FunctionCall{
					Name:      "tool_two",
					Arguments: "{}",
				},
				Arguments: map[string]any{},
			},
		},
		finalResp: "steered response",
	}

	msgBus := bus.NewMessageBus()
	al := NewAgentLoop(cfg, msgBus, provider)
	al.RegisterTool(tool1)
	al.RegisterTool(tool2)

	// Start processing in a goroutine
	type result struct {
		resp string
		err  error
	}
	resultCh := make(chan result, 1)

	go func() {
		resp, err := al.ProcessDirectWithChannel(
			context.Background(),
			"do something",
			"test-session",
			"test",
			"chat1",
		)
		resultCh <- result{resp, err}
	}()

	// Wait for tool_one to start executing, then enqueue a steering message
	select {
	case <-tool1ExecCh:
		// tool_one has started executing
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for tool_one to start")
	}

	if err := steerActiveForTest(al, providers.Message{Role: "user", Content: "change course"}); err != nil {
		t.Fatal(err)
	}

	// Get the result
	select {
	case r := <-resultCh:
		if r.err != nil {
			t.Fatalf("unexpected error: %v", r.err)
		}
		if r.resp != "steered response" {
			t.Fatalf("expected 'steered response', got %q", r.resp)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for agent loop to complete")
	}

	// The provider should have been called twice:
	// 1. first call returned tool calls
	// 2. second call (after steering) returned the final response
	provider.mu.Lock()
	calls := provider.calls
	provider.mu.Unlock()
	if calls != 2 {
		t.Fatalf("expected 2 provider calls, got %d", calls)
	}
}

func TestAgentLoop_Steering_InitialPoll(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	// Provider that captures messages it receives
	var capturedMessages []providers.Message
	var capMu sync.Mutex
	provider := &capturingMockProvider{
		response: "ack",
		captureFn: func(msgs []providers.Message) {
			capMu.Lock()
			capturedMessages = make([]providers.Message, len(msgs))
			copy(capturedMessages, msgs)
			capMu.Unlock()
		},
	}

	msgBus := bus.NewMessageBus()
	al := NewAgentLoop(cfg, msgBus, provider)

	// Enqueue a steering message before processing starts
	target, err := al.resolveInboundDispatchTarget(bus.InboundMessage{
		Context: bus.InboundContext{
			Channel: "test", ChatID: "chat1", ChatType: "direct", SenderID: "cron",
		},
		SessionKey: "test-session",
	})
	if err != nil {
		t.Fatal(err)
	}
	if enqueueErr := al.enqueueSteeringMessageWithSender(
		target.runtimeSessionScope(), target.Agent.ID, "",
		providers.Message{Role: "user", Content: "pre-enqueued steering"},
	); enqueueErr != nil {
		t.Fatal(enqueueErr)
	}

	// Process a normal message - the initial steering poll should inject the steering message
	_, err = al.ProcessDirectWithChannel(
		context.Background(),
		"initial message",
		"test-session",
		"test",
		"chat1",
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The steering message should have been injected into the conversation
	capMu.Lock()
	msgs := capturedMessages
	capMu.Unlock()

	// Look for the steering message in the captured messages
	found := false
	for _, m := range msgs {
		if strings.Contains(m.Content, "pre-enqueued steering") {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected steering message to be injected into conversation context")
	}
}

func TestAgentLoop_Run_AutoContinuesLateSteeringMessage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	msgBus := bus.NewMessageBus()
	spoolDir := filepath.Join(tmpDir, "state", "ingress-spool", "inbound")
	spool, err := bus.NewInboundSpool(spoolDir)
	if err != nil {
		t.Fatalf("NewInboundSpool failed: %v", err)
	}
	msgBus.SetInboundSpool(spool)
	provider := &lateSteeringProvider{
		firstCallStarted: make(chan struct{}),
		releaseFirstCall: make(chan struct{}),
	}
	al := NewAgentLoop(cfg, msgBus, provider)

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- al.Run(runCtx)
	}()

	first := bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  "test",
			ChatID:   "chat1",
			ChatType: "direct",
			SenderID: "user1",
		},
		Content: "first message",
	}
	late := bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  "test",
			ChatID:   "chat1",
			ChatType: "direct",
			SenderID: "user1",
		},
		Content: "late append",
	}

	pubCtx, pubCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer pubCancel()
	if err := msgBus.PublishInbound(pubCtx, first); err != nil {
		t.Fatalf("publish first inbound: %v", err)
	}

	select {
	case <-provider.firstCallStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first provider call to start")
	}

	if err := msgBus.PublishInbound(pubCtx, late); err != nil {
		t.Fatalf("publish late inbound: %v", err)
	}
	waitForSpoolEntries(t, spoolDir, "*.processing", 2)

	close(provider.releaseFirstCall)

	subCtx, subCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer subCancel()

	var out1 bus.OutboundMessage
	select {
	case out1 = <-msgBus.OutboundChan():
	case <-subCtx.Done():
		t.Fatal("expected outbound response")
	}
	if out1.Content != "continued response" {
		t.Fatalf("expected continued response, got %q", out1.Content)
	}
	if got := strings.TrimSpace(out1.Context.Raw[metadataKeyMessageKind]); got != messageKindFinalReply {
		t.Fatalf("expected continued response to be marked %q, got %q", messageKindFinalReply, got)
	}

	noExtraCtx, cancelNoExtra := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelNoExtra()
	select {
	case out2 := <-msgBus.OutboundChan():
		t.Fatalf(
			"expected stale direct response to be suppressed, got extra outbound %q",
			out2.Content,
		)
	case <-noExtraCtx.Done():
	}

	cancelRun()
	select {
	case err := <-runErrCh:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Run to stop")
	}

	provider.mu.Lock()
	calls := provider.calls
	secondMessages := append([]providers.Message(nil), provider.secondCallMessages...)
	provider.mu.Unlock()

	if calls != 2 {
		t.Fatalf("expected 2 provider calls, got %d", calls)
	}

	foundLateMessage := false
	for _, msg := range secondMessages {
		if userMessageContains(msg, "late append") {
			foundLateMessage = true
			break
		}
	}
	if !foundLateMessage {
		t.Fatal("expected queued late message to be processed in an automatic follow-up turn")
	}
	waitForSpoolEntries(t, spoolDir, "*.processing", 0)
}

func TestAgentLoop_Run_BatchesDeferredMessagesBySenderIntoOneContinuationTurn(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	msgBus := bus.NewMessageBus()
	spoolDir := filepath.Join(tmpDir, "state", "ingress-spool", "inbound")
	spool, err := bus.NewInboundSpool(spoolDir)
	if err != nil {
		t.Fatalf("NewInboundSpool failed: %v", err)
	}
	msgBus.SetInboundSpool(spool)
	provider := &lateSteeringProvider{
		firstCallStarted: make(chan struct{}),
		releaseFirstCall: make(chan struct{}),
	}
	al := NewAgentLoop(cfg, msgBus, provider)

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- al.Run(runCtx)
	}()

	first := bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  "test",
			ChatID:   "chat1",
			ChatType: "group",
			SenderID: "user-a",
		},
		Content: "message from A",
	}
	b1 := bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  "test",
			ChatID:   "chat1",
			ChatType: "group",
			SenderID: "user-b",
		},
		Content: "message B1",
	}
	b2 := bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  "test",
			ChatID:   "chat1",
			ChatType: "group",
			SenderID: "user-b",
		},
		Content: "message B2",
	}

	pubCtx, pubCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer pubCancel()
	if err := msgBus.PublishInbound(pubCtx, first); err != nil {
		t.Fatalf("publish first inbound: %v", err)
	}

	select {
	case <-provider.firstCallStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first provider call to start")
	}

	if err := msgBus.PublishInbound(pubCtx, b1); err != nil {
		t.Fatalf("publish b1 inbound: %v", err)
	}
	if err := msgBus.PublishInbound(pubCtx, b2); err != nil {
		t.Fatalf("publish b2 inbound: %v", err)
	}
	waitForSpoolEntries(t, spoolDir, "*.processing", 3)

	close(provider.releaseFirstCall)

	subCtx, subCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer subCancel()

	select {
	case <-msgBus.OutboundChan():
	case <-subCtx.Done():
		t.Fatal("expected outbound response")
	}

	cancelRun()
	select {
	case err := <-runErrCh:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Run to stop")
	}

	provider.mu.Lock()
	calls := provider.calls
	secondMessages := append([]providers.Message(nil), provider.secondCallMessages...)
	provider.mu.Unlock()

	if calls != 2 {
		t.Fatalf("expected 2 provider calls, got %d", calls)
	}

	foundB1 := false
	foundB2 := false
	for _, msg := range secondMessages {
		if msg.Role != "user" {
			continue
		}
		switch {
		case userMessageContains(msg, "message B1"):
			foundB1 = true
		case userMessageContains(msg, "message B2"):
			foundB2 = true
		}
	}
	if !foundB1 || !foundB2 {
		t.Fatalf(
			"expected grouped continuation to contain both B messages, got %#v",
			secondMessages,
		)
	}
	waitForSpoolEntries(t, spoolDir, "*.processing", 0)
}

func TestAgentLoop_Run_ContinuationPreservesSenderAffinityAcrossDeferredTurns(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	msgBus := bus.NewMessageBus()
	spoolDir := filepath.Join(tmpDir, "state", "ingress-spool", "inbound")
	spool, err := bus.NewInboundSpool(spoolDir)
	if err != nil {
		t.Fatalf("NewInboundSpool failed: %v", err)
	}
	msgBus.SetInboundSpool(spool)

	provider := &stagedContinuationProvider{
		firstCallStarted:  make(chan struct{}),
		releaseFirstCall:  make(chan struct{}),
		secondCallStarted: make(chan struct{}),
		releaseSecondCall: make(chan struct{}),
	}
	al := NewAgentLoop(cfg, msgBus, provider)

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- al.Run(runCtx)
	}()

	msgA := bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  "test",
			ChatID:   "chat1",
			ChatType: "group",
			SenderID: "user-a",
		},
		Content: "message from A",
	}
	msgB1 := bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  "test",
			ChatID:   "chat1",
			ChatType: "group",
			SenderID: "user-b",
		},
		Content: "message B1",
	}
	msgB2 := bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  "test",
			ChatID:   "chat1",
			ChatType: "group",
			SenderID: "user-b",
		},
		Content: "message B2",
	}
	msgC := bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  "test",
			ChatID:   "chat1",
			ChatType: "group",
			SenderID: "user-c",
		},
		Content: "message C1",
	}

	pubCtx, pubCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer pubCancel()
	if err := msgBus.PublishInbound(pubCtx, msgA); err != nil {
		t.Fatalf("publish A inbound: %v", err)
	}

	select {
	case <-provider.firstCallStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first provider call to start")
	}

	if err := msgBus.PublishInbound(pubCtx, msgB1); err != nil {
		t.Fatalf("publish B1 inbound: %v", err)
	}
	if err := msgBus.PublishInbound(pubCtx, msgB2); err != nil {
		t.Fatalf("publish B2 inbound: %v", err)
	}

	close(provider.releaseFirstCall)

	select {
	case <-provider.secondCallStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for continuation turn for sender B")
	}

	if err := msgBus.PublishInbound(pubCtx, msgC); err != nil {
		t.Fatalf("publish C inbound: %v", err)
	}

	close(provider.releaseSecondCall)

	deadline := time.Now().Add(5 * time.Second)
	for {
		provider.mu.Lock()
		calls := provider.calls
		provider.mu.Unlock()
		if calls >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for chained continuation turns to finish")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancelRun()
	select {
	case err := <-runErrCh:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Run to stop")
	}

	provider.mu.Lock()
	calls := provider.calls
	secondMessages := append([]providers.Message(nil), provider.secondMessages...)
	thirdMessages := append([]providers.Message(nil), provider.thirdMessages...)
	provider.mu.Unlock()

	if calls != 3 {
		t.Fatalf("expected 3 provider calls, got %d", calls)
	}

	secondHasB1 := false
	secondHasB2 := false
	secondHasC1 := false
	for _, msg := range secondMessages {
		if msg.Role != "user" {
			continue
		}
		switch {
		case userMessageContains(msg, "message B1"):
			secondHasB1 = true
		case userMessageContains(msg, "message B2"):
			secondHasB2 = true
		case userMessageContains(msg, "message C1"):
			secondHasC1 = true
		}
	}
	if !secondHasB1 || !secondHasB2 {
		t.Fatalf("expected B continuation to contain both B messages, got %#v", secondMessages)
	}
	if secondHasC1 {
		t.Fatalf("expected B continuation to exclude C message, got %#v", secondMessages)
	}

	thirdHasC1 := false
	for _, msg := range thirdMessages {
		if userMessageContains(msg, "message C1") {
			thirdHasC1 = true
			break
		}
	}
	if !thirdHasC1 {
		t.Fatalf("expected third continuation to contain C message, got %#v", thirdMessages)
	}
	waitForSpoolEntries(t, spoolDir, "*.processing", 0)
}

func TestAgentLoop_Run_ReleasesInjectedSteeringSpoolOnContinuationSaveFailure(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	msgBus := bus.NewMessageBus()
	spoolDir := filepath.Join(tmpDir, "state", "ingress-spool", "inbound")
	spool, err := bus.NewInboundSpool(spoolDir)
	if err != nil {
		t.Fatalf("NewInboundSpool failed: %v", err)
	}
	msgBus.SetInboundSpool(spool)

	provider := &lateSteeringProvider{
		firstCallStarted: make(chan struct{}),
		releaseFirstCall: make(chan struct{}),
	}
	al := NewAgentLoop(cfg, msgBus, provider)
	monitoredBus := &recordingMessageBus{MessageBus: al.bus}
	al.bus = monitoredBus

	agent := al.registry.GetDefaultAgent()
	if agent == nil {
		t.Fatal("expected default agent")
	}
	agent.Sessions = &saveFailOnContentSessionStore{
		SessionStore: session.NewSessionManager(""),
		content:      "continued response",
		err:          errors.New("session save failed on continuation"),
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- al.Run(runCtx)
	}()

	first := bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  "test",
			ChatID:   "chat1",
			ChatType: "group",
			SenderID: "user-a",
		},
		Content: "message from A",
	}
	late := bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  "test",
			ChatID:   "chat1",
			ChatType: "group",
			SenderID: "user-b",
		},
		Content: "late append",
	}

	pubCtx, pubCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer pubCancel()
	if err := msgBus.PublishInbound(pubCtx, first); err != nil {
		t.Fatalf("publish first inbound: %v", err)
	}

	select {
	case <-provider.firstCallStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first provider call to start")
	}
	active := al.GetActiveTurn()
	if active == nil || active.SessionKey == "" {
		t.Fatal("expected active turn with session key")
	}
	sessionKey := active.SessionKey

	if err := msgBus.PublishInbound(pubCtx, late); err != nil {
		t.Fatalf("publish late inbound: %v", err)
	}
	waitForSpoolEntries(t, spoolDir, "*.processing", 2)
	deadline := time.Now().Add(2 * time.Second)
	for al.pendingSteeringCountForScope(testRuntimeSessionScope(al, sessionKey)) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for late message to enter steering queue")
		}
		time.Sleep(10 * time.Millisecond)
	}

	close(provider.releaseFirstCall)

	deadline = time.Now().Add(5 * time.Second)
	for {
		processing, procErr := filepath.Glob(filepath.Join(spoolDir, "*.processing"))
		if procErr != nil {
			t.Fatalf("glob processing entries: %v", procErr)
		}
		if len(processing) == 0 && monitoredBus.releaseCount() == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"expected continuation save failure to release steering spool entry, processing=%v releases=%d",
				processing,
				monitoredBus.releaseCount(),
			)
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancelRun()
	select {
	case err := <-runErrCh:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Run to stop")
	}
}

func waitForSpoolEntries(t *testing.T, dir, pattern string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		matches, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil {
			t.Fatalf("glob spool entries: %v", err)
		}
		if len(matches) == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"spool entries matching %q = %d, want %d (%v)",
				pattern,
				len(matches),
				want,
				matches,
			)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAgentLoop_Run_QueuedVoiceMessageIsTranscribedBeforeSteering(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	msgBus := bus.NewMessageBus()
	provider := &lateSteeringProvider{
		firstCallStarted: make(chan struct{}),
		releaseFirstCall: make(chan struct{}),
	}
	al := NewAgentLoop(cfg, msgBus, provider)

	store := media.NewFileMediaStore()
	audioPath := filepath.Join(tmpDir, "voice.ogg")
	if err := os.WriteFile(audioPath, []byte("fake audio"), 0o644); err != nil {
		t.Fatalf("write audio fixture: %v", err)
	}
	ref, err := store.Store(audioPath, media.MediaMeta{
		Filename:      "voice.ogg",
		ContentType:   "audio/ogg",
		CleanupPolicy: media.CleanupPolicyForgetOnly,
	}, "scope-voice")
	if err != nil {
		t.Fatalf("store audio fixture: %v", err)
	}
	al.SetMediaStore(store)
	al.SetTranscriber(&fixedTranscriber{text: "and also two pieces of bread"})

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- al.Run(runCtx)
	}()

	first := bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  "test",
			ChatID:   "chat1",
			ChatType: "direct",
			SenderID: "user1",
		},
		Content: "first meal",
	}
	late := bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  "test",
			ChatID:   "chat1",
			ChatType: "direct",
			SenderID: "user1",
		},
		Content: "[voice]",
		Media:   []string{ref},
	}

	pubCtx, pubCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer pubCancel()
	if err := msgBus.PublishInbound(pubCtx, first); err != nil {
		t.Fatalf("publish first inbound: %v", err)
	}

	select {
	case <-provider.firstCallStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first provider call to start")
	}

	if err := msgBus.PublishInbound(pubCtx, late); err != nil {
		t.Fatalf("publish late voice inbound: %v", err)
	}

	close(provider.releaseFirstCall)

	subCtx, subCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer subCancel()
	select {
	case <-msgBus.OutboundChan():
	case <-subCtx.Done():
		t.Fatal("expected outbound response")
	}

	cancelRun()
	select {
	case err := <-runErrCh:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Run to stop")
	}

	provider.mu.Lock()
	secondMessages := append([]providers.Message(nil), provider.secondCallMessages...)
	provider.mu.Unlock()

	foundTranscribedVoice := false
	for _, msg := range secondMessages {
		if msg.Role == "user" &&
			strings.Contains(msg.Content, "[voice: and also two pieces of bread]") {
			foundTranscribedVoice = true
			break
		}
	}
	if !foundTranscribedVoice {
		t.Fatalf(
			"expected queued voice message to be transcribed before steering injection, got %#v",
			secondMessages,
		)
	}
}

func TestAgentLoop_Run_PendingStopStillContinuesQueuedFollowUp(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
				MaxParallelTurns:  1,
			},
		},
	}

	msgBus := bus.NewMessageBus()
	provider := &lateSteeringProvider{
		firstCallStarted: make(chan struct{}),
		releaseFirstCall: make(chan struct{}),
	}
	al := NewAgentLoop(cfg, msgBus, provider)

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- al.Run(runCtx)
	}()
	defer func() {
		cancelRun()
		select {
		case err := <-runErrCh:
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for Run to stop")
		}
	}()

	blockerSessionKey := session.BuildOpaqueSessionKey("agent:main:test:blocker")
	targetSessionKey := session.BuildOpaqueSessionKey("agent:main:test:target")
	blockerCtx := bus.InboundContext{
		Channel:  "test",
		ChatID:   "blocker-chat",
		ChatType: "direct",
		SenderID: "user1",
	}
	targetCtx := bus.InboundContext{
		Channel:  "test",
		ChatID:   "target-chat",
		ChatType: "direct",
		SenderID: "user1",
	}

	if err := msgBus.PublishInbound(context.Background(), bus.InboundMessage{
		Context:    blockerCtx,
		Content:    "block worker pool",
		SessionKey: blockerSessionKey,
	}); err != nil {
		t.Fatalf("PublishInbound(blocker) error = %v", err)
	}

	select {
	case <-provider.firstCallStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for blocker turn to start")
	}

	if err := msgBus.PublishInbound(context.Background(), bus.InboundMessage{
		Context:    targetCtx,
		Content:    "skip this turn",
		SessionKey: targetSessionKey,
	}); err != nil {
		t.Fatalf("PublishInbound(target start) error = %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		ts := al.getActiveTurnState(testRuntimeSessionScope(al, targetSessionKey))
		if ts != nil && strings.HasPrefix(ts.turnID, pendingTurnPrefix) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for pending placeholder")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := msgBus.PublishInbound(context.Background(), bus.InboundMessage{
		Context:    targetCtx,
		Content:    "/stop",
		SessionKey: targetSessionKey,
	}); err != nil {
		t.Fatalf("PublishInbound(/stop) error = %v", err)
	}

	deadline = time.Now().Add(2 * time.Second)
	stopSeen := false
	for !stopSeen {
		select {
		case outbound := <-msgBus.OutboundChan():
			if outbound.ChatID == "target-chat" &&
				outbound.Content == "Task stopped. Current task was canceled." {
				stopSeen = true
			}
		case <-time.After(10 * time.Millisecond):
			if time.Now().After(deadline) {
				t.Fatal("timeout waiting for /stop reply")
			}
		}
	}

	if err := msgBus.PublishInbound(context.Background(), bus.InboundMessage{
		Context:    targetCtx,
		Content:    "run this instead",
		SessionKey: targetSessionKey,
	}); err != nil {
		t.Fatalf("PublishInbound(follow-up) error = %v", err)
	}

	deadline = time.Now().Add(2 * time.Second)
	for al.pendingSteeringCountForScope(testRuntimeSessionScope(al, targetSessionKey)) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for follow-up to enter scoped steering queue")
		}
		time.Sleep(10 * time.Millisecond)
	}

	close(provider.releaseFirstCall)

	deadline = time.Now().Add(5 * time.Second)
	followUpSeen := false
	for !followUpSeen {
		select {
		case outbound := <-msgBus.OutboundChan():
			if outbound.ChatID == "target-chat" && outbound.Content == "continued response" {
				followUpSeen = true
			}
		case <-time.After(10 * time.Millisecond):
			if time.Now().After(deadline) {
				t.Fatal("timeout waiting for queued follow-up continuation")
			}
		}
	}

	deadline = time.Now().Add(2 * time.Second)
	for {
		if al.GetActiveTurnBySession(targetSessionKey) == nil &&
			al.pendingSteeringCountForScope(testRuntimeSessionScope(al, targetSessionKey)) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for target session to go idle")
		}
		time.Sleep(10 * time.Millisecond)
	}

	provider.mu.Lock()
	calls := provider.calls
	secondMessages := append([]providers.Message(nil), provider.secondCallMessages...)
	provider.mu.Unlock()

	if calls != 2 {
		t.Fatalf("expected 2 provider calls (blocker + continuation), got %d", calls)
	}

	foundFollowUp := false
	for _, msg := range secondMessages {
		if userMessageContains(msg, "run this instead") {
			foundFollowUp = true
		}
		if userMessageContains(msg, "skip this turn") {
			t.Fatalf("unexpected canceled message in continuation context: %q", msg.Content)
		}
	}
	if !foundFollowUp {
		t.Fatal("expected queued follow-up to be processed after pending stop")
	}
}

func TestAgentLoop_Steering_DirectResponseContinuesWithQueuedMessage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	sessionKey := session.BuildMainSessionKey(routing.DefaultAgentID)
	provider := &blockingDirectProvider{
		firstStarted: make(chan struct{}),
		releaseFirst: make(chan struct{}),
		firstResp:    "stale direct response",
		finalResp:    "fresh response after steering",
	}

	msgBus := bus.NewMessageBus()
	al := NewAgentLoop(cfg, msgBus, provider)

	resultCh := make(chan struct {
		resp string
		err  error
	}, 1)
	go func() {
		resp, err := al.ProcessDirectWithChannel(
			context.Background(),
			"initial request",
			sessionKey,
			"test",
			"chat1",
		)
		resultCh <- struct {
			resp string
			err  error
		}{resp: resp, err: err}
	}()

	select {
	case <-provider.firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first LLM call to start")
	}

	if err := steerActiveForTest(al, providers.Message{Role: "user", Content: "follow-up instruction"}); err != nil {
		t.Fatalf("Steer failed: %v", err)
	}
	close(provider.releaseFirst)

	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("unexpected error: %v", result.err)
		}
		if result.resp != "fresh response after steering" {
			t.Fatalf("expected refreshed response, got %q", result.resp)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for ProcessDirectWithChannel")
	}

	provider.mu.Lock()
	calls := provider.calls
	provider.mu.Unlock()
	if calls != 2 {
		t.Fatalf("expected 2 provider calls, got %d", calls)
	}

	if msgs := al.dequeueSteeringMessagesForScope(testRuntimeSessionScope(al, sessionKey)); len(msgs) != 0 {
		t.Fatalf("expected steering queue to be empty after continuation, got %v", msgs)
	}
}

func TestAgentLoop_Steering_DirectResponseInjectsQueuedMessageOnce(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	sessionKey := session.BuildMainSessionKey(routing.DefaultAgentID)
	provider := &blockingDirectProvider{
		firstStarted: make(chan struct{}),
		releaseFirst: make(chan struct{}),
		firstResp:    "stale direct response",
		finalResp:    "fresh response after steering",
	}

	msgBus := bus.NewMessageBus()
	al := NewAgentLoop(cfg, msgBus, provider)

	resultCh := make(chan struct {
		resp string
		err  error
	}, 1)
	go func() {
		resp, err := al.ProcessDirectWithChannel(
			context.Background(),
			"initial request",
			sessionKey,
			"test",
			"chat1",
		)
		resultCh <- struct {
			resp string
			err  error
		}{resp: resp, err: err}
	}()

	select {
	case <-provider.firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first LLM call to start")
	}

	if err := steerActiveForTest(al, providers.Message{Role: "user", Content: "single follow-up"}); err != nil {
		t.Fatalf("Steer failed: %v", err)
	}
	close(provider.releaseFirst)

	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("unexpected error: %v", result.err)
		}
		if result.resp != "fresh response after steering" {
			t.Fatalf("expected refreshed response, got %q", result.resp)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for ProcessDirectWithChannel")
	}

	provider.mu.Lock()
	secondMessages := append([]providers.Message(nil), provider.secondMessages...)
	provider.mu.Unlock()

	count := 0
	for _, msg := range secondMessages {
		if userMessageContains(msg, "single follow-up") {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected queued steering once in second LLM call, got %d", count)
	}
}

func TestAgentLoop_AgentForRuntimeScope_UsesStoredScopeMetadata(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
			List: []config.AgentConfig{
				{ID: "sales", Default: true},
				{ID: "support"},
			},
		},
	}

	al := NewAgentLoop(cfg, bus.NewMessageBus(), &mockProvider{})
	support, ok := al.registry.GetAgent("support")
	if !ok || support == nil {
		t.Fatal("expected support agent")
	}

	metaStore, ok := support.Sessions.(session.MetadataAwareSessionStore)
	if !ok {
		t.Fatal("support session store does not support metadata")
	}

	alias := "agent:support:slack:channel:c001"
	key := session.BuildOpaqueSessionKey(alias)
	scope := &session.SessionScope{
		Version:    session.ScopeVersionV1,
		AgentID:    "support",
		Channel:    "slack",
		Account:    "default",
		Dimensions: []string{"chat"},
		Values: map[string]string{
			"chat": "channel:c001",
		},
	}
	metaStore.EnsureSessionMetadata(key, scope, []string{alias})

	got := al.agentForRuntimeScope(newRuntimeSessionScope(support.Workspace, key), "")
	if got == nil {
		t.Fatal("agentForRuntimeScope() returned nil")
	}
	if got.ID != "support" {
		t.Fatalf("agentForRuntimeScope() = %q, want %q", got.ID, "support")
	}
}

func TestAgentLoop_Continue_PreservesSteeringMedia(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	store := media.NewFileMediaStore()
	pngPath := filepath.Join(tmpDir, "steer.png")
	pngHeader := []byte{
		0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A,
		0x00, 0x00, 0x00, 0x0D,
		0x49, 0x48, 0x44, 0x52,
		0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x02,
		0x00, 0x00, 0x00,
		0x90, 0x77, 0x53, 0xDE,
	}
	if err = os.WriteFile(pngPath, pngHeader, 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	ref, err := store.Store(
		pngPath,
		media.MediaMeta{Filename: "steer.png", ContentType: "image/png"},
		"test",
	)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	var capturedMessages []providers.Message
	var capMu sync.Mutex
	provider := &capturingMockProvider{
		response: "ack",
		captureFn: func(msgs []providers.Message) {
			capMu.Lock()
			defer capMu.Unlock()
			capturedMessages = append([]providers.Message(nil), msgs...)
		},
	}

	sessionKey := session.BuildMainSessionKey(routing.DefaultAgentID)
	msgBus := bus.NewMessageBus()
	al := NewAgentLoop(cfg, msgBus, provider)
	al.SetMediaStore(store)

	agent := al.GetRegistry().GetDefaultAgent()
	if err = al.enqueueSteeringMessageWithSender(
		newRuntimeSessionScope(agent.Workspace, sessionKey), agent.ID, "", providers.Message{
			Role:    "user",
			Content: "describe this image",
			Media:   []string{ref},
		}); err != nil {
		t.Fatalf("Steer failed: %v", err)
	}

	resp, err := al.Continue(context.Background(), tmpDir, sessionKey, "test", "chat1")
	if err != nil {
		t.Fatalf("Continue failed: %v", err)
	}
	if resp != "ack" {
		t.Fatalf("expected ack, got %q", resp)
	}

	capMu.Lock()
	msgs := append([]providers.Message(nil), capturedMessages...)
	capMu.Unlock()

	foundResolvedMedia := false
	for _, msg := range msgs {
		if msg.Role != "user" || !strings.Contains(msg.Content, "describe this image") {
			continue
		}
		if strings.Contains(msg.Content, "[image:") {
			foundResolvedMedia = true
			break
		}
	}
	if !foundResolvedMedia {
		t.Fatal("expected continue path to inject image path tag into the provider request")
	}

	defaultAgent := al.registry.GetDefaultAgent()
	if defaultAgent == nil {
		t.Fatal("expected default agent")
	}
	history := defaultAgent.Sessions.GetHistory(sessionKey)
	foundOriginalRef := false
	for _, msg := range history {
		if msg.Role == "user" && len(msg.Media) == 1 && msg.Media[0] == ref {
			foundOriginalRef = true
			break
		}
	}
	if !foundOriginalRef {
		t.Fatal("expected original steering media ref to be preserved in session history")
	}
}

func TestAgentLoop_InterruptGraceful_UsesTerminalNoToolCall(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	tool1ExecCh := make(chan struct{})
	tool1 := &slowTool{name: "tool_one", duration: 50 * time.Millisecond, execCh: tool1ExecCh}
	tool2 := &slowTool{name: "tool_two", duration: 50 * time.Millisecond}

	provider := &gracefulCaptureProvider{
		toolCalls: []providers.ToolCall{
			{
				ID:   "call_1",
				Type: "function",
				Name: "tool_one",
				Function: &providers.FunctionCall{
					Name:      "tool_one",
					Arguments: "{}",
				},
				Arguments: map[string]any{},
			},
			{
				ID:   "call_2",
				Type: "function",
				Name: "tool_two",
				Function: &providers.FunctionCall{
					Name:      "tool_two",
					Arguments: "{}",
				},
				Arguments: map[string]any{},
			},
		},
		finalResp: "graceful summary",
	}

	msgBus := bus.NewMessageBus()
	al := NewAgentLoop(cfg, msgBus, provider)
	al.RegisterTool(tool1)
	al.RegisterTool(tool2)
	sessionKey := session.BuildMainSessionKey(routing.DefaultAgentID)

	runtimeCh, closeRuntimeEvents := subscribeRuntimeEventsForTest(
		t,
		al,
		32,
		runtimeevents.KindAgentInterruptReceived,
		runtimeevents.KindAgentTurnEnd,
	)
	defer closeRuntimeEvents()

	type result struct {
		resp string
		err  error
	}
	resultCh := make(chan result, 1)
	go func() {
		resp, err := al.ProcessDirectWithChannel(
			context.Background(),
			"do something",
			sessionKey,
			"test",
			"chat1",
		)
		resultCh <- result{resp: resp, err: err}
	}()

	select {
	case <-tool1ExecCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for tool_one to start")
	}

	active := al.GetActiveTurn()
	if active == nil {
		t.Fatal("expected active turn while tool is running")
	}
	if active.SessionKey != sessionKey {
		t.Fatalf("expected active session %q, got %q", sessionKey, active.SessionKey)
	}
	if active.Channel != "test" || active.ChatID != "chat1" {
		t.Fatalf("unexpected active turn target: %#v", active)
	}

	if err := al.InterruptGraceful("wrap it up"); err != nil {
		t.Fatalf("InterruptGraceful failed: %v", err)
	}

	select {
	case r := <-resultCh:
		if r.err != nil {
			t.Fatalf("unexpected error: %v", r.err)
		}
		if r.resp != "graceful summary" {
			t.Fatalf("expected graceful summary, got %q", r.resp)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for graceful interrupt result")
	}

	if active := al.GetActiveTurn(); active != nil {
		t.Fatalf("expected no active turn after completion, got %#v", active)
	}

	provider.mu.Lock()
	terminalMessages := append([]providers.Message(nil), provider.terminalMessages...)
	terminalToolsCount := provider.terminalToolsCount
	calls := provider.calls
	provider.mu.Unlock()

	if calls != 2 {
		t.Fatalf("expected 2 provider calls, got %d", calls)
	}
	if terminalToolsCount != 0 {
		t.Fatalf(
			"expected graceful terminal call to disable tools, got %d tool defs",
			terminalToolsCount,
		)
	}

	foundHint := false
	foundSkipped := false
	expectedHint := "Interrupt requested. Stop scheduling tools and provide a short final summary.\n\n" +
		"Interrupt hint: wrap it up"
	for _, msg := range terminalMessages {
		if msg.Role == "user" && msg.Content == expectedHint {
			foundHint = true
		}
		if msg.Role == "tool" && msg.ToolCallID == "call_2" &&
			msg.Content == "Skipped due to graceful interrupt." {
			foundSkipped = true
		}
	}
	if !foundHint {
		t.Fatal("expected graceful terminal call to include interrupt hint message")
	}
	if !foundSkipped {
		t.Fatal("expected remaining tool to be marked as skipped after graceful interrupt")
	}

	events := collectRuntimeEventStream(runtimeCh)
	interruptEvt, ok := findRuntimeEvent(events, runtimeevents.KindAgentInterruptReceived)
	if !ok {
		t.Fatal("expected interrupt received event")
	}
	interruptPayload, ok := interruptEvt.Payload.(InterruptReceivedPayload)
	if !ok {
		t.Fatalf("expected InterruptReceivedPayload, got %T", interruptEvt.Payload)
	}
	if interruptPayload.Kind != InterruptKindGraceful {
		t.Fatalf("expected graceful interrupt payload, got %q", interruptPayload.Kind)
	}

	turnEndEvt, ok := findRuntimeEvent(events, runtimeevents.KindAgentTurnEnd)
	if !ok {
		t.Fatal("expected turn end event")
	}
	turnEndPayload, ok := turnEndEvt.Payload.(TurnEndPayload)
	if !ok {
		t.Fatalf("expected TurnEndPayload, got %T", turnEndEvt.Payload)
	}
	if turnEndPayload.Status != TurnEndStatusCompleted {
		t.Fatalf("expected completed turn after graceful interrupt, got %q", turnEndPayload.Status)
	}
}

func TestAgentLoop_InterruptHard_RestoresSession(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	msgBus := bus.NewMessageBus()
	provider := &toolCallProvider{
		toolCalls: []providers.ToolCall{
			{
				ID:   "call_1",
				Type: "function",
				Name: "cancel_tool",
				Function: &providers.FunctionCall{
					Name:      "cancel_tool",
					Arguments: "{}",
				},
				Arguments: map[string]any{},
			},
		},
		finalResp: "should not happen",
	}

	al := NewAgentLoop(cfg, msgBus, provider)
	started := make(chan struct{})
	al.RegisterTool(&interruptibleTool{name: "cancel_tool", started: started})
	sessionKey := session.BuildMainSessionKey(routing.DefaultAgentID)

	defaultAgent := al.registry.GetDefaultAgent()
	if defaultAgent == nil {
		t.Fatal("expected default agent")
	}

	originalHistory := []providers.Message{
		{Role: "user", Content: "before"},
		{Role: "assistant", Content: "after"},
	}
	defaultAgent.Sessions.SetHistory(sessionKey, originalHistory)

	originalHistory = defaultAgent.Sessions.GetHistory(sessionKey)

	runtimeCh, closeRuntimeEvents := subscribeRuntimeEventsForTest(
		t,
		al,
		16,
		runtimeevents.KindAgentInterruptReceived,
		runtimeevents.KindAgentTurnEnd,
	)
	defer closeRuntimeEvents()

	type result struct {
		resp string
		err  error
	}
	resultCh := make(chan result, 1)
	go func() {
		resp, err := al.ProcessDirectWithChannel(
			context.Background(),
			"do work",
			sessionKey,
			"test",
			"chat1",
		)
		resultCh <- result{resp: resp, err: err}
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for interruptible tool to start")
	}

	if active := al.GetActiveTurn(); active == nil {
		t.Fatal("expected active turn before hard abort")
	}

	if err := al.InterruptHard(); err != nil {
		t.Fatalf("InterruptHard failed: %v", err)
	}

	select {
	case r := <-resultCh:
		if r.err != nil {
			t.Fatalf("unexpected error: %v", r.err)
		}
		if r.resp != "" {
			t.Fatalf("expected no final response after hard abort, got %q", r.resp)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for hard abort result")
	}

	if active := al.GetActiveTurn(); active != nil {
		t.Fatalf("expected no active turn after hard abort, got %#v", active)
	}

	finalHistory := defaultAgent.Sessions.GetHistory(sessionKey)
	if !reflect.DeepEqual(finalHistory, originalHistory) {
		t.Fatalf("expected history rollback after hard abort, got %#v", finalHistory)
	}

	events := collectRuntimeEventStream(runtimeCh)
	interruptEvt, ok := findRuntimeEvent(events, runtimeevents.KindAgentInterruptReceived)
	if !ok {
		t.Fatal("expected interrupt received event")
	}
	interruptPayload, ok := interruptEvt.Payload.(InterruptReceivedPayload)
	if !ok {
		t.Fatalf("expected InterruptReceivedPayload, got %T", interruptEvt.Payload)
	}
	if interruptPayload.Kind != InterruptKindHard {
		t.Fatalf("expected hard interrupt payload, got %q", interruptPayload.Kind)
	}

	turnEndEvt, ok := findRuntimeEvent(events, runtimeevents.KindAgentTurnEnd)
	if !ok {
		t.Fatal("expected turn end event")
	}
	turnEndPayload, ok := turnEndEvt.Payload.(TurnEndPayload)
	if !ok {
		t.Fatalf("expected TurnEndPayload, got %T", turnEndEvt.Payload)
	}
	if turnEndPayload.Status != TurnEndStatusAborted {
		t.Fatalf("expected aborted turn, got %q", turnEndPayload.Status)
	}
}

func TestAgentLoop_StopCommand_AbortsActiveTurnAndClearsQueuedSteering(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	msgBus := bus.NewMessageBus()
	provider := &toolCallProvider{
		toolCalls: []providers.ToolCall{
			{
				ID:   "call_1",
				Type: "function",
				Name: "cancel_tool",
				Function: &providers.FunctionCall{
					Name:      "cancel_tool",
					Arguments: "{}",
				},
				Arguments: map[string]any{},
			},
		},
		finalResp: "should not continue",
	}

	al := NewAgentLoop(cfg, msgBus, provider)
	started := make(chan struct{})
	al.RegisterTool(&interruptibleTool{name: "cancel_tool", started: started})
	sessionKey := session.BuildMainSessionKey(routing.DefaultAgentID)

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- al.Run(runCtx)
	}()
	defer func() {
		cancelRun()
		select {
		case err := <-runErrCh:
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for Run to stop")
		}
	}()

	baseMsg := testInboundMessage(bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  "test",
			ChatID:   "chat1",
			ChatType: "direct",
			SenderID: "user1",
		},
		SessionKey: sessionKey,
	})

	if err := msgBus.PublishInbound(context.Background(), bus.InboundMessage{
		Context:    baseMsg.Context,
		Content:    "do work",
		SessionKey: sessionKey,
	}); err != nil {
		t.Fatalf("PublishInbound(start) error = %v", err)
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for interruptible tool to start")
	}

	if err := msgBus.PublishInbound(context.Background(), bus.InboundMessage{
		Context:    baseMsg.Context,
		Content:    "follow up after cancel",
		SessionKey: sessionKey,
	}); err != nil {
		t.Fatalf("PublishInbound(follow-up) error = %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for al.pendingSteeringCountForScope(testRuntimeSessionScope(al, sessionKey)) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for follow-up message to enter steering queue")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := msgBus.PublishInbound(context.Background(), bus.InboundMessage{
		Context:    baseMsg.Context,
		Content:    "/stop",
		SessionKey: sessionKey,
	}); err != nil {
		t.Fatalf("PublishInbound(/stop) error = %v", err)
	}

	select {
	case outbound := <-msgBus.OutboundChan():
		want := "Task stopped. \"do work\" was canceled."
		if outbound.Content != want {
			t.Fatalf("stop reply = %q, want %q", outbound.Content, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for /stop reply")
	}

	deadline = time.Now().Add(5 * time.Second)
	for al.GetActiveTurnBySession(sessionKey) != nil {
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for active turn to stop")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := al.pendingSteeringCountForScope(testRuntimeSessionScope(al, sessionKey)); got != 0 {
		t.Fatalf("expected cleared steering queue, got %d pending message(s)", got)
	}

	select {
	case outbound := <-msgBus.OutboundChan():
		t.Fatalf("unexpected outbound after stop: %q", outbound.Content)
	case <-time.After(300 * time.Millisecond):
	}

	provider.mu.Lock()
	calls := provider.calls
	provider.mu.Unlock()
	if calls != 1 {
		t.Fatalf("expected provider to stop before follow-up turn, got %d calls", calls)
	}
}

// capturingMockProvider captures messages sent to Chat for inspection.
type capturingMockProvider struct {
	response  string
	calls     int
	captureFn func([]providers.Message)
}

func (m *capturingMockProvider) Chat(
	ctx context.Context,
	messages []providers.Message,
	tools []providers.ToolDefinition,
	model string,
	opts map[string]any,
) (*providers.LLMResponse, error) {
	m.calls++
	if m.captureFn != nil {
		m.captureFn(messages)
	}
	return &providers.LLMResponse{
		Content:   m.response,
		ToolCalls: []providers.ToolCall{},
	}, nil
}

func (m *capturingMockProvider) GetDefaultModel() string {
	return "capturing-mock"
}

func TestAgentLoop_Steering_SkippedToolsHaveErrorResults(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	execCh := make(chan struct{})
	tool1 := &slowTool{name: "slow_tool", duration: 50 * time.Millisecond, execCh: execCh}
	tool2 := &slowTool{name: "skipped_tool", duration: 50 * time.Millisecond}

	// Provider that captures messages on the second call (after tools)
	var secondCallMessages []providers.Message
	var capMu sync.Mutex
	callCount := 0

	provider := &toolCallProvider{
		toolCalls: []providers.ToolCall{
			{
				ID:   "call_1",
				Type: "function",
				Name: "slow_tool",
				Function: &providers.FunctionCall{
					Name:      "slow_tool",
					Arguments: "{}",
				},
				Arguments: map[string]any{},
			},
			{
				ID:   "call_2",
				Type: "function",
				Name: "skipped_tool",
				Function: &providers.FunctionCall{
					Name:      "skipped_tool",
					Arguments: "{}",
				},
				Arguments: map[string]any{},
			},
		},
		finalResp: "done",
	}

	// Wrap provider to capture messages on second call
	wrappedProvider := &wrappingProvider{
		inner: provider,
		onChat: func(msgs []providers.Message) {
			capMu.Lock()
			callCount++
			if callCount >= 2 {
				secondCallMessages = make([]providers.Message, len(msgs))
				copy(secondCallMessages, msgs)
			}
			capMu.Unlock()
		},
	}

	msgBus := bus.NewMessageBus()
	al := NewAgentLoop(cfg, msgBus, wrappedProvider)
	al.RegisterTool(tool1)
	al.RegisterTool(tool2)

	resultCh := make(chan string, 1)
	go func() {
		resp, _ := al.ProcessDirectWithChannel(
			context.Background(), "go", "test-session", "test", "chat1",
		)
		resultCh <- resp
	}()

	<-execCh
	if err := steerActiveForTest(al, providers.Message{Role: "user", Content: "interrupt!"}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-resultCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}

	// Check that the skipped tool result message is in the conversation
	capMu.Lock()
	msgs := secondCallMessages
	capMu.Unlock()

	foundSkipped := false
	for _, m := range msgs {
		if m.Role == "tool" && m.ToolCallID == "call_2" &&
			m.Content == queuedSteeringDeferredToolResult {
			foundSkipped = true
			break
		}
	}
	if !foundSkipped {
		// Log what we actually got
		for i, m := range msgs {
			t.Logf(
				"msg[%d]: role=%s toolCallID=%s content=%s",
				i,
				m.Role,
				m.ToolCallID,
				truncate(m.Content, 80),
			)
		}
		t.Fatal("expected skipped tool result for call_2")
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// wrappingProvider wraps another provider to hook into Chat calls.
type wrappingProvider struct {
	inner  providers.LLMProvider
	onChat func([]providers.Message)
}

func (w *wrappingProvider) Chat(
	ctx context.Context,
	messages []providers.Message,
	tools []providers.ToolDefinition,
	model string,
	opts map[string]any,
) (*providers.LLMResponse, error) {
	if w.onChat != nil {
		w.onChat(messages)
	}
	return w.inner.Chat(ctx, messages, tools, model, opts)
}

func (w *wrappingProvider) GetDefaultModel() string {
	return w.inner.GetDefaultModel()
}

// Ensure NormalizeToolCall handles our test tool calls.
func init() {
	// This is a no-op init; we just need the tool call tests to work
	// with the proper argument serialization.
	_ = json.Marshal
}
