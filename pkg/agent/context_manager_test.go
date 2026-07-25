package agent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/providers"
)

// ---------------------------------------------------------------------------
// Factory registry tests
// ---------------------------------------------------------------------------

func TestRegisterContextManager_Success(t *testing.T) {
	cleanup := resetCMRegistry()
	defer cleanup()

	factory := func(cfg json.RawMessage, al *AgentLoop) (ContextManager, error) {
		return &noopContextManager{}, nil
	}
	if err := RegisterContextManager("test_cm", factory); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	f, ok := lookupContextManager("test_cm")
	if !ok {
		t.Fatal("expected factory to be registered")
	}
	if f == nil {
		t.Fatal("expected non-nil factory")
	}
}

func TestRegisterContextManager_EmptyName(t *testing.T) {
	cleanup := resetCMRegistry()
	defer cleanup()

	err := RegisterContextManager("", func(cfg json.RawMessage, al *AgentLoop) (ContextManager, error) {
		return &noopContextManager{}, nil
	})
	if err == nil {
		t.Fatal("expected error for empty name")
	}
	if !strings.Contains(err.Error(), "name is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRegisterContextManager_NilFactory(t *testing.T) {
	cleanup := resetCMRegistry()
	defer cleanup()

	err := RegisterContextManager("nil_factory", nil)
	if err == nil {
		t.Fatal("expected error for nil factory")
	}
	if !strings.Contains(err.Error(), "factory is nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRegisterContextManager_Duplicate(t *testing.T) {
	cleanup := resetCMRegistry()
	defer cleanup()

	factory := func(cfg json.RawMessage, al *AgentLoop) (ContextManager, error) {
		return &noopContextManager{}, nil
	}
	if err := RegisterContextManager("dup_cm", factory); err != nil {
		t.Fatalf("first registration failed: %v", err)
	}
	err := RegisterContextManager("dup_cm", factory)
	if err == nil {
		t.Fatal("expected error for duplicate registration")
	}
	if !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLookupContextManager_Unknown(t *testing.T) {
	cleanup := resetCMRegistry()
	defer cleanup()

	_, ok := lookupContextManager("nonexistent")
	if ok {
		t.Fatal("expected lookup to fail for unknown name")
	}
}

// ---------------------------------------------------------------------------
// resolveContextManager tests
// ---------------------------------------------------------------------------

func TestResolveContextManager_Default(t *testing.T) {
	cleanup := resetCMRegistry()
	defer cleanup()

	factory := func(_ json.RawMessage, _ *AgentLoop) (ContextManager, error) {
		return &noopContextManager{}, nil
	}
	if err := RegisterContextManager("seahorse", factory); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         t.TempDir(),
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
				ContextManager:    "",
			},
		},
	}
	al := newCMTestAgentLoop(cfg)

	if al.contextManagerInitErr != nil {
		t.Fatalf("default context manager failed: %v", al.contextManagerInitErr)
	}
	if _, ok := al.contextManager.(*noopContextManager); !ok {
		t.Fatalf("expected registered Seahorse manager, got %T", al.contextManager)
	}
}

func TestResolveContextManager_None(t *testing.T) {
	cleanup := resetCMRegistry()
	defer cleanup()

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         t.TempDir(),
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
				ContextManager:    "none",
			},
		},
	}
	al := newCMTestAgentLoop(cfg)

	if al.contextManagerInitErr != nil {
		t.Fatalf("none context manager failed: %v", al.contextManagerInitErr)
	}
	if _, ok := al.contextManager.(*noneContextManager); !ok {
		t.Fatalf("expected *noneContextManager, got %T", al.contextManager)
	}
}

func TestResolveContextManager_UnknownFailsClosed(t *testing.T) {
	cleanup := resetCMRegistry()
	defer cleanup()

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         t.TempDir(),
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
				ContextManager:    "unknown_cm",
			},
		},
	}
	al := newCMTestAgentLoop(cfg)

	if al.contextManagerInitErr == nil {
		t.Fatal("expected unknown context manager error")
	}
	if _, ok := al.contextManager.(*failedContextManager); !ok {
		t.Fatalf("expected *failedContextManager, got %T", al.contextManager)
	}
}

func TestResolveContextManager_RegisteredFactory(t *testing.T) {
	cleanup := resetCMRegistry()
	defer cleanup()

	factory := func(cfg json.RawMessage, al *AgentLoop) (ContextManager, error) {
		return &noopContextManager{}, nil
	}
	if err := RegisterContextManager("custom_cm", factory); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         t.TempDir(),
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
				ContextManager:    "custom_cm",
			},
		},
	}
	al := newCMTestAgentLoop(cfg)

	if _, ok := al.contextManager.(*noopContextManager); !ok {
		t.Fatalf("expected *noopContextManager, got %T", al.contextManager)
	}
}

func TestResolveContextManager_FactoryError(t *testing.T) {
	cleanup := resetCMRegistry()
	defer cleanup()

	factory := func(cfg json.RawMessage, al *AgentLoop) (ContextManager, error) {
		return nil, os.ErrPermission
	}
	if err := RegisterContextManager("broken_cm", factory); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         t.TempDir(),
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
				ContextManager:    "broken_cm",
			},
		},
	}
	al := newCMTestAgentLoop(cfg)

	if !errors.Is(al.contextManagerInitErr, os.ErrPermission) {
		t.Fatalf("context manager error = %v, want permission error", al.contextManagerInitErr)
	}
	if _, ok := al.contextManager.(*failedContextManager); !ok {
		t.Fatalf("expected *failedContextManager, got %T", al.contextManager)
	}
}

func TestNewAgentLoopCheckedReturnsContextManagerError(t *testing.T) {
	cfg := testConfig(t)
	cfg.Agents.Defaults.ContextManager = "missing"

	al, err := NewAgentLoopChecked(cfg, bus.NewMessageBus(), &simpleMockProvider{response: "test"})
	if err == nil || !strings.Contains(err.Error(), `unknown context manager "missing"`) {
		t.Fatalf("NewAgentLoopChecked() error = %v", err)
	}
	if al != nil {
		t.Fatalf("NewAgentLoopChecked() loop = %T, want nil", al)
	}
}

func TestNoneContextManagerIsStatelessAndClearable(t *testing.T) {
	cfg := testConfig(t)
	al := newCMTestAgentLoop(cfg)
	agent := al.registry.GetDefaultAgent()
	agent.Sessions.SetHistory("session", []providers.Message{{Role: "user", Content: "stored"}})
	agent.Sessions.SetSummary("session", "summary")

	resp, err := al.contextManager.Assemble(t.Context(), &AssembleRequest{
		Agent: agent, SessionKey: "session",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.History) != 0 || resp.Summary != "" {
		t.Fatalf("none Assemble() = %#v, want empty context", resp)
	}
	if err := al.contextManager.Clear(t.Context(), agent, "session"); err != nil {
		t.Fatal(err)
	}
	if history := agent.Sessions.GetHistory("session"); len(history) != 0 {
		t.Fatalf("history after Clear() = %#v", history)
	}
	if summary := agent.Sessions.GetSummary("session"); summary != "" {
		t.Fatalf("summary after Clear() = %q", summary)
	}
}

// ---------------------------------------------------------------------------
// Legacy Assemble tests
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Legacy Compact overflow tests
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Legacy Compact post-turn tests
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Legacy Ingest tests
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Mock ContextManager — verifies dispatch through AgentLoop
// ---------------------------------------------------------------------------

func TestAgentLoop_UsesCustomContextManager(t *testing.T) {
	cleanup := resetCMRegistry()
	defer cleanup()

	mock := &trackingContextManager{}
	factory := func(cfg json.RawMessage, al *AgentLoop) (ContextManager, error) {
		return mock, nil
	}
	if err := RegisterContextManager("tracking_cm", factory); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         t.TempDir(),
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
				ContextManager:    "tracking_cm",
			},
		},
	}
	al := newCMTestAgentLoop(cfg)

	// Verify the mock was installed
	if al.contextManager != mock {
		t.Fatalf("expected mock context manager, got %T", al.contextManager)
	}

	// Direct method calls
	_, err := mock.Assemble(context.Background(), &AssembleRequest{
		SessionKey: "s1",
		Budget:     8000,
		MaxTokens:  4096,
	})
	if err != nil {
		t.Fatalf("Assemble error: %v", err)
	}
	if mock.assembleCalls.Load() != 1 {
		t.Fatalf("expected 1 assemble call, got %d", mock.assembleCalls.Load())
	}

	err = mock.Compact(context.Background(), &CompactRequest{
		SessionKey: "s1",
		Reason:     ContextCompressReasonRetry,
	})
	if err != nil {
		t.Fatalf("Compact error: %v", err)
	}
	if mock.compactCalls.Load() != 1 {
		t.Fatalf("expected 1 compact call, got %d", mock.compactCalls.Load())
	}

	err = mock.Ingest(context.Background(), &IngestRequest{
		SessionKey: "s1",
		Message:    providers.Message{Role: "user", Content: "test"},
	})
	if err != nil {
		t.Fatalf("Ingest error: %v", err)
	}
	if mock.ingestCalls.Load() != 1 {
		t.Fatalf("expected 1 ingest call, got %d", mock.ingestCalls.Load())
	}
}

func TestIngestCalledDuringTurn(t *testing.T) {
	cleanup := resetCMRegistry()
	defer cleanup()

	mock := &trackingContextManager{}
	factory := func(cfg json.RawMessage, al *AgentLoop) (ContextManager, error) {
		return mock, nil
	}
	if err := RegisterContextManager("ingest_track_cm", factory); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         t.TempDir(),
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
				ContextManager:    "ingest_track_cm",
			},
		},
	}

	msgBus := bus.NewMessageBus()
	al := NewAgentLoop(cfg, msgBus, &simpleMockProvider{response: "done"})
	defaultAgent := al.registry.GetDefaultAgent()
	if defaultAgent == nil {
		t.Fatal("expected default agent")
	}

	// Run a turn — ingestMessage is called for user message and final assistant message
	_, err := al.runAgentLoop(context.Background(), defaultAgent, processOptions{
		SessionKey:      "session-ingest-turn",
		Channel:         "cli",
		ChatID:          "direct",
		UserMessage:     "test ingest",
		DefaultResponse: defaultResponse,
		EnableSummary:   false,
		SendResponse:    false,
	})
	if err != nil {
		t.Fatalf("runAgentLoop failed: %v", err)
	}

	// Should have at least 2 ingest calls: user message + final assistant message
	if mock.ingestCalls.Load() < 2 {
		t.Fatalf("expected >= 2 ingest calls during turn, got %d", mock.ingestCalls.Load())
	}
}

func TestClearCommandRoutedAgentCallsContextManagerClear(t *testing.T) {
	cleanup := resetCMRegistry()
	defer cleanup()

	mock := &trackingContextManager{}
	factory := func(cfg json.RawMessage, al *AgentLoop) (ContextManager, error) {
		return mock, nil
	}
	if err := RegisterContextManager("clear_track_cm", factory); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	workspace := t.TempDir()
	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         filepath.Join(workspace, "default"),
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
				ContextManager:    "clear_track_cm",
			},
			List: []config.AgentConfig{
				{
					ID:        "main",
					Default:   true,
					Workspace: filepath.Join(workspace, "main"),
				},
				{
					ID:        "support",
					Workspace: filepath.Join(workspace, "support"),
				},
			},
			Dispatch: &config.DispatchConfig{
				Rules: []config.DispatchRule{
					{
						Name:  "support-dingtalk",
						Agent: "support",
						When: config.DispatchSelector{
							Channel: "dingtalk",
						},
					},
				},
			},
		},
		Session: config.SessionConfig{
			Dimensions: []string{"chat"},
		},
	}

	al := NewAgentLoop(cfg, bus.NewMessageBus(), &simpleMockProvider{response: "done"})
	if al.contextManager != mock {
		t.Fatalf("expected mock context manager, got %T", al.contextManager)
	}

	msg := testInboundMessage(bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  "dingtalk",
			ChatID:   "chat1",
			ChatType: "direct",
			SenderID: "user1",
		},
		Content: "/clear",
	})
	route, _, err := al.resolveMessageRoute(msg)
	if err != nil {
		t.Fatalf("resolveMessageRoute() error = %v", err)
	}
	sessionKey := al.allocateRouteSession(route, msg).SessionKey

	if _, err := al.processMessage(context.Background(), msg); err != nil {
		t.Fatalf("processMessage() error = %v", err)
	}

	if got := mock.clearCalls.Load(); got != 1 {
		t.Fatalf("Clear calls = %d, want 1", got)
	}
	mock.mu.Lock()
	gotKey := mock.lastClearKey
	mock.mu.Unlock()
	if gotKey != sessionKey {
		t.Fatalf("Clear session key = %q, want %q", gotKey, sessionKey)
	}
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// noopContextManager is a minimal ContextManager that does nothing.
type noopContextManager struct{}

func (m *noopContextManager) Assemble(_ context.Context, req *AssembleRequest) (*AssembleResponse, error) {
	return &AssembleResponse{}, nil
}
func (m *noopContextManager) Compact(_ context.Context, _ *CompactRequest) error { return nil }
func (m *noopContextManager) Ingest(_ context.Context, _ *IngestRequest) error   { return nil }
func (m *noopContextManager) Clear(_ context.Context, _ *AgentInstance, _ string) error {
	return nil
}

type staticContextManager struct {
	response *AssembleResponse
}

func (m *staticContextManager) Assemble(
	_ context.Context,
	_ *AssembleRequest,
) (*AssembleResponse, error) {
	return m.response, nil
}

func (m *staticContextManager) Compact(_ context.Context, _ *CompactRequest) error {
	return nil
}

func (m *staticContextManager) Ingest(_ context.Context, _ *IngestRequest) error {
	return nil
}

func (m *staticContextManager) Clear(_ context.Context, _ *AgentInstance, _ string) error {
	return nil
}

// trackingContextManager tracks call counts for each method.
type trackingContextManager struct {
	assembleCalls atomic.Int64
	compactCalls  atomic.Int64
	ingestCalls   atomic.Int64
	clearCalls    atomic.Int64
	mu            sync.Mutex
	lastAssemble  *AssembleRequest
	lastCompact   *CompactRequest
	lastIngest    *IngestRequest
	lastClearKey  string
}

func (m *trackingContextManager) Assemble(_ context.Context, req *AssembleRequest) (*AssembleResponse, error) {
	m.assembleCalls.Add(1)
	m.mu.Lock()
	m.lastAssemble = req
	m.mu.Unlock()
	return &AssembleResponse{}, nil
}

func (m *trackingContextManager) Compact(_ context.Context, req *CompactRequest) error {
	m.compactCalls.Add(1)
	m.mu.Lock()
	m.lastCompact = req
	m.mu.Unlock()
	return nil
}

func (m *trackingContextManager) Ingest(_ context.Context, req *IngestRequest) error {
	m.ingestCalls.Add(1)
	m.mu.Lock()
	m.lastIngest = req
	m.mu.Unlock()
	return nil
}

func (m *trackingContextManager) Clear(
	_ context.Context,
	_ *AgentInstance,
	sessionKey string,
) error {
	m.clearCalls.Add(1)
	m.mu.Lock()
	m.lastClearKey = sessionKey
	m.mu.Unlock()
	return nil
}

// resetCMRegistry clears the global factory registry and returns a cleanup
// function that restores the original state after the test.
func resetCMRegistry() func() {
	cmRegistryMu.Lock()
	original := make(map[string]ContextManagerFactory, len(cmRegistry))
	for k, v := range cmRegistry {
		original[k] = v
	}
	cmRegistry = make(map[string]ContextManagerFactory)
	cmRegistryMu.Unlock()

	return func() {
		cmRegistryMu.Lock()
		cmRegistry = original
		cmRegistryMu.Unlock()
	}
}

func testConfig(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         t.TempDir(),
				ModelName:         "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
				ContextManager:    "none",
			},
		},
	}
}

func newCMTestAgentLoop(cfg *config.Config) *AgentLoop {
	msgBus := bus.NewMessageBus()
	return NewAgentLoop(cfg, msgBus, &simpleMockProvider{response: "test"})
}

func TestComputeAssembledContextUsage_UsesAssembledHistoryAndSummaryReserve(t *testing.T) {
	cfg := testConfig(t)
	al := newCMTestAgentLoop(cfg)
	agent := al.registry.GetDefaultAgent()
	if agent == nil {
		t.Fatal("expected default agent")
	}

	agent.Sessions.AddFullMessage("ctx-session", providers.Message{
		Role:    "user",
		Content: "hello",
	})
	agent.Sessions.SetSummary("ctx-session", "brief summary")

	got, gotCount, fitsBudget := computeAssembledContextUsage(
		context.Background(),
		cfg,
		agent,
		al.contextManager,
		processOptions{},
		"ctx-session",
	)
	resp, err := al.contextManager.Assemble(context.Background(), &AssembleRequest{
		SessionKey:    "ctx-session",
		Budget:        agent.ContextWindow,
		MaxTokens:     agent.MaxTokens,
		ReserveTokens: estimateNonHistoryPromptReserveForProcessOptions(cfg, agent, processOptions{}, ""),
	})
	if err != nil || resp == nil {
		t.Fatalf("assemble failed: %v", err)
	}
	expectedHistoryTokens := 0
	for _, msg := range resp.History {
		expectedHistoryTokens += EstimateMessageTokens(msg)
	}
	expectedUsed := expectedHistoryTokens +
		estimateNonHistoryPromptReserveForProcessOptions(cfg, agent, processOptions{}, resp.Summary)
	if got == nil {
		t.Fatal("expected assembled usage result")
	}
	if !fitsBudget {
		t.Fatal("expected assembled usage estimate to fit budget")
	}
	if got.UsedTokens != expectedUsed {
		t.Fatalf("assembled used tokens = %d, want %d", got.UsedTokens, expectedUsed)
	}
	if got.HistoryTokens != expectedHistoryTokens {
		t.Fatalf("assembled history tokens = %d, want %d", got.HistoryTokens, expectedHistoryTokens)
	}
	if gotCount != len(resp.History) {
		t.Fatalf("assembled history count = %d, want %d", gotCount, len(resp.History))
	}
}

func TestComputeAssembledContextUsage_ReportsOverBudgetPrompt(t *testing.T) {
	cfg := testConfig(t)
	cfg.Agents.Defaults.MaxTokens = 64
	cfg.Agents.Defaults.ContextWindow = 96
	al := newCMTestAgentLoop(cfg)
	agent := al.registry.GetDefaultAgent()
	if agent == nil {
		t.Fatal("expected default agent")
	}

	agent.Sessions.AddFullMessage("ctx-tight", providers.Message{Role: "user", Content: "hello"})

	got, _, fitsBudget := computeAssembledContextUsage(
		context.Background(),
		cfg,
		agent,
		al.contextManager,
		processOptions{},
		"ctx-tight",
	)
	if got == nil {
		t.Fatal("expected assembled usage result")
	}
	if fitsBudget {
		t.Fatal("expected assembled usage to report over-budget prompt")
	}
	if got.UsedTokens <= got.CompressAtTokens {
		t.Fatalf("assembled used tokens = %d, want > compressAt %d", got.UsedTokens, got.CompressAtTokens)
	}
}

func TestComputeAssembledContextUsage_NoHistorySkipsSessionAssembly(t *testing.T) {
	cfg := testConfig(t)
	cfg.Agents.Defaults.MaxTokens = 64
	cfg.Agents.Defaults.ContextWindow = 5000
	al := newCMTestAgentLoop(cfg)
	agent := al.registry.GetDefaultAgent()
	if agent == nil {
		t.Fatal("expected default agent")
	}

	for i := 0; i < 80; i++ {
		agent.Sessions.AddFullMessage("ctx-nohistory", providers.Message{
			Role:    "user",
			Content: strings.Repeat("history ", 80),
		})
	}

	got, gotCount, fitsBudget := computeAssembledContextUsage(
		context.Background(),
		cfg,
		agent,
		al.contextManager,
		processOptions{NoHistory: true},
		"ctx-nohistory",
	)
	if got == nil {
		t.Fatal("expected assembled usage result")
	}
	if gotCount != 0 {
		t.Fatalf("assembled history count = %d, want 0 when no-history skips session assembly", gotCount)
	}
	if got.HistoryTokens != 0 {
		t.Fatalf("assembled history tokens = %d, want 0 when no-history skips session assembly", got.HistoryTokens)
	}
	if fitsBudget != (got.UsedTokens <= got.CompressAtTokens) {
		t.Fatalf(
			"assembled fitsBudget = %t, want derived comparison %t",
			fitsBudget,
			got.UsedTokens <= got.CompressAtTokens,
		)
	}
}

func TestComputeAssembledContextUsage_AllowsNilTools(t *testing.T) {
	cfg := testConfig(t)
	al := newCMTestAgentLoop(cfg)
	agent := al.registry.GetDefaultAgent()
	if agent == nil {
		t.Fatal("expected default agent")
	}

	agent.Tools = nil
	agent.Sessions.AddFullMessage("ctx-nil-tools", providers.Message{
		Role:    "user",
		Content: "hello",
	})
	al.contextManager = &staticContextManager{response: &AssembleResponse{
		History: agent.Sessions.GetHistory("ctx-nil-tools"),
	}}

	got, gotCount, fitsBudget := computeAssembledContextUsage(
		context.Background(),
		cfg,
		agent,
		al.contextManager,
		processOptions{},
		"ctx-nil-tools",
	)
	if got == nil {
		t.Fatal("expected assembled usage result")
	}
	if !fitsBudget {
		t.Fatal("expected assembled usage estimate to fit budget without tools")
	}
	if gotCount != 1 {
		t.Fatalf("assembled history count = %d, want 1", gotCount)
	}
	if got.HistoryTokens <= 0 {
		t.Fatalf("assembled history tokens = %d, want > 0", got.HistoryTokens)
	}
	if got.UsedTokens < got.HistoryTokens {
		t.Fatalf("assembled used tokens = %d, want >= history tokens %d", got.UsedTokens, got.HistoryTokens)
	}
}

func TestEstimateNonHistoryPromptReserveForProcessOptions_PreservesSystemWhenToolsNil(t *testing.T) {
	cfg := testConfig(t)
	al := newCMTestAgentLoop(cfg)
	agent := al.registry.GetDefaultAgent()
	if agent == nil {
		t.Fatal("expected default agent")
	}

	withTools := estimateNonHistoryPromptReserveForProcessOptions(
		cfg,
		agent,
		processOptions{},
		"summary text",
	)
	if withTools <= 0 {
		t.Fatalf("reserve with tools = %d, want > 0", withTools)
	}

	agent.Tools = nil
	withoutTools := estimateNonHistoryPromptReserveForProcessOptions(
		cfg,
		agent,
		processOptions{},
		"summary text",
	)
	if withoutTools <= 0 {
		t.Fatalf("reserve without tools = %d, want > 0 from system/context tokens", withoutTools)
	}
	if withoutTools >= withTools {
		t.Fatalf("reserve without tools = %d, want < reserve with tools %d", withoutTools, withTools)
	}
}
