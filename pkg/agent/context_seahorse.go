//go:build !mipsle && !netbsd && !(freebsd && arm)

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/memory"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/providers/protocoltypes"
	"github.com/sipeed/picoclaw/pkg/seahorse"
	"github.com/sipeed/picoclaw/pkg/session"
	"github.com/sipeed/picoclaw/pkg/tokenizer"
	toolpolicy "github.com/sipeed/picoclaw/pkg/tools/policy"
)

// seahorseContextManager adapts seahorse.Engine to agent.ContextManager.
type seahorseContextManager struct {
	runtimes        map[string]*seahorseAgentRuntime
	defaultAgentID  string
	al              *AgentLoop // for resolving the agent that owns a session
	locks           [64]sync.Mutex
	reconciliations atomic.Uint64
	closeOnce       sync.Once
	closeErr        error
}

type seahorseAgentRuntime struct {
	engine    *seahorse.Engine
	sessions  session.SessionStore
	workspace string
	agentID   string
}

const seahorseReconciliationGeneration = 1

// newSeahorseContextManager creates a seahorse-backed ContextManager.
func newSeahorseContextManager(rawConfig json.RawMessage, al *AgentLoop) (ContextManager, error) {
	if al == nil {
		return nil, fmt.Errorf("seahorse: AgentLoop is required")
	}

	mgr := &seahorseContextManager{
		runtimes: make(map[string]*seahorseAgentRuntime),
		al:       al,
	}
	defaultAgent := al.registry.GetDefaultAgent()
	if defaultAgent != nil {
		mgr.defaultAgentID = defaultAgent.ID
	}
	for _, agentID := range al.registry.ListAgentIDs() {
		agent, ok := al.registry.GetAgent(agentID)
		if !ok || agent == nil {
			continue
		}
		runtime, err := newSeahorseAgentRuntime(
			rawConfig,
			al,
			agent,
			seahorseAgentDBPath(agent, mgr.defaultAgentID),
		)
		if err != nil {
			_ = mgr.Close()
			return nil, fmt.Errorf("seahorse: create runtime for agent %q: %w", agentID, err)
		}
		mgr.runtimes[agentID] = runtime
		retrieval := runtime.engine.GetRetrieval()
		registerToolIfAllowed(agent, seahorse.NewGrepTool(retrieval))
		registerToolIfAllowed(agent, seahorse.NewExpandTool(retrieval))
	}
	if len(mgr.runtimes) == 0 {
		return nil, fmt.Errorf("seahorse: no agents available")
	}

	return mgr, nil
}

func newSeahorseAgentRuntime(
	rawConfig json.RawMessage,
	al *AgentLoop,
	agent *AgentInstance,
	dbPath string,
) (*seahorseAgentRuntime, error) {
	seahorseConfig, err := resolveSeahorseConfig(rawConfig, dbPath, al.cfg.Tools.ResultRetention)
	if err != nil {
		return nil, err
	}
	if len(al.registry.ListAgentIDs()) > 1 && seahorseConfig.DBPath != dbPath {
		return nil, fmt.Errorf("custom dbPath is not supported with multiple agents")
	}
	engine, err := seahorse.NewEngine(
		seahorseConfig,
		providerToCompleteFn(agent.Provider, agent.Model),
	)
	if err != nil {
		return nil, fmt.Errorf("create engine: %w", err)
	}
	return &seahorseAgentRuntime{
		engine:    engine,
		sessions:  agent.Sessions,
		workspace: agent.Workspace,
		agentID:   agent.ID,
	}, nil
}

func seahorseAgentDBPath(agent *AgentInstance, defaultAgentID string) string {
	filename := "seahorse.db"
	if agent.ID != defaultAgentID {
		filename = fmt.Sprintf("seahorse-%s.db", agent.ID)
	}
	return filepath.Join(agent.Workspace, "sessions", filename)
}

func (m *seahorseContextManager) runtimeFor(agent *AgentInstance) (*seahorseAgentRuntime, error) {
	agentID := m.defaultAgentID
	if agent != nil && agent.ID != "" {
		agentID = agent.ID
	}
	runtime, ok := m.runtimes[agentID]
	if !ok {
		return nil, fmt.Errorf("seahorse: no runtime for agent %q", agentID)
	}
	return runtime, nil
}

func resolveSeahorseConfig(
	rawConfig json.RawMessage,
	dbPath string,
	retention toolpolicy.ResultRetentionPolicy,
) (seahorse.Config, error) {
	seahorseConfig := seahorse.Config{DBPath: dbPath}
	if len(rawConfig) > 0 {
		if err := json.Unmarshal(rawConfig, &seahorseConfig); err != nil {
			return seahorse.Config{}, fmt.Errorf("seahorse: parse config: %w", err)
		}
		if seahorseConfig.DBPath == "" {
			seahorseConfig.DBPath = dbPath
		}
	}
	seahorseConfig.ResultRetentionPolicy = retention
	return seahorseConfig, nil
}

// providerToCompleteFn wraps providers.LLMProvider as a seahorse.CompleteFn.
func providerToCompleteFn(provider providers.LLMProvider, model string) seahorse.CompleteFn {
	return func(ctx context.Context, prompt string, opts seahorse.CompleteOptions) (string, error) {
		resp, err := provider.Chat(
			ctx,
			[]providers.Message{{Role: "user", Content: prompt}},
			nil, // no tools for summarization
			model,
			map[string]any{
				"max_tokens":       opts.MaxTokens,
				"temperature":      opts.Temperature,
				"prompt_cache_key": "seahorse",
			},
		)
		if err != nil {
			return "", err
		}
		return resp.Content, nil
	}
}

// Assemble builds budget-aware context from seahorse SQLite.
func (m *seahorseContextManager) Assemble(ctx context.Context, req *AssembleRequest) (*AssembleResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("seahorse assemble: nil request")
	}
	runtime, runtimeErr := m.runtimeFor(req.Agent)
	if runtimeErr != nil {
		return nil, runtimeErr
	}
	unlock := m.lockSession(runtime.agentID + ":" + req.SessionKey)
	defer unlock()
	if err := m.ensureConversationProvenance(ctx, runtime, req.SessionKey); err != nil {
		return nil, err
	}
	if err := m.ensureReconciledRuntime(ctx, runtime, req.SessionKey); err != nil {
		return nil, err
	}

	budget := req.Budget
	if budget <= 0 {
		return nil, fmt.Errorf("seahorse assemble: context window must be positive")
	}

	// Reserve space for model response and non-history prompt/tool material.
	effectiveBudget := budget - req.MaxTokens - req.ReserveTokens
	if effectiveBudget <= 0 {
		logger.ErrorCF("agent", "mandatory prompt content exceeds context window", map[string]any{
			"context_window":      budget,
			"output_reserve":      req.MaxTokens,
			"non_history_reserve": req.ReserveTokens,
			"available_context":   effectiveBudget,
		})
		return nil, fmt.Errorf(
			"mandatory prompt content cannot fit context window: window=%d output_reserve=%d non_history_reserve=%d",
			budget,
			req.MaxTokens,
			req.ReserveTokens,
		)
	}

	result, err := runtime.engine.Assemble(ctx, req.SessionKey, seahorse.AssembleInput{
		Budget: effectiveBudget,
	})
	if err != nil {
		logger.ErrorCF("seahorse", "context assembly failed closed", map[string]any{
			"session_key":         req.SessionKey,
			"context_window":      budget,
			"output_reserve":      req.MaxTokens,
			"non_history_reserve": req.ReserveTokens,
			"available_context":   effectiveBudget,
			"error":               err.Error(),
		})
		return nil, fmt.Errorf("seahorse assemble: %w", err)
	}

	history := seahorseToProviderMessages(result)

	// Summary is already formatted as XML with system prompt addition by assembler
	response := &AssembleResponse{
		History: history,
		Summary: result.Summary,
	}
	if result.Budget != nil {
		response.Budget = &ContextBudgetReport{
			ContextWindow:            budget,
			OutputReserve:            req.MaxTokens,
			NonHistoryReserve:        req.ReserveTokens,
			AvailableContext:         result.Budget.TotalBudget,
			HistoryBudget:            result.Budget.HistoryBudget,
			SummaryBudget:            result.Budget.SummaryBudget,
			SourceHistoryTokens:      result.Budget.SourceHistoryTokens,
			SourceSummaryTokens:      result.Budget.SourceSummaryTokens,
			SelectedHistoryTokens:    result.Budget.SelectedHistoryTokens,
			SelectedSummaryTokens:    result.Budget.SelectedSummaryTokens,
			RequestedRecentTailTurns: result.Budget.RequestedRecentTailTurns,
			RecentTailTurns:          result.Budget.RecentTailTurns,
			RecentTailTokens:         result.Budget.RecentTailTokens,
			RecentTailOverflowTokens: result.Budget.RecentTailOverflowTokens,
			RecentTailDegraded:       result.Budget.RecentTailDegraded,
			Truncated:                result.Budget.Truncated,
			NeedsCompaction:          result.Budget.NeedsCompaction,
			PressureReasons:          append([]string(nil), result.Budget.PressureReasons...),
		}
	}
	return response, nil
}

// Compact compresses conversation history via seahorse summarization.
func (m *seahorseContextManager) Compact(ctx context.Context, req *CompactRequest) error {
	if req == nil {
		return nil
	}
	runtime, runtimeErr := m.runtimeFor(req.Agent)
	if runtimeErr != nil {
		return runtimeErr
	}
	unlock := m.lockSession(runtime.agentID + ":" + req.SessionKey)
	defer unlock()
	if err := m.ensureConversationProvenance(ctx, runtime, req.SessionKey); err != nil {
		return err
	}
	if err := m.ensureReconciledRuntime(ctx, runtime, req.SessionKey); err != nil {
		return err
	}

	// For model overflow retry, use aggressive CompactUntilUnder to guarantee
	// the next LLM request has a smaller assembled history. Proactive pressure
	// must stay latency-bounded for interactive turns; SetupTurn performs a
	// cheap history trim after this normal compact pass if the prompt is still
	// over budget.
	if (req.Reason == ContextCompressReasonRetry ||
		(req.Reason == ContextCompressReasonProactive && runtime.engine.AbsoluteBudgetsEnabled())) &&
		req.Budget > 0 {
		result, compactErr := runtime.engine.CompactUntilUnder(ctx, req.SessionKey, req.Budget)
		if compactErr == nil && compactResultHasProgress(result) {
			m.emitCompactEvent(req, result)
		}
		return compactErr
	}

	result, err := runtime.engine.Compact(ctx, req.SessionKey, seahorse.CompactInput{
		Force:  req.Reason == ContextCompressReasonRetry,
		Budget: &req.Budget,
	})
	if err == nil && compactResultHasProgress(result) {
		m.emitCompactEvent(req, result)
	}
	return err
}

func compactResultHasProgress(result *seahorse.CompactResult) bool {
	return result != nil &&
		(result.TokensSaved > 0 || result.LeafSummaries > 0 || result.CondensedSummaries > 0)
}

func (m *seahorseContextManager) emitCompactEvent(req *CompactRequest, result *seahorse.CompactResult) {
	if m.al == nil || req == nil {
		return
	}
	scope := req.TraceScope
	workspace := strings.TrimSpace(req.Workspace)
	agentID := ""
	if req.Agent != nil {
		agentID = req.Agent.ID
		if workspace == "" {
			workspace = req.Agent.Workspace
		}
	}
	if scope.Workspace == "" {
		scope = runtimeevents.NewTraceScope(workspace, scope.TurnID)
	}
	m.al.emitEvent(
		runtimeevents.KindAgentContextCompress,
		HookMeta{
			TraceScope: scope,
			SessionKey: req.SessionKey,
			AgentID:    agentID,
			Source:     "seahorse",
			TracePath:  "turn.context.compress",
		},
		ContextCompressPayload{
			Reason:             req.Reason,
			HistoryBudget:      req.Budget,
			TokensSaved:        result.TokensSaved,
			SummariesCreated:   len(result.SummariesCreated),
			LeafSummaries:      result.LeafSummaries,
			CondensedSummaries: result.CondensedSummaries,
		},
	)
}

func (m *seahorseContextManager) Close() error {
	m.closeOnce.Do(func() {
		closeErrors := make([]error, 0, len(m.runtimes))
		for _, runtime := range m.runtimes {
			closeErrors = append(closeErrors, runtime.engine.Close())
		}
		m.closeErr = errors.Join(closeErrors...)
	})
	return m.closeErr
}

// Ingest records a message after the canonical store has already appended it.
func (m *seahorseContextManager) Ingest(ctx context.Context, req *IngestRequest) error {
	if req == nil {
		return nil
	}
	runtime, runtimeErr := m.runtimeFor(req.Agent)
	if runtimeErr != nil {
		return runtimeErr
	}
	unlock := m.lockSession(runtime.agentID + ":" + req.SessionKey)
	defer unlock()
	if err := m.ensureConversationProvenance(ctx, runtime, req.SessionKey); err != nil {
		return err
	}
	if req.CanonicalWriteErr != nil {
		if canonicalHistoryContains(runtime.sessions, req.SessionKey, req.Message) {
			return m.ensureReconciledRuntime(ctx, runtime, req.SessionKey)
		}
		logger.WarnCF("seahorse", "canonical history write failed; ingesting live message without watermark",
			map[string]any{"session": req.SessionKey, "error": req.CanonicalWriteErr.Error()})
		msg := providerToSeahorseMessage(req.Message)
		_, ingestErr := runtime.engine.Ingest(ctx, req.SessionKey, []seahorse.Message{msg})
		return ingestErr
	}
	store := runtime.sessions
	if store == nil {
		msg := providerToSeahorseMessage(req.Message)
		_, ingestErr := runtime.engine.Ingest(ctx, req.SessionKey, []seahorse.Message{msg})
		return ingestErr
	}
	revision, err := historyRevision(store, req.SessionKey)
	if err != nil {
		return fmt.Errorf("seahorse ingest revision: %w", err)
	}
	state, err := runtime.engine.GetRetrieval().Store().GetReconciliationState(ctx, req.SessionKey)
	if err != nil {
		return err
	}
	if state != nil && !revision.Dirty && state.SchemaGeneration == seahorseReconciliationGeneration &&
		state.SourceRevision+1 == revision.Revision && state.SourceCount+1 == revision.Count &&
		state.SourceSkip == revision.Skip {
		msg := providerToSeahorseMessage(req.Message)
		if _, err := runtime.engine.Ingest(ctx, req.SessionKey, []seahorse.Message{msg}); err != nil {
			return err
		}
		return m.setReconciliationState(ctx, runtime, req.SessionKey, revision)
	}

	return m.ensureReconciledRuntime(ctx, runtime, req.SessionKey)
}

func (m *seahorseContextManager) ensureConversationProvenance(
	ctx context.Context,
	runtime *seahorseAgentRuntime,
	sessionKey string,
) error {
	metadataStore, ok := runtime.sessions.(session.MetadataAwareSessionStore)
	if !ok {
		return nil
	}
	scope := metadataStore.GetSessionScope(sessionKey)
	if scope == nil || scope.RouteScopeKey == "" || scope.AgentID == "" {
		return nil
	}
	if err := runtime.engine.GetRetrieval().Store().SetConversationProvenance(
		ctx,
		sessionKey,
		scope.RouteScopeKey,
		scope.AgentID,
	); err != nil {
		return fmt.Errorf("seahorse conversation provenance: %w", err)
	}
	return nil
}

func canonicalHistoryContains(store session.SessionStore, key string, target providers.Message) bool {
	reader, ok := store.(session.ErrorAwareHistoryReader)
	if !ok {
		return false
	}
	history, err := reader.GetHistoryWithError(key)
	if err != nil {
		return false
	}
	target.CreatedAt = nil
	for i := len(history) - 1; i >= 0; i-- {
		candidate := history[i]
		candidate.CreatedAt = nil
		if reflect.DeepEqual(candidate, target) {
			return true
		}
	}
	return false
}

// Clear removes all stored context for a session (seahorse DB + JSONL).
func (m *seahorseContextManager) Clear(
	ctx context.Context,
	agent *AgentInstance,
	sessionKey string,
) error {
	runtime, err := m.runtimeFor(agent)
	if err != nil {
		return err
	}
	unlock := m.lockSession(runtime.agentID + ":" + sessionKey)
	defer unlock()
	if err := runtime.engine.ClearSession(ctx, sessionKey); err != nil {
		return err
	}
	sessions := runtime.sessions
	if sessions != nil {
		sessions.SetHistory(sessionKey, []providers.Message{})
		sessions.SetSummary(sessionKey, "")
		if err := sessions.Save(sessionKey); err != nil {
			return err
		}
		revision, err := historyRevision(sessions, sessionKey)
		if err != nil {
			return err
		}
		return m.setReconciliationState(ctx, runtime, sessionKey, revision)
	}
	return nil
}

func (m *seahorseContextManager) reconcile(
	ctx context.Context,
	runtime *seahorseAgentRuntime,
	sessionKey string,
) error {
	history, err := canonicalHistory(runtime.sessions, sessionKey)
	if err != nil {
		return err
	}
	msgs := make([]seahorse.Message, len(history))
	for i, h := range history {
		msgs[i] = providerToSeahorseMessage(h)
	}
	if len(msgs) == 0 {
		return runtime.engine.ClearSession(ctx, sessionKey)
	}
	return runtime.engine.Bootstrap(ctx, sessionKey, msgs)
}

func canonicalHistory(store session.SessionStore, key string) ([]providers.Message, error) {
	if reader, ok := store.(session.ErrorAwareHistoryReader); ok {
		return reader.GetHistoryWithError(key)
	}
	return store.GetHistory(key), nil
}

func (m *seahorseContextManager) ensureReconciled(
	ctx context.Context,
	sessionKey string,
	store session.SessionStore,
) error {
	runtime, err := m.runtimeFor(nil)
	if err != nil {
		return err
	}
	runtime.sessions = store
	return m.ensureReconciledRuntime(ctx, runtime, sessionKey)
}

func (m *seahorseContextManager) ensureReconciledRuntime(
	ctx context.Context,
	runtime *seahorseAgentRuntime,
	sessionKey string,
) error {
	if runtime.sessions == nil {
		return nil
	}
	revision, err := historyRevision(runtime.sessions, sessionKey)
	if err != nil {
		return fmt.Errorf("seahorse history revision: %w", err)
	}
	state, err := runtime.engine.GetRetrieval().Store().GetReconciliationState(ctx, sessionKey)
	if err != nil {
		return err
	}
	if reconciliationMatches(state, revision) {
		return nil
	}
	started := time.Now()
	m.reconciliations.Add(1)
	if err := m.reconcile(ctx, runtime, sessionKey); err != nil {
		return fmt.Errorf("seahorse reconcile: %w", err)
	}
	if err := m.setReconciliationState(ctx, runtime, sessionKey, revision); err != nil {
		return err
	}
	logger.InfoCF("seahorse", "reconciled canonical history", map[string]any{
		"session": sessionKey, "messages": revision.Count, "duration": time.Since(started),
	})
	return nil
}

func reconciliationMatches(state *seahorse.ReconciliationState, revision memory.HistoryRevision) bool {
	return state != nil && !revision.Dirty &&
		state.SchemaGeneration == seahorseReconciliationGeneration &&
		state.SourceRevision == revision.Revision && state.SourceCount == revision.Count &&
		state.SourceSkip == revision.Skip && state.SourceFileSize == revision.FileSize &&
		state.SourceModTimeNS == revision.ModTimeNS
}

func (m *seahorseContextManager) setReconciliationState(
	ctx context.Context,
	runtime *seahorseAgentRuntime,
	key string,
	revision memory.HistoryRevision,
) error {
	return runtime.engine.GetRetrieval().Store().SetReconciliationState(ctx, seahorse.ReconciliationState{
		SessionKey: key, SourceRevision: revision.Revision, SourceCount: revision.Count,
		SourceSkip: revision.Skip, SourceFileSize: revision.FileSize,
		SourceModTimeNS: revision.ModTimeNS, SchemaGeneration: seahorseReconciliationGeneration,
	})
}

func historyRevision(store session.SessionStore, key string) (memory.HistoryRevision, error) {
	provider, ok := store.(session.HistoryRevisionProvider)
	if !ok {
		return memory.HistoryRevision{}, fmt.Errorf("session store does not expose history revisions")
	}
	return provider.GetHistoryRevision(key)
}

func (m *seahorseContextManager) lockSession(key string) func() {
	var hash uint32 = 2166136261
	for _, char := range key {
		hash ^= uint32(char)
		hash *= 16777619
	}
	lock := &m.locks[hash%uint32(len(m.locks))]
	lock.Lock()
	return lock.Unlock
}

// StartBackgroundReconciliation starts after gateway readiness and never
// delays inbound channel startup.
func (m *seahorseContextManager) StartBackgroundReconciliation(ctx context.Context) {
	go func() {
		for _, agentID := range m.al.registry.ListAgentIDs() {
			agent, ok := m.al.registry.GetAgent(agentID)
			if !ok || agent.Sessions == nil {
				continue
			}
			runtime, err := m.runtimeFor(agent)
			if err != nil {
				logger.WarnCF("seahorse", "background reconciliation skipped", map[string]any{
					"agent": agentID, "error": err.Error(),
				})
				continue
			}
			for _, key := range runtime.sessions.ListSessions() {
				unlock := m.lockSession(runtime.agentID + ":" + key)
				err := m.ensureReconciledRuntime(ctx, runtime, key)
				unlock()
				if err != nil && ctx.Err() == nil {
					logger.WarnCF("seahorse", "background reconciliation failed", map[string]any{
						"session": key, "error": err.Error(),
					})
				}
			}
		}
	}()
}

// providerToSeahorseMessage converts a providers.Message to a seahorse.Message.
func providerToSeahorseMessage(msg protocoltypes.Message) seahorse.Message {
	result := seahorse.Message{
		Role:             msg.Role,
		Content:          msg.Content,
		ModelName:        msg.ModelName,
		ReasoningContent: msg.ReasoningContent,
		TokenCount:       tokenizer.EstimateMessageTokens(msg),
		CreatedAt:        normalizeSeahorseMessageCreatedAt(msg.CreatedAt),
	}

	// Convert ToolCalls → MessageParts
	for _, tc := range msg.ToolCalls {
		name := tc.Name
		arguments := ""
		if tc.Function != nil {
			name = tc.Function.Name
			arguments = tc.Function.Arguments
		} else if len(tc.Arguments) > 0 {
			if encoded, err := json.Marshal(tc.Arguments); err == nil {
				arguments = string(encoded)
			}
		}
		part := seahorse.MessagePart{
			Type:       "tool_use",
			Name:       name,
			Arguments:  arguments,
			ToolCallID: tc.ID,
		}
		result.Parts = append(result.Parts, part)
	}

	// Convert tool result
	if msg.ToolCallID != "" {
		part := seahorse.MessagePart{
			Type:             "tool_result",
			ToolCallID:       msg.ToolCallID,
			ToolResultStatus: string(msg.ToolResultStatus),
			Text:             msg.Content,
		}
		result.Parts = append(result.Parts, part)
	}

	// Convert media attachments
	for _, mediaURI := range msg.Media {
		part := seahorse.MessagePart{
			Type:     "media",
			MediaURI: mediaURI,
		}
		result.Parts = append(result.Parts, part)
	}

	return result
}

func normalizeSeahorseMessageCreatedAt(createdAt *time.Time) time.Time {
	if createdAt == nil || createdAt.IsZero() {
		return time.Time{}
	}
	return createdAt.UTC().Truncate(time.Second)
}

// seahorseToProviderMessages converts a seahorse.AssembleResult to []providers.Message.
func seahorseToProviderMessages(result *seahorse.AssembleResult) []protocoltypes.Message {
	messages := make([]protocoltypes.Message, 0, len(result.Messages))

	// Convert assembled messages (which already include summary XML messages)
	for _, msg := range result.Messages {
		pm := protocoltypes.Message{
			Role:             msg.Role,
			Content:          msg.Content,
			ModelName:        msg.ModelName,
			ReasoningContent: msg.ReasoningContent,
		}

		// Reconstruct ToolCalls from parts
		for _, part := range msg.Parts {
			if part.Type == "tool_use" {
				pm.ToolCalls = append(pm.ToolCalls, protocoltypes.ToolCall{
					ID:   part.ToolCallID,
					Type: "function", // Required by OpenAI-compatible APIs (GLM, etc.)
					Function: &protocoltypes.FunctionCall{
						Name:      part.Name,
						Arguments: part.Arguments,
					},
				})
			}
			if part.Type == "tool_result" {
				pm.ToolCallID = part.ToolCallID
				pm.ToolResultStatus = protocoltypes.ToolResultStatus(part.ToolResultStatus)
				if pm.Content == "" && part.Text != "" {
					pm.Content = part.Text
				}
			}
			if part.Type == "media" && part.MediaURI != "" {
				pm.Media = append(pm.Media, part.MediaURI)
			}
		}

		messages = append(messages, pm)
	}

	return messages
}

func init() {
	if err := RegisterContextManager("seahorse", newSeahorseContextManager); err != nil {
		panic(fmt.Sprintf("register seahorse context manager: %v", err))
	}
}
