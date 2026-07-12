//go:build !mipsle && !netbsd && !(freebsd && arm)

package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/memory"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/providers/protocoltypes"
	"github.com/sipeed/picoclaw/pkg/seahorse"
	"github.com/sipeed/picoclaw/pkg/session"
	"github.com/sipeed/picoclaw/pkg/tokenizer"
)

// seahorseContextManager adapts seahorse.Engine to agent.ContextManager.
type seahorseContextManager struct {
	engine          *seahorse.Engine
	sessions        session.SessionStore
	al              *AgentLoop // for resolving the agent that owns a session
	locks           [64]sync.Mutex
	reconciliations atomic.Uint64
}

const seahorseReconciliationGeneration = 1

// newSeahorseContextManager creates a seahorse-backed ContextManager.
func newSeahorseContextManager(rawConfig json.RawMessage, al *AgentLoop) (ContextManager, error) {
	if al == nil {
		return nil, fmt.Errorf("seahorse: AgentLoop is required")
	}

	// Resolve workspace for DB path
	// DB stores session data, so it goes in sessions/ directory
	agent := al.registry.GetDefaultAgent()
	dbPath := agent.Workspace + "/sessions/seahorse.db"

	// Create CompleteFn from provider
	completeFn := providerToCompleteFn(agent.Provider, agent.Model)

	seahorseConfig := seahorse.Config{
		DBPath: dbPath,
	}
	if len(rawConfig) > 0 {
		if err := json.Unmarshal(rawConfig, &seahorseConfig); err != nil {
			return nil, fmt.Errorf("seahorse: parse config: %w", err)
		}
		if seahorseConfig.DBPath == "" {
			seahorseConfig.DBPath = dbPath
		}
	}

	// Create engine
	engine, err := seahorse.NewEngine(seahorseConfig, completeFn)
	if err != nil {
		return nil, fmt.Errorf("seahorse: create engine: %w", err)
	}

	mgr := &seahorseContextManager{
		engine:   engine,
		sessions: agent.Sessions,
		al:       al,
	}

	// Register seahorse tools with the agent's tool registry
	retrieval := mgr.engine.GetRetrieval()
	al.RegisterTool(seahorse.NewGrepTool(retrieval))
	al.RegisterTool(seahorse.NewExpandTool(retrieval))

	return mgr, nil
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
	unlock := m.lockSession(req.SessionKey)
	defer unlock()
	if err := m.ensureReconciled(ctx, req.SessionKey, m.sessionStore(req.SessionKey)); err != nil {
		return nil, err
	}

	budget := req.Budget
	if budget <= 0 {
		budget = 100000
	}

	// Reserve space for model response and non-history prompt/tool material.
	effectiveBudget := budget - req.MaxTokens - req.ReserveTokens
	if effectiveBudget <= 0 {
		// Reserve >= budget is a configuration/problem-size issue. Use 50% as
		// a defensive minimum so assembly still returns some bounded context.
		logger.WarnCF("agent", "context reserve exceeds budget, using 50% fallback",
			map[string]any{
				"budget":         budget,
				"max_tokens":     req.MaxTokens,
				"reserve_tokens": req.ReserveTokens,
			})
		effectiveBudget = budget / 2
	}

	result, err := m.engine.Assemble(ctx, req.SessionKey, seahorse.AssembleInput{
		Budget: effectiveBudget,
	})
	if err != nil {
		return nil, fmt.Errorf("seahorse assemble: %w", err)
	}

	history := seahorseToProviderMessages(result)

	// Summary is already formatted as XML with system prompt addition by assembler
	return &AssembleResponse{
		History: history,
		Summary: result.Summary,
	}, nil
}

// Compact compresses conversation history via seahorse summarization.
func (m *seahorseContextManager) Compact(ctx context.Context, req *CompactRequest) error {
	if req == nil {
		return nil
	}
	unlock := m.lockSession(req.SessionKey)
	defer unlock()
	if err := m.ensureReconciled(ctx, req.SessionKey, m.sessionStore(req.SessionKey)); err != nil {
		return err
	}

	// For model overflow retry, use aggressive CompactUntilUnder to guarantee
	// the next LLM request has a smaller assembled history. Proactive pressure
	// must stay latency-bounded for interactive turns; SetupTurn performs a
	// cheap history trim after this normal compact pass if the prompt is still
	// over budget.
	if req.Reason == ContextCompressReasonRetry && req.Budget > 0 {
		_, err := m.engine.CompactUntilUnder(ctx, req.SessionKey, req.Budget)
		return err
	}

	_, err := m.engine.Compact(ctx, req.SessionKey, seahorse.CompactInput{
		Force:  req.Reason == ContextCompressReasonRetry,
		Budget: &req.Budget,
	})
	return err
}

// Ingest records a message after the canonical store has already appended it.
func (m *seahorseContextManager) Ingest(ctx context.Context, req *IngestRequest) error {
	if req == nil {
		return nil
	}
	unlock := m.lockSession(req.SessionKey)
	defer unlock()
	if req.CanonicalWriteErr != nil {
		store := m.sessionStore(req.SessionKey)
		if canonicalHistoryContains(store, req.SessionKey, req.Message) {
			return m.ensureReconciled(ctx, req.SessionKey, store)
		}
		logger.WarnCF("seahorse", "canonical history write failed; ingesting live message without watermark",
			map[string]any{"session": req.SessionKey, "error": req.CanonicalWriteErr.Error()})
		msg := providerToSeahorseMessage(req.Message)
		_, err := m.engine.Ingest(ctx, req.SessionKey, []seahorse.Message{msg})
		return err
	}
	store := m.sessionStore(req.SessionKey)
	if store == nil {
		msg := providerToSeahorseMessage(req.Message)
		_, err := m.engine.Ingest(ctx, req.SessionKey, []seahorse.Message{msg})
		return err
	}
	revision, err := historyRevision(store, req.SessionKey)
	if err != nil {
		return fmt.Errorf("seahorse ingest revision: %w", err)
	}
	state, err := m.engine.GetRetrieval().Store().GetReconciliationState(ctx, req.SessionKey)
	if err != nil {
		return err
	}
	if state != nil && !revision.Dirty && state.SchemaGeneration == seahorseReconciliationGeneration &&
		state.SourceRevision+1 == revision.Revision && state.SourceCount+1 == revision.Count &&
		state.SourceSkip == revision.Skip {
		msg := providerToSeahorseMessage(req.Message)
		if _, err := m.engine.Ingest(ctx, req.SessionKey, []seahorse.Message{msg}); err != nil {
			return err
		}
		return m.setReconciliationState(ctx, req.SessionKey, revision)
	}

	return m.ensureReconciled(ctx, req.SessionKey, store)
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
func (m *seahorseContextManager) Clear(ctx context.Context, sessionKey string) error {
	unlock := m.lockSession(sessionKey)
	defer unlock()
	if err := m.engine.ClearSession(ctx, sessionKey); err != nil {
		return err
	}
	// The session may belong to a routed (non-default) agent whose JSONL
	// store differs from the bootstrap store, so clear the owner's store.
	sessions := m.sessions
	if m.al != nil {
		if agent := m.al.agentForSession(sessionKey); agent != nil && agent.Sessions != nil {
			sessions = agent.Sessions
		}
	}
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
		return m.setReconciliationState(ctx, sessionKey, revision)
	}
	return nil
}

func (m *seahorseContextManager) reconcile(ctx context.Context, sessionKey string, store session.SessionStore) error {
	history, err := canonicalHistory(store, sessionKey)
	if err != nil {
		return err
	}
	msgs := make([]seahorse.Message, len(history))
	for i, h := range history {
		msgs[i] = providerToSeahorseMessage(h)
	}
	if len(msgs) == 0 {
		return m.engine.ClearSession(ctx, sessionKey)
	}
	return m.engine.Bootstrap(ctx, sessionKey, msgs)
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
	if store == nil {
		return nil
	}
	revision, err := historyRevision(store, sessionKey)
	if err != nil {
		return fmt.Errorf("seahorse history revision: %w", err)
	}
	state, err := m.engine.GetRetrieval().Store().GetReconciliationState(ctx, sessionKey)
	if err != nil {
		return err
	}
	if reconciliationMatches(state, revision) {
		return nil
	}
	started := time.Now()
	m.reconciliations.Add(1)
	if err := m.reconcile(ctx, sessionKey, store); err != nil {
		return fmt.Errorf("seahorse reconcile: %w", err)
	}
	if err := m.setReconciliationState(ctx, sessionKey, revision); err != nil {
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
	key string,
	revision memory.HistoryRevision,
) error {
	return m.engine.GetRetrieval().Store().SetReconciliationState(ctx, seahorse.ReconciliationState{
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

func (m *seahorseContextManager) sessionStore(key string) session.SessionStore {
	if m.al != nil {
		if agent := m.al.agentForSession(key); agent != nil && agent.Sessions != nil {
			return agent.Sessions
		}
	}
	return m.sessions
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
			for _, key := range agent.Sessions.ListSessions() {
				owner := m.al.agentForSession(key)
				if owner != nil && owner.ID != agent.ID {
					continue
				}
				unlock := m.lockSession(key)
				err := m.ensureReconciled(ctx, key, agent.Sessions)
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
		part := seahorse.MessagePart{
			Type:       "tool_use",
			Name:       tc.Function.Name,
			Arguments:  tc.Function.Arguments,
			ToolCallID: tc.ID,
		}
		result.Parts = append(result.Parts, part)
	}

	// Convert tool result
	if msg.ToolCallID != "" {
		part := seahorse.MessagePart{
			Type:       "tool_result",
			ToolCallID: msg.ToolCallID,
			Text:       msg.Content,
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
