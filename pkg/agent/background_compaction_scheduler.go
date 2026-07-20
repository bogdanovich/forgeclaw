package agent

import (
	"context"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/logger"
)

type backgroundCompactionRunner struct {
	contextManager func() ContextManager
	running        sync.Map
}

func (r *backgroundCompactionRunner) scheduleBackgroundCompaction(
	agent *AgentInstance,
	sessionKey string,
	reason ContextCompressReason,
	budget int,
	messageKind string,
) {
	contextManager := r.currentContextManager()
	if contextManager == nil || agent == nil || sessionKey == "" {
		return
	}
	key := agent.ID + ":" + sessionKey
	if _, loaded := r.running.LoadOrStore(key, struct{}{}); loaded {
		logger.DebugCF("agent", "Background context compaction already running", map[string]any{
			"agent_id":     agent.ID,
			"session_key":  sessionKey,
			"reason":       reason,
			"message_kind": messageKind,
		})
		return
	}

	go func() {
		defer r.running.Delete(key)

		compactCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		startedAt := time.Now()
		logger.DebugCF("agent", "Background context compaction started", map[string]any{
			"agent_id":     agent.ID,
			"session_key":  sessionKey,
			"reason":       reason,
			"budget":       budget,
			"message_kind": messageKind,
		})
		if err := contextManager.Compact(
			compactCtx,
			&CompactRequest{
				Agent:      agent,
				SessionKey: sessionKey,
				Reason:     reason,
				Budget:     budget,
			},
		); err != nil {
			logger.WarnCF("agent", "Background context compaction failed", map[string]any{
				"agent_id":     agent.ID,
				"session_key":  sessionKey,
				"reason":       reason,
				"message_kind": messageKind,
				"duration_ms":  time.Since(startedAt).Milliseconds(),
				"error":        err.Error(),
			})
			return
		}
		logger.InfoCF("agent", "Background context compaction completed", map[string]any{
			"agent_id":     agent.ID,
			"session_key":  sessionKey,
			"reason":       reason,
			"message_kind": messageKind,
			"duration_ms":  time.Since(startedAt).Milliseconds(),
		})
	}()
}

func (r *backgroundCompactionRunner) currentContextManager() ContextManager {
	if r == nil || r.contextManager == nil {
		return nil
	}
	return r.contextManager()
}
