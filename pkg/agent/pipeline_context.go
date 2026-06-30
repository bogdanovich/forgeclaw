// PicoClaw - Ultra-lightweight personal AI agent

package agent

import (
	"context"

	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
)

func (p *Pipeline) ingestMessage(ctx context.Context, ts *turnState, msg providers.Message) {
	if p == nil || ts == nil || p.ContextManager == nil {
		return
	}
	if err := p.ContextManager.Ingest(ctx, &IngestRequest{
		SessionKey: ts.sessionKey,
		Message:    msg,
	}); err != nil {
		logger.WarnCF("agent", "Context manager ingest failed", map[string]any{
			"session_key": ts.sessionKey,
			"error":       err.Error(),
		})
	}
}

func (p *Pipeline) scheduleBackgroundCompaction(
	agent *AgentInstance,
	sessionKey string,
	reason ContextCompressReason,
	budget int,
	messageKind string,
) {
	if p == nil || p.BackgroundCompaction == nil {
		return
	}
	p.BackgroundCompaction.scheduleBackgroundCompaction(agent, sessionKey, reason, budget, messageKind)
}
