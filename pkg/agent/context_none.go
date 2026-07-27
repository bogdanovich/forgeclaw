package agent

import (
	"context"
	"fmt"

	"github.com/bogdanovich/mintclaw/pkg/providers"
)

// noneContextManager disables stored context assembly and compaction.
type noneContextManager struct{}

func (m *noneContextManager) Assemble(
	_ context.Context,
	_ *AssembleRequest,
) (*AssembleResponse, error) {
	return &AssembleResponse{}, nil
}

func (m *noneContextManager) Compact(_ context.Context, _ *CompactRequest) error {
	return nil
}

func (m *noneContextManager) Ingest(_ context.Context, _ *IngestRequest) error {
	return nil
}

func (m *noneContextManager) Clear(
	_ context.Context,
	agent *AgentInstance,
	sessionKey string,
) error {
	if agent == nil || agent.Sessions == nil {
		return fmt.Errorf("sessions not initialized")
	}
	agent.Sessions.SetHistory(sessionKey, []providers.Message{})
	agent.Sessions.SetSummary(sessionKey, "")
	return agent.Sessions.Save(sessionKey)
}

// failedContextManager preserves the construction error across direct callers
// that use NewAgentLoop instead of NewAgentLoopChecked.
type failedContextManager struct {
	err error
}

func (m *failedContextManager) Assemble(
	_ context.Context,
	_ *AssembleRequest,
) (*AssembleResponse, error) {
	return nil, m.err
}

func (m *failedContextManager) Compact(_ context.Context, _ *CompactRequest) error {
	return m.err
}

func (m *failedContextManager) Ingest(_ context.Context, _ *IngestRequest) error {
	return m.err
}

func (m *failedContextManager) Clear(
	_ context.Context,
	_ *AgentInstance,
	_ string,
) error {
	return m.err
}
