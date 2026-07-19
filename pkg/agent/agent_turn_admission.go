package agent

import (
	"context"

	"github.com/sipeed/picoclaw/pkg/routing"
)

type agentTurnAdmissionsKey struct{}

func buildAgentTurnSemaphores(registry *AgentRegistry) map[string]chan struct{} {
	semaphores := make(map[string]chan struct{})
	if registry == nil {
		return semaphores
	}
	for _, agentID := range registry.ListAgentIDs() {
		agent, ok := registry.GetAgent(agentID)
		if !ok || agent == nil || agent.MaxParallelTurns <= 0 {
			continue
		}
		semaphores[agentID] = make(chan struct{}, agent.MaxParallelTurns)
	}
	return semaphores
}

func (al *AgentLoop) acquireAgentTurn(
	ctx context.Context,
	agentID string,
) (context.Context, func(), error) {
	agentID = routing.NormalizeAgentID(agentID)
	if agentID == "" || al == nil {
		return ctx, func() {}, nil
	}
	if admissions, ok := ctx.Value(agentTurnAdmissionsKey{}).(map[string]struct{}); ok {
		if _, admitted := admissions[agentID]; admitted {
			return ctx, func() {}, nil
		}
	}

	semaphore := al.agentTurnSems[agentID]
	if semaphore == nil {
		return ctx, func() {}, nil
	}
	select {
	case semaphore <- struct{}{}:
	case <-ctx.Done():
		return ctx, nil, ctx.Err()
	}

	admissions := make(map[string]struct{})
	if inherited, ok := ctx.Value(agentTurnAdmissionsKey{}).(map[string]struct{}); ok {
		for admittedAgentID := range inherited {
			admissions[admittedAgentID] = struct{}{}
		}
	}
	admissions[agentID] = struct{}{}
	admittedCtx := context.WithValue(ctx, agentTurnAdmissionsKey{}, admissions)
	return admittedCtx, func() { <-semaphore }, nil
}
