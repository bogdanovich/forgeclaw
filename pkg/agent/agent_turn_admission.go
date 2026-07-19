package agent

import (
	"context"
	"sync"

	"github.com/sipeed/picoclaw/pkg/routing"
)

type agentTurnAdmissionsKey struct{}

type agentTurnAdmissionController struct {
	mu      sync.Mutex
	limits  map[string]int
	active  map[string]int
	changed chan struct{}
}

func newAgentTurnAdmissionController(registry *AgentRegistry) *agentTurnAdmissionController {
	controller := &agentTurnAdmissionController{
		limits:  make(map[string]int),
		active:  make(map[string]int),
		changed: make(chan struct{}),
	}
	controller.update(registry)
	return controller
}

func agentTurnLimits(registry *AgentRegistry) map[string]int {
	limits := make(map[string]int)
	if registry == nil {
		return limits
	}
	for _, agentID := range registry.ListAgentIDs() {
		agent, ok := registry.GetAgent(agentID)
		if !ok || agent == nil || agent.MaxParallelTurns <= 0 {
			continue
		}
		limits[agentID] = agent.MaxParallelTurns
	}
	return limits
}

func (c *agentTurnAdmissionController) update(registry *AgentRegistry) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.limits = agentTurnLimits(registry)
	c.notifyLocked()
	c.mu.Unlock()
}

func (c *agentTurnAdmissionController) acquire(ctx context.Context, agentID string) (func(), error) {
	for {
		c.mu.Lock()
		limit := c.limits[agentID]
		if limit <= 0 || c.active[agentID] < limit {
			c.active[agentID]++
			c.mu.Unlock()
			return func() { c.release(agentID) }, nil
		}
		changed := c.changed
		c.mu.Unlock()

		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (c *agentTurnAdmissionController) release(agentID string) {
	c.mu.Lock()
	if c.active[agentID] <= 1 {
		delete(c.active, agentID)
	} else {
		c.active[agentID]--
	}
	c.notifyLocked()
	c.mu.Unlock()
}

func (c *agentTurnAdmissionController) notifyLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}

func (al *AgentLoop) acquireAgentTurn(
	ctx context.Context,
	agentID string,
) (context.Context, func(), error) {
	agentID = routing.NormalizeAgentID(agentID)
	if agentID == "" || al == nil || al.agentTurnAdmissions == nil {
		return ctx, func() {}, nil
	}
	if admissions, ok := ctx.Value(agentTurnAdmissionsKey{}).(map[string]struct{}); ok {
		if _, admitted := admissions[agentID]; admitted {
			return ctx, func() {}, nil
		}
	}

	release, err := al.agentTurnAdmissions.acquire(ctx, agentID)
	if err != nil {
		return ctx, nil, err
	}

	admissions := cloneAgentTurnAdmissions(ctx)
	admissions[agentID] = struct{}{}
	admittedCtx := context.WithValue(ctx, agentTurnAdmissionsKey{}, admissions)
	return admittedCtx, release, nil
}

func inheritAgentTurnAdmissions(dst context.Context, src context.Context) context.Context {
	admissions := cloneAgentTurnAdmissions(src)
	if len(admissions) == 0 {
		return dst
	}
	return context.WithValue(dst, agentTurnAdmissionsKey{}, admissions)
}

func cloneAgentTurnAdmissions(ctx context.Context) map[string]struct{} {
	cloned := make(map[string]struct{})
	if ctx == nil {
		return cloned
	}
	if inherited, ok := ctx.Value(agentTurnAdmissionsKey{}).(map[string]struct{}); ok {
		for agentID := range inherited {
			cloned[agentID] = struct{}{}
		}
	}
	return cloned
}
