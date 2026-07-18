package agent

import (
	"fmt"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/session"
)

func lifecyclePolicy(cfg config.SessionLifecycleConfig) session.LifecyclePolicy {
	return session.LifecyclePolicy{
		Strategy:    cfg.Strategy,
		Period:      cfg.Period,
		Timezone:    cfg.Timezone,
		IdleTimeout: time.Duration(cfg.IdleTimeoutMinutes) * time.Minute,
		MaxAge:      time.Duration(cfg.MaxAgeMinutes) * time.Minute,
	}
}

func (al *AgentLoop) currentSessionTime() time.Time {
	if al != nil && al.sessionNow != nil {
		return al.sessionNow()
	}
	return time.Now()
}

func (al *AgentLoop) applySessionLifecycle(
	allocation session.Allocation,
	configPolicy config.SessionLifecycleConfig,
) (session.Allocation, error) {
	routeScopeKey := strings.TrimSpace(allocation.RouteScopeKey)
	if routeScopeKey == "" {
		routeScopeKey = strings.TrimSpace(allocation.SessionKey)
		allocation.RouteScopeKey = routeScopeKey
	}
	if !configPolicy.Enabled() {
		allocation.Scope.RouteScopeKey = routeScopeKey
		allocation.SessionKey = routeScopeKey
		return allocation, nil
	}
	policy := lifecyclePolicy(configPolicy)
	switch policy.NormalizedStrategy() {
	case session.LifecycleNever:
		allocation.Scope.RouteScopeKey = routeScopeKey
		allocation.SessionKey = routeScopeKey
		return allocation, nil
	case session.LifecycleCalendar:
		epoch, err := session.CalendarEpoch(policy, al.currentSessionTime())
		if err != nil {
			return session.Allocation{}, err
		}
		allocation.Scope = session.ApplyEpoch(allocation.Scope, routeScopeKey, epoch)
		allocation.SessionKey = session.BuildSessionKey(allocation.Scope)
		allocation.SessionAliases = nil
		return allocation, nil
	}
	if al == nil || al.state == nil {
		return session.Allocation{}, fmt.Errorf("session lifecycle state manager is not initialized")
	}

	epoch, err := al.state.ResolveSessionEpoch(routeScopeKey, policy, al.currentSessionTime())
	if err != nil {
		return session.Allocation{}, err
	}
	if epoch == nil {
		allocation.Scope.RouteScopeKey = routeScopeKey
		allocation.SessionKey = routeScopeKey
		return allocation, nil
	}

	allocation.Scope = session.ApplyEpoch(allocation.Scope, routeScopeKey, *epoch)
	allocation.SessionKey = session.BuildSessionKey(allocation.Scope)
	// Rotated epochs intentionally do not alias legacy route history. Old
	// conversations remain available for explicit historical retrieval.
	allocation.SessionAliases = nil
	return allocation, nil
}

func (al *AgentLoop) touchActiveSessionLifecycle(target *inboundDispatchTarget) {
	if al == nil || al.state == nil || target == nil || target.Allocation.Scope.Epoch == nil {
		return
	}
	if target.Allocation.Scope.Epoch.Strategy != session.LifecycleIdle {
		return
	}
	err := al.state.TouchSessionEpoch(
		target.Allocation.RouteScopeKey,
		target.Allocation.Scope.Epoch.ID,
		al.currentSessionTime(),
	)
	if err != nil {
		logger.WarnCF("agent", "Failed to update active session lifecycle", map[string]any{
			"route_scope_key": target.Allocation.RouteScopeKey,
			"session_epoch":   target.Allocation.Scope.Epoch.ID,
			"error":           err.Error(),
		})
	}
}

func sessionEpochID(scope *session.SessionScope) string {
	if scope == nil || scope.Epoch == nil {
		return ""
	}
	return scope.Epoch.ID
}
