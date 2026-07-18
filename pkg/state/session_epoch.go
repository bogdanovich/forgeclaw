package state

import (
	"fmt"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/session"
)

func (sm *Manager) ResolveSessionEpoch(
	routeScopeKey string,
	policy session.LifecyclePolicy,
	now time.Time,
) (*session.SessionEpoch, error) {
	routeScopeKey = strings.TrimSpace(routeScopeKey)
	if routeScopeKey == "" {
		return nil, fmt.Errorf("route scope key is required")
	}
	if now.IsZero() {
		now = time.Now()
	}

	switch policy.NormalizedStrategy() {
	case session.LifecycleNever:
		return nil, nil
	case session.LifecycleCalendar:
		epoch, err := session.CalendarEpoch(policy, now)
		if err != nil {
			return nil, err
		}
		return &epoch, nil
	case session.LifecycleIdle, session.LifecycleMaxAge:
		return sm.resolveStatefulSessionEpoch(routeScopeKey, policy, now)
	default:
		return nil, fmt.Errorf("unsupported session lifecycle strategy %q", policy.Strategy)
	}
}

func (sm *Manager) resolveStatefulSessionEpoch(
	routeScopeKey string,
	policy session.LifecyclePolicy,
	now time.Time,
) (*session.SessionEpoch, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	strategy := policy.NormalizedStrategy()
	if strategy == session.LifecycleIdle && policy.IdleTimeout <= 0 {
		return nil, fmt.Errorf("idle lifecycle requires a positive timeout")
	}
	if strategy == session.LifecycleMaxAge && policy.MaxAge <= 0 {
		return nil, fmt.Errorf("max-age lifecycle requires a positive duration")
	}

	checkpoint, found := sm.state.SessionEpochs[routeScopeKey]
	rotate := !found || checkpoint.Strategy != strategy || checkpoint.StartedAt.IsZero()
	if !rotate {
		switch strategy {
		case session.LifecycleIdle:
			rotate = !checkpoint.LastActivityAt.IsZero() &&
				now.Sub(checkpoint.LastActivityAt) >= policy.IdleTimeout
		case session.LifecycleMaxAge:
			rotate = now.Sub(checkpoint.StartedAt) >= policy.MaxAge
		}
	}

	changed := rotate
	if rotate {
		checkpoint = SessionEpochState{
			Strategy:       strategy,
			EpochID:        strategy + ":" + now.UTC().Format(time.RFC3339Nano),
			StartedAt:      now,
			LastActivityAt: now,
		}
	} else if strategy == session.LifecycleIdle && now.After(checkpoint.LastActivityAt) {
		checkpoint.LastActivityAt = now
		changed = true
	}

	if !changed {
		return &session.SessionEpoch{
			Strategy: checkpoint.Strategy,
			ID:       checkpoint.EpochID,
			Start:    checkpoint.StartedAt,
		}, nil
	}

	if sm.state.SessionEpochs == nil {
		sm.state.SessionEpochs = make(map[string]SessionEpochState)
	}
	sm.state.SessionEpochs[routeScopeKey] = checkpoint
	sm.state.Timestamp = now
	if err := sm.saveAtomic(); err != nil {
		return nil, fmt.Errorf("persist session epoch: %w", err)
	}

	return &session.SessionEpoch{
		Strategy: checkpoint.Strategy,
		ID:       checkpoint.EpochID,
		Start:    checkpoint.StartedAt,
	}, nil
}

func (sm *Manager) TouchSessionEpoch(routeScopeKey, epochID string, now time.Time) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	routeScopeKey = strings.TrimSpace(routeScopeKey)
	epochID = strings.TrimSpace(epochID)
	if routeScopeKey == "" || epochID == "" {
		return fmt.Errorf("route scope key and epoch ID are required")
	}
	if now.IsZero() {
		now = time.Now()
	}
	if len(sm.state.SessionEpochs) == 0 {
		return nil
	}
	checkpoint, found := sm.state.SessionEpochs[routeScopeKey]
	if !found || checkpoint.EpochID != epochID || !now.After(checkpoint.LastActivityAt) {
		return nil
	}
	checkpoint.LastActivityAt = now
	sm.state.SessionEpochs[routeScopeKey] = checkpoint
	sm.state.Timestamp = now
	if err := sm.saveAtomic(); err != nil {
		return fmt.Errorf("persist session epoch activity: %w", err)
	}
	return nil
}
