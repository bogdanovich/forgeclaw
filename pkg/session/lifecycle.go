package session

import (
	"fmt"
	"strings"
	"time"
)

const (
	LifecycleNever    = "never"
	LifecycleCalendar = "calendar"
	LifecycleIdle     = "idle"
	LifecycleMaxAge   = "max_age"
)

type LifecyclePolicy struct {
	Strategy    string
	Period      string
	Timezone    string
	IdleTimeout time.Duration
	MaxAge      time.Duration
}

func (p LifecyclePolicy) NormalizedStrategy() string {
	strategy := strings.ToLower(strings.TrimSpace(p.Strategy))
	if strategy == "" {
		return LifecycleNever
	}
	return strategy
}

func CalendarEpoch(policy LifecyclePolicy, now time.Time) (SessionEpoch, error) {
	location, err := time.LoadLocation(strings.TrimSpace(policy.Timezone))
	if err != nil {
		return SessionEpoch{}, fmt.Errorf("load lifecycle timezone: %w", err)
	}
	local := now.In(location)
	period := strings.ToLower(strings.TrimSpace(policy.Period))

	var id string
	var start time.Time
	switch period {
	case "day":
		id = local.Format("2006-01-02")
		start = time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, location)
	case "week":
		year, week := local.ISOWeek()
		id = fmt.Sprintf("%04d-W%02d", year, week)
		weekdayOffset := (int(local.Weekday()) + 6) % 7
		monday := local.AddDate(0, 0, -weekdayOffset)
		start = time.Date(monday.Year(), monday.Month(), monday.Day(), 0, 0, 0, 0, location)
	case "month":
		id = local.Format("2006-01")
		start = time.Date(local.Year(), local.Month(), 1, 0, 0, 0, 0, location)
	default:
		return SessionEpoch{}, fmt.Errorf("unsupported calendar lifecycle period %q", policy.Period)
	}

	return SessionEpoch{
		Strategy: LifecycleCalendar,
		ID:       period + ":" + policy.Timezone + ":" + id,
		Start:    start,
	}, nil
}

func ApplyEpoch(routeScope SessionScope, routeScopeKey string, epoch SessionEpoch) SessionScope {
	scope := *CloneScope(&routeScope)
	scope.Version = ScopeVersionV2
	scope.RouteScopeKey = strings.TrimSpace(routeScopeKey)
	scope.Epoch = &epoch
	return scope
}
