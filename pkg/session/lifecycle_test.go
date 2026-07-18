package session

import (
	"testing"
	"time"
)

func TestCalendarEpochUsesConfiguredTimezone(t *testing.T) {
	policy := LifecyclePolicy{
		Strategy: LifecycleCalendar,
		Period:   "day",
		Timezone: "America/Los_Angeles",
	}

	beforeMidnight, err := CalendarEpoch(policy, time.Date(2026, 7, 18, 6, 59, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("CalendarEpoch() error = %v", err)
	}
	afterMidnight, err := CalendarEpoch(policy, time.Date(2026, 7, 18, 7, 1, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("CalendarEpoch() error = %v", err)
	}

	if beforeMidnight.ID != "day:America/Los_Angeles:2026-07-17" {
		t.Fatalf("before-midnight epoch = %q", beforeMidnight.ID)
	}
	if afterMidnight.ID != "day:America/Los_Angeles:2026-07-18" {
		t.Fatalf("after-midnight epoch = %q", afterMidnight.ID)
	}
}

func TestCalendarEpochWeekStartsOnISOMonday(t *testing.T) {
	epoch, err := CalendarEpoch(LifecyclePolicy{
		Strategy: LifecycleCalendar,
		Period:   "week",
		Timezone: "UTC",
	}, time.Date(2027, 1, 3, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("CalendarEpoch() error = %v", err)
	}

	if epoch.ID != "week:UTC:2026-W53" {
		t.Fatalf("epoch ID = %q, want ISO week 2026-W53", epoch.ID)
	}
	if want := time.Date(2026, 12, 28, 0, 0, 0, 0, time.UTC); !epoch.Start.Equal(want) {
		t.Fatalf("epoch start = %v, want %v", epoch.Start, want)
	}
}

func TestApplyEpochCreatesIsolatedStructuredScope(t *testing.T) {
	routeScope := SessionScope{
		Version: ScopeVersionV1,
		AgentID: "main",
		Channel: "telegram",
		Values:  map[string]string{"chat": "direct:42"},
	}
	routeKey := BuildSessionKey(routeScope)
	epoch := SessionEpoch{Strategy: LifecycleCalendar, ID: "day:UTC:2026-07-17"}

	epochScope := ApplyEpoch(routeScope, routeKey, epoch)
	if epochScope.Version != ScopeVersionV2 || epochScope.RouteScopeKey != routeKey {
		t.Fatalf("epoch scope = %#v", epochScope)
	}
	if got := BuildSessionKey(epochScope); got == routeKey {
		t.Fatal("epoch session key must differ from stable route key")
	}
	if routeScope.Epoch != nil || routeScope.RouteScopeKey != "" {
		t.Fatal("ApplyEpoch mutated the route scope")
	}
}
