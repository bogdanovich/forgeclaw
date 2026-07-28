package nodes

import (
	"strings"
	"testing"
	"time"
)

func TestTerminalOpenPlanBindsCompleteOwnerAndAuthority(t *testing.T) {
	plan, err := PrepareTerminalOpenPlan(TerminalOpenPlan{
		OpenID:          "open_test",
		IdempotencyKey:  "terminal-open-test",
		NodeID:          ID("node_test"),
		Owner:           testTerminalOwner(),
		CatalogHash:     strings.Repeat("a", 64),
		AuthorityDigest: strings.Repeat("b", 64),
		WorkingScope:    "workspace",
		Columns:         80,
		Rows:            24,
		ApprovalMode:    "session_start",
	}, time.Unix(1_700_000_000, 0), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.ValidateAgainstHash(plan.PlanHash); err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(*TerminalOpenPlan){
		"actor":     func(value *TerminalOpenPlan) { value.Owner.ActorID = "actor_other" },
		"agent":     func(value *TerminalOpenPlan) { value.Owner.AgentID = "agent_other" },
		"route":     func(value *TerminalOpenPlan) { value.Owner.RouteID = "route_other" },
		"session":   func(value *TerminalOpenPlan) { value.Owner.SessionID = "session_other" },
		"workspace": func(value *TerminalOpenPlan) { value.Owner.WorkspaceID = "workspace_other" },
		"target":    func(value *TerminalOpenPlan) { value.Owner.Target = "target_other" },
		"profile":   func(value *TerminalOpenPlan) { value.Owner.Profile = "profile_other" },
		"catalog":   func(value *TerminalOpenPlan) { value.CatalogHash = strings.Repeat("c", 64) },
		"authority": func(value *TerminalOpenPlan) { value.AuthorityDigest = strings.Repeat("d", 64) },
		"scope":     func(value *TerminalOpenPlan) { value.WorkingScope = "other" },
		"size":      func(value *TerminalOpenPlan) { value.Columns++ },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := plan
			mutate(&changed)
			if err := changed.ValidateAgainstHash(plan.PlanHash); err == nil {
				t.Fatal("mutated terminal plan retained authority")
			}
		})
	}
}

func TestTerminalOpenPlanRejectsBearerOnlyOrUnapprovedRequests(t *testing.T) {
	plan := TerminalOpenPlan{
		OpenID:          "open_test",
		IdempotencyKey:  "terminal-open-test",
		NodeID:          ID("node_test"),
		Owner:           testTerminalOwner(),
		CatalogHash:     strings.Repeat("a", 64),
		AuthorityDigest: strings.Repeat("b", 64),
		WorkingScope:    "workspace",
		Columns:         80,
		Rows:            24,
		ApprovalMode:    "session_start",
	}
	for name, mutate := range map[string]func(*TerminalOpenPlan){
		"missing owner": func(value *TerminalOpenPlan) { value.Owner = TerminalOwner{} },
		"bearer id":     func(value *TerminalOpenPlan) { value.Owner.ActorID = "" },
		"wrong mode":    func(value *TerminalOpenPlan) { value.ApprovalMode = "automatic" },
		"oversize":      func(value *TerminalOpenPlan) { value.Columns = 401 },
	} {
		t.Run(name, func(t *testing.T) {
			changed := plan
			mutate(&changed)
			if _, err := PrepareTerminalOpenPlan(
				changed,
				time.Unix(1_700_000_000, 0),
				time.Minute,
			); err == nil {
				t.Fatal("invalid terminal plan accepted")
			}
		})
	}
}

func testTerminalOwner() TerminalOwner {
	return TerminalOwner{
		ActorID:     "actor_test",
		AgentID:     "agent_test",
		RouteID:     "route_test",
		SessionID:   "session_test",
		WorkspaceID: "workspace_test",
		Target:      "target_test",
		Profile:     "owner-root",
	}
}
