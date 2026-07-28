package nodes

import (
	"encoding/base64"
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

func TestTerminalEventValidationIsTypeSpecific(t *testing.T) {
	output := TerminalEvent{
		Version: TerminalProtocolVersion, Type: "output", TerminalID: "terminal_test",
		Cursor: 1, DataBase64: base64.StdEncoding.EncodeToString([]byte("x")),
	}
	if size, err := output.Validate(); err != nil || size != 1 {
		t.Fatalf("output validation = (%d, %v)", size, err)
	}
	valid := []TerminalEvent{
		{
			Version: TerminalProtocolVersion, Type: "ack", TerminalID: "terminal_test",
			AcceptedSequence: 1, State: "live",
		},
		{
			Version: TerminalProtocolVersion, Type: "denied", TerminalID: "terminal_test",
			State: "live", Reason: "invalid_sequence",
		},
		{
			Version: TerminalProtocolVersion, Type: "closed", TerminalID: "terminal_test",
			State: "closed", Reason: "exit", StartedAt: 1, CompletedAt: 2,
			TerminationConfirmed: true,
		},
		{
			Version: TerminalProtocolVersion, Type: "unknown", TerminalID: "terminal_test",
			State: "unknown", Reason: "disconnect", StartedAt: 1,
		},
	}
	for _, event := range valid {
		if _, err := event.Validate(); err != nil {
			t.Fatalf("valid %s event: %v", event.Type, err)
		}
	}
	invalid := []TerminalEvent{
		{Version: TerminalProtocolVersion, Type: "mystery", TerminalID: "terminal_test"},
		{Version: TerminalProtocolVersion, Type: "output", TerminalID: "terminal_test", Cursor: 1},
		{
			Version: TerminalProtocolVersion, Type: "ack", TerminalID: "terminal_test",
			AcceptedSequence: 1, State: "live", DataBase64: output.DataBase64,
		},
		{
			Version: TerminalProtocolVersion, Type: "closed", TerminalID: "terminal_test",
			State: "closed", Reason: "exit", StartedAt: 1,
		},
		{
			Version: TerminalProtocolVersion + 1, Type: "ack", TerminalID: "terminal_test",
			AcceptedSequence: 1, State: "live",
		},
	}
	for _, event := range invalid {
		if _, err := event.Validate(); err == nil {
			t.Fatalf("invalid event accepted: %#v", event)
		}
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
