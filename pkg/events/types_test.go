package events

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestTraceScopeNormalizesAndRequiresWorkspaceAndTurn(t *testing.T) {
	scope := NewTraceScope(" /workspace/main ", " turn-1 ")
	if !scope.Complete() || scope.Workspace != "/workspace/main" || scope.TurnID != "turn-1" {
		t.Fatalf("normalized trace scope = %+v", scope)
	}
	if NewTraceScope("", "turn-1").Complete() || NewTraceScope("/workspace/main", "").Complete() {
		t.Fatal("incomplete trace scope reported complete")
	}
}

func TestScopeSerializesTraceIdentityAsFlatFields(t *testing.T) {
	data, err := json.Marshal(Scope{
		Workspace: "/workspace/main",
		TurnID:    "turn-1",
		AgentID:   "main",
	})
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if !strings.Contains(text, `"workspace":"/workspace/main"`) ||
		!strings.Contains(text, `"turn_id":"turn-1"`) || strings.Contains(text, `"trace_scope"`) {
		t.Fatalf("serialized scope = %s", data)
	}
}
