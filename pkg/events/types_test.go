package events

import (
	"encoding/json"
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

func TestScopeJSONFlattensTraceScope(t *testing.T) {
	encoded, err := json.Marshal(Scope{
		TraceScope: NewTraceScope("/workspace/main", "turn-1"),
		AgentID:    "main",
	})
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["workspace"] != "/workspace/main" || decoded["turn_id"] != "turn-1" {
		t.Fatalf("encoded scope = %s", encoded)
	}
	if _, nested := decoded["TraceScope"]; nested {
		t.Fatalf("trace scope must be flat: %s", encoded)
	}
}
