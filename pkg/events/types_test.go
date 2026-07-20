package events

import "testing"

func TestTraceScopeNormalizesAndRequiresWorkspaceAndTurn(t *testing.T) {
	scope := NewTraceScope(" /workspace/main ", " turn-1 ")
	if !scope.Complete() || scope.Workspace != "/workspace/main" || scope.TurnID != "turn-1" {
		t.Fatalf("normalized trace scope = %+v", scope)
	}
	if NewTraceScope("", "turn-1").Complete() || NewTraceScope("/workspace/main", "").Complete() {
		t.Fatal("incomplete trace scope reported complete")
	}
}
