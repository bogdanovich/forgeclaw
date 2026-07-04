package agent

import "testing"

func TestTurnRuntimeHostFilterSensitiveData_NilSafe(t *testing.T) {
	const content = "hello sk-test world"

	var nilLoop *AgentLoop
	if got := nilLoop.filterSensitiveData(content); got != content {
		t.Fatalf("nil AgentLoop filterSensitiveData() = %q, want original content", got)
	}

	al := &AgentLoop{}
	if got := al.filterSensitiveData(content); got != content {
		t.Fatalf("nil config filterSensitiveData() = %q, want original content", got)
	}
}
