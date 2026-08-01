package config

import "testing"

func TestEffectiveMCPSessionLossReplay(t *testing.T) {
	tests := []struct {
		name   string
		policy MCPSessionLossReplay
		want   MCPSessionLossReplay
	}{
		{name: "omitted defaults to once", want: MCPSessionLossReplayOnce},
		{name: "once", policy: MCPSessionLossReplayOnce, want: MCPSessionLossReplayOnce},
		{name: "never", policy: MCPSessionLossReplayNever, want: MCPSessionLossReplayNever},
		{name: "normalizes case and whitespace", policy: " NEVER ", want: MCPSessionLossReplayNever},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := MCPServerConfig{SessionLossReplay: test.policy}
			if got := EffectiveMCPSessionLossReplay(server); got != test.want {
				t.Fatalf("EffectiveMCPSessionLossReplay() = %q, want %q", got, test.want)
			}
			if err := ValidateMCPSessionLossReplay(server); err != nil {
				t.Fatalf("ValidateMCPSessionLossReplay() error = %v", err)
			}
		})
	}
}

func TestValidateMCPSessionLossReplayRejectsUnknownPolicy(t *testing.T) {
	server := MCPServerConfig{SessionLossReplay: "automatic"}
	if err := ValidateMCPSessionLossReplay(server); err == nil {
		t.Fatal("ValidateMCPSessionLossReplay() error = nil, want unsupported policy")
	}
}
