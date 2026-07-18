package toolpolicy

import "testing"

func TestResultRetentionPolicyValidate(t *testing.T) {
	tests := []struct {
		name    string
		policy  ResultRetentionPolicy
		wantErr bool
	}{
		{
			name: "valid",
			policy: ResultRetentionPolicy{
				"read":  {Mode: ResultRetentionTransient},
				"write": {Mode: ResultRetentionDurable, Receipt: "Write completed."},
			},
		},
		{
			name:    "unknown mode",
			policy:  ResultRetentionPolicy{"tool": {Mode: "unknown"}},
			wantErr: true,
		},
		{
			name:    "missing receipt",
			policy:  ResultRetentionPolicy{"tool": {Mode: ResultRetentionDurable}},
			wantErr: true,
		},
		{
			name:    "untrimmed tool name",
			policy:  ResultRetentionPolicy{" tool": {Mode: ResultRetentionTransient}},
			wantErr: true,
		},
		{
			name: "receipt on transient mode",
			policy: ResultRetentionPolicy{
				"tool": {Mode: ResultRetentionTransient, Receipt: "unused"},
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.policy.Validate()
			if (err != nil) != test.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}
