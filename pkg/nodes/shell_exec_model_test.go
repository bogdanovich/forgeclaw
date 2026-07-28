package nodes

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestShellExecModelInputSchemaExposesOnlySafeAliases(t *testing.T) {
	contract := CommandModelContract{
		Availability:      ModelAvailable,
		TimeoutSecondsMax: 30,
		OutputBytesMax:    4096,
		ResultKind:        "json",
		AuthorityDigest:   strings.Repeat("a", 64),
		ApprovalMode:      "each_command",
		Constraints: CommandModelConstraints{
			ProfileAliases:   []string{"owner"},
			WorkingScopes:    []string{"workspace"},
			EnvironmentNames: []string{"LANG"},
		},
		Guidance: []string{"Use the configured owner profile."},
		Examples: []json.RawMessage{
			json.RawMessage(
				`{"profile":"owner","script":"printf ok","cwd":"workspace","env":{"LANG":"C"},"timeout_seconds":5}`,
			),
		},
	}
	schema, err := ShellExecModelInputSchema(contract)
	if err != nil {
		t.Fatal(err)
	}
	if err := contract.Validate(schema); err != nil {
		t.Fatal(err)
	}
	for _, hidden := range []string{"/bin/sh", "/srv/private", "uid", "broker"} {
		if strings.Contains(string(schema), hidden) {
			t.Fatalf("shell model schema leaked %q: %s", hidden, schema)
		}
	}
}

func TestShellExecModelInputSchemaFailsClosedWithoutProfiles(t *testing.T) {
	contract := CommandModelContract{
		Availability:      ModelUnavailable,
		TimeoutSecondsMax: 30,
		OutputBytesMax:    4096,
		ResultKind:        "json",
		ApprovalMode:      "each_command",
		Constraints: CommandModelConstraints{
			WorkingScopes: []string{"workspace"},
		},
		Guidance: []string{},
		Examples: []json.RawMessage{
			json.RawMessage(
				`{"profile":"invented","script":"true","cwd":"workspace","env":{},"timeout_seconds":5}`,
			),
		},
	}
	schema, err := ShellExecModelInputSchema(contract)
	if err != nil {
		t.Fatal(err)
	}
	if err := contract.Validate(schema); err == nil {
		t.Fatal("profile-free shell schema accepted an invented profile")
	}
}

func TestValidateShellExecModelInputEnforcesByteCeilings(t *testing.T) {
	valid := map[string]any{
		"script": strings.Repeat("x", MaxShellExecScriptBytes),
		"env":    map[string]any{},
	}
	if err := ValidateShellExecModelInput(valid); err != nil {
		t.Fatalf("valid byte limits rejected: %v", err)
	}
	tests := []struct {
		name  string
		input map[string]any
	}{
		{
			name: "multibyte script",
			input: map[string]any{
				"script": strings.Repeat("界", MaxShellExecScriptBytes/3+1),
				"env":    map[string]any{},
			},
		},
		{
			name: "aggregate environment",
			input: map[string]any{
				"script": "true",
				"env": map[string]any{
					"LANG": strings.Repeat("x", MaxShellExecEnvironmentBytes/2),
					"TERM": strings.Repeat("y", MaxShellExecEnvironmentBytes/2),
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateShellExecModelInput(test.input); err == nil {
				t.Fatal("oversized shell input was accepted")
			}
		})
	}
}
