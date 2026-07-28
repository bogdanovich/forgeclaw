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
