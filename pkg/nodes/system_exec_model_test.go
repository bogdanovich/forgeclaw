package nodes

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSystemExecModelInputSchemaAcceptsOnlyVisibleAliases(t *testing.T) {
	contract := CommandModelContract{
		Availability:      ModelAvailable,
		TimeoutSecondsMax: 12,
		OutputBytesMax:    2048,
		ResultKind:        "json",
		Constraints: CommandModelConstraints{
			ExecutableAliases: []string{"diagnostic"},
			WorkingScopes:     []string{"workspace"},
			EnvironmentNames:  []string{"LANG"},
		},
		Guidance: []string{"Use the diagnostic alias."},
		Examples: []json.RawMessage{
			json.RawMessage(
				`{"argv":["diagnostic","--version"],"cwd":"workspace","timeout_seconds":5,"env":{"LANG":"C"}}`,
			),
		},
	}
	schema, err := SystemExecModelInputSchema(contract)
	if err != nil {
		t.Fatal(err)
	}
	if err := contract.Validate(schema); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(schema), "/usr/bin") {
		t.Fatalf("model schema leaked a host path: %s", schema)
	}

	hidden := contract
	hidden.Examples = []json.RawMessage{
		json.RawMessage(
			`{"argv":["/usr/bin/diagnostic"],"cwd":"/srv/workspace","timeout_seconds":5,"env":{}}`,
		),
	}
	if err := hidden.Validate(schema); err == nil {
		t.Fatal("model schema accepted hidden raw enforcement paths")
	}
}

func TestSystemExecModelInputSchemaFailsClosedWithoutAliases(t *testing.T) {
	contract := CommandModelContract{
		Availability:      ModelPartiallyDescribed,
		TimeoutSecondsMax: 30,
		OutputBytesMax:    4096,
		ResultKind:        "json",
		Guidance:          []string{},
		Examples:          []json.RawMessage{},
	}
	schema, err := SystemExecModelInputSchema(contract)
	if err != nil {
		t.Fatal(err)
	}
	contract.Examples = []json.RawMessage{
		json.RawMessage(
			`{"argv":["anything"],"cwd":"anywhere","timeout_seconds":5,"env":{}}`,
		),
	}
	if err := contract.Validate(schema); err == nil {
		t.Fatal("alias-free model schema accepted an invocation")
	}
}
