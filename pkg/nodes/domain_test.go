package nodes

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestCapabilityCatalogHashIsCanonical(t *testing.T) {
	first := CapabilityCatalog{Commands: []CommandDescriptor{
		descriptor("system.exec.v1", `{"type":"object","properties":{"b":{"type":"string"},"a":{"type":"string"}}}`),
		descriptor("node.info.v1", `{"type":"object"}`),
	}}
	second := CapabilityCatalog{Commands: []CommandDescriptor{
		descriptor("node.info.v1", `{"type":"object"}`),
		descriptor("system.exec.v1", `{"properties":{"a":{"type":"string"},"b":{"type":"string"}},"type":"object"}`),
	}}

	firstHash, err := first.Hash()
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := second.Hash()
	if err != nil {
		t.Fatal(err)
	}
	if firstHash != secondHash {
		t.Fatalf("catalog hashes differ: %s != %s", firstHash, secondHash)
	}
}

func TestCapabilityCatalogHashPreservesLargeIntegers(t *testing.T) {
	first := CapabilityCatalog{Commands: []CommandDescriptor{
		descriptor("system.exec.v1", `{"type":"integer","maximum":9007199254740992}`),
	}}
	second := CapabilityCatalog{Commands: []CommandDescriptor{
		descriptor("system.exec.v1", `{"type":"integer","maximum":9007199254740993}`),
	}}

	firstHash, err := first.Hash()
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := second.Hash()
	if err != nil {
		t.Fatal(err)
	}
	if firstHash == secondHash {
		t.Fatalf("distinct large integers produced the same hash: %s", firstHash)
	}
}

func TestCapabilityCatalogHashNormalizesEquivalentNumbers(t *testing.T) {
	first := CapabilityCatalog{Commands: []CommandDescriptor{
		descriptor("system.exec.v1", `{"type":"number","multipleOf":1.0}`),
	}}
	second := CapabilityCatalog{Commands: []CommandDescriptor{
		descriptor("system.exec.v1", `{"multipleOf":1e0,"type":"number"}`),
	}}
	firstHash, err := first.Hash()
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := second.Hash()
	if err != nil {
		t.Fatal(err)
	}
	if firstHash != secondHash {
		t.Fatalf("equivalent numbers produced different hashes: %s != %s", firstHash, secondHash)
	}
}

func TestCapabilityCatalogRejectsDuplicateSchemaMembers(t *testing.T) {
	catalog := CapabilityCatalog{Commands: []CommandDescriptor{
		descriptor("system.exec.v1", `{"type":"object","type":"array"}`),
	}}
	if _, err := catalog.Hash(); !errors.Is(err, ErrInvalidCapability) {
		t.Fatalf("Hash() error = %v", err)
	}
}

func TestCapabilityCatalogRejectsInvalidJSONSchema(t *testing.T) {
	catalog := CapabilityCatalog{Commands: []CommandDescriptor{
		descriptor("system.exec.v1", `{"type":"not-a-json-schema-type"}`),
	}}
	if err := catalog.Validate(); !errors.Is(err, ErrInvalidCapability) {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestCommandDescriptorDerivesCapability(t *testing.T) {
	value := descriptor("system.exec.v1", `{}`)
	if got := value.Capability(); got != "system" {
		t.Fatalf("Capability() = %q, want system", got)
	}
}

func TestCapabilityCatalogRejectsInvalidDescriptors(t *testing.T) {
	invalidRisk := descriptor("system.exec.v1", `{}`)
	invalidRisk.Risk = Risk("unsafe")
	tests := []struct {
		name    string
		catalog CapabilityCatalog
	}{
		{
			name: "unversioned command",
			catalog: CapabilityCatalog{
				Commands: []CommandDescriptor{descriptor("system.exec", `{}`)},
			},
		},
		{name: "invalid risk", catalog: CapabilityCatalog{Commands: []CommandDescriptor{invalidRisk}}},
		{
			name: "array schema",
			catalog: CapabilityCatalog{
				Commands: []CommandDescriptor{descriptor("system.exec.v1", `[]`)},
			},
		},
		{name: "duplicate", catalog: CapabilityCatalog{Commands: []CommandDescriptor{
			descriptor("system.exec.v1", `{}`), descriptor("system.exec.v1", `{}`),
		}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.catalog.Validate(); !errors.Is(err, ErrInvalidCapability) {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func TestSnapshotValidation(t *testing.T) {
	snapshot := Snapshot{
		ID:      ID("node_ed25519-example"),
		Aliases: []Alias{"build-box"},
		State:   StateConnected,
		Catalog: CapabilityCatalog{},
	}
	if err := snapshot.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	snapshot.Aliases = append(snapshot.Aliases, "build-box")
	if err := snapshot.Validate(); !errors.Is(err, ErrInvalidNode) {
		t.Fatalf("duplicate aliases error = %v", err)
	}
}

func TestSnapshotValidatesCatalogHash(t *testing.T) {
	catalog := CapabilityCatalog{Commands: []CommandDescriptor{
		descriptor("node.info.v1", `{}`),
	}}
	hash, err := catalog.Hash()
	if err != nil {
		t.Fatal(err)
	}
	snapshot := Snapshot{
		ID:          ID("node_example"),
		State:       StateConnected,
		Catalog:     catalog,
		CatalogHash: hash,
	}
	if err := snapshot.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	snapshot.CatalogHash = strings.Repeat("0", 64)
	if err := snapshot.Validate(); !errors.Is(err, ErrInvalidNode) {
		t.Fatalf("stale catalog hash error = %v", err)
	}
	snapshot.CatalogHash = "not-a-digest"
	if err := snapshot.Validate(); !errors.Is(err, ErrInvalidNode) {
		t.Fatalf("malformed catalog hash error = %v", err)
	}
}

func descriptor(name string, input string) CommandDescriptor {
	return CommandDescriptor{
		Name:         name,
		InputSchema:  json.RawMessage(input),
		OutputSchema: json.RawMessage(`{"type":"object"}`),
		Risk:         RiskRead,
	}
}
