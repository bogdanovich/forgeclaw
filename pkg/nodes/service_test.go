package nodes

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCommandDescriptorBindsSchemasToServiceAuthority(t *testing.T) {
	descriptor := serviceActionDescriptorFixture()
	if err := descriptor.Validate(); err != nil {
		t.Fatalf("valid descriptor: %v", err)
	}

	extraAlias := descriptor
	extraAlias.InputSchema = json.RawMessage(
		`{"oneOf":[{"additionalProperties":false,"properties":{"action":{"const":"restart"},` +
			`"service":{"const":"database"}},"required":["service","action"],"type":"object"}]}`,
	)
	if err := extraAlias.Validate(); err == nil || !strings.Contains(err.Error(), "input schema") {
		t.Fatalf("extra alias validation error = %v", err)
	}

	alteredOutput := descriptor
	alteredOutput.OutputSchema = json.RawMessage(`{"type":"object"}`)
	if err := alteredOutput.Validate(); err == nil || !strings.Contains(err.Error(), "output schema") {
		t.Fatalf("altered output validation error = %v", err)
	}
}

func TestCloneSnapshotIsolatesNestedServiceAuthority(t *testing.T) {
	descriptor := serviceActionDescriptorFixture()
	original := Snapshot{
		Catalog: CapabilityCatalog{Commands: []CommandDescriptor{descriptor}},
	}
	cloned := cloneSnapshot(original)
	cloned.Catalog.Commands[0].ServiceProfiles[0].Services[0].Alias = "database"
	cloned.Catalog.Commands[0].ServiceProfiles[0].Services[0].Actions[0] = ServiceActionStop

	service := original.Catalog.Commands[0].ServiceProfiles[0].Services[0]
	if service.Alias != "vpn" || service.Actions[0] != ServiceActionRestart {
		t.Fatalf("clone mutated retained snapshot authority: %#v", service)
	}
}

func serviceActionDescriptorFixture() CommandDescriptor {
	profiles := []ServiceProfileDescriptor{{
		Alias:    "server-services",
		Revision: "server-services-v1",
		Manager:  "systemd",
		Services: []ServiceDescriptor{{
			Alias: "vpn", Actions: []ServiceAction{ServiceActionRestart},
		}},
		LogLimits: ServiceLogLimits{
			EntriesMax: 100, BytesMax: 4096, AgeSecondsMax: 3600,
		},
		ActionApproval: "required",
	}}
	return CommandDescriptor{
		Name:            "service.action.v1",
		InputSchema:     ServiceCommandInputSchema("service.action.v1", profiles),
		OutputSchema:    ServiceCommandOutputSchema("service.action.v1"),
		Risk:            RiskPrivileged,
		ServiceProfiles: profiles,
	}
}
