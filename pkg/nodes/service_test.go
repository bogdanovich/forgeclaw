package nodes

import (
	"encoding/json"
	"fmt"
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

func TestServiceActionSchemaDeduplicatesAuthorityAcrossProfiles(t *testing.T) {
	descriptor := serviceActionDescriptorFixture()
	second := descriptor.ServiceProfiles[0]
	second.Alias = "other-services"
	second.Revision = "other-services-v1"
	second.Services = CloneServiceProfileDescriptors([]ServiceProfileDescriptor{second})[0].Services
	descriptor.ServiceProfiles = append([]ServiceProfileDescriptor{second}, descriptor.ServiceProfiles...)
	descriptor.InputSchema = ServiceCommandInputSchema(descriptor.Name, descriptor.ServiceProfiles)
	if err := descriptor.Validate(); err != nil {
		t.Fatalf("two-profile descriptor: %v", err)
	}
	_, input, err := canonicalInvocationInputValue(
		json.RawMessage(`{"service":"vpn","action":"restart"}`),
	)
	if err == nil {
		err = validateInvocationInput(descriptor.InputSchema, input)
	}
	if err != nil {
		t.Fatalf("authorized duplicate pair was unusable: %v", err)
	}
}

func TestServiceActionSchemaFitsMaximumSingleProfileAuthority(t *testing.T) {
	actions := []ServiceAction{
		ServiceActionDisable,
		ServiceActionEnable,
		ServiceActionReload,
		ServiceActionRestart,
		ServiceActionStart,
		ServiceActionStop,
	}
	services := make([]ServiceDescriptor, MaxServicesPerProfile)
	for index := range services {
		prefix := fmt.Sprintf("service_%02d_", index)
		services[index] = ServiceDescriptor{
			Alias:   prefix + strings.Repeat("x", MaxAliasLength-len(prefix)),
			Actions: append([]ServiceAction(nil), actions...),
		}
	}
	profiles := []ServiceProfileDescriptor{{
		Alias:          "maximum-services",
		Revision:       "maximum-services-v1",
		Manager:        "systemd",
		Services:       services,
		LogLimits:      ServiceLogLimits{EntriesMax: 1, BytesMax: 1, AgeSecondsMax: 1},
		ActionApproval: "required",
	}}
	descriptor := CommandDescriptor{
		Name:            "service.action.v1",
		InputSchema:     ServiceCommandInputSchema("service.action.v1", profiles),
		OutputSchema:    ServiceCommandOutputSchema("service.action.v1"),
		Risk:            RiskPrivileged,
		ServiceProfiles: profiles,
	}
	if len(descriptor.InputSchema) > MaxSchemaBytes {
		t.Fatalf("maximum action schema = %d bytes, limit %d", len(descriptor.InputSchema), MaxSchemaBytes)
	}
	if err := descriptor.Validate(); err != nil {
		t.Fatalf("maximum single-profile authority: %v", err)
	}
}

func TestProjectServiceLogsPreservesStricterContractOutputLimit(t *testing.T) {
	profiles := []ServiceProfileDescriptor{{
		Alias:    "server-services",
		Revision: "server-services-v1",
		Manager:  "systemd",
		Services: []ServiceDescriptor{{Alias: "vpn", Logs: true}},
		LogLimits: ServiceLogLimits{
			EntriesMax: 100, BytesMax: 256 * 1024, AgeSecondsMax: 3600,
		},
		ActionApproval: "required",
	}}
	descriptor := CommandDescriptor{
		Name:         "service.logs.v1",
		InputSchema:  ServiceCommandInputSchema("service.logs.v1", profiles),
		OutputSchema: ServiceCommandOutputSchema("service.logs.v1"),
		Risk:         RiskRead,
		ModelContract: &CommandModelContract{
			Availability: ModelUnavailable, TimeoutSecondsMax: 30,
			OutputBytesMax: 64 * 1024, ResultKind: "json",
			AuthorityDigest: strings.Repeat("a", 64),
			Guidance:        []string{}, Examples: []json.RawMessage{},
		},
		ServiceProfiles: profiles,
	}
	projected, ok := ProjectServiceDescriptorForProfile(descriptor, "server-services")
	if !ok {
		t.Fatal("project service logs descriptor")
	}
	if projected.ModelContract.OutputBytesMax != 64*1024 {
		t.Fatalf(
			"projected output limit = %d, want %d",
			projected.ModelContract.OutputBytesMax,
			64*1024,
		)
	}
}

func TestServiceProfileRejectsDescriptionProjectionBeyondBudget(t *testing.T) {
	services := make([]ServiceDescriptor, MaxServicesPerProfile)
	for index := range services {
		services[index] = ServiceDescriptor{
			Alias:       fmt.Sprintf("service_%02d", index),
			Description: strings.Repeat("d", MaxServiceDescriptionProjectionBytes/MaxServicesPerProfile+1),
			Status:      true,
		}
	}
	profile := ServiceProfileDescriptor{
		Alias:          "maximum-services",
		Revision:       "maximum-services-v1",
		Manager:        "systemd",
		Services:       services,
		LogLimits:      ServiceLogLimits{EntriesMax: 1, BytesMax: 1, AgeSecondsMax: 1},
		ActionApproval: "required",
	}
	if err := profile.Validate(); err == nil || !strings.Contains(err.Error(), "projection budget") {
		t.Fatalf("description projection validation error = %v", err)
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
