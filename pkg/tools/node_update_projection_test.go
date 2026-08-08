package tools

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

func TestProjectUpdateDescriptorRequiresExactTargetProfile(t *testing.T) {
	descriptor := updateProjectionDescriptor(t)
	if _, available := projectDescriptorForTarget(descriptor, "", "", ""); available {
		t.Fatal("update descriptor was visible without a target profile")
	}
	if _, available := projectDescriptorForTarget(descriptor, "", "", "missing"); available {
		t.Fatal("update descriptor was visible for an unknown target profile")
	}
	projected, available := projectDescriptorForTarget(descriptor, "", "", "stable")
	if !available {
		t.Fatal("stable update descriptor was unavailable")
	}
	if len(projected.UpdateProfiles) != 1 || projected.UpdateProfiles[0].Alias != "stable" ||
		projected.ModelContract == nil || projected.ModelContract.ApprovalMode != "each_command" {
		t.Fatalf("projected descriptor = %#v", projected)
	}
	expected := nodes.NodeUpdateInputSchema(projected.UpdateProfiles)
	if !bytes.Equal(canonicalTestJSON(t, projected.InputSchema), canonicalTestJSON(t, expected)) {
		t.Fatalf("projected schema = %s; want %s", projected.InputSchema, expected)
	}
	encoded, err := json.Marshal(projected)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte("nightly-current")) || bytes.Contains(encoded, []byte("v1.3.0-nightly.1")) {
		t.Fatalf("projection leaked another profile: %s", encoded)
	}
}

func updateProjectionDescriptor(t *testing.T) nodes.CommandDescriptor {
	t.Helper()
	profiles := []nodes.UpdateProfileDescriptor{
		{
			Alias: "nightly", Revision: "nightly-v1", Channel: "nightly", Approval: "required",
			Releases: []nodes.UpdateReleaseDescriptor{{
				Alias: "nightly-current", Version: "v1.3.0-nightly.1",
			}},
		},
		{
			Alias: "stable", Revision: "stable-v1", Channel: "stable", Approval: "required",
			Releases: []nodes.UpdateReleaseDescriptor{{Alias: "stable-current", Version: "v1.2.3"}},
		},
	}
	descriptor := nodes.CommandDescriptor{
		Name:         "node.update.v1",
		InputSchema:  nodes.NodeUpdateInputSchema(profiles),
		OutputSchema: json.RawMessage(`{"type":"object","additionalProperties":false}`),
		Risk:         nodes.RiskPrivileged,
		ModelContract: &nodes.CommandModelContract{
			Availability: nodes.ModelAvailable, TimeoutSecondsMax: 300, OutputBytesMax: 4096,
			ResultKind: "json", ApprovalMode: "each_command",
			Constraints: nodes.CommandModelConstraints{ProfileAliases: []string{"nightly", "stable"}},
			Guidance:    []string{}, Examples: []json.RawMessage{},
		},
		UpdateProfiles: profiles,
	}
	if err := descriptor.Validate(); err != nil {
		t.Fatalf("test descriptor: %v", err)
	}
	return descriptor
}

func canonicalTestJSON(t *testing.T, value []byte) []byte {
	t.Helper()
	var decoded any
	if err := json.Unmarshal(value, &decoded); err != nil {
		t.Fatal(err)
	}
	canonical, err := json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	return canonical
}
