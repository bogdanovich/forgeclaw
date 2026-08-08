package companion

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
	nodeupdate "github.com/bogdanovich/mintclaw/pkg/nodes/update"
)

func TestNormalizeUpdateConfigurationProjectsOnlyModelSafeAuthority(t *testing.T) {
	sources, policies, err := normalizeUpdateConfiguration(
		testUpdateSources(t),
		UpdatePolicies{
			"stable-node": {
				Enabled:  true,
				Revision: "stable-node-v1",
				Source:   "production",
				Channel:  nodeupdate.ChannelStable,
				Releases: map[string]UpdateReleaseConfig{
					"stable-2026-08": {
						Tag: "v1.2.3", Version: "v1.2.3", Description: "August stable release",
					},
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("normalizeUpdateConfiguration() error = %v", err)
	}
	if sources["production"].trustedKey.KeyID == "" || policies["stable-node"].Approval != "required" {
		t.Fatalf("normalized config = %#v, %#v", sources, policies)
	}
	descriptors, err := updateProfileDescriptors(policies)
	if err != nil {
		t.Fatalf("updateProfileDescriptors() error = %v", err)
	}
	if len(descriptors) != 1 || descriptors[0].Alias != "stable-node" ||
		len(descriptors[0].Releases) != 1 || descriptors[0].Releases[0].Alias != "stable-2026-08" {
		t.Fatalf("descriptors = %#v", descriptors)
	}
	encoded := strings.Join([]string{
		descriptors[0].Alias,
		descriptors[0].Revision,
		descriptors[0].Releases[0].Alias,
		descriptors[0].Releases[0].Version,
	}, " ")
	if strings.Contains(encoded, "github.com") || strings.Contains(encoded, sources["production"].PublicKey) {
		t.Fatalf("descriptor exposed release trust internals: %s", encoded)
	}
}

func TestNormalizeUpdateConfigurationFailsClosed(t *testing.T) {
	validSources := testUpdateSources(t)
	validPolicy := UpdatePolicyProfile{
		Enabled:  true,
		Revision: "stable-v1",
		Source:   "production",
		Channel:  nodeupdate.ChannelStable,
		Releases: map[string]UpdateReleaseConfig{
			"current": {Tag: "v1.2.3", Version: "v1.2.3"},
		},
	}
	tests := []struct {
		name     string
		sources  UpdateSources
		policies UpdatePolicies
	}{
		{name: "source without policy", sources: validSources},
		{name: "missing source", policies: UpdatePolicies{"stable": validPolicy}},
		{name: "disabled retains release", sources: validSources, policies: UpdatePolicies{
			"stable": {
				Revision: "stable-v1", Source: "production", Channel: nodeupdate.ChannelStable,
				Releases: validPolicy.Releases,
			},
		}},
		{name: "enabled without release", sources: validSources, policies: UpdatePolicies{
			"stable": {
				Enabled: true, Revision: "stable-v1", Source: "production", Channel: nodeupdate.ChannelStable,
			},
		}},
		{name: "nightly stable version", sources: validSources, policies: UpdatePolicies{
			"nightly": {
				Enabled: true, Revision: "nightly-v1", Source: "production", Channel: nodeupdate.ChannelNightly,
				Releases: validPolicy.Releases,
			},
		}},
		{name: "duplicate version", sources: validSources, policies: UpdatePolicies{
			"stable": {
				Enabled: true, Revision: "stable-v1", Source: "production", Channel: nodeupdate.ChannelStable,
				Releases: map[string]UpdateReleaseConfig{
					"first":  {Tag: "v1.2.3", Version: "v1.2.3"},
					"second": {Tag: "release-v1.2.3", Version: "v1.2.3"},
				},
			},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := normalizeUpdateConfiguration(test.sources, test.policies); err == nil {
				t.Fatal("configuration was accepted")
			}
		})
	}

	revoked := testUpdateSources(t)
	source := revoked["production"]
	source.Revoked = true
	revoked["production"] = source
	if _, _, err := normalizeUpdateConfiguration(revoked, UpdatePolicies{"stable": validPolicy}); err == nil {
		t.Fatal("enabled policy accepted a revoked trust source")
	}
}

func TestNormalizeUpdateSourcesRejectsUntrustedEndpointsAndKeys(t *testing.T) {
	for name, source := range map[string]UpdateSourceConfig{
		"plaintext": {BaseURL: "http://example.com/releases", PublicKey: testUpdatePublicKey(t)},
		"credentials": {
			BaseURL: "https://user@example.com/releases", PublicKey: testUpdatePublicKey(t),
		},
		"query": {BaseURL: "https://example.com/releases?token=x", PublicKey: testUpdatePublicKey(t)},
		"traversal": {
			BaseURL: "https://example.com/releases/../other", PublicKey: testUpdatePublicKey(t),
		},
		"bad key": {BaseURL: "https://example.com/releases", PublicKey: "not-a-key"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := normalizeUpdateSources(UpdateSources{"production": source}); err == nil {
				t.Fatal("source was accepted")
			}
		})
	}
}

func TestUpdateDescriptorBindsEnumeratedReleaseSchema(t *testing.T) {
	profile := nodes.UpdateProfileDescriptor{
		Alias: "stable", Revision: "stable-v1", Channel: "stable", Approval: "required",
		Releases: []nodes.UpdateReleaseDescriptor{{Alias: "current", Version: "v1.2.3"}},
	}
	descriptor := nodes.CommandDescriptor{
		Name:           "node.update.v1",
		InputSchema:    nodes.NodeUpdateInputSchema([]nodes.UpdateProfileDescriptor{profile}),
		OutputSchema:   []byte(`{"type":"object","additionalProperties":false}`),
		Risk:           nodes.RiskPrivileged,
		UpdateProfiles: []nodes.UpdateProfileDescriptor{profile},
	}
	if err := descriptor.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	descriptor.InputSchema = []byte(`{"type":"object","additionalProperties":true}`)
	if err := descriptor.Validate(); err == nil {
		t.Fatal("descriptor accepted input outside the enumerated release schema")
	}
}

func testUpdateSources(t *testing.T) UpdateSources {
	t.Helper()
	return UpdateSources{
		"production": {
			BaseURL:   "https://github.com/bogdanovich/mintclaw/releases/download",
			PublicKey: testUpdatePublicKey(t),
		},
	}
}

func testUpdatePublicKey(t *testing.T) string {
	t.Helper()
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return base64.RawStdEncoding.EncodeToString(publicKey)
}
