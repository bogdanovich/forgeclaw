package protocol

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/jsonschema-go/jsonschema"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

func TestEmbeddedSchemasAreValidJSON(t *testing.T) {
	for _, name := range []string{
		"envelope.v1",
		"command-descriptor.v1",
		"execution-plan.v1",
		"node-auth.v1",
	} {
		data, err := Schema(name)
		if err != nil {
			t.Fatalf("Schema(%q) error = %v", name, err)
		}
		if !json.Valid(data) {
			t.Fatalf("Schema(%q) is invalid JSON", name)
		}
	}
}

func TestExecutionPlanSchemaMatchesDomain(t *testing.T) {
	descriptor := nodes.CommandDescriptor{
		Name:         "node.info.v1",
		InputSchema:  json.RawMessage(`{"type":"object","additionalProperties":false}`),
		OutputSchema: json.RawMessage(`{"type":"object"}`),
		Risk:         nodes.RiskRead,
	}
	plan, err := nodes.PrepareExecutionPlan(nodes.InvocationRequest{
		InvocationID:     "inv_test",
		IdempotencyKey:   "idem_test",
		NodeID:           nodes.ID("node_test"),
		CatalogHash:      strings.Repeat("a", 64),
		Command:          descriptor.Name,
		Input:            json.RawMessage(`{}`),
		AgentID:          "main",
		SessionID:        "telegram:chat-1",
		ActorID:          "user-1",
		TimeoutSeconds:   30,
		OutputLimitBytes: 4096,
	}, descriptor, "local", "policy-1", time.Unix(1, 0), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	var instance any
	if unmarshalErr := json.Unmarshal(data, &instance); unmarshalErr != nil {
		t.Fatal(unmarshalErr)
	}
	if validationErr := resolveSchema(t, "execution-plan.v1").Validate(instance); validationErr != nil {
		t.Fatalf("schema rejected execution plan %s: %v", data, validationErr)
	}
	serviceProfile := nodes.ServiceProfileDescriptor{
		Alias: "server-services", Revision: "server-services-v1", Manager: "systemd",
		Services: []nodes.ServiceDescriptor{{Alias: "vpn", Status: true}},
		LogLimits: nodes.ServiceLogLimits{
			EntriesMax: 100, BytesMax: 4096, AgeSecondsMax: 3600,
		},
		ActionApproval: "required",
	}
	serviceDescriptor := nodes.CommandDescriptor{
		Name: "service.status.v1",
		InputSchema: nodes.ServiceCommandInputSchema(
			"service.status.v1",
			[]nodes.ServiceProfileDescriptor{serviceProfile},
		),
		OutputSchema: nodes.ServiceCommandOutputSchema("service.status.v1"),
		Risk:         nodes.RiskRead,
		ModelContract: &nodes.CommandModelContract{
			Availability: nodes.ModelUnavailable, TimeoutSecondsMax: 30,
			OutputBytesMax: 4096, ResultKind: "json",
			AuthorityDigest: strings.Repeat("b", 64), Guidance: []string{},
			Examples: []json.RawMessage{},
		},
		ServiceProfiles: []nodes.ServiceProfileDescriptor{serviceProfile},
	}
	serviceDescriptor, ok := nodes.ProjectServiceDescriptorForProfile(
		serviceDescriptor,
		"server-services",
	)
	if !ok {
		t.Fatal("project service schema fixture")
	}
	servicePlan, err := nodes.PrepareExecutionPlan(nodes.InvocationRequest{
		InvocationID: "inv_service", IdempotencyKey: "idem_service", NodeID: nodes.ID("node_test"),
		CatalogHash: strings.Repeat("a", 64), Command: serviceDescriptor.Name,
		ServiceProfile: "server-services", Input: json.RawMessage(`{"service":"vpn"}`),
		AgentID: "main", SessionID: "telegram:chat-1", ActorID: "user-1",
		TimeoutSeconds: 30, OutputLimitBytes: 4096,
	}, serviceDescriptor, "local", "policy-1", time.Unix(1, 0), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	data, err = json.Marshal(servicePlan)
	if err != nil {
		t.Fatal(err)
	}
	if unmarshalErr := json.Unmarshal(data, &instance); unmarshalErr != nil {
		t.Fatal(unmarshalErr)
	}
	if validationErr := resolveSchema(t, "execution-plan.v1").Validate(instance); validationErr != nil {
		t.Fatalf("schema rejected service execution plan %s: %v", data, validationErr)
	}

	plan.Command = "system." + strings.Repeat("x", 120) + ".v1"
	data, err = json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	if unmarshalErr := json.Unmarshal(data, &instance); unmarshalErr != nil {
		t.Fatal(unmarshalErr)
	}
	if validationErr := resolveSchema(t, "execution-plan.v1").Validate(instance); validationErr == nil {
		t.Fatal("schema accepted an overlong command")
	}
	if err := plan.Validate(); err == nil {
		t.Fatal("domain accepted an overlong command")
	}
}

func TestEnvelopeSchemaMatchesCodecContract(t *testing.T) {
	resolved := resolveSchema(t, "envelope.v1")

	fixtures := []string{
		`{"type":"request","id":"req_1","method":"node.info","params":{}}`,
		`{"type":"response","id":"req_1","ok":true,"result":{}}`,
		`{"type":"response","id":"req_1","ok":false,"error":{"code":"FAILED","message":"failed"}}`,
		`{"type":"event","event":"node.ready","payload":{}}`,
		`{"type":"event","event":"node.ready","payload":{},"future_optional":{"enabled":true}}`,
	}
	for _, fixture := range fixtures {
		var instance any
		if err := json.Unmarshal([]byte(fixture), &instance); err != nil {
			t.Fatal(err)
		}
		if err := resolved.Validate(instance); err != nil {
			t.Fatalf("schema rejected %s: %v", fixture, err)
		}
		if _, err := Decode([]byte(fixture)); err != nil {
			t.Fatalf("codec rejected %s: %v", fixture, err)
		}
	}

	invalidFixtures := []string{
		`{"type":"event","event":"node.ready","payload":{},"id":""}`,
		`{"type":"request","id":"req_1","method":"node.info","params":{},"idempotency_key":""}`,
		`{"type":"request","id":"req_1","method":"node.info","params":{},"idempotency_key":null}`,
		`{"type":"response","id":"req_1","ok":true,"result":{},"error":null}`,
		`{"type":"response","id":"req_1","ok":false,"error":{"code":"FAILED","message":"failed","details":null}}`,
	}
	for _, fixture := range invalidFixtures {
		var instance any
		if err := json.Unmarshal([]byte(fixture), &instance); err != nil {
			t.Fatal(err)
		}
		if err := resolved.Validate(instance); err == nil {
			t.Fatalf("schema accepted invalid fixture %s", fixture)
		}
		if _, err := Decode([]byte(fixture)); err == nil {
			t.Fatalf("codec accepted invalid fixture %s", fixture)
		}
	}
}

func TestCommandDescriptorSchemaAndDomainConformance(t *testing.T) {
	resolved := resolveSchema(t, "command-descriptor.v1")
	tests := []struct {
		name       string
		descriptor nodes.CommandDescriptor
		schemaOK   bool
		domainOK   bool
	}{
		{
			name: "valid",
			descriptor: nodes.CommandDescriptor{
				Name:         "system.exec.v1",
				InputSchema:  json.RawMessage(`{"type":"object"}`),
				OutputSchema: json.RawMessage(`{"type":"object"}`),
				Risk:         nodes.RiskWrite,
			},
			schemaOK: true,
			domainOK: true,
		},
		{
			name: "valid model contract",
			descriptor: nodes.CommandDescriptor{
				Name:         "system.exec.v1",
				InputSchema:  json.RawMessage(`{"type":"object","additionalProperties":false}`),
				OutputSchema: json.RawMessage(`{"type":"object"}`),
				Risk:         nodes.RiskWrite,
				ModelContract: &nodes.CommandModelContract{
					Availability:      nodes.ModelPartiallyDescribed,
					TimeoutSecondsMax: 30,
					OutputBytesMax:    4096,
					ResultKind:        "json",
					Guidance:          []string{},
					Examples:          []json.RawMessage{},
				},
			},
			schemaOK: true,
			domainOK: true,
		},
		{
			name: "overlong command",
			descriptor: nodes.CommandDescriptor{
				Name:         "system." + strings.Repeat("x", 120) + ".v1",
				InputSchema:  json.RawMessage(`{}`),
				OutputSchema: json.RawMessage(`{}`),
				Risk:         nodes.RiskRead,
			},
		},
		{
			name: "valid service descriptor",
			descriptor: func() nodes.CommandDescriptor {
				profiles := []nodes.ServiceProfileDescriptor{{
					Alias:    "server-services",
					Revision: "server-services-v1",
					Manager:  "systemd",
					Services: []nodes.ServiceDescriptor{{Alias: "vpn", Status: true}},
					LogLimits: nodes.ServiceLogLimits{
						EntriesMax: 100, BytesMax: 4096, AgeSecondsMax: 3600,
					},
					ActionApproval: "required",
				}}
				return nodes.CommandDescriptor{
					Name:            "service.status.v1",
					InputSchema:     nodes.ServiceCommandInputSchema("service.status.v1", profiles),
					OutputSchema:    nodes.ServiceCommandOutputSchema("service.status.v1"),
					Risk:            nodes.RiskRead,
					ServiceProfiles: profiles,
				}
			}(),
			schemaOK: true,
			domainOK: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := json.Marshal(test.descriptor)
			if err != nil {
				t.Fatal(err)
			}
			var instance any
			if err := json.Unmarshal(data, &instance); err != nil {
				t.Fatal(err)
			}
			if got := resolved.Validate(instance) == nil; got != test.schemaOK {
				t.Fatalf("schema accepted = %v, want %v", got, test.schemaOK)
			}
			if got := test.descriptor.Validate() == nil; got != test.domainOK {
				t.Fatalf("domain accepted = %v, want %v", got, test.domainOK)
			}
		})
	}
}

func TestNodeAuthSchemaMatchesDomainPayloads(t *testing.T) {
	resolved := resolveSchema(t, "node-auth.v1")
	registry, err := nodes.NewFileRegistry(filepath.Join(t.TempDir(), "registry.json"), 4)
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := nodes.NewAuthenticator(registry, nodes.AdmissionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	challenge, err := authenticator.IssueChallenge()
	if err != nil {
		t.Fatal(err)
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := nodes.NewIdentityProof(
		privateKey, challenge.Nonce, nodes.ProtocolV1, nodes.ProtocolV1,
		"v0.1.0", "linux", "amd64", nodes.CapabilityCatalog{},
		nodes.ExecutionProfile{Executor: "local", PolicyRevision: "policy-1"},
	)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := authenticator.Authenticate(proof)
	if err != nil {
		t.Fatal(err)
	}
	result := admission.Result
	payloads := []any{
		challenge,
		proof,
		result,
	}
	for _, payload := range payloads {
		data, marshalErr := json.Marshal(payload)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		var instance any
		if unmarshalErr := json.Unmarshal(data, &instance); unmarshalErr != nil {
			t.Fatal(unmarshalErr)
		}
		if validationErr := resolved.Validate(instance); validationErr != nil {
			t.Fatalf("schema rejected %s: %v", data, validationErr)
		}
	}
}

func resolveSchema(t *testing.T, name string) *jsonschema.Resolved {
	t.Helper()
	data, err := Schema(name)
	if err != nil {
		t.Fatal(err)
	}
	var schema jsonschema.Schema
	if unmarshalErr := json.Unmarshal(data, &schema); unmarshalErr != nil {
		t.Fatal(unmarshalErr)
	}
	resolved, err := schema.Resolve(&jsonschema.ResolveOptions{
		Loader: func(uri *url.URL) (*jsonschema.Schema, error) {
			const prefix = "https://mintclaw.dev/schemas/nodes/"
			name, ok := strings.CutPrefix(uri.String(), prefix)
			if !ok {
				return nil, fmt.Errorf("unsupported node schema URI %q", uri)
			}
			data, loadErr := Schema(strings.TrimSuffix(name, ".json"))
			if loadErr != nil {
				return nil, loadErr
			}
			var loaded jsonschema.Schema
			if unmarshalErr := json.Unmarshal(data, &loaded); unmarshalErr != nil {
				return nil, unmarshalErr
			}
			return &loaded, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func TestUnknownSchemaFails(t *testing.T) {
	if _, err := Schema("missing"); err == nil {
		t.Fatal("Schema(missing) succeeded")
	}
}
