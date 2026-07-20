package protocol

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"

	"github.com/sipeed/picoclaw/pkg/nodes"
)

func TestEmbeddedSchemasAreValidJSON(t *testing.T) {
	for _, name := range []string{"envelope.v1", "command-descriptor.v1"} {
		data, err := Schema(name)
		if err != nil {
			t.Fatalf("Schema(%q) error = %v", name, err)
		}
		if !json.Valid(data) {
			t.Fatalf("Schema(%q) is invalid JSON", name)
		}
	}
}

func TestEnvelopeSchemaMatchesCodecContract(t *testing.T) {
	resolved := resolveSchema(t, "envelope.v1")

	fixtures := []string{
		`{"type":"request","id":"req_1","method":"node.info","params":{}}`,
		`{"type":"response","id":"req_1","ok":true,"result":{}}`,
		`{"type":"response","id":"req_1","ok":false,"error":{"code":"FAILED","message":"failed"}}`,
		`{"type":"event","event":"node.ready","payload":{}}`,
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
			name: "overlong command",
			descriptor: nodes.CommandDescriptor{
				Name:         "system." + strings.Repeat("x", 120) + ".v1",
				InputSchema:  json.RawMessage(`{}`),
				OutputSchema: json.RawMessage(`{}`),
				Risk:         nodes.RiskRead,
			},
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
	resolved, err := schema.Resolve(nil)
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
