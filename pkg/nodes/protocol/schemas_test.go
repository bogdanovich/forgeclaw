package protocol

import (
	"encoding/json"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
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
	data, err := Schema("envelope.v1")
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

func TestUnknownSchemaFails(t *testing.T) {
	if _, err := Schema("missing"); err == nil {
		t.Fatal("Schema(missing) succeeded")
	}
}
